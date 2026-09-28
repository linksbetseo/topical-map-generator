import { Priority } from "./rate-limit.ts";
import type { HeliusRpc, RpcResult } from "./helius.ts";

/**
 * Wallet history from the canonical Solana RPC (read-only): `getSignaturesForAddress` (1 credit per
 * call, up to 1000 signatures) + `getTransaction` (1 credit per transaction), Helius Free 10 req/s.
 *
 * Unlike a parsed-transfer view, `getTransaction` carries the pre/post balances of every account, so
 * the economic effect on a wallet is read from balance changes (raw integer amounts with decimals and
 * the token-account owner) instead of being summed from transfer lists — which double counts SOL when
 * a route both transfers WSOL and unwraps it (reproduced on mainnet 2026-09-28, see tests).
 *
 * `CompactTx` keeps everything needed to recompute any wallet's balance changes for a transaction;
 * instruction data and logs are dropped. It is the durable cache format (EXTRACTOR_VERSION).
 */
export const TX_EXTRACTOR_VERSION = 1;

export interface CompactTokenBalance {
  accountIndex: number;
  mint: string;
  owner: string | null;
  programId: string | null;
  /** raw integer amount as a decimal string */
  amount: string;
  decimals: number;
}

export interface CompactTx {
  v: number;
  signature: string;
  slot: number;
  /** seconds since epoch (RPC blockTime), null when the node does not know it */
  blockTime: number | null;
  err: unknown;
  /** lamports, paid by accountKeys[0] */
  fee: number;
  accountKeys: string[];
  preBalances: number[];
  postBalances: number[];
  preTokenBalances: CompactTokenBalance[];
  postTokenBalances: CompactTokenBalance[];
  programIds: string[];
}

interface RpcTokenBalance {
  accountIndex: number;
  mint: string;
  owner?: string;
  programId?: string;
  uiTokenAmount: { amount: string; decimals: number };
}

interface RpcInstruction {
  programId?: string;
}

export interface RpcTransaction {
  slot: number;
  blockTime: number | null;
  meta: {
    err: unknown;
    fee: number;
    preBalances: number[];
    postBalances: number[];
    preTokenBalances?: RpcTokenBalance[];
    postTokenBalances?: RpcTokenBalance[];
    innerInstructions?: Array<{ instructions: RpcInstruction[] }>;
    loadedAddresses?: { writable?: string[]; readonly?: string[] };
  } | null;
  transaction: {
    signatures: string[];
    message: { accountKeys: Array<string | { pubkey: string }>; instructions?: RpcInstruction[] };
  };
}

export function compactTransaction(tx: RpcTransaction): CompactTx {
  if (!tx.meta) throw new Error(`transaction ${tx.transaction.signatures[0]} has no meta`);
  const keys = tx.transaction.message.accountKeys.map((k) => (typeof k === "string" ? k : k.pubkey));
  // jsonParsed already lists loaded (ALT) addresses in accountKeys; with json encoding they come separately
  if (keys.length < tx.meta.preBalances.length && tx.meta.loadedAddresses) {
    keys.push(...(tx.meta.loadedAddresses.writable ?? []), ...(tx.meta.loadedAddresses.readonly ?? []));
  }
  if (keys.length !== tx.meta.preBalances.length) throw new Error(`account keys (${keys.length}) do not match balances (${tx.meta.preBalances.length})`);
  const tb = (l: RpcTokenBalance[] | undefined): CompactTokenBalance[] =>
    (l ?? []).map((b) => ({ accountIndex: b.accountIndex, mint: b.mint, owner: b.owner ?? null, programId: b.programId ?? null, amount: b.uiTokenAmount.amount, decimals: b.uiTokenAmount.decimals }));
  const programs = new Set<string>();
  for (const i of tx.transaction.message.instructions ?? []) if (i.programId) programs.add(i.programId);
  for (const inner of tx.meta.innerInstructions ?? []) for (const i of inner.instructions) if (i.programId) programs.add(i.programId);
  return {
    v: TX_EXTRACTOR_VERSION,
    signature: tx.transaction.signatures[0]!,
    slot: tx.slot,
    blockTime: tx.blockTime,
    err: tx.meta.err ?? null,
    fee: tx.meta.fee,
    accountKeys: keys,
    preBalances: tx.meta.preBalances,
    postBalances: tx.meta.postBalances,
    preTokenBalances: tb(tx.meta.preTokenBalances),
    postTokenBalances: tb(tx.meta.postTokenBalances),
    programIds: [...programs].sort(),
  };
}

export interface SignatureInfo {
  signature: string;
  slot: number;
  blockTime: number | null;
  failed: boolean;
}

export interface SignatureScan {
  /** newest first, only signatures with blockTime >= gteTime (and <= lteTime) */
  signatures: SignatureInfo[];
  /** true when the scan reached a signature older than gteTime or the end of the address history */
  complete: boolean;
  calls: number;
  oldestSeen: number | null;
  newestSeen: number | null;
}

export interface TxCache {
  get(signatures: string[]): Promise<Map<string, CompactTx>>;
  put(txs: CompactTx[]): Promise<void>;
}

export type Pacer = () => Promise<void>;

/** Spaces calls to at most one per `minIntervalMs` across all callers sharing the pacer. */
export function pacer(minIntervalMs: number, sleep: (ms: number) => Promise<void>, nowMs: () => number): Pacer {
  let next = 0;
  return async () => {
    const now = nowMs();
    const at = Math.max(now, next);
    next = at + minIntervalMs;
    if (at > now) await sleep(at - now);
  };
}

export class SolanaHistory {
  calls = 0;
  cacheHits = 0;
  constructor(
    private readonly rpc: HeliusRpc,
    private readonly cache: TxCache,
    private readonly pace: Pacer = async () => undefined,
    private readonly retry = { attempts: 4, backoffMs: 1_000, sleep: (ms: number) => new Promise<void>((r) => setTimeout(r, ms)) },
  ) {}

  private async call<T>(method: string, params: unknown): Promise<RpcResult<T>> {
    let r: RpcResult<T> = { ok: false, code: "PROVIDER_ERROR", detail: "not called" };
    for (let i = 0; i <= this.retry.attempts; i++) {
      if (i > 0) await this.retry.sleep(this.retry.backoffMs * 2 ** (i - 1));
      await this.pace();
      this.calls++;
      r = await this.rpc.call<T>(method, params, Priority.DISCOVERY);
      if (r.ok || (r.code !== "RATE_LIMITED" && r.code !== "PROVIDER_UNAVAILABLE")) return r;
    }
    return r;
  }

  /** All signatures of `address` in [gteTime, lteTime] (seconds), newest first, at most `maxPages` calls of 1000. */
  async signatures(address: string, q: { gteTime: number; lteTime: number; maxPages: number }): Promise<RpcResult<SignatureScan>> {
    const out: SignatureInfo[] = [];
    let before: string | undefined;
    let calls = 0;
    let oldestSeen: number | null = null;
    let newestSeen: number | null = null;
    for (let page = 0; page < q.maxPages; page++) {
      const opts: Record<string, unknown> = { limit: 1000, commitment: "confirmed" };
      if (before) opts.before = before;
      const r = await this.call<Array<{ signature: string; slot: number; blockTime?: number | null; err: unknown }>>("getSignaturesForAddress", [address, opts]);
      calls++;
      if (!r.ok) return r;
      if (r.value.length === 0) return { ok: true, value: { signatures: out, complete: true, calls, oldestSeen, newestSeen }, receivedAt: r.receivedAt };
      for (const s of r.value) {
        const t = s.blockTime ?? null;
        if (t !== null) {
          oldestSeen = oldestSeen === null ? t : Math.min(oldestSeen, t);
          newestSeen = newestSeen === null ? t : Math.max(newestSeen, t);
        }
        if (t !== null && t < q.gteTime) return { ok: true, value: { signatures: out, complete: true, calls, oldestSeen, newestSeen }, receivedAt: r.receivedAt };
        if (t === null || t <= q.lteTime) out.push({ signature: s.signature, slot: s.slot, blockTime: t, failed: s.err !== null && s.err !== undefined });
      }
      before = r.value[r.value.length - 1]!.signature;
    }
    return { ok: true, value: { signatures: out, complete: false, calls, oldestSeen, newestSeen }, receivedAt: new Date() };
  }

  /** Transactions by signature, from the cache when present. Returns per-signature errors instead of failing the batch. */
  async transactions(sigs: string[], concurrency = 4): Promise<{ txs: Map<string, CompactTx>; errors: Map<string, string> }> {
    const txs = await this.cache.get(sigs);
    this.cacheHits += txs.size;
    const errors = new Map<string, string>();
    const todo = sigs.filter((s) => !txs.has(s));
    let idx = 0;
    const fresh: CompactTx[] = [];
    const worker = async () => {
      while (idx < todo.length) {
        const sig = todo[idx++]!;
        const r = await this.call<RpcTransaction | null>("getTransaction", [sig, { encoding: "jsonParsed", maxSupportedTransactionVersion: 1, commitment: "confirmed" }]);
        if (!r.ok) {
          errors.set(sig, `${r.code} ${r.detail}`);
          continue;
        }
        if (!r.value) {
          errors.set(sig, "NOT_FOUND");
          continue;
        }
        try {
          const c = compactTransaction(r.value);
          txs.set(sig, c);
          fresh.push(c);
        } catch (e) {
          errors.set(sig, `EXTRACT ${e instanceof Error ? e.message : String(e)}`);
        }
        if (fresh.length >= 100) await this.cache.put(fresh.splice(0));
      }
    };
    await Promise.all(Array.from({ length: Math.max(1, concurrency) }, worker));
    if (fresh.length) await this.cache.put(fresh);
    return { txs, errors };
  }
}
