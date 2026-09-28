import { D, ReasonCode, reason, type Dec, type Reason } from "@solbot/domain";
import { NetworkError, parseJsonExact, type ReadOnlyTransport } from "./transport.ts";
import { Priority, type SlidingWindowLimiter } from "./rate-limit.ts";

/**
 * Helius RPC (read-only). The URL carries the API key: never log it (see redactUrl).
 * DAS `getTokenAccounts` contract from helius-sdk src/types/das.ts @ b76a792 (2026-09-08).
 */

export type RpcResult<T> = { ok: true; value: T; receivedAt: Date } | { ok: false; code: "PROVIDER_UNAVAILABLE" | "PROVIDER_ERROR" | "RATE_LIMITED"; detail: string };

export class HeliusRpc {
  private nextId = 1;

  constructor(
    private readonly transport: ReadOnlyTransport,
    private readonly limiter: SlidingWindowLimiter,
    private readonly rpcUrl: string,
    /** Helius DAS (getTokenAccounts) available; false for a plain Solana RPC endpoint. */
    readonly supportsDas = true,
  ) {}

  async call<T>(method: string, params: unknown, priority: Priority, bigintKeys: ReadonlySet<string> = new Set()): Promise<RpcResult<T>> {
    if (!(await this.limiter.acquire(priority, priority <= Priority.ENTRY ? 10_000 : 0))) return { ok: false, code: "RATE_LIMITED", detail: "local budget" };
    let res;
    try {
      res = await this.transport.request("POST", this.rpcUrl, { json: { jsonrpc: "2.0", id: this.nextId++, method, params } });
    } catch (e) {
      if (e instanceof NetworkError) return { ok: false, code: "PROVIDER_UNAVAILABLE", detail: e.message };
      throw e;
    }
    if (res.status === 429) return { ok: false, code: "RATE_LIMITED", detail: "HTTP 429" };
    if (res.status >= 500) return { ok: false, code: "PROVIDER_UNAVAILABLE", detail: `HTTP ${res.status}` };
    if (res.status !== 200) return { ok: false, code: "PROVIDER_ERROR", detail: `HTTP ${res.status}` };
    let body: { result?: T; error?: { message?: string } };
    try {
      body = parseJsonExact(res.text, bigintKeys) as typeof body;
    } catch (e) {
      return { ok: false, code: "PROVIDER_ERROR", detail: e instanceof Error ? e.message : "bad JSON" };
    }
    if (body.error) return { ok: false, code: "PROVIDER_ERROR", detail: String(body.error.message ?? "rpc error").slice(0, 300) };
    return { ok: true, value: body.result as T, receivedAt: res.receivedAt };
  }

  async getAccountInfo(address: string, priority: Priority = Priority.ENTRY): Promise<RpcResult<{ owner: string; data: Uint8Array; slot: number } | null>> {
    const r = await this.call<{ context: { slot: number }; value: { owner: string; data: [string, string] } | null }>(
      "getAccountInfo",
      [address, { encoding: "base64", commitment: "confirmed" }],
      priority,
    );
    if (!r.ok) return r;
    const v = r.value.value;
    if (!v) return { ok: true, value: null, receivedAt: r.receivedAt };
    if (!Array.isArray(v.data) || v.data[1] !== "base64") return { ok: false, code: "PROVIDER_ERROR", detail: "unexpected account encoding" };
    return { ok: true, value: { owner: v.owner, data: new Uint8Array(Buffer.from(v.data[0], "base64")), slot: r.value.context.slot }, receivedAt: r.receivedAt };
  }

  /** Pages through every token account of a mint. Incomplete pagination => not available (no guessing). */
  async getAllTokenAccounts(
    mint: string,
    opts: { pageLimit?: number; maxPages?: number; priority?: Priority } = {},
  ): Promise<RpcResult<{ accounts: Array<{ owner: string; amountRaw: bigint }>; lastIndexedSlot: number | null; complete: boolean }>> {
    const limit = opts.pageLimit ?? 1000;
    const maxPages = opts.maxPages ?? 20;
    const accounts: Array<{ owner: string; amountRaw: bigint }> = [];
    let cursor: string | undefined;
    let lastIndexedSlot: number | null = null;
    let receivedAt = new Date(0);
    for (let page = 0; page < maxPages; page++) {
      const params: Record<string, unknown> = { mint, limit, options: { showZeroBalance: false } };
      if (cursor) params.cursor = cursor;
      const r = await this.call<{ token_accounts?: Array<{ owner?: string; amount?: bigint }>; cursor?: string; last_indexed_slot?: number }>(
        "getTokenAccounts",
        params,
        opts.priority ?? Priority.ENTRY,
        new Set(["amount", "delegated_amount"]),
      );
      if (!r.ok) return r;
      receivedAt = r.receivedAt;
      lastIndexedSlot = r.value.last_indexed_slot ?? lastIndexedSlot;
      const list = r.value.token_accounts ?? [];
      for (const a of list) {
        if (typeof a.owner !== "string" || typeof a.amount !== "bigint") return { ok: false, code: "PROVIDER_ERROR", detail: "token account without owner/amount" };
        accounts.push({ owner: a.owner, amountRaw: a.amount });
      }
      if (!r.value.cursor || list.length < limit) return { ok: true, value: { accounts, lastIndexedSlot, complete: true }, receivedAt };
      cursor = r.value.cursor;
    }
    return { ok: true, value: { accounts, lastIndexedSlot, complete: false }, receivedAt };
  }
}

/** Plain Solana RPC can serve every Helius call used here except DAS; holders then use getProgramAccounts. */
export { HeliusRpc as SolanaRpc };

const B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
function b58(bytes: Uint8Array): string {
  let n = 0n;
  for (const b of bytes) n = (n << 8n) + BigInt(b);
  let out = "";
  while (n > 0n) {
    out = B58_ALPHABET[Number(n % 58n)] + out;
    n /= 58n;
  }
  for (const b of bytes) {
    if (b !== 0) break;
    out = "1" + out;
  }
  return out;
}

/**
 * Every token account of a mint via standard `getProgramAccounts` (no DAS, no API key).
 * dataSlice returns owner (offset 32, 32 B) + amount (offset 64, u64 LE). Public endpoints may
 * refuse this for large mints; a refusal is reported, never replaced by a partial list.
 */
export async function tokenAccountsViaProgramAccounts(
  rpc: HeliusRpc,
  mint: string,
  tokenProgram: string,
  priority: Priority = Priority.ENTRY,
): Promise<RpcResult<{ accounts: Array<{ owner: string; amountRaw: bigint }>; lastIndexedSlot: number | null; complete: boolean }>> {
  const filters: unknown[] = [{ memcmp: { offset: 0, bytes: mint } }];
  if (tokenProgram === "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA") filters.unshift({ dataSize: 165 });
  const r = await rpc.call<{ context?: { slot: number }; value?: unknown[] } | unknown[]>(
    "getProgramAccounts",
    [tokenProgram, { encoding: "base64", commitment: "confirmed", withContext: true, dataSlice: { offset: 32, length: 40 }, filters }],
    priority,
  );
  if (!r.ok) return r;
  const list = Array.isArray(r.value) ? r.value : (r.value.value ?? []);
  const slot = Array.isArray(r.value) ? null : (r.value.context?.slot ?? null);
  const accounts: Array<{ owner: string; amountRaw: bigint }> = [];
  for (const item of list as Array<{ account?: { data?: [string, string] } }>) {
    const d = item.account?.data;
    if (!Array.isArray(d) || d[1] !== "base64") return { ok: false, code: "PROVIDER_ERROR", detail: "unexpected account encoding" };
    const bytes = new Uint8Array(Buffer.from(d[0], "base64"));
    if (bytes.length !== 40) return { ok: false, code: "PROVIDER_ERROR", detail: `data slice length ${bytes.length}` };
    const amount = new DataView(bytes.buffer, bytes.byteOffset, 40).getBigUint64(32, true);
    accounts.push({ owner: b58(bytes.subarray(0, 32)), amountRaw: amount });
  }
  return { ok: true, value: { accounts, lastIndexedSlot: slot, complete: true }, receivedAt: r.receivedAt };
}

export interface HolderConcentration {
  ok: boolean;
  reasons: Reason[];
  /** Distinct owners with non-zero balance, after excluding documented infrastructure. Definition: owner-consolidated token accounts. */
  holderCount: number;
  top10Bps: number | null;
  largestBps: number | null;
  excludedInfraRaw: bigint;
  coverageBps: number | null;
  definition: string;
}

/**
 * Owner-consolidated concentration (brief §5). Only documented infrastructure addresses are excluded;
 * a large holder is never removed because it hurts the score. If data is incomplete, the filter is NOT met.
 */
export function holderConcentration(
  accounts: ReadonlyArray<{ owner: string; amountRaw: bigint }>,
  supplyRaw: bigint,
  complete: boolean,
  infra: ReadonlySet<string>,
): HolderConcentration {
  const definition = "owner-consolidated SPL token accounts, non-zero balance, excluding documented infra_registry owners";
  const base = { holderCount: 0, top10Bps: null, largestBps: null, excludedInfraRaw: 0n, coverageBps: null, definition };
  if (!complete) return { ok: false, reasons: [reason(ReasonCode.HOLDER_DATA_UNAVAILABLE, "pagination incomplete")], ...base };
  if (supplyRaw <= 0n) return { ok: false, reasons: [reason(ReasonCode.HOLDER_DATA_UNAVAILABLE, "zero supply")], ...base };

  const byOwner = new Map<string, bigint>();
  let total = 0n;
  for (const a of accounts) {
    if (a.amountRaw <= 0n) continue;
    total += a.amountRaw;
    byOwner.set(a.owner, (byOwner.get(a.owner) ?? 0n) + a.amountRaw);
  }
  const coverageBps = Number((total * 10_000n) / supplyRaw);
  // Accounts must explain (almost) the whole supply; otherwise the indexer view is partial.
  if (total * 10_000n < supplyRaw * 9_990n || total > supplyRaw) {
    return { ok: false, reasons: [reason(ReasonCode.HOLDER_DATA_UNAVAILABLE, `accounts cover ${coverageBps} bps of supply`)], ...base, coverageBps };
  }
  let excluded = 0n;
  const holders: bigint[] = [];
  for (const [owner, amt] of byOwner) {
    if (infra.has(owner)) excluded += amt;
    else holders.push(amt);
  }
  holders.sort((a, b) => (a > b ? -1 : a < b ? 1 : 0));
  const top10 = holders.slice(0, 10).reduce((a, b) => a + b, 0n);
  const toBps = (x: bigint) => Number((x * 10_000n + supplyRaw - 1n) / supplyRaw); // ceil: conservative
  return {
    ok: true,
    reasons: [],
    holderCount: holders.length,
    top10Bps: toBps(top10),
    largestBps: holders.length ? toBps(holders[0]!) : 0,
    excludedInfraRaw: excluded,
    coverageBps,
    definition,
  };
}

export function decimalOrNull(v: unknown): Dec | null {
  return typeof v === "number" && Number.isFinite(v) ? new D(String(v)) : null;
}
