import { NATIVE_SOL, USDC_MINT } from "@solbot/domain";

/**
 * Multi-asset double-entry ledger with trading (clearing) accounts (ASSUMPTION A19).
 *
 * Invariants enforced on every post:
 *  - every transaction balances to zero *per asset*,
 *  - holding buckets (wallet / reserved / rent_locked) never go negative,
 *  - append-only: corrections are compensating transactions,
 *  - idempotency: the same key posts at most once; a different payload under the same key is an error.
 */

export type Asset = string; // mint address, or NATIVE_SOL for lamports

export const Bucket = {
  WALLET: "wallet",
  RESERVED: "reserved",
  RENT_LOCKED: "rent_locked",
  OPENING: "equity:opening",
  EXTERNAL: "equity:external",
  SWAP_CLEARING: "clearing:swap",
  FEE_NETWORK: "expense:network_fee",
  FEE_PRIORITY: "expense:priority_fee",
  FEE_FAILED_TX: "expense:failed_tx",
  FEE_CLOSE_ACCOUNT: "expense:close_account",
  FEE_TRANSFER: "expense:transfer_fee",
  WRITE_OFF: "expense:write_off",
} as const;
export type Bucket = (typeof Bucket)[keyof typeof Bucket];

/** Buckets whose balances are assets of the (virtual) wallet. */
export const HOLDING_BUCKETS: ReadonlySet<string> = new Set([Bucket.WALLET, Bucket.RESERVED, Bucket.RENT_LOCKED]);

export interface LedgerEntry {
  bucket: Bucket;
  asset: Asset;
  /** signed raw amount; positive = debit to the bucket (increase of a holding). */
  amountRaw: bigint;
}

export type LedgerTxKind =
  | "OPENING"
  | "RESERVE"
  | "RELEASE"
  | "BUY_FILL"
  | "SELL_FILL"
  | "FAILED_ATTEMPT_FEE"
  | "RENT_LOCK"
  | "RENT_RECOVERY"
  | "WRITE_OFF"
  | "COMPENSATION";

export interface LedgerTransaction {
  id: string;
  sessionId: string;
  idempotencyKey: string;
  kind: LedgerTxKind;
  at: Date;
  entries: readonly LedgerEntry[];
  refs?: Readonly<Record<string, string>>;
  memo?: string;
}

export class LedgerError extends Error {
  constructor(
    public readonly code: "UNBALANCED" | "NEGATIVE_BALANCE" | "EMPTY" | "ZERO_ENTRY" | "IDEMPOTENCY_CONFLICT" | "SESSION_MISMATCH",
    message: string,
  ) {
    super(message);
  }
}

export function key(bucket: string, asset: Asset): string {
  return `${bucket}|${asset}`;
}

export function assertBalanced(entries: readonly LedgerEntry[]): void {
  if (entries.length === 0) throw new LedgerError("EMPTY", "transaction without entries");
  const sums = new Map<Asset, bigint>();
  for (const e of entries) {
    if (e.amountRaw === 0n) throw new LedgerError("ZERO_ENTRY", `zero entry ${e.bucket} ${e.asset}`);
    sums.set(e.asset, (sums.get(e.asset) ?? 0n) + e.amountRaw);
  }
  for (const [asset, sum] of sums) {
    if (sum !== 0n) throw new LedgerError("UNBALANCED", `asset ${asset} does not balance: ${sum}`);
  }
}

function sameTx(a: LedgerTransaction, b: LedgerTransaction): boolean {
  if (a.kind !== b.kind || a.entries.length !== b.entries.length) return false;
  return a.entries.every((e, i) => {
    const o = b.entries[i]!;
    return e.bucket === o.bucket && e.asset === o.asset && e.amountRaw === o.amountRaw;
  });
}

export class Ledger {
  private readonly txs: LedgerTransaction[] = [];
  private readonly byKey = new Map<string, LedgerTransaction>();
  private readonly balances = new Map<string, bigint>();

  constructor(public readonly sessionId: string) {}

  /** Posts a transaction; returns false if an identical transaction with the same key was already posted. */
  post(tx: LedgerTransaction): boolean {
    if (tx.sessionId !== this.sessionId) throw new LedgerError("SESSION_MISMATCH", "transaction for another session");
    const existing = this.byKey.get(tx.idempotencyKey);
    if (existing) {
      if (sameTx(existing, tx)) return false;
      throw new LedgerError("IDEMPOTENCY_CONFLICT", `different payload under key ${tx.idempotencyKey}`);
    }
    assertBalanced(tx.entries);

    const next = new Map<string, bigint>();
    for (const e of tx.entries) {
      const k = key(e.bucket, e.asset);
      next.set(k, (next.get(k) ?? this.balances.get(k) ?? 0n) + e.amountRaw);
    }
    for (const [k, v] of next) {
      const bucket = k.split("|")[0]!;
      if (HOLDING_BUCKETS.has(bucket) && v < 0n) {
        throw new LedgerError("NEGATIVE_BALANCE", `${k} would become ${v}`);
      }
    }
    for (const [k, v] of next) this.balances.set(k, v);
    const frozen: LedgerTransaction = Object.freeze({ ...tx, entries: Object.freeze(tx.entries.map((e) => Object.freeze({ ...e }))) });
    this.txs.push(frozen);
    this.byKey.set(tx.idempotencyKey, frozen);
    return true;
  }

  balance(bucket: string, asset: Asset): bigint {
    return this.balances.get(key(bucket, asset)) ?? 0n;
  }

  /** Sum of wallet + reserved (+ rent_locked if requested) for an asset. */
  holding(asset: Asset, opts: { includeRent?: boolean } = {}): bigint {
    let v = this.balance(Bucket.WALLET, asset) + this.balance(Bucket.RESERVED, asset);
    if (opts.includeRent) v += this.balance(Bucket.RENT_LOCKED, asset);
    return v;
  }

  /** Token assets currently held (excluding base assets), with raw quantity. */
  tokenHoldings(): Map<Asset, bigint> {
    const out = new Map<Asset, bigint>();
    for (const [k, v] of this.balances) {
      const [bucket, asset] = k.split("|") as [string, string];
      if ((bucket === Bucket.WALLET || bucket === Bucket.RESERVED) && asset !== USDC_MINT && asset !== NATIVE_SOL && v !== 0n) {
        out.set(asset, (out.get(asset) ?? 0n) + v);
      }
    }
    return out;
  }

  allBalances(): Array<{ bucket: string; asset: Asset; amountRaw: bigint }> {
    return [...this.balances.entries()]
      .map(([k, v]) => {
        const [bucket, asset] = k.split("|") as [string, string];
        return { bucket, asset, amountRaw: v };
      })
      .sort((a, b) => (a.bucket + a.asset).localeCompare(b.bucket + b.asset));
  }

  transactions(): readonly LedgerTransaction[] {
    return this.txs;
  }

  /** Rebuilds balances from transactions and compares with the running state (reconciliation). */
  verifyIdentity(): { ok: boolean; mismatches: string[] } {
    const recomputed = new Map<string, bigint>();
    for (const tx of this.txs) {
      assertBalanced(tx.entries);
      for (const e of tx.entries) {
        const k = key(e.bucket, e.asset);
        recomputed.set(k, (recomputed.get(k) ?? 0n) + e.amountRaw);
      }
    }
    const mismatches: string[] = [];
    const keys = new Set([...recomputed.keys(), ...this.balances.keys()]);
    for (const k of keys) {
      if ((recomputed.get(k) ?? 0n) !== (this.balances.get(k) ?? 0n)) mismatches.push(k);
    }
    // Per asset the sum over all buckets must be zero (closed system).
    const perAsset = new Map<string, bigint>();
    for (const [k, v] of recomputed) {
      const asset = k.split("|")[1]!;
      perAsset.set(asset, (perAsset.get(asset) ?? 0n) + v);
    }
    for (const [asset, v] of perAsset) if (v !== 0n) mismatches.push(`asset-sum:${asset}`);
    return { ok: mismatches.length === 0, mismatches };
  }

  static replay(sessionId: string, txs: readonly LedgerTransaction[]): Ledger {
    const l = new Ledger(sessionId);
    for (const t of txs) l.post(t);
    return l;
  }
}
