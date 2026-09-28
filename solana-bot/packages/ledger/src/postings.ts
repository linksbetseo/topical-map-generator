import { NATIVE_SOL, USDC_MINT, type Dec } from "@solbot/domain";
import { Bucket, type LedgerEntry, type LedgerTransaction, type LedgerTxKind } from "./ledger.ts";

/**
 * Builders for ledger transactions. They only describe balance movements; USD valuation
 * lives in fills/fee items with explicit FX (brief §9.3, §11).
 */

export type FeeKind =
  | "POOL_LP" // included in route output
  | "PLATFORM" // Jupiter platform fee (included in quote)
  | "TOKEN_TRANSFER_FEE"
  | "BASE_NETWORK"
  | "PRIORITY"
  | "TIP"
  | "RENT_LOCK"
  | "RENT_RECOVERY"
  | "CLOSE_ACCOUNT"
  | "FAILED_TX";

export type FeeSource = "QUOTE" | "MODEL_ESTIMATE" | "CHAIN" | "CONFIG";

export interface FeeItem {
  kind: FeeKind;
  asset: string;
  amountRaw: bigint;
  /** USD per whole unit of `asset` at the time of the event; null if unknown. */
  usdFx: Dec | null;
  source: FeeSource;
  /** Already reflected in the quoted output: informational only, never deducted again. */
  includedInQuote: boolean;
  isEstimate: boolean;
}

interface TxBase {
  id: string;
  sessionId: string;
  idempotencyKey: string;
  at: Date;
  refs?: Record<string, string>;
  memo?: string;
}

function tx(base: TxBase, kind: LedgerTxKind, entries: LedgerEntry[]): LedgerTransaction {
  const t: LedgerTransaction = {
    id: base.id,
    sessionId: base.sessionId,
    idempotencyKey: base.idempotencyKey,
    kind,
    at: base.at,
    entries: entries.filter((e) => e.amountRaw !== 0n),
  };
  if (base.refs) (t as { refs?: Record<string, string> }).refs = base.refs;
  if (base.memo) (t as { memo?: string }).memo = base.memo;
  return t;
}

function move(from: string, to: string, asset: string, amount: bigint): LedgerEntry[] {
  if (amount < 0n) throw new RangeError("move amount must be non-negative");
  return [
    { bucket: from as LedgerEntry["bucket"], asset, amountRaw: -amount },
    { bucket: to as LedgerEntry["bucket"], asset, amountRaw: amount },
  ];
}

export function openingTx(base: TxBase, usdcRaw: bigint, lamports: bigint): LedgerTransaction {
  return tx(base, "OPENING", [...move(Bucket.OPENING, Bucket.WALLET, USDC_MINT, usdcRaw), ...move(Bucket.OPENING, Bucket.WALLET, NATIVE_SOL, lamports)]);
}

export function reserveTx(base: TxBase, parts: { usdcRaw: bigint; lamports: bigint }): LedgerTransaction {
  return tx(base, "RESERVE", [...move(Bucket.WALLET, Bucket.RESERVED, USDC_MINT, parts.usdcRaw), ...move(Bucket.WALLET, Bucket.RESERVED, NATIVE_SOL, parts.lamports)]);
}

export function releaseTx(base: TxBase, parts: { usdcRaw: bigint; lamports: bigint }): LedgerTransaction {
  return tx(base, "RELEASE", [...move(Bucket.RESERVED, Bucket.WALLET, USDC_MINT, parts.usdcRaw), ...move(Bucket.RESERVED, Bucket.WALLET, NATIVE_SOL, parts.lamports)]);
}

/** Only fee items that are *not* included in the quote move balances. */
export function feeEntries(items: readonly FeeItem[], fromBucket: string): LedgerEntry[] {
  const out: LedgerEntry[] = [];
  for (const f of items) {
    if (f.includedInQuote || f.amountRaw === 0n) continue;
    const expense =
      f.kind === "BASE_NETWORK"
        ? Bucket.FEE_NETWORK
        : f.kind === "PRIORITY" || f.kind === "TIP"
          ? Bucket.FEE_PRIORITY
          : f.kind === "FAILED_TX"
            ? Bucket.FEE_FAILED_TX
            : f.kind === "CLOSE_ACCOUNT"
              ? Bucket.FEE_CLOSE_ACCOUNT
              : f.kind === "TOKEN_TRANSFER_FEE"
                ? Bucket.FEE_TRANSFER
                : null;
    if (expense === null) throw new RangeError(`fee kind ${f.kind} cannot be posted as an expense`);
    out.push(...move(fromBucket, expense, f.asset, f.amountRaw));
  }
  return out;
}

export interface BuyFillPosting {
  usdcInRaw: bigint;
  tokenMint: string;
  tokenOutRaw: bigint;
  fees: readonly FeeItem[];
  rentLamports: bigint;
}

/** Buy consumes the reservation made for this intent (USDC notional + SOL for fees and rent). */
export function buyFillTx(base: TxBase, p: BuyFillPosting): LedgerTransaction {
  return tx(base, "BUY_FILL", [
    ...move(Bucket.RESERVED, Bucket.SWAP_CLEARING, USDC_MINT, p.usdcInRaw),
    ...move(Bucket.SWAP_CLEARING, Bucket.WALLET, p.tokenMint, p.tokenOutRaw),
    ...feeEntries(p.fees, Bucket.RESERVED),
    ...move(Bucket.RESERVED, Bucket.RENT_LOCKED, NATIVE_SOL, p.rentLamports),
  ]);
}

export interface SellFillPosting {
  tokenMint: string;
  tokenInRaw: bigint;
  usdcOutRaw: bigint;
  fees: readonly FeeItem[];
}

export function sellFillTx(base: TxBase, p: SellFillPosting): LedgerTransaction {
  return tx(base, "SELL_FILL", [
    ...move(Bucket.WALLET, Bucket.SWAP_CLEARING, p.tokenMint, p.tokenInRaw),
    ...move(Bucket.SWAP_CLEARING, Bucket.WALLET, USDC_MINT, p.usdcOutRaw),
    ...feeEntries(p.fees, Bucket.WALLET),
  ]);
}

export function failedAttemptFeeTx(base: TxBase, fees: readonly FeeItem[], fromBucket: string): LedgerTransaction {
  return tx(base, "FAILED_ATTEMPT_FEE", feeEntries(fees, fromBucket));
}

/** Closing our own empty token account: rent returns to wallet, close fee is a cost. Not trading profit. */
export function rentRecoveryTx(base: TxBase, rentLamports: bigint, closeFees: readonly FeeItem[]): LedgerTransaction {
  return tx(base, "RENT_RECOVERY", [...move(Bucket.RENT_LOCKED, Bucket.WALLET, NATIVE_SOL, rentLamports), ...feeEntries(closeFees, Bucket.WALLET)]);
}
