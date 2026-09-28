import { D, NATIVE_SOL, SOL_DECIMALS, USDC_DECIMALS, USDC_MINT, ValuationStatus, rawToUsd, type Dec } from "@solbot/domain";
import { Bucket, type Ledger } from "./ledger.ts";

export interface FxSnapshot {
  usdcUsd: Dec | null;
  solUsd: Dec | null;
  at: Date;
  source: string;
}

export interface PositionMark {
  mint: string;
  status: ValuationStatus;
  /** Net USDC (raw) from a fresh sell quote for the whole position, fees in quote already deducted. */
  netUsdcRaw: bigint | null;
  /** Modeled exit transaction costs not included in the quote (lamports). */
  exitFeesLamports: bigint;
  quoteAt: Date | null;
}

export interface EquityBreakdown {
  ok: true;
  at: Date;
  usdcUsd: Dec;
  solUsd: Dec;
  usdcHeldUsd: Dec; // wallet + reserved
  solSpendableUsd: Dec; // wallet + reserved lamports
  positionsLowerBoundUsd: Dec;
  pendingLiabilitiesUsd: Dec;
  equityLiquidLowerBoundUsd: Dec;
  rentLockedUsd: Dec;
  rentRecoverableUsd: Dec;
  equityTotalLowerBoundUsd: Dec;
  /** Equity using only fresh valuations; null when any position is not FRESH. */
  equityTotalFreshUsd: Dec | null;
  /** True when a position lacks valuation because the provider is down (PAUSED_DATA, not a loss). */
  hasProviderUncertainty: boolean;
  uncertain: Array<{ mint: string; status: ValuationStatus }>;
}

export type EquityResult = EquityBreakdown | { ok: false; reason: "FX_MISSING"; missing: string[] };

/**
 * equity_liquid_lower_bound = USDC(wallet+reserved)×USDC/USD + SOL(wallet+reserved)×SOL/USD
 *                           + Σ conservative net sell quotes − unbooked liabilities
 * equity_total_lower_bound  = liquid + conservative recoverable rent
 * Fees already posted reduce balances and are not subtracted again (brief §11).
 */
export function computeEquity(
  ledger: Ledger,
  fx: FxSnapshot,
  marks: ReadonlyMap<string, PositionMark>,
  opts: { closeAccountFeeLamportsPerPosition: bigint },
): EquityResult {
  const missing: string[] = [];
  if (!fx.usdcUsd) missing.push("USDC/USD");
  if (!fx.solUsd) missing.push("SOL/USD");
  if (missing.length > 0 || !fx.usdcUsd || !fx.solUsd) return { ok: false, reason: "FX_MISSING", missing };
  const usdcUsd = fx.usdcUsd;
  const solUsd = fx.solUsd;

  const usdcHeldUsd = rawToUsd(ledger.holding(USDC_MINT), USDC_DECIMALS, usdcUsd);
  const solSpendableUsd = rawToUsd(ledger.holding(NATIVE_SOL), SOL_DECIMALS, solUsd);

  let positionsLowerBoundUsd = new D(0);
  let liabilitiesLamports = 0n;
  let allFresh = true;
  let hasProviderUncertainty = false;
  const uncertain: EquityBreakdown["uncertain"] = [];
  const holdings = ledger.tokenHoldings();

  for (const [mint] of holdings) {
    const m = marks.get(mint);
    const status = m?.status ?? ValuationStatus.UNKNOWN_VALUATION;
    liabilitiesLamports += m?.exitFeesLamports ?? 0n;
    if (status === ValuationStatus.FRESH && m && m.netUsdcRaw !== null) {
      positionsLowerBoundUsd = positionsLowerBoundUsd.add(rawToUsd(m.netUsdcRaw, USDC_DECIMALS, usdcUsd));
    } else {
      // No fresh executable quote: conservative lower bound is 0. Not a realized loss.
      allFresh = false;
      uncertain.push({ mint, status });
      if (status === ValuationStatus.UNKNOWN_VALUATION || status === ValuationStatus.STALE) hasProviderUncertainty = true;
    }
  }

  const pendingLiabilitiesUsd = rawToUsd(liabilitiesLamports, SOL_DECIMALS, solUsd);
  const equityLiquidLowerBoundUsd = usdcHeldUsd.add(solSpendableUsd).add(positionsLowerBoundUsd).sub(pendingLiabilitiesUsd);

  const rentLamports = ledger.balance(Bucket.RENT_LOCKED, NATIVE_SOL);
  const rentLockedUsd = rawToUsd(rentLamports, SOL_DECIMALS, solUsd);
  const closeCost = opts.closeAccountFeeLamportsPerPosition * BigInt(holdings.size);
  const recoverable = rentLamports > closeCost ? rentLamports - closeCost : 0n;
  const rentRecoverableUsd = rawToUsd(recoverable, SOL_DECIMALS, solUsd);
  const equityTotalLowerBoundUsd = equityLiquidLowerBoundUsd.add(rentRecoverableUsd);

  return {
    ok: true,
    at: fx.at,
    usdcUsd,
    solUsd,
    usdcHeldUsd,
    solSpendableUsd,
    positionsLowerBoundUsd,
    pendingLiabilitiesUsd,
    equityLiquidLowerBoundUsd,
    rentLockedUsd,
    rentRecoverableUsd,
    equityTotalLowerBoundUsd,
    equityTotalFreshUsd: allFresh ? equityTotalLowerBoundUsd : null,
    hasProviderUncertainty,
    uncertain,
  };
}
