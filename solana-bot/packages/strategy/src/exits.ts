import { D, HOUR_MS, ReasonCode, type Dec } from "@solbot/domain";
import type { Config } from "@solbot/config";

/**
 * Exit rules in priority order (brief §7.3). Returns the first rule that fires.
 * Thresholds trigger an exit *attempt*; they never define the fill price.
 */
export interface PositionState {
  costUsd: Dec;
  entryFilledAt: Date;
  /** Highest fresh net liquidation value since the trailing stop was activated (null if never). */
  trailingPeakUsd: Dec | null;
  /** Entry wallets and quantity each held at our entry vs now (confirmed swaps only). */
  entryWallets: ReadonlyArray<{ wallet: string; qtyAtEntryRaw: bigint; qtyNowRaw: bigint | null }>;
}

export interface ExitContext {
  now: Date;
  tEnd: Date;
  policyViolation: string | null;
  emergencyWindDown: boolean;
  /** Fresh net liquidation value (USD) of the whole position; null when not fresh / unknown. */
  netLiquidationUsd: Dec | null;
}

export type ExitDecision =
  | { exit: false; trailingPeakUsd: Dec | null; nlrBps: number | null }
  | { exit: true; code: string; kind: "NORMAL" | "EMERGENCY"; detail: string; trailingPeakUsd: Dec | null; nlrBps: number | null };

export function netLiquidationReturnBps(netLiqUsd: Dec, costUsd: Dec): number {
  return netLiqUsd.sub(costUsd).div(costUsd).mul(10_000).toDecimalPlaces(0, D.ROUND_FLOOR).toNumber();
}

export function evaluateExit(p: PositionState, ctx: ExitContext, cfg: Config): ExitDecision {
  const e = cfg.exits;
  const nlr = ctx.netLiquidationUsd !== null ? netLiquidationReturnBps(ctx.netLiquidationUsd, p.costUsd) : null;

  // trailing peak bookkeeping uses only fresh values
  let peak = p.trailingPeakUsd;
  if (ctx.netLiquidationUsd !== null && nlr !== null) {
    if (peak !== null) peak = D.max(peak, ctx.netLiquidationUsd);
    else if (nlr >= e.trailing_activation_bps) peak = ctx.netLiquidationUsd;
  }
  const base = { trailingPeakUsd: peak, nlrBps: nlr };
  const out = (code: string, kind: "NORMAL" | "EMERGENCY", detail: string): ExitDecision => ({ exit: true, code, kind, detail, ...base });

  if (ctx.policyViolation) return out(ReasonCode.EXIT_POLICY_VIOLATION, "EMERGENCY", ctx.policyViolation);
  if (ctx.emergencyWindDown) return out(ReasonCode.EXIT_EMERGENCY, "EMERGENCY", "session wind-down / owner flatten");
  if (nlr !== null && nlr <= -e.stop_loss_bps) return out(ReasonCode.EXIT_STOP_LOSS, "NORMAL", `NLR ${nlr} bps`);

  const reducers = p.entryWallets.filter(
    (w) => w.qtyNowRaw !== null && w.qtyAtEntryRaw > 0n && (w.qtyAtEntryRaw - w.qtyNowRaw) * 10_000n >= w.qtyAtEntryRaw * BigInt(e.distribution_min_reduction_bps),
  );
  if (reducers.length >= e.distribution_min_wallets) return out(ReasonCode.EXIT_DISTRIBUTION, "NORMAL", `${reducers.length} entry wallets reduced >= ${e.distribution_min_reduction_bps} bps`);

  if (peak !== null && ctx.netLiquidationUsd !== null) {
    const floor = peak.mul(10_000 - e.trailing_drawdown_bps).div(10_000);
    if (ctx.netLiquidationUsd.lte(floor)) return out(ReasonCode.EXIT_TRAILING_STOP, "NORMAL", `value ${ctx.netLiquidationUsd.toFixed(4)} <= ${floor.toFixed(4)}`);
  }
  if (nlr !== null && nlr >= e.take_profit_bps) return out(ReasonCode.EXIT_TAKE_PROFIT, "NORMAL", `NLR ${nlr} bps`);
  if (ctx.now.getTime() - p.entryFilledAt.getTime() >= e.time_stop_hours * HOUR_MS) return out(ReasonCode.EXIT_TIME_STOP, "NORMAL", `held ${e.time_stop_hours} h`);
  if (ctx.now >= ctx.tEnd) return out(ReasonCode.EXIT_SESSION_END, "NORMAL", "T_end reached");
  return { exit: false, ...base };
}
