import {
  D,
  HOUR_MS,
  ReasonCode,
  SOL_DECIMALS,
  SessionState,
  USDC_DECIMALS,
  rawToUsd,
  reason,
  sessionAllowsEntries,
  usdToRawFloor,
  type Dec,
  type Reason,
} from "@solbot/domain";
import type { Config } from "@solbot/config";

/**
 * Risk engine: pure functions over an explicit snapshot. Evaluated before capital is
 * reserved and again immediately before execution (brief §3). Returns *all* failing
 * reasons so the rejection funnel is complete.
 */

export interface OpenExposure {
  mint: string;
  /** Remaining acquisition cost in USD (v1: full entry cost). */
  remainingCostUsd: Dec;
  /** Conservative liquidation value (0 when no route / unknown). */
  conservativeLiquidationUsd: Dec;
  deployerGroup: string | null;
}

export interface RiskSnapshot {
  now: Date;
  sessionState: SessionState;
  tEnd: Date;
  entriesPausedByOwner: boolean;
  /** Conservative total equity (lower bound) at `now`. */
  equityUsd: Dec;
  /** true when the equity above contains provider-outage valuations (PAUSED_DATA, no loss verdict). */
  equityHasProviderUncertainty: boolean;
  equityAtUtcDayStartUsd: Dec;
  peakEquityUsd: Dec;
  initialEquityUsd: Dec;
  usdcUsd: Dec;
  solUsd: Dec;
  freeUsdcRaw: bigint;
  freeLamports: bigint;
  positions: readonly OpenExposure[];
  /** Entry reservations not yet resolved (count toward positions, exposure and daily notional). */
  pendingEntries: ReadonlyArray<{ mint: string; notionalUsd: Dec; deployerGroup: string | null }>;
  unresolvedOrderMints: ReadonlySet<string>;
  anyStatusUnknown: boolean;
  entryAttemptsToday: number;
  /** Filled entry notional today + notional of unresolved attempts. */
  entryNotionalTodayUsd: Dec;
  lastClosedAtByMint: ReadonlyMap<string, Date>;
  staleData: readonly string[];
  reconciliationOk: boolean;
}

export interface EntryCandidate {
  mint: string;
  deployerGroup: string | null;
  entryFeesLamports: bigint; // modeled network + priority for the entry attempt
  exitFeesLamports: bigint; // modeled network + priority for a normal exit
  rentLamports: bigint;
}

export interface EntryDecision {
  approved: boolean;
  reasons: Reason[];
  notionalUsd: Dec | null;
  usdcRaw: bigint | null;
  /** SOL to reserve for this attempt: entry fees + rent. */
  reserveLamports: bigint | null;
  checks: Record<string, string>;
}

const usdD = (s: string) => new D(s);
const bpsOf = (x: Dec, b: number) => x.mul(b).div(10_000);

export function lamportsToUsd(l: bigint, solUsd: Dec): Dec {
  return rawToUsd(l, SOL_DECIMALS, solUsd);
}

export function exposureUsd(snap: RiskSnapshot): Dec {
  let total = new D(0);
  for (const p of snap.positions) total = total.add(D.max(p.remainingCostUsd, p.conservativeLiquidationUsd));
  for (const p of snap.pendingEntries) total = total.add(p.notionalUsd);
  return total;
}

/** Lamports that must stay available for exits of every held position (+ the new one) and a margin. */
export function requiredSolReserveLamports(snap: RiskSnapshot, cfg: Config, positionsAfter: number, exitFeesLamports: bigint): bigint {
  const emergencyCapLamports = usdToRawFloor(usdD(cfg.risk.emergency_exit_fee_cap_usd), SOL_DECIMALS, snap.solUsd);
  const perPosition = exitFeesLamports > emergencyCapLamports ? exitFeesLamports : emergencyCapLamports;
  const closeFee = BigInt(cfg.execution.base_fee_lamports_per_signature);
  const margin = usdToRawFloor(usdD(cfg.risk.sol_reserve_margin_usd), SOL_DECIMALS, snap.solUsd);
  return BigInt(positionsAfter) * (perPosition + closeFee) + margin;
}

export function evaluateEntry(c: EntryCandidate, snap: RiskSnapshot, cfg: Config): EntryDecision {
  const reasons: Reason[] = [];
  const checks: Record<string, string> = {};

  // --- session / global gates
  if (!sessionAllowsEntries(snap.sessionState)) reasons.push(reason(ReasonCode.SESSION_NOT_RUNNING, snap.sessionState));
  if (snap.entriesPausedByOwner) reasons.push(reason(ReasonCode.ENTRIES_PAUSED, "owner PAUSE_ENTRIES"));
  const cutoff = snap.tEnd.getTime() - cfg.experiment.entry_cutoff_before_end_hours * HOUR_MS;
  if (snap.now.getTime() >= cutoff) reasons.push(reason(ReasonCode.ENTRY_WINDOW_CLOSED, `entries end at ${new Date(cutoff).toISOString()}`));
  if (snap.staleData.length > 0) reasons.push(reason(ReasonCode.DATA_STALE, snap.staleData.join(",")));
  if (snap.equityHasProviderUncertainty) reasons.push(reason(ReasonCode.UNKNOWN_VALUATION, "open position without valuation"));
  if (!snap.reconciliationOk) reasons.push(reason(ReasonCode.RECONCILIATION_MISMATCH));
  if (snap.anyStatusUnknown) reasons.push(reason(ReasonCode.UNRESOLVED_ORDER, "an order has STATUS_UNKNOWN"));

  // --- per-mint gates
  if (snap.positions.some((p) => p.mint === c.mint) || snap.pendingEntries.some((p) => p.mint === c.mint)) {
    reasons.push(reason(ReasonCode.POSITION_ALREADY_OPEN));
  }
  if (snap.unresolvedOrderMints.has(c.mint)) reasons.push(reason(ReasonCode.UNRESOLVED_ORDER, c.mint));
  const closedAt = snap.lastClosedAtByMint.get(c.mint);
  if (closedAt && snap.now.getTime() - closedAt.getTime() < cfg.signal.mint_cooldown_hours * HOUR_MS) {
    reasons.push(reason(ReasonCode.MINT_COOLDOWN, `closed at ${closedAt.toISOString()}`));
  }
  if (c.deployerGroup !== null) {
    const groups = [...snap.positions.map((p) => p.deployerGroup), ...snap.pendingEntries.map((p) => p.deployerGroup)];
    if (groups.includes(c.deployerGroup)) reasons.push(reason(ReasonCode.DEPLOYER_GROUP_OVERLAP, c.deployerGroup));
  }

  // --- counts and daily limits
  const openCount = snap.positions.length + snap.pendingEntries.length;
  checks.open_or_reserved_positions = String(openCount);
  if (openCount >= cfg.sizing.max_open_positions) reasons.push(reason(ReasonCode.MAX_POSITIONS_REACHED, `${openCount}/${cfg.sizing.max_open_positions}`));
  checks.entry_attempts_today = String(snap.entryAttemptsToday);
  if (snap.entryAttemptsToday >= cfg.sizing.max_entry_attempts_per_utc_day) reasons.push(reason(ReasonCode.DAILY_ATTEMPT_LIMIT));

  // --- sizing: min(25 USD, 5% equity, exposure room, free USDC, daily notional room)
  const equity = snap.equityUsd;
  const exposure = exposureUsd(snap);
  const exposureRoom = D.max(0, bpsOf(equity, cfg.sizing.max_exposure_equity_bps).sub(exposure));
  const freeUsdcUsd = rawToUsd(snap.freeUsdcRaw, USDC_DECIMALS, snap.usdcUsd);
  const dailyRoom = D.max(0, usdD(cfg.sizing.max_entry_notional_per_utc_day_usd).sub(snap.entryNotionalTodayUsd));
  const notional = D.min(usdD(cfg.sizing.max_position_usd), bpsOf(equity, cfg.sizing.max_position_equity_bps), exposureRoom, freeUsdcUsd, dailyRoom).toDecimalPlaces(
    6,
    D.ROUND_DOWN,
  );
  checks.equity_usd = equity.toFixed(6);
  checks.exposure_usd = exposure.toFixed(6);
  checks.exposure_room_usd = exposureRoom.toFixed(6);
  checks.free_usdc_usd = freeUsdcUsd.toFixed(6);
  checks.daily_notional_room_usd = dailyRoom.toFixed(6);
  checks.notional_usd = notional.toFixed(6);

  if (notional.lt(usdD(cfg.sizing.min_position_usd))) {
    const limiting = dailyRoom.lt(usdD(cfg.sizing.min_position_usd))
      ? ReasonCode.DAILY_NOTIONAL_LIMIT
      : exposureRoom.lt(usdD(cfg.sizing.min_position_usd))
        ? ReasonCode.EXPOSURE_LIMIT
        : freeUsdcUsd.lt(usdD(cfg.sizing.min_position_usd))
          ? ReasonCode.INSUFFICIENT_USDC
          : ReasonCode.NOTIONAL_BELOW_MINIMUM;
    reasons.push(reason(limiting, `notional ${notional.toFixed(2)} < minimum ${cfg.sizing.min_position_usd}`));
  }

  // --- fees, rent and SOL reserve
  const entryFeesUsd = lamportsToUsd(c.entryFeesLamports, snap.solUsd);
  checks.entry_fees_usd = entryFeesUsd.toFixed(6);
  if (entryFeesUsd.gt(usdD(cfg.risk.entry_fee_cap_usd))) reasons.push(reason(ReasonCode.FEE_CAP_EXCEEDED, `entry fees ${entryFeesUsd.toFixed(4)} USD`));
  const rentUsd = lamportsToUsd(c.rentLamports, snap.solUsd);
  checks.rent_usd = rentUsd.toFixed(6);
  if (rentUsd.gt(usdD(cfg.risk.max_rent_per_entry_usd))) reasons.push(reason(ReasonCode.RENT_CAP_EXCEEDED, `rent ${rentUsd.toFixed(4)} USD`));

  const reserveLamports = c.entryFeesLamports + c.rentLamports;
  const required = requiredSolReserveLamports(snap, cfg, openCount + 1, c.exitFeesLamports);
  checks.free_lamports = snap.freeLamports.toString();
  checks.required_lamports_after_entry = required.toString();
  if (snap.freeLamports - reserveLamports < required) {
    reasons.push(reason(ReasonCode.INSUFFICIENT_SOL_RESERVE, `free ${snap.freeLamports}, entry needs ${reserveLamports}, must keep ${required}`));
  }

  const approved = reasons.length === 0;
  return {
    approved,
    reasons,
    notionalUsd: approved ? notional : null,
    usdcRaw: approved ? usdToRawFloor(notional, USDC_DECIMALS, snap.usdcUsd) : null,
    reserveLamports: approved ? reserveLamports : null,
    checks,
  };
}

export type LossAction = "NONE" | "PAUSED_DATA" | "EXIT_ONLY" | "HALTED_RISK";

export interface LossEvaluation {
  action: LossAction;
  reasons: Reason[];
  dailyThresholdUsd: Dec;
  dailyLossUsd: Dec;
  sessionLossUsd: Dec;
  drawdownUsd: Dec;
  drawdownThresholdUsd: Dec;
}

/**
 * Loss triggers (brief §8). A provider outage never counts as an economic loss:
 * with unknown valuations we pause data-dependent actions and re-evaluate later (A22).
 * Triggers start exit attempts; they are not guaranteed maximum losses.
 */
export function evaluateLossTriggers(snap: RiskSnapshot, cfg: Config): LossEvaluation {
  const dailyThresholdUsd = D.min(usdD(cfg.risk.daily_loss_max_usd), bpsOf(snap.equityAtUtcDayStartUsd, cfg.risk.daily_loss_equity_bps));
  const dailyLossUsd = snap.equityAtUtcDayStartUsd.sub(snap.equityUsd);
  const sessionLossUsd = snap.initialEquityUsd.sub(snap.equityUsd);
  const drawdownUsd = snap.peakEquityUsd.sub(snap.equityUsd);
  const drawdownThresholdUsd = D.min(usdD(cfg.risk.drawdown_max_usd), bpsOf(snap.peakEquityUsd, cfg.risk.drawdown_peak_bps));
  const base = { dailyThresholdUsd, dailyLossUsd, sessionLossUsd, drawdownUsd, drawdownThresholdUsd };

  if (snap.equityHasProviderUncertainty) {
    return { action: "PAUSED_DATA", reasons: [reason(ReasonCode.UNKNOWN_VALUATION, "loss triggers deferred until valuations return")], ...base };
  }
  const reasons: Reason[] = [];
  let action: LossAction = "NONE";
  if (sessionLossUsd.gte(usdD(cfg.risk.session_loss_usd))) {
    reasons.push(reason(ReasonCode.SESSION_LOSS_TRIGGER, `loss ${sessionLossUsd.toFixed(2)} USD from T0`));
    action = "HALTED_RISK";
  }
  if (drawdownUsd.gte(drawdownThresholdUsd) && drawdownUsd.gt(0)) {
    reasons.push(reason(ReasonCode.DRAWDOWN_TRIGGER, `drawdown ${drawdownUsd.toFixed(2)} >= ${drawdownThresholdUsd.toFixed(2)} USD`));
    action = "HALTED_RISK";
  }
  if (dailyLossUsd.gte(dailyThresholdUsd) && dailyLossUsd.gt(0)) {
    reasons.push(reason(ReasonCode.DAILY_LOSS_TRIGGER, `daily loss ${dailyLossUsd.toFixed(2)} >= ${dailyThresholdUsd.toFixed(2)} USD`));
    if (action === "NONE") action = "EXIT_ONLY";
  }
  return { action, reasons, ...base };
}

export type ExitKind = "NORMAL" | "EMERGENCY";

export function exitLimits(cfg: Config, kind: ExitKind): { slippageBps: number; feeCapUsd: Dec } {
  return kind === "EMERGENCY"
    ? { slippageBps: cfg.risk.emergency_exit_slippage_bps, feeCapUsd: usdD(cfg.risk.emergency_exit_fee_cap_usd) }
    : { slippageBps: cfg.risk.exit_slippage_bps, feeCapUsd: usdD(cfg.risk.exit_fee_cap_usd) };
}
