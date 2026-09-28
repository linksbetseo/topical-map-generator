/**
 * Historical copy replay (spec v2 §5, §11–12): what a FOLLOWER would have earned copying one leader
 * buy with its own delay, size, costs and exits. The leader's own result is not used.
 *
 * Price data are trade candles (USD, sparse — seconds/minutes without trades are absent). Rules,
 * all chosen to err against the follower:
 *  - entry at the first second ≥ buy time + delay: the HIGH of that 1 s candle if it traded in that
 *    second, else the close of the last traded second before it; then + slippage scenario;
 *  - exits evaluated on 1 m candles starting at the first full minute after entry; stops are checked
 *    with the peak known BEFORE the candle, then the peak is updated with the candle's high;
 *  - within one candle, a stop is assumed to hit before take-profit;
 *  - a stop fills at min(stop level, candle close) — a gap through the stop fills at the close;
 *    take-profit fills at its level; time stop at the close of the candle containing T+4 h;
 *  - no trade at all after entry ⇒ EXIT_UNPRICED: last price shown, conservative value 0.
 * Q0/Q1 quotes and route changes cannot be reconstructed from candles: this is a scenario model,
 * not a fill; price impact of 25 USD is assumed inside the slippage scenario.
 */
export interface ReplayCandle {
  unix_time: number;
  o: number;
  h: number;
  l: number;
  c: number;
}

export interface ReplayParams {
  delaySec: number;
  sizeUsd: number;
  /** extra adverse price per side, bps (scenario, not a measurement) */
  slippageBps: number;
  /** network + priority fees per side, USD */
  feePerSideUsd: number;
  stopLossBps: number;
  takeProfitBps: number;
  trailActivationBps: number;
  trailDrawdownBps: number;
  timeStopSec: number;
  /** false = no SL/TP/trailing (only the leader-exit and time stop) */
  useOwnExits?: boolean;
  /** leader's first sell of this token after the buy (unix s); the follower exits `delaySec` later */
  leaderExitTime?: number;
}

export type ExitReason = "STOP_LOSS" | "TAKE_PROFIT" | "TRAILING_STOP" | "TIME_STOP" | "LEADER_EXIT" | "EXIT_UNPRICED";

export type ReplayResult =
  | { status: "NO_ENTRY_PRICE"; detail: string }
  | {
      status: "FILLED";
      /** 1s = traded second; 1m = fallback to the HIGH of the entry minute (no 1 s history) */
      entryGranularity: "1s" | "1m";
      entryTime: number;
      entryPrice: number;
      exitTime: number;
      exitPrice: number;
      exitReason: ExitReason;
      grossReturn: number;
      pnlUsd: number;
      /** for EXIT_UNPRICED: PnL at the last seen price (not booked) */
      pnlAtLastPriceUsd?: number;
      peakReturn: number;
    };

export function replayCopy(buyTime: number, entryCandles1s: readonly ReplayCandle[], pathCandles1m: readonly ReplayCandle[], p: ReplayParams): ReplayResult {
  const at = buyTime + p.delaySec;
  const s1 = [...entryCandles1s].sort((a, b) => a.unix_time - b.unix_time);
  const same = s1.find((c) => c.unix_time === at);
  const before = [...s1].reverse().find((c) => c.unix_time < at);
  let raw = same ? same.h : before ? before.c : undefined;
  let granularity: "1s" | "1m" = "1s";
  if (raw === undefined && s1.length === 0) {
    // no 1 s history (Birdeye keeps ~15 days): the high of the minute containing the entry second
    const minute = [...pathCandles1m].sort((a, b) => a.unix_time - b.unix_time).find((c) => c.unix_time + 60 > at && c.unix_time <= at + 60);
    raw = minute?.h;
    granularity = "1m";
  }
  if (raw === undefined || !(raw > 0)) return { status: "NO_ENTRY_PRICE", detail: `no trade near ${at}` };
  const slip = p.slippageBps / 10_000;
  const entry = raw * (1 + slip);
  const sl = entry * (1 - p.stopLossBps / 10_000);
  const tp = entry * (1 + p.takeProfitBps / 10_000);
  const act = entry * (1 + p.trailActivationBps / 10_000);
  const end = at + p.timeStopSec;
  const path = [...pathCandles1m].filter((c) => c.unix_time >= Math.ceil(at / 60) * 60 && c.unix_time <= end).sort((a, b) => a.unix_time - b.unix_time);

  let peak = entry;
  const fin = (exitTime: number, px: number, reason: ExitReason, extra: Partial<Extract<ReplayResult, { status: "FILLED" }>> = {}): ReplayResult => {
    const exitPrice = px * (1 - slip);
    const gross = exitPrice / entry;
    return {
      status: "FILLED",
      entryGranularity: granularity,
      entryTime: at,
      entryPrice: entry,
      exitTime,
      exitPrice,
      exitReason: reason,
      grossReturn: gross - 1,
      pnlUsd: p.sizeUsd * gross - p.sizeUsd - 2 * p.feePerSideUsd,
      peakReturn: peak / entry - 1,
      ...extra,
    };
  };
  const own = p.useOwnExits ?? true;
  const leaderOut = p.leaderExitTime !== undefined ? p.leaderExitTime + p.delaySec : undefined;
  for (const c of path) {
    // the leader sold: exit in the minute we learn it, at that minute's LOW (conservative)
    if (leaderOut !== undefined && c.unix_time + 60 > leaderOut && leaderOut >= at) return fin(Math.max(c.unix_time, leaderOut), c.l, "LEADER_EXIT");
    if (own) {
      // stops use the peak known before this candle
      if (c.l <= sl) return fin(c.unix_time, Math.min(sl, c.c), "STOP_LOSS");
      if (peak >= act) {
        const trail = peak * (1 - p.trailDrawdownBps / 10_000);
        if (c.l <= trail) return fin(c.unix_time, Math.min(trail, c.c), "TRAILING_STOP");
      }
      if (c.h >= tp) return fin(c.unix_time, tp, "TAKE_PROFIT");
    }
    if (c.h > peak) peak = c.h;
    if (c.unix_time + 60 > end) return fin(end, c.c, "TIME_STOP");
  }
  const last = path[path.length - 1];
  if (last && last.unix_time + 60 >= end - 60) return fin(end, last.c, "TIME_STOP");
  // no trades until the time stop: the position could not be priced — conservative value 0
  const lastPx = last ? last.c : raw;
  const zero: ReplayResult = { status: "FILLED", entryGranularity: granularity, entryTime: at, entryPrice: entry, exitTime: end, exitPrice: 0, exitReason: "EXIT_UNPRICED", grossReturn: -1, pnlUsd: -p.sizeUsd - p.feePerSideUsd, peakReturn: peak / entry - 1 };
  return { ...zero, pnlAtLastPriceUsd: p.sizeUsd * ((lastPx * (1 - slip)) / entry) - p.sizeUsd - 2 * p.feePerSideUsd };
}
