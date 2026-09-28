import { D, type Dec } from "@solbot/domain";

/** Pure trade statistics. Small samples are labelled, never extrapolated or annualized. */
export interface ClosedTrade {
  positionId: string;
  mint: string;
  entryAt: Date;
  exitAt: Date;
  costUsd: Dec;
  pnlUsd: Dec;
  exitReason: string;
  entryNotionalUsd: Dec;
  exitProceedsUsd: Dec;
  feesUsd: Dec;
}

export interface TradeStats {
  closed: number;
  distinctTokens: number;
  wins: number;
  losses: number;
  winRate: { value: Dec | null; n: number };
  avgWinUsd: Dec | null;
  avgLossUsd: Dec | null;
  medianPnlUsd: Dec | null;
  profitFactor: { value: Dec | null; status: "OK" | "INSUFFICIENT_NO_LOSSES" | "NO_TRADES" };
  expectancyUsd: Dec | null;
  totalPnlUsd: Dec;
  pnlWithoutBestTokenUsd: Dec;
  top3WinnersShareBps: number | null;
  turnoverUsd: Dec;
  feesUsd: Dec;
  feesPctOfTurnoverBps: number | null;
}

const median = (xs: Dec[]): Dec | null => {
  if (xs.length === 0) return null;
  const s = [...xs].sort((a, b) => a.cmp(b));
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m]! : s[m - 1]!.add(s[m]!).div(2);
};

export function tradeStats(trades: readonly ClosedTrade[]): TradeStats {
  const wins = trades.filter((t) => t.pnlUsd.gt(0));
  const losses = trades.filter((t) => t.pnlUsd.lt(0));
  const sum = (xs: readonly ClosedTrade[], f: (t: ClosedTrade) => Dec) => xs.reduce((a, t) => a.add(f(t)), new D(0));
  const gp = sum(wins, (t) => t.pnlUsd);
  const gl = sum(losses, (t) => t.pnlUsd.neg());
  const total = sum(trades, (t) => t.pnlUsd);
  const byToken = new Map<string, Dec>();
  for (const t of trades) byToken.set(t.mint, (byToken.get(t.mint) ?? new D(0)).add(t.pnlUsd));
  const best = [...byToken.entries()].sort((a, b) => b[1].cmp(a[1]))[0];
  const topWins = wins.map((t) => t.pnlUsd).sort((a, b) => b.cmp(a)).slice(0, 3);
  const turnover = sum(trades, (t) => t.entryNotionalUsd.add(t.exitProceedsUsd));
  const fees = sum(trades, (t) => t.feesUsd);
  return {
    closed: trades.length,
    distinctTokens: byToken.size,
    wins: wins.length,
    losses: losses.length,
    winRate: { value: trades.length ? new D(wins.length).div(trades.length) : null, n: trades.length },
    avgWinUsd: wins.length ? gp.div(wins.length) : null,
    avgLossUsd: losses.length ? gl.neg().div(losses.length) : null,
    medianPnlUsd: median(trades.map((t) => t.pnlUsd)),
    profitFactor: trades.length === 0 ? { value: null, status: "NO_TRADES" } : gl.eq(0) ? { value: null, status: "INSUFFICIENT_NO_LOSSES" } : { value: gp.div(gl), status: "OK" },
    expectancyUsd: trades.length ? total.div(trades.length) : null,
    totalPnlUsd: total,
    pnlWithoutBestTokenUsd: best && best[1].gt(0) ? total.sub(best[1]) : total,
    top3WinnersShareBps: gp.gt(0) ? topWins.reduce((a, b) => a.add(b), new D(0)).div(gp).mul(10_000).toDecimalPlaces(0).toNumber() : null,
    turnoverUsd: turnover,
    feesUsd: fees,
    feesPctOfTurnoverBps: turnover.gt(0) ? fees.div(turnover).mul(10_000).toDecimalPlaces(0).toNumber() : null,
  };
}

/** Max drawdown of an equity series (USD and bps of the running peak). */
export function maxDrawdown(series: ReadonlyArray<{ at: Date; value: Dec }>): { usd: Dec; bps: number; peakAt: Date | null; troughAt: Date | null } {
  let peak: { at: Date; value: Dec } | null = null;
  let best = { usd: new D(0), bps: 0, peakAt: null as Date | null, troughAt: null as Date | null };
  for (const p of series) {
    if (!peak || p.value.gt(peak.value)) peak = p;
    const dd = peak.value.sub(p.value);
    if (dd.gt(best.usd)) {
      best = { usd: dd, bps: peak.value.gt(0) ? dd.div(peak.value).mul(10_000).toDecimalPlaces(0).toNumber() : 0, peakAt: peak.at, troughAt: p.at };
    }
  }
  return best;
}

export function percentile(values: readonly number[], p: number): number | null {
  if (values.length === 0) return null;
  const s = [...values].sort((a, b) => a - b);
  const idx = Math.min(s.length - 1, Math.max(0, Math.ceil((p / 100) * s.length) - 1));
  return s[idx]!;
}
