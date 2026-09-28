import { D, ReasonCode, reason, utcDayKey, type Dec, type Reason } from "@solbot/domain";
import type { Config } from "@solbot/config";

/**
 * Wallet episode reconstruction and qualification (brief §6). Statistics of foreign wallets are a
 * reconstruction with stated coverage, not an audit. Transfers are never purchases at price zero.
 */

export interface WalletEvent {
  wallet: string;
  mint: string;
  blockTime: Date;
  availableAt: Date;
  /** SWAP_BUY / SWAP_SELL are confirmed swaps; transfers change quantity without a known price. */
  kind: "SWAP_BUY" | "SWAP_SELL" | "TRANSFER_IN" | "TRANSFER_OUT";
  tokenRaw: bigint;
  /** USD value at the time of the event (for swaps); null = unknown FX (reduces coverage). */
  usd: Dec | null;
  signature: string;
}

export type EpisodeStatus = "CLOSED" | "OPEN_MARKED" | "OPEN_ZERO_LOWER_BOUND" | "UNKNOWN_COST_BASIS";

export interface Episode {
  wallet: string;
  mint: string;
  start: Date;
  end: Date | null;
  costUsd: Dec;
  proceedsUsd: Dec;
  pnlUsd: Dec | null;
  status: EpisodeStatus;
}

export interface Reconstruction {
  episodes: Episode[];
  swapVolumeUsdKnown: Dec;
  swapEventsTotal: number;
  swapEventsUnknownUsd: number;
}

/** An episode is closed when the remaining quantity is at most 0.1% of its peak (provider amounts are UI floats). */
export const EPISODE_DUST_FRACTION_DENOM = 1_000n;

/**
 * Episodes: from zero position to back to zero (or dust), all buys/sells included.
 * Open positions at T0 use a conservative mark (if given) else a zero lower bound, so unsold losses count.
 */
export function reconstructEpisodes(events: readonly WalletEvent[], t0: Date, marksUsdPerRaw: ReadonlyMap<string, Dec>, dustRaw = 0n): Reconstruction {
  const sorted = events.filter((e) => e.blockTime < t0 && e.availableAt < t0).sort((a, b) => a.blockTime.getTime() - b.blockTime.getTime());
  const byKey = new Map<string, WalletEvent[]>();
  for (const e of sorted) {
    const k = `${e.wallet}|${e.mint}`;
    const l = byKey.get(k) ?? [];
    l.push(e);
    byKey.set(k, l);
  }
  const episodes: Episode[] = [];
  let swapVolumeUsdKnown = new D(0);
  let swapEventsTotal = 0;
  let swapEventsUnknownUsd = 0;

  for (const list of byKey.values()) {
    let qty = 0n;
    let peak = 0n;
    const isDust = (q: bigint) => q <= dustRaw || (peak > 0n && q * EPISODE_DUST_FRACTION_DENOM <= peak);
    let cur: { start: Date; cost: Dec; proceeds: Dec; unknown: boolean } | null = null;
    for (const e of list) {
      if (e.kind === "SWAP_BUY" || e.kind === "SWAP_SELL") {
        swapEventsTotal++;
        if (e.usd === null) swapEventsUnknownUsd++;
        else swapVolumeUsdKnown = swapVolumeUsdKnown.add(e.usd);
      }
      if (!cur && (e.kind === "SWAP_BUY" || e.kind === "TRANSFER_IN")) {
        cur = { start: e.blockTime, cost: new D(0), proceeds: new D(0), unknown: false };
        qty = 0n;
        peak = 0n;
      }
      if (!cur) {
        // selling something we never saw acquired: history incomplete for this pair
        continue;
      }
      switch (e.kind) {
        case "SWAP_BUY":
          qty += e.tokenRaw;
          if (e.usd === null) cur.unknown = true;
          else cur.cost = cur.cost.add(e.usd);
          break;
        case "TRANSFER_IN":
          qty += e.tokenRaw;
          cur.unknown = true; // UNKNOWN_COST_BASIS, never cost 0
          break;
        case "SWAP_SELL":
          qty -= e.tokenRaw;
          if (e.usd === null) cur.unknown = true;
          else cur.proceeds = cur.proceeds.add(e.usd);
          break;
        case "TRANSFER_OUT":
          qty -= e.tokenRaw;
          cur.unknown = true; // tokens left without proceeds: cannot be scored
          break;
      }
      if (qty > peak) peak = qty;
      if (isDust(qty)) {
        episodes.push({
          wallet: e.wallet,
          mint: e.mint,
          start: cur.start,
          end: e.blockTime,
          costUsd: cur.cost,
          proceedsUsd: cur.proceeds,
          pnlUsd: cur.unknown ? null : cur.proceeds.sub(cur.cost),
          status: cur.unknown ? "UNKNOWN_COST_BASIS" : "CLOSED",
        });
        cur = null;
        qty = 0n;
        peak = 0n;
      }
    }
    if (cur && !isDust(qty)) {
      const first = list[0]!;
      const mark = marksUsdPerRaw.get(first.mint);
      const openValue = mark ? mark.mul(qty.toString()) : new D(0);
      episodes.push({
        wallet: first.wallet,
        mint: first.mint,
        start: cur.start,
        end: null,
        costUsd: cur.cost,
        proceedsUsd: cur.proceeds,
        pnlUsd: cur.unknown ? null : cur.proceeds.add(openValue).sub(cur.cost),
        status: cur.unknown ? "UNKNOWN_COST_BASIS" : mark ? "OPEN_MARKED" : "OPEN_ZERO_LOWER_BOUND",
      });
    }
  }
  return { episodes, swapVolumeUsdKnown, swapEventsTotal, swapEventsUnknownUsd };
}

export interface WalletMetrics {
  closedEpisodes: number;
  scoredEpisodes: number;
  distinctTokens: number;
  activeDays: number;
  coverageBps: number;
  totalPnlUsd: Dec;
  grossProfitUsd: Dec;
  grossLossUsd: Dec;
  profitFactor: Dec | null;
  losingEpisodes: number;
  winRateBps: number | null;
  maxTokenShareOfPositivePnlBps: number | null;
  unknownCostBasisEpisodes: number;
}

export function walletMetrics(r: Reconstruction, events: readonly WalletEvent[], t0: Date): WalletMetrics {
  const scored = r.episodes.filter((e) => e.pnlUsd !== null);
  const closed = r.episodes.filter((e) => e.status === "CLOSED");
  let gp = new D(0);
  let gl = new D(0);
  const posByToken = new Map<string, Dec>();
  let losing = 0;
  let wins = 0;
  for (const e of scored) {
    const p = e.pnlUsd!;
    if (p.gt(0)) {
      gp = gp.add(p);
      wins++;
      posByToken.set(e.mint, (posByToken.get(e.mint) ?? new D(0)).add(p));
    } else if (p.lt(0)) {
      gl = gl.add(p.neg());
      losing++;
    }
  }
  const maxTok = [...posByToken.values()].reduce((a, b) => (b.gt(a) ? b : a), new D(0));
  const days = new Set(events.filter((e) => e.blockTime < t0).map((e) => utcDayKey(e.blockTime)));
  const coverageBps = r.swapEventsTotal === 0 ? 0 : Math.floor(((r.swapEventsTotal - r.swapEventsUnknownUsd) * 10_000) / r.swapEventsTotal);
  return {
    closedEpisodes: closed.length,
    scoredEpisodes: scored.length,
    distinctTokens: new Set(closed.map((e) => e.mint)).size,
    activeDays: days.size,
    coverageBps,
    totalPnlUsd: gp.sub(gl),
    grossProfitUsd: gp,
    grossLossUsd: gl,
    profitFactor: gl.gt(0) ? gp.div(gl) : null,
    losingEpisodes: losing,
    winRateBps: scored.length ? Math.floor((wins * 10_000) / scored.length) : null,
    maxTokenShareOfPositivePnlBps: gp.gt(0) ? maxTok.div(gp).mul(10_000).toDecimalPlaces(0, D.ROUND_UP).toNumber() : null,
    unknownCostBasisEpisodes: r.episodes.filter((e) => e.status === "UNKNOWN_COST_BASIS").length,
  };
}

export interface QualificationResult {
  status: "QUALIFIED" | "REJECTED";
  reasons: Reason[];
  metrics: WalletMetrics;
}

export function qualifyWallet(m: WalletMetrics, flags: { infrastructure: boolean; deployerOfObservedToken: boolean }, cfg: Config): QualificationResult {
  const w = cfg.wallets;
  const reasons: Reason[] = [];
  const req = (ok: boolean, detail: string) => {
    if (!ok) reasons.push(reason(ReasonCode.WALLET_NOT_QUALIFIED, detail));
  };
  req(m.closedEpisodes >= w.min_episodes, `closed episodes ${m.closedEpisodes} < ${w.min_episodes}`);
  req(m.distinctTokens >= w.min_distinct_tokens, `distinct tokens ${m.distinctTokens} < ${w.min_distinct_tokens}`);
  req(m.activeDays >= w.min_active_days, `active days ${m.activeDays} < ${w.min_active_days}`);
  req(m.coverageBps >= w.min_volume_coverage_bps, `coverage ${m.coverageBps} bps < ${w.min_volume_coverage_bps}`);
  req(m.totalPnlUsd.gt(0), `total PnL ${m.totalPnlUsd.toFixed(2)} USD not positive`);
  req(m.losingEpisodes >= w.min_losing_episodes, `losing episodes ${m.losingEpisodes} < ${w.min_losing_episodes} (profit factor not meaningful)`);
  req(m.profitFactor !== null && m.profitFactor.gte(w.min_profit_factor), `profit factor ${m.profitFactor?.toFixed(2) ?? "n/a"} < ${w.min_profit_factor}`);
  req(m.maxTokenShareOfPositivePnlBps !== null && m.maxTokenShareOfPositivePnlBps <= w.max_single_token_positive_pnl_bps, `single token share ${m.maxTokenShareOfPositivePnlBps} bps`);
  req(!flags.infrastructure, "infrastructure role");
  req(!flags.deployerOfObservedToken, "deployer of an observed token");
  return { status: reasons.length === 0 ? "QUALIFIED" : "REJECTED", reasons, metrics: m };
}
