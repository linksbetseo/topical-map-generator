/**
 * Historical confluence signals (spec v2 §9) from candidate wallets' trade histories.
 * A signal for a mint fires at the first moment when ≥ minWallets distinct wallets have each bought
 * ≥ minBuyUsd in total within the trailing window, and each of them still retains ≥ minRetainedBps of
 * what it bought in that window (sells before the decision reduce retention; later sells are ignored —
 * no look-ahead). One signal per mint per cooldown. Clusters are NOT resolved here: every wallet counts
 * as its own cluster (stated in the report).
 */
export interface HistTrade {
  wallet: string;
  mint: string;
  side: "BUY" | "SELL";
  t: number;
  usd: number;
  /** token quantity (UI units are fine: only ratios are used) */
  qty: number;
}

export interface HistSignal {
  mint: string;
  t: number;
  wallets: string[];
  buyUsd: number[];
}

export function detectSignals(trades: readonly HistTrade[], p: { minWallets: number; windowSec: number; minBuyUsd: number; minRetainedBps: number; cooldownSec: number }): HistSignal[] {
  const byMint = new Map<string, HistTrade[]>();
  for (const t of trades) {
    const l = byMint.get(t.mint) ?? [];
    l.push(t);
    byMint.set(t.mint, l);
  }
  const out: HistSignal[] = [];
  for (const [mint, list] of byMint) {
    list.sort((a, b) => a.t - b.t || (a.side === "SELL" ? -1 : 1));
    let lastSignal = -Infinity;
    for (const ev of list) {
      if (ev.side !== "BUY" || ev.t - lastSignal < p.cooldownSec) continue;
      const now = ev.t;
      const from = now - p.windowSec;
      const per = new Map<string, { usd: number; bought: number; sold: number }>();
      for (const x of list) {
        if (x.t > now) break;
        if (x.t < from) continue;
        const w = per.get(x.wallet) ?? { usd: 0, bought: 0, sold: 0 };
        if (x.side === "BUY") {
          w.usd += x.usd;
          w.bought += x.qty;
        } else if (w.bought > 0) w.sold += x.qty; // only sells after a counted buy
        per.set(x.wallet, w);
      }
      const ok = [...per.entries()].filter(([, w]) => w.usd >= p.minBuyUsd && w.bought > 0 && ((w.bought - Math.min(w.sold, w.bought)) / w.bought) * 10_000 >= p.minRetainedBps);
      if (ok.length >= p.minWallets) {
        out.push({ mint, t: now, wallets: ok.map(([w]) => w), buyUsd: ok.map(([, w]) => +w.usd.toFixed(2)) });
        lastSignal = now;
      }
    }
  }
  return out.sort((a, b) => a.t - b.t);
}
