import { D, ReasonCode, reason, type Dec, type Reason } from "@solbot/domain";
import type { Config } from "@solbot/config";

/**
 * confluence_v1 (brief §7.1): an explicit hypothesis about convergent buys, not a probability of profit.
 * Pure function of data available at `now` (available_at <= now). Late events never repair a past decision.
 */

export interface FlowEvent {
  wallet: string;
  mint: string;
  side: "BUY" | "SELL";
  tokenRaw: bigint;
  usd: Dec | null;
  blockTime: Date;
  availableAt: Date;
  confirmed: boolean;
  signature: string;
}

export interface WalletStatus {
  qualified: boolean;
  clusterId: string;
  linkCheck: "CHECKED" | "UNKNOWN";
}

export interface ConfluenceSignal {
  mint: string;
  episodeKey: string;
  detectedAt: Date;
  ttlUntil: Date;
  wallets: Array<{ wallet: string; clusterId: string; buyUsd: string; netBoughtRaw: string; retainedBps: number; firstBuyAt: string; signatures: string[] }>;
  clusters: string[];
  /** Human text uses "clusters without a detected link in the examined data", never "independent". */
  summary: string;
}

export interface ConfluenceEvaluation {
  signal: ConfluenceSignal | null;
  reasons: Reason[];
  excluded: Array<{ wallet: string; reason: string }>;
}

export function evaluateConfluence(mint: string, now: Date, events: readonly FlowEvent[], wallets: ReadonlyMap<string, WalletStatus>, cfg: Config): ConfluenceEvaluation {
  const s = cfg.signal;
  const windowStart = now.getTime() - s.window_seconds * 1000;
  const excluded: ConfluenceEvaluation["excluded"] = [];
  const visible = events.filter((e) => e.mint === mint && e.availableAt <= now && e.confirmed);

  const perWallet = new Map<string, { bought: bigint; sold: bigint; usd: Dec; first: Date; sigs: string[]; ok: boolean }>();
  for (const e of visible) {
    if (e.side !== "BUY" || e.blockTime.getTime() < windowStart || e.blockTime > now) continue;
    const lagMs = e.availableAt.getTime() - e.blockTime.getTime();
    const st = wallets.get(e.wallet);
    let why: string | null = null;
    if (!st || !st.qualified) why = "wallet not qualified";
    else if (st.linkCheck !== "CHECKED") why = "link check UNKNOWN";
    else if (lagMs > s.max_delivery_lag_seconds * 1000) why = `delivered ${lagMs} ms after block time`;
    else if (e.usd === null) why = "buy USD value unknown";
    else if (e.usd.lt(s.min_buy_usd)) why = `buy ${e.usd.toFixed(2)} USD < ${s.min_buy_usd}`;
    if (why) {
      excluded.push({ wallet: e.wallet, reason: why });
      continue;
    }
    const w = perWallet.get(e.wallet) ?? { bought: 0n, sold: 0n, usd: new D(0), first: e.blockTime, sigs: [], ok: true };
    w.bought += e.tokenRaw;
    w.usd = w.usd.add(e.usd!);
    if (e.blockTime < w.first) w.first = e.blockTime;
    w.sigs.push(e.signature);
    perWallet.set(e.wallet, w);
  }
  // Retention: sells after the first counted buy (visible now) reduce net retained quantity.
  for (const e of visible) {
    if (e.side !== "SELL") continue;
    const w = perWallet.get(e.wallet);
    if (w && e.blockTime >= w.first && e.blockTime <= now) w.sold += e.tokenRaw;
  }
  const qualifying: ConfluenceSignal["wallets"] = [];
  for (const [wallet, w] of perWallet) {
    const net = w.bought - w.sold;
    const retainedBps = w.bought > 0n ? Number(((net < 0n ? 0n : net) * 10_000n) / w.bought) : 0;
    if (retainedBps < s.min_retained_bps) {
      excluded.push({ wallet, reason: `retained ${retainedBps} bps < ${s.min_retained_bps}` });
      continue;
    }
    qualifying.push({
      wallet,
      clusterId: wallets.get(wallet)!.clusterId,
      buyUsd: w.usd.toFixed(2),
      netBoughtRaw: net.toString(),
      retainedBps,
      firstBuyAt: w.first.toISOString(),
      signatures: w.sigs.sort(),
    });
  }
  qualifying.sort((a, b) => a.firstBuyAt.localeCompare(b.firstBuyAt) || a.wallet.localeCompare(b.wallet));
  const clusters = [...new Set(qualifying.map((q) => q.clusterId))];
  if (qualifying.length < s.min_wallets || clusters.length < s.min_clusters) {
    return {
      signal: null,
      reasons: [reason(ReasonCode.CONFLUENCE_NOT_MET, `${qualifying.length} wallets / ${clusters.length} clusters`, { wallets: qualifying.length, clusters: clusters.length, required_wallets: s.min_wallets, required_clusters: s.min_clusters })],
      excluded,
    };
  }
  // one wallet per cluster, earliest first, defines the episode
  const byCluster = new Map<string, (typeof qualifying)[number]>();
  for (const q of qualifying) if (!byCluster.has(q.clusterId)) byCluster.set(q.clusterId, q);
  const anchor = [...byCluster.values()].slice(0, s.min_clusters);
  const episodeKey = `${mint}:${anchor.map((a) => a.signatures[0]).join(",")}`;
  return {
    signal: {
      mint,
      episodeKey,
      detectedAt: now,
      ttlUntil: new Date(now.getTime() + s.ttl_seconds * 1000),
      wallets: qualifying,
      clusters,
      summary: `${qualifying.length} kwalifikowane portfele z ${clusters.length} klastrów bez wykrytego powiązania w badanych danych`,
    },
    reasons: [],
    excluded,
  };
}
