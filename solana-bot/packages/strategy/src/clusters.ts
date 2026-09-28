import { D, type Dec } from "@solbot/domain";
import type { Config } from "@solbot/config";

/**
 * Conservative link heuristics (brief §6.3). Edges are *potential* links with evidence, not proof of
 * common ownership. Infrastructure funders (exchanges, bridges, launchpads, routers, distributors)
 * never link wallets. An unchecked wallet is UNKNOWN and cannot count toward a signal.
 */

export interface Transfer {
  from: string;
  to: string;
  usd: Dec;
  at: Date;
  signature: string;
}

export interface Buy {
  wallet: string;
  mint: string;
  at: Date;
}

export interface Edge {
  a: string;
  b: string;
  kind: "DIRECT_TRANSFERS" | "COMMON_FUNDER";
  evidence: string[];
  confidence: "medium" | "high";
  validFrom: Date;
  validTo: Date;
}

const pair = (x: string, y: string): [string, string] => (x < y ? [x, y] : [y, x]);
const DAY = 86_400_000;

export function deriveEdges(
  watched: ReadonlySet<string>,
  transfers: readonly Transfer[],
  buys: readonly Buy[],
  infra: ReadonlySet<string>,
  asOf: Date,
  cfg: Config,
): Edge[] {
  const w = cfg.wallets;
  const minUsd = new D(w.edge_min_transfer_usd);
  const since = asOf.getTime() - w.qualification_window_days * DAY;
  const validTo = new Date(asOf.getTime() + w.edge_validity_days * DAY);
  const edges: Edge[] = [];
  const inWindow = transfers.filter((t) => t.at.getTime() >= since && t.at < asOf && t.usd.gte(minUsd));

  // (a) >= 2 direct transfers of >= 20 USD between two watched wallets
  const direct = new Map<string, string[]>();
  for (const t of inWindow) {
    if (!watched.has(t.from) || !watched.has(t.to) || t.from === t.to) continue;
    const k = pair(t.from, t.to).join("|");
    direct.set(k, [...(direct.get(k) ?? []), t.signature]);
  }
  for (const [k, sigs] of direct) {
    if (sigs.length >= w.edge_min_direct_transfers) {
      const [a, b] = k.split("|") as [string, string];
      edges.push({ a, b, kind: "DIRECT_TRANSFERS", evidence: sigs, confidence: "high", validFrom: asOf, validTo });
    }
  }

  // (b) common non-infrastructure funder (>= 20 USD each) AND >= 3 shared mints bought within 60 s in the last 7 days
  const fundedBy = new Map<string, Set<string>>();
  for (const t of inWindow) {
    if (infra.has(t.from) || !watched.has(t.to)) continue;
    const s = fundedBy.get(t.from) ?? new Set<string>();
    s.add(t.to);
    fundedBy.set(t.from, s);
  }
  const recent = buys.filter((b) => b.at.getTime() >= asOf.getTime() - w.edge_common_funder_lookback_days * DAY && b.at < asOf);
  const buysBy = new Map<string, Buy[]>();
  for (const b of recent) buysBy.set(b.wallet, [...(buysBy.get(b.wallet) ?? []), b]);
  const seen = new Set<string>();
  for (const [funder, funded] of fundedBy) {
    const list = [...funded];
    for (let i = 0; i < list.length; i++) {
      for (let j = i + 1; j < list.length; j++) {
        const [a, b] = pair(list[i]!, list[j]!);
        const k = `${a}|${b}`;
        if (seen.has(k)) continue;
        const shared = new Set<string>();
        for (const x of buysBy.get(a) ?? []) {
          for (const y of buysBy.get(b) ?? []) {
            if (x.mint === y.mint && Math.abs(x.at.getTime() - y.at.getTime()) <= w.edge_common_funder_max_gap_seconds * 1000) shared.add(x.mint);
          }
        }
        if (shared.size >= w.edge_common_funder_min_shared_mints) {
          seen.add(k);
          edges.push({ a, b, kind: "COMMON_FUNDER", evidence: [`funder:${funder}`, ...[...shared].map((m) => `mint:${m}`)], confidence: "medium", validFrom: asOf, validTo });
        }
      }
    }
  }
  return edges;
}

/** Union-find over valid edges. Returns cluster id per wallet ("unknown" for unchecked wallets). */
export function buildClusters(wallets: readonly string[], linkChecked: ReadonlySet<string>, edges: readonly Edge[], at: Date): Map<string, { clusterId: string; linkCheck: "CHECKED" | "UNKNOWN" }> {
  const parent = new Map<string, string>(wallets.map((w) => [w, w]));
  const find = (x: string): string => {
    let r = x;
    while (parent.get(r)! !== r) r = parent.get(r)!;
    parent.set(x, r);
    return r;
  };
  for (const e of edges) {
    if (e.validFrom > at || e.validTo < at || !parent.has(e.a) || !parent.has(e.b)) continue;
    const ra = find(e.a);
    const rb = find(e.b);
    if (ra !== rb) parent.set(ra < rb ? rb : ra, ra < rb ? ra : rb);
  }
  const out = new Map<string, { clusterId: string; linkCheck: "CHECKED" | "UNKNOWN" }>();
  for (const w of wallets) out.set(w, { clusterId: `c:${find(w)}`, linkCheck: linkChecked.has(w) ? "CHECKED" : "UNKNOWN" });
  return out;
}
