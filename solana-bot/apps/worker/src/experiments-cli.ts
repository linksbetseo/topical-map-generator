/**
 * Offline experiments on data already cached from Birdeye (0 CU: no network, cache only).
 *   REPLAY_CACHE_DIR=... EXP_PILOT=replay-<wallet>.json EXP_CONFLUENCE=confluence-replay.json pnpm --filter @solbot/worker experiments
 *
 * Pre-registered pass criterion (fixed before looking at results): a variant PASSES only if, at the
 * pessimistic slippage (150 bps per side) and 15 s delay, total PnL is > 0 in BOTH halves of the
 * period AND > 0 after removing the single best trade. Everything else is reported as FAIL.
 * All experiments reuse the same trades (multiple testing: a pass here is a hypothesis for a forward
 * test, not evidence).
 */
import { createHash } from "node:crypto";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { USDC_MINT, WSOL_MINT, systemClock } from "@solbot/domain";
import { BirdeyeClient, ReadOnlyTransport, pacer, type BirdeyeTrade, type Candle } from "@solbot/providers";
import { detectSignals, replayCopy, USDT_MINT, type HistTrade, type ReplayParams, type ReplayResult } from "@solbot/strategy";

const cacheDir = process.env.REPLAY_CACHE_DIR ?? ".cache/birdeye";
const cache = new Map<string, unknown>();
for (const f of readdirSync(cacheDir)) {
  const j = JSON.parse(readFileSync(join(cacheDir, f), "utf8")) as { key: string; data: unknown };
  cache.set(j.key, j.data);
}
const candles = (mint: string, type: "1s" | "1m", from: number, to: number): Candle[] | null => {
  const d = cache.get(`/defi/v3/ohlcv?address=${mint}&type=${type}&currency=usd&time_from=${from}&time_to=${to}`) as { items?: Candle[] } | undefined;
  return d ? (d.items ?? []) : null;
};
// optional fetching of missing candles (EXP_FETCH_CU_BUDGET > 0), written to the same cache
const fetchBudget = Number(process.env.EXP_FETCH_CU_BUDGET ?? 0);
const be =
  fetchBudget > 0 && process.env.BIRDEYE_API_KEY
    ? new BirdeyeClient(
        new ReadOnlyTransport(fetch as never, systemClock, 40_000),
        process.env.BIRDEYE_API_KEY,
        {
          async get(k) {
            return cache.get(k);
          },
          async put(k, v) {
            cache.set(k, v);
            writeFileSync(join(cacheDir, createHash("sha256").update(k).digest("hex") + ".json"), JSON.stringify({ key: k, fetchedAt: new Date().toISOString(), data: v }));
          },
        },
        pacer(1_100, (ms) => new Promise((r) => setTimeout(r, ms)), () => Date.now()),
      )
    : null;
if (be) be.cuBudget = fetchBudget;
async function ensureCandles(mint: string, t: number): Promise<void> {
  if (!be) return;
  if (!candles(mint, "1s", t - 30, t + 75)) await be.candles(mint, "1s", t - 30, t + 75);
  if (!candles(mint, "1m", t, t + 4 * 3600 + 120)) await be.candles(mint, "1m", t, t + 4 * 3600 + 120);
}

// ------------------------------------------------------------------ event sets
interface Ev { set: string; t: number; mint: string; leaderExit?: number; firstBuy?: boolean }
const pilot = JSON.parse(readFileSync(process.env.EXP_PILOT!, "utf8")) as { wallet: string; rows: Array<{ t: number; mint: string; leaderSell?: number }> };
const conf = JSON.parse(readFileSync(process.env.EXP_CONFLUENCE!, "utf8")) as { rows2: Array<{ t: number; mint: string; wallets: string[] }> };

// trader histories reconstructed from cached pages (all runs), deduplicated
const BASES = new Set([WSOL_MINT, USDC_MINT, USDT_MINT]);
const seen = new Set<string>();
const byWallet = new Map<string, BirdeyeTrade[]>();
for (const [k, v] of cache) {
  if (!k.startsWith("/trader/txs/seek_by_time")) continue;
  const addr = new URLSearchParams(k.split("?")[1]).get("address")!;
  for (const tr of (v as { items?: BirdeyeTrade[] }).items ?? []) {
    const id = `${tr.tx_hash}:${(tr as unknown as { ins_index?: number }).ins_index ?? ""}:${(tr as unknown as { inner_ins_index?: number }).inner_ins_index ?? ""}:${addr}`;
    if (seen.has(id)) continue;
    seen.add(id);
    const l = byWallet.get(addr) ?? [];
    l.push(tr);
    byWallet.set(addr, l);
  }
}
const hist: HistTrade[] = [];
for (const [wallet, trs] of byWallet)
  for (const tr of trs) {
    const legs = [tr.base, tr.quote];
    const got = legs.find((l) => l.type_swap === "to");
    const gave = legs.find((l) => l.type_swap === "from");
    if (!got || !gave) continue;
    if (!BASES.has(got.address) && BASES.has(gave.address)) hist.push({ wallet, mint: got.address, side: "BUY", t: tr.block_unix_time, usd: tr.volume_usd ?? 0, qty: Math.abs(got.ui_amount ?? 0) });
    else if (BASES.has(got.address) && !BASES.has(gave.address)) hist.push({ wallet, mint: gave.address, side: "SELL", t: tr.block_unix_time, usd: tr.volume_usd ?? 0, qty: Math.abs(gave.ui_amount ?? 0) });
  }
const leaderBuys = hist.filter((h) => h.wallet === pilot.wallet && h.side === "BUY");
const events: Ev[] = [
  ...pilot.rows.map((r) => ({ set: "single:AAN5n1", t: r.t, mint: r.mint, ...(r.leaderSell !== undefined ? { leaderExit: r.leaderSell } : {}), firstBuy: !leaderBuys.some((b) => b.mint === r.mint && b.t < r.t) })),
  ...conf.rows2.map((r) => {
    const exit = hist.filter((h) => h.side === "SELL" && h.mint === r.mint && h.t > r.t && r.wallets.includes(h.wallet)).sort((a, b) => a.t - b.t)[0]?.t;
    return { set: "confluence:2w/180s", t: r.t, mint: r.mint, ...(exit !== undefined ? { leaderExit: exit } : {}) };
  }),
];

// ------------------------------------------------------------------ helpers
const BASEP: Omit<ReplayParams, "delaySec" | "slippageBps"> = { sizeUsd: 25, feePerSideUsd: 0.03, stopLossBps: 1000, takeProfitBps: 2500, trailActivationBps: 1500, trailDrawdownBps: 800, timeStopSec: 4 * 3600 };
const OFF = 1_000_000; // bps large enough to never trigger
type Filled = Extract<ReplayResult, { status: "FILLED" }>;
function run(evs: Ev[], p: Partial<ReplayParams> & { leader?: boolean }, delay = 15, slip = 150) {
  const out: Array<{ t: number; pnl: number }> = [];
  for (const e of evs) {
    const s1 = candles(e.mint, "1s", e.t - 30, e.t + 75);
    const m1 = candles(e.mint, "1m", e.t, e.t + 4 * 3600 + 120);
    if (!s1 || !m1) continue; // not cached → skipped (reported via n)
    const r = replayCopy(e.t, s1, m1, { ...BASEP, ...p, delaySec: delay, slippageBps: slip, ...(p.leader ? { useOwnExits: false, ...(e.leaderExit !== undefined ? { leaderExitTime: e.leaderExit } : {}) } : {}) });
    if (r.status === "FILLED") out.push({ t: e.t, pnl: (r as Filled).pnlUsd });
  }
  return out;
}
function stats(rs: Array<{ t: number; pnl: number }>) {
  const s = [...rs].sort((a, b) => a.t - b.t);
  const half = Math.floor(s.length / 2);
  const sum = (x: Array<{ pnl: number }>) => x.reduce((a, b) => a + b.pnl, 0);
  const tot = sum(s);
  const best = s.length ? Math.max(...s.map((x) => x.pnl)) : 0;
  const h1 = sum(s.slice(0, half));
  const h2 = sum(s.slice(half));
  return { n: s.length, total: +tot.toFixed(2), perTrade: s.length ? +(tot / s.length).toFixed(2) : 0, win: s.length ? +(s.filter((x) => x.pnl > 0).length / s.length).toFixed(2) : 0, half1: +h1.toFixed(2), half2: +h2.toFixed(2), withoutBest: +(tot - best).toFixed(2), pass: s.length >= 10 && h1 > 0 && h2 > 0 && tot - best > 0 };
}

const report: Record<string, unknown> = {};
const sets = [...new Set(events.map((e) => e.set))];

// 1) exit grid (15 s delay, 150 bps pessimistic + 50 bps shown)
const grid: Array<{ name: string; p: Partial<ReplayParams> & { leader?: boolean } }> = [];
for (const sl of [1000, 2000, 3000, OFF])
  for (const tp of [2500, OFF])
    for (const ts of [1800, 3600, 14400])
      grid.push({ name: `SL ${sl === OFF ? "off" : `-${sl / 100}%`} / TP ${tp === OFF ? "off" : `+${tp / 100}%`} / ${ts / 60} min`, p: { stopLossBps: sl, takeProfitBps: tp, trailActivationBps: tp === OFF ? OFF : 1500, timeStopSec: ts } });
for (const ts of [1800, 3600, 14400]) grid.push({ name: `za liderem / max ${ts / 60} min`, p: { leader: true, timeStopSec: ts } });
const g: Record<string, unknown> = {};
for (const set of sets) {
  const evs = events.filter((e) => e.set === set);
  g[set] = grid.map((v) => ({ variant: v.name, pess150: stats(run(evs, v.p)), opt50: stats(run(evs, v.p, 15, 50)) }));
}
report.exitGrid = g;

// 2) looser confluence (counts only; replay would need new candles)
const base = { minBuyUsd: 100, minRetainedBps: 8_000, cooldownSec: 86_400 };
report.looserConfluence = Object.fromEntries(
  [180, 900, 3600, 6 * 3600].flatMap((w) => [2, 3, 4].map((n) => [`${n} portfele / ${w >= 3600 ? `${w / 3600} h` : `${w / 60} min`}`, detectSignals(hist, { ...base, windowSec: w, minWallets: n }).length])),
);
report.historyCoverage = { wallets: byWallet.size, riskTrades: hist.length };

// 3) no-chase filter: skip entries where price at entry is > 5% above 30 s earlier (1 s data only)
function preMove(e: Ev, delay = 15): number | null {
  const s1 = candles(e.mint, "1s", e.t - 30, e.t + 75);
  if (!s1 || s1.length === 0) return null;
  const at = e.t + delay;
  const before = s1.filter((c) => c.unix_time <= at - 30).sort((a, b) => b.unix_time - a.unix_time)[0] ?? s1.filter((c) => c.unix_time <= e.t).sort((a, b) => b.unix_time - a.unix_time)[0];
  const now = s1.filter((c) => c.unix_time <= at).sort((a, b) => b.unix_time - a.unix_time)[0];
  return before && now ? now.c / before.c - 1 : null;
}
const nc: Record<string, unknown> = {};
for (const set of sets) {
  const evs = events.filter((e) => e.set === set);
  const withPre = evs.map((e) => ({ e, m: preMove(e) })).filter((x) => x.m !== null);
  const calm = withPre.filter((x) => x.m! <= 0.05).map((x) => x.e);
  const chased = withPre.filter((x) => x.m! > 0.05).map((x) => x.e);
  nc[set] = {
    with1sData: withPre.length,
    calm_own: stats(run(calm, {})),
    calm_leader: stats(run(calm, { leader: true })),
    chased_own: stats(run(chased, {})),
  };
}
report.noChase = nc;

// 4) first buy of a token vs adding to a position (single leader)
const pl = events.filter((e) => e.set === "single:AAN5n1");
report.firstVsAdd = {
  first_own: stats(run(pl.filter((e) => e.firstBuy), {})),
  first_leader: stats(run(pl.filter((e) => e.firstBuy), { leader: true })),
  add_own: stats(run(pl.filter((e) => !e.firstBuy), {})),
  add_leader: stats(run(pl.filter((e) => !e.firstBuy), { leader: true })),
};

// 2b) replay of looser 3-wallet windows (fetches candles within EXP_FETCH_CU_BUDGET)
const exitFor = (sg: { t: number; mint: string; wallets: string[] }) => hist.filter((h) => h.side === "SELL" && h.mint === sg.mint && h.t > sg.t && sg.wallets.includes(h.wallet)).sort((a, b) => a.t - b.t)[0]?.t;
const loose: Record<string, unknown> = {};
for (const w of [3600, 6 * 3600]) {
  const sigs = detectSignals(hist, { ...base, windowSec: w, minWallets: 3 });
  for (const sg of sigs) await ensureCandles(sg.mint, sg.t);
  const evs: Ev[] = sigs.map((sg) => { const x = exitFor(sg); return { set: `3w/${w / 3600}h`, t: sg.t, mint: sg.mint, ...(x !== undefined ? { leaderExit: x } : {}) }; });
  loose[`3 portfele / ${w / 3600} h`] = {
    signals: sigs.length,
    own_spec: stats(run(evs, {})),
    own_spec_opt50: stats(run(evs, {}, 15, 50)),
    leader: stats(run(evs, { leader: true })),
    sl30_60min: stats(run(evs, { stopLossBps: 3000, takeProfitBps: OFF, trailActivationBps: OFF, timeStopSec: 3600 })),
  };
}
report.looseReplay = loose;
// 5) walk-forward: choose leaders ONLY from week-1 behaviour, copy their week-2 buys
{
  const from = Date.parse(process.env.EXP_WINDOW_FROM ?? "2026-09-14T16:12:13Z") / 1000;
  const to = Date.parse(process.env.EXP_WINDOW_TO ?? "2026-09-28T12:07:13Z") / 1000;
  const mid = from + 7 * 86_400;
  const w1 = hist.filter((h) => h.t >= from && h.t < mid);
  // week-1 score: cash flow of tokens bought AND ≥90% sold within week 1 (closed round trips)
  const score = new Map<string, { realized: number; closed: number; wins: number }>();
  const key = (h: HistTrade) => `${h.wallet}|${h.mint}`;
  const pos = new Map<string, { buyUsd: number; sellUsd: number; buyQty: number; sellQty: number }>();
  for (const h of w1) {
    const p = pos.get(key(h)) ?? { buyUsd: 0, sellUsd: 0, buyQty: 0, sellQty: 0 };
    if (h.side === "BUY") {
      p.buyUsd += h.usd;
      p.buyQty += h.qty;
    } else if (p.buyQty > 0) {
      p.sellUsd += h.usd;
      p.sellQty += h.qty;
    }
    pos.set(key(h), p);
  }
  for (const [k, p] of pos) {
    if (p.buyQty === 0 || p.sellQty < 0.9 * p.buyQty) continue;
    const w = k.split("|")[0]!;
    const sc = score.get(w) ?? { realized: 0, closed: 0, wins: 0 };
    sc.realized += p.sellUsd - p.buyUsd;
    sc.closed++;
    if (p.sellUsd > p.buyUsd) sc.wins++;
    score.set(w, sc);
  }
  const leaders = [...score.entries()].filter(([, v]) => v.closed >= 10 && v.realized > 0).sort((a, b) => b[1].realized - a[1].realized);
  const maxEntries = Number(process.env.EXP_WF_MAX_ENTRIES ?? 200);
  const chosen: string[] = [];
  const entries: Ev[] = [];
  const lastMint = new Map<string, number>();
  for (const [w] of leaders) {
    if (chosen.length >= 10) break;
    const buys = hist.filter((h) => h.wallet === w && h.side === "BUY" && h.t >= mid && h.t <= to && h.usd >= 100).sort((a, b) => a.t - b.t);
    if (entries.length + buys.length > maxEntries && chosen.length > 0) continue;
    chosen.push(w);
    for (const b of buys) {
      const prev = lastMint.get(b.mint);
      if (prev !== undefined && b.t - prev < 86_400) continue;
      lastMint.set(b.mint, b.t);
      const x = hist.filter((h) => h.wallet === w && h.side === "SELL" && h.mint === b.mint && h.t > b.t).sort((a, c) => a.t - c.t)[0]?.t;
      entries.push({ set: "walk-forward", t: b.t, mint: b.mint, ...(x !== undefined ? { leaderExit: x } : {}) });
    }
  }
  entries.sort((a, b) => a.t - b.t);
  let fetched = 0;
  for (const e of entries) {
    try {
      await ensureCandles(e.mint, e.t);
      fetched++;
    } catch {
      break; // budget reached: remaining entries are skipped (n shows what was replayed)
    }
  }
  report.walkForward = {
    week1: { from: new Date(from * 1000).toISOString(), to: new Date(mid * 1000).toISOString() },
    week2: { from: new Date(mid * 1000).toISOString(), to: new Date(to * 1000).toISOString() },
    eligibleLeaders: leaders.length,
    chosen: chosen.map((w) => ({ wallet: w, ...score.get(w) })),
    entries: entries.length,
    candlesFetchedFor: fetched,
    own_spec: stats(run(entries, {})),
    own_spec_opt50: stats(run(entries, {}, 15, 50)),
    leader: stats(run(entries, { leader: true })),
    leader_opt50: stats(run(entries, { leader: true }, 15, 50)),
    sl30_60min: stats(run(entries, { stopLossBps: 3000, takeProfitBps: OFF, trailActivationBps: OFF, timeStopSec: 3600 })),
    noStop_60min_opt50: stats(run(entries, { stopLossBps: OFF, takeProfitBps: OFF, trailActivationBps: OFF, timeStopSec: 3600 }, 15, 50)),
  };
}
report.cuSpent = be ? be.cu : 0;

writeFileSync(process.env.EXP_OUT ?? "experiments.json", JSON.stringify(report, null, 1));
console.log(JSON.stringify(report, null, 1));
