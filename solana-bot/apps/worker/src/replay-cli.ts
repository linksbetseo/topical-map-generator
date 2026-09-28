/**
 * Pilot: historical copy replay of ONE leader wallet with Birdeye data (read-only).
 *   BIRDEYE_API_KEY=... pnpm --filter @solbot/worker replay-wallet <wallet>
 * Env: REPLAY_DAYS (30), REPLAY_MAX_BUYS (250), REPLAY_MIN_BUY_USD (100), REPLAY_CACHE_DIR (.cache/birdeye),
 *      REPLAY_OUT (replay-<wallet>.json).
 *
 * Leakage warning (spec v2 §7): the leader was chosen with knowledge of the same period, so this is a
 * feasibility check of copying (delay, costs, own exits), not evidence of an edge. The holdout is the
 * forward 168 h session. Single-leader copy, not the 3-cluster confluence signal.
 */
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { USDC_MINT, WSOL_MINT, systemClock } from "@solbot/domain";
import { BirdeyeClient, ReadOnlyTransport, pacer, type BirdeyeTrade, type JsonCache } from "@solbot/providers";
import { replayCopy, USDT_MINT, type ReplayParams } from "@solbot/strategy";

const key = process.env.BIRDEYE_API_KEY;
if (!key) throw new Error("BIRDEYE_API_KEY required");
const wallet = process.argv[2];
if (!wallet) throw new Error("usage: replay-wallet <wallet>");
const num = (k: string, d: number) => (process.env[k] ? Number(process.env[k]) : d);
const days = num("REPLAY_DAYS", 30);
const maxBuys = num("REPLAY_MAX_BUYS", 250);
const minBuyUsd = num("REPLAY_MIN_BUY_USD", 100);
const cacheDir = process.env.REPLAY_CACHE_DIR ?? ".cache/birdeye";
mkdirSync(cacheDir, { recursive: true });

const fileCache: JsonCache = {
  async get(k) {
    const f = join(cacheDir, createHash("sha256").update(k).digest("hex") + ".json");
    return existsSync(f) ? JSON.parse(readFileSync(f, "utf8")).data : undefined;
  },
  async put(k, v) {
    writeFileSync(join(cacheDir, createHash("sha256").update(k).digest("hex") + ".json"), JSON.stringify({ key: k, fetchedAt: new Date().toISOString(), data: v }));
  },
};
const be = new BirdeyeClient(new ReadOnlyTransport(fetch as never, systemClock, 40_000), key, fileCache, pacer(1_100, (ms) => new Promise((r) => setTimeout(r, ms)), () => Date.now()));
const log = (m: string) => console.log(`${new Date().toISOString()} ${m}`);

const BASES = new Set([WSOL_MINT, USDC_MINT, USDT_MINT]);
const now = Math.floor(Date.now() / 1000);
const tsNow = now - 4 * 3600 - 300; // buys must have a complete 4 h exit window
const sw = await be.traderSwaps(wallet, { afterTime: now - days * 86_400, beforeTime: tsNow, maxItems: 10_000 });
if (!sw.ok) throw new Error(`trader swaps: ${sw.code} ${sw.detail}`);
log(`swaps: ${sw.value.trades.length} (complete=${sw.value.complete})`);

// leader buys of risk tokens paid with a base asset
interface Buy { t: number; mint: string; symbol: string; priceUsd: number | null; usd: number; sig: string; leaderSell?: number }
const buys: Buy[] = [];
const sells: Array<{ t: number; mint: string }> = [];
for (const tr of sw.value.trades as BirdeyeTrade[]) {
  const legs = [tr.base, tr.quote];
  const got = legs.find((l) => l.type_swap === "to");
  const gave = legs.find((l) => l.type_swap === "from");
  if (got && gave && BASES.has(got.address) && !BASES.has(gave.address)) sells.push({ t: tr.block_unix_time, mint: gave.address });
  if (!got || !gave || BASES.has(got.address) || !BASES.has(gave.address)) continue;
  buys.push({ t: tr.block_unix_time, mint: got.address, symbol: got.symbol ?? "?", priceUsd: got.price ?? got.nearest_price ?? null, usd: tr.volume_usd ?? 0, sig: tr.tx_hash });
}
buys.sort((a, b) => a.t - b.t);
// signal-like filters: ≥ min USD, one entry per mint per 24 h (first buy), newest maxBuys
const lastByMint = new Map<string, number>();
const eligible = buys.filter((b) => {
  if (b.usd < minBuyUsd) return false;
  const prev = lastByMint.get(b.mint);
  if (prev !== undefined && b.t - prev < 86_400) return false;
  lastByMint.set(b.mint, b.t);
  return true;
});
const selected = eligible.slice(-maxBuys);
for (const b of selected) b.leaderSell = sells.filter((x) => x.mint === b.mint && x.t > b.t).sort((a, c) => a.t - c.t)[0]?.t;
log(`buys: ${buys.length}, eligible (≥${minBuyUsd} USD, 1/mint/24h): ${eligible.length}, replayed: ${selected.length}`);

const base: Omit<ReplayParams, "delaySec" | "slippageBps"> = {
  sizeUsd: 25,
  feePerSideUsd: 0.03, // base fee + priority/tip assumption (scenario)
  stopLossBps: 1000,
  takeProfitBps: 2500,
  trailActivationBps: 1500,
  trailDrawdownBps: 800,
  timeStopSec: 4 * 3600,
};
const DELAYS = [0, 5, 15, 30, 60];
const SLIPS = [50, 150];
const rows: Array<Record<string, unknown>> = [];
let i = 0;
for (const b of selected) {
  i++;
  const s1 = await be.candles(b.mint, "1s", b.t - 30, b.t + 75);
  const m1 = await be.candles(b.mint, "1m", b.t, b.t + 4 * 3600 + 120);
  if (!s1.ok || !m1.ok) {
    rows.push({ ...b, error: `${s1.ok ? "" : s1.detail} ${m1.ok ? "" : m1.detail}`.trim() });
    continue;
  }
  const res: Record<string, unknown> = {};
  for (const d of DELAYS)
    for (const sl of SLIPS) {
      res[`d${d}_s${sl}`] = replayCopy(b.t, s1.value, m1.value, { ...base, delaySec: d, slippageBps: sl });
      // variant: follow the leader's exit (own stops off, 4 h cap)
      res[`d${d}_s${sl}_leaderexit`] = replayCopy(b.t, s1.value, m1.value, { ...base, delaySec: d, slippageBps: sl, useOwnExits: false, ...(b.leaderSell !== undefined ? { leaderExitTime: b.leaderSell } : {}) });
    }
  rows.push({ ...b, candles1s: s1.value.length, candles1m: m1.value.length, res });
  if (i % 25 === 0) log(`${i}/${selected.length} replayed, birdeye calls ${be.calls}, cache hits ${be.cacheHits}`);
}

type Filled = { status: "FILLED"; pnlUsd: number; exitReason: string; grossReturn: number; pnlAtLastPriceUsd?: number };
const summary: Record<string, unknown> = {};
for (const d of DELAYS)
  for (const sl of SLIPS)
  for (const v of ["", "_leaderexit"]) {
    const k = `d${d}_s${sl}${v}`;
    const rs = rows.map((r) => (r.res as Record<string, { status: string }> | undefined)?.[k]).filter(Boolean) as Array<{ status: string }>;
    const f = rs.filter((r) => r.status === "FILLED") as Filled[];
    const pnl = f.map((r) => r.pnlUsd).sort((a, b) => a - b);
    const tot = pnl.reduce((a, b) => a + b, 0);
    const gp = pnl.filter((x) => x > 0).reduce((a, b) => a + b, 0);
    const gl = -pnl.filter((x) => x < 0).reduce((a, b) => a + b, 0);
    const reasons: Record<string, number> = {};
    for (const r of f) reasons[r.exitReason] = (reasons[r.exitReason] ?? 0) + 1;
    summary[k] = {
      trades: f.length,
      noEntryPrice: rs.length - f.length,
      winRate: f.length ? +(f.filter((r) => r.pnlUsd > 0).length / f.length).toFixed(3) : null,
      totalPnlUsd: +tot.toFixed(2),
      expectancyUsd: f.length ? +(tot / f.length).toFixed(3) : null,
      medianPnlUsd: pnl.length ? +pnl[Math.floor((pnl.length - 1) / 2)]!.toFixed(3) : null,
      profitFactor: gl > 0 ? +(gp / gl).toFixed(3) : null,
      totalWithoutBestUsd: pnl.length ? +(tot - pnl[pnl.length - 1]!).toFixed(2) : null,
      unpricedAtLastPriceUsd: +f.filter((r) => r.exitReason === "EXIT_UNPRICED").reduce((a, r) => a + (r.pnlAtLastPriceUsd ?? 0) - r.pnlUsd, 0).toFixed(2),
      exitReasons: reasons,
      entryFrom1m: f.filter((r) => (r as unknown as { entryGranularity: string }).entryGranularity === "1m").length,
    };
  }
const out = process.env.REPLAY_OUT ?? `replay-${wallet}.json`;
writeFileSync(out, JSON.stringify({ wallet, generatedAt: new Date().toISOString(), days, minBuyUsd, params: base, swaps: sw.value.trades.length, swapsComplete: sw.value.complete, buys: buys.length, eligible: eligible.length, replayed: selected.length, birdeyeCalls: be.calls, cacheHits: be.cacheHits, summary, rows }, null, 1));
console.log(JSON.stringify({ wallet, buys: buys.length, eligible: eligible.length, replayed: selected.length, birdeyeCalls: be.calls, summary }, null, 1));
