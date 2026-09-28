/**
 * Historical test of the confluence signal (≥3 distinct candidate wallets buying one token within
 * 180 s) with Birdeye data, read-only.
 *   BIRDEYE_API_KEY=... pnpm --filter @solbot/worker replay-confluence
 * Env: CR_DAYS (14 — 1 s candles exist ~15 days back), CR_MAX_WALLETS (60), CR_MAX_PAGES_PER_WALLET (15),
 *      CR_CU_BUDGET (20000), CR_EXTRA_WALLETS (comma list), REPLAY_CACHE_DIR, CR_OUT.
 *
 * Limits stated in the output: candidates are chosen from rankings of a window that overlaps the
 * test window (look-ahead in selection, favours the result); wallets are not clustered (each wallet
 * counts as its own cluster); token filters (pool age, liquidity, security) are not applied; candles
 * replace Q0/Q1 quotes.
 */
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { USDC_MINT, WSOL_MINT, systemClock } from "@solbot/domain";
import { BirdeyeClient, BudgetExceededError, ReadOnlyTransport, pacer, type BirdeyeTrade, type JsonCache } from "@solbot/providers";
import { detectSignals, replayCopy, USDT_MINT, type HistSignal, type HistTrade, type ReplayParams, type ReplayResult } from "@solbot/strategy";

const key = process.env.BIRDEYE_API_KEY;
if (!key) throw new Error("BIRDEYE_API_KEY required");
const num = (k: string, d: number) => (process.env[k] ? Number(process.env[k]) : d);
const days = num("CR_DAYS", 14);
const maxWallets = num("CR_MAX_WALLETS", 60);
const maxPages = num("CR_MAX_PAGES_PER_WALLET", 15);
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
be.cuBudget = num("CR_CU_BUDGET", 20_000);
const log = (m: string) => console.log(`${new Date().toISOString()} ${m} [cu≈${be.cu}]`);
const BASES = new Set([WSOL_MINT, USDC_MINT, USDT_MINT]);
const now = Math.floor(Date.now() / 1000);
const windowEnd = now - 4 * 3600 - 300;
const windowStart = now - days * 86_400;

// 1) candidates: 30 d ranking by realized PnL, min 100 trades; keep only realized > 0 AND total > 0
//    (drops "realize small wins, hold big losers"); skip very high-frequency wallets (history cost).
const pool = new Map<string, { source: string; realized: number; total: number; trades: number }>();
for (const offset of [0, 100, 200]) {
  const r = await be.gainers({ type: "30d", sortBy: "realized_pnl", offset, limit: 100, minTrade: 100 });
  if (!r.ok) throw new Error(`gainers: ${r.code} ${r.detail}`);
  for (const g of r.value) {
    if (g.realized_pnl > 0 && g.realized_pnl + g.unrealized_pnl > 0 && g.trade_count <= 3_000 && !pool.has(g.address))
      pool.set(g.address, { source: "birdeye_30d_realized", realized: g.realized_pnl, total: g.realized_pnl + g.unrealized_pnl, trades: g.trade_count });
  }
}
for (const w of (process.env.CR_EXTRA_WALLETS ?? "").split(",").filter(Boolean)) if (!pool.has(w)) pool.set(w, { source: "p0_diagnostics", realized: 0, total: 0, trades: 0 });
const ranked = [...pool.entries()].sort((a, b) => (a[1].source === "p0_diagnostics" ? -1 : 0) - (b[1].source === "p0_diagnostics" ? -1 : 0) || b[1].total - a[1].total);
const candidates = ranked.slice(0, maxWallets);
log(`candidate pool ${pool.size}, using ${candidates.length}`);

// 2) histories in the test window
const trades: HistTrade[] = [];
const histories: Array<{ wallet: string; source: string; swaps: number; complete: boolean; error?: string }> = [];
let stoppedByBudget = false;
for (const [wallet, info] of candidates) {
  let r;
  try {
    r = await be.traderSwaps(wallet, { afterTime: windowStart, beforeTime: windowEnd, maxItems: maxPages * 100 });
  } catch (e) {
    if (e instanceof BudgetExceededError) {
      stoppedByBudget = true;
      break;
    }
    throw e;
  }
  if (!r.ok) {
    histories.push({ wallet, source: info.source, swaps: 0, complete: false, error: `${r.code} ${r.detail}` });
    continue;
  }
  histories.push({ wallet, source: info.source, swaps: r.value.trades.length, complete: r.value.complete });
  if (!r.value.complete) continue; // truncated histories would bias signal counts; excluded and reported
  for (const tr of r.value.trades as BirdeyeTrade[]) {
    const legs = [tr.base, tr.quote];
    const got = legs.find((l) => l.type_swap === "to");
    const gave = legs.find((l) => l.type_swap === "from");
    if (!got || !gave) continue;
    if (!BASES.has(got.address) && BASES.has(gave.address)) trades.push({ wallet, mint: got.address, side: "BUY", t: tr.block_unix_time, usd: tr.volume_usd ?? 0, qty: Math.abs(got.ui_amount ?? 0) });
    else if (BASES.has(got.address) && !BASES.has(gave.address)) trades.push({ wallet, mint: gave.address, side: "SELL", t: tr.block_unix_time, usd: tr.volume_usd ?? 0, qty: Math.abs(gave.ui_amount ?? 0) });
  }
  if (histories.length % 10 === 0) log(`${histories.length}/${candidates.length} histories`);
}
const used = histories.filter((h) => h.complete && !h.error).length;
log(`histories: ${histories.length} fetched, ${used} complete and used, ${trades.length} risk-token trades, stoppedByBudget=${stoppedByBudget}`);

// 3) signals
const base = { minBuyUsd: 100, minRetainedBps: 8_000, cooldownSec: 86_400, windowSec: 180 };
const sig3 = detectSignals(trades, { ...base, minWallets: 3 });
const sig2 = detectSignals(trades, { ...base, minWallets: 2 });
log(`signals: 3-wallet ${sig3.length}, 2-wallet (diagnostic) ${sig2.length}`);

// 4) replay (3-wallet first, then 2-wallet while budget lasts)
const P: Omit<ReplayParams, "delaySec" | "slippageBps"> = { sizeUsd: 25, feePerSideUsd: 0.03, stopLossBps: 1000, takeProfitBps: 2500, trailActivationBps: 1500, trailDrawdownBps: 800, timeStopSec: 4 * 3600 };
const DELAYS = [5, 15, 30];
const SLIPS = [50, 150];
const firstSellAfter = (s: HistSignal) =>
  trades.filter((x) => x.side === "SELL" && x.mint === s.mint && x.t > s.t && s.wallets.includes(x.wallet)).sort((a, b) => a.t - b.t)[0]?.t;
async function replaySet(label: string, sigs: HistSignal[]) {
  const rows: Array<Record<string, unknown>> = [];
  for (const s of sigs) {
    let s1, m1;
    try {
      s1 = await be.candles(s.mint, "1s", s.t - 30, s.t + 75);
      m1 = await be.candles(s.mint, "1m", s.t, s.t + 4 * 3600 + 120);
    } catch (e) {
      if (e instanceof BudgetExceededError) {
        stoppedByBudget = true;
        break;
      }
      throw e;
    }
    if (!s1.ok || !m1.ok) {
      rows.push({ ...s, error: `${s1.ok ? "" : s1.detail} ${m1.ok ? "" : m1.detail}`.trim() });
      continue;
    }
    const exitT = firstSellAfter(s);
    const res: Record<string, ReplayResult> = {};
    for (const d of DELAYS)
      for (const sl of SLIPS) {
        res[`d${d}_s${sl}`] = replayCopy(s.t, s1.value, m1.value, { ...P, delaySec: d, slippageBps: sl });
        res[`d${d}_s${sl}_signalexit`] = replayCopy(s.t, s1.value, m1.value, { ...P, delaySec: d, slippageBps: sl, useOwnExits: false, ...(exitT !== undefined ? { leaderExitTime: exitT } : {}) });
      }
    rows.push({ ...s, candles1s: s1.value.length, candles1m: m1.value.length, res });
  }
  log(`${label}: replayed ${rows.length}/${sigs.length}`);
  return rows;
}
function summarize(rows: Array<Record<string, unknown>>) {
  const out: Record<string, unknown> = {};
  const keys = new Set<string>();
  for (const r of rows) for (const k of Object.keys((r.res as object) ?? {})) keys.add(k);
  for (const k of [...keys].sort()) {
    const rs = rows.map((r) => (r.res as Record<string, ReplayResult> | undefined)?.[k]).filter((x): x is ReplayResult => !!x);
    const f = rs.filter((r): r is Extract<ReplayResult, { status: "FILLED" }> => r.status === "FILLED");
    const pnl = f.map((r) => r.pnlUsd).sort((a, b) => a - b);
    const tot = pnl.reduce((a, b) => a + b, 0);
    const gp = pnl.filter((x) => x > 0).reduce((a, b) => a + b, 0);
    const gl = -pnl.filter((x) => x < 0).reduce((a, b) => a + b, 0);
    const reasons: Record<string, number> = {};
    for (const r of f) reasons[r.exitReason] = (reasons[r.exitReason] ?? 0) + 1;
    out[k] = {
      trades: f.length,
      noEntryPrice: rs.length - f.length,
      winRate: f.length ? +(f.filter((r) => r.pnlUsd > 0).length / f.length).toFixed(3) : null,
      totalPnlUsd: +tot.toFixed(2),
      expectancyUsd: f.length ? +(tot / f.length).toFixed(3) : null,
      medianPnlUsd: pnl.length ? +pnl[Math.floor((pnl.length - 1) / 2)]!.toFixed(3) : null,
      profitFactor: gl > 0 ? +(gp / gl).toFixed(3) : null,
      totalWithoutBestUsd: pnl.length ? +(tot - pnl[pnl.length - 1]!).toFixed(2) : null,
      exitReasons: reasons,
    };
  }
  return out;
}
const rows3 = await replaySet("3-wallet", sig3);
const done = new Set(sig3.map((s) => `${s.mint}:${s.t}`));
const rows2 = stoppedByBudget ? [] : await replaySet("2-wallet", sig2.filter((s) => !done.has(`${s.mint}:${s.t}`)));
const out = process.env.CR_OUT ?? "confluence-replay.json";
const result = {
  generatedAt: new Date().toISOString(),
  window: { from: new Date(windowStart * 1000).toISOString(), to: new Date(windowEnd * 1000).toISOString() },
  candidates: candidates.length,
  historiesUsed: used,
  histories,
  trades: trades.length,
  signals3: sig3.length,
  signals2: sig2.length,
  birdeyeCalls: be.calls,
  cacheHits: be.cacheHits,
  cuEstimate: be.cu,
  stoppedByBudget,
  summary3: summarize(rows3),
  summary2: summarize(rows2),
  rows3,
  rows2,
};
writeFileSync(out, JSON.stringify(result, null, 1));
console.log(JSON.stringify({ candidates: candidates.length, historiesUsed: used, trades: trades.length, signals3: sig3.length, signals2: sig2.length, cu: be.cu, stoppedByBudget, summary3: result.summary3 }, null, 1));
