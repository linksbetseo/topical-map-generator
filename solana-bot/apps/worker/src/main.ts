/**
 * Worker process (PAPER). Read-only providers only; no signer exists in this build.
 *   DATABASE_URL=... JUPITER_API_KEY=... HELIUS_API_KEY=... pnpm --filter @solbot/worker start
 */
import { readFileSync } from "node:fs";
import { SessionState, sessionManagesPositions, systemClock } from "@solbot/domain";
import { effectiveJupiterRps, jupiterBudget, loadRuntimeEnv, parseConfig, redactSecrets } from "@solbot/config";
import { createPool, getSession, migrate } from "@solbot/db";
import { HeliusRpc, JupiterClient, Priority, ReadOnlyTransport, SlidingWindowLimiter } from "@solbot/providers";
import { PaperEngine } from "./engine.ts";
import { DbFlowStore, DbWalletBook, LiveMarketData } from "./live.ts";
import { TelegramNotifier } from "./telegram.ts";
import { nullNotifier } from "./ports.ts";
import { normalizeEnhancedSwap } from "./helius-flow.ts";

const env = loadRuntimeEnv(); // throws if a signing secret is present
if (!env.databaseUrl) throw new Error("DATABASE_URL required");
const cfg = parseConfig(process.env.CONFIG_PATH ? JSON.parse(readFileSync(process.env.CONFIG_PATH, "utf8")) : {});
const secrets = [env.jupiterApiKey, env.heliusApiKey, env.telegramBotToken];
const log = (msg: string) => console.log(`${new Date().toISOString()} ${redactSecrets(msg, secrets)}`);

const pool = createPool(env.databaseUrl);
await migrate(pool);

const transport = new ReadOnlyTransport(fetch as never, systemClock, 10_000);
const jup = new JupiterClient(transport, new SlidingWindowLimiter(systemClock, Math.floor(effectiveJupiterRps(cfg, !!env.jupiterApiKey) * 60)), env.jupiterApiKey);
const budget = jupiterBudget(cfg, effectiveJupiterRps(cfg, !!env.jupiterApiKey));
if (!budget.ok) log(`WARNING Jupiter budget: need ${budget.requiredPerMinute}/min, have ${budget.availablePerMinute}/min; readiness will block the start`);
// no Helius key => public Solana RPC (no DAS; holders via getProgramAccounts; conservative rate)
const rpc = new HeliusRpc(transport, new SlidingWindowLimiter(systemClock, env.rpc.kind === "helius" ? 300 : 120), env.rpc.url, env.rpc.supportsDas);
log(`rpc: ${env.rpc.kind}, jupiter: ${env.jupiterApiKey ? "api key" : "keyless (0.5 RPS)"}`);
const market = new LiveMarketData(pool, jup, rpc, cfg);
const notifier = env.telegramBotToken && env.telegramChatId ? new TelegramNotifier(env.telegramBotToken, env.telegramChatId, fetch, log) : nullNotifier;

const sessionId =
  process.env.SESSION_ID ??
  (await pool.query<{ id: string }>(`SELECT id FROM sessions WHERE state NOT IN ('COMPLETED','INCOMPLETE') ORDER BY created_at DESC LIMIT 1`)).rows[0]?.id;
if (!sessionId) {
  log("no active session; create one via the API (POST /api/sessions)");
  process.exit(0);
}
const wallets = await new DbWalletBook(pool, sessionId).load();
const flows = new DbFlowStore(pool, () => wallets.watched());
const workerId = `worker-${process.pid}`;
const engine = new PaperEngine({ pool, clock: systemClock, cfg, sessionId, quotes: jup.withPriority(Priority.EXIT), market, flows, wallets, notifier, workerId });

log(`worker started for ${sessionId}; recovered ${await engine.recover()} unresolved items`);
let stop = false;
process.on("SIGTERM", () => (stop = true));
process.on("SIGINT", () => (stop = true));

let lastTick = 0;
let lastFx = 0;
let lastBeat = 0;
let lastEventCheck = new Date();
while (!stop) {
  const now = Date.now();
  try {
    const s = await getSession(pool, sessionId);
    if (!s || s.state === SessionState.COMPLETED || s.state === SessionState.INCOMPLETE) break;
    if (!sessionManagesPositions(s.state)) {
      // collecting mode before T0: engine.tick() is not running, so beat here for /health/ready
      if (now - lastBeat >= 10_000) {
        await pool.query(
          `INSERT INTO heartbeats (worker_id, session_id, last_beat_at, started_at) VALUES ($1,$2,$3,$3) ON CONFLICT (worker_id) DO UPDATE SET last_beat_at=$3, session_id=$2`,
          [workerId, sessionId, new Date(now)],
        );
        lastBeat = now;
      }
      // collecting mode before T0: FX history for readiness (30 min healthy data)
      if (now - lastFx >= 30_000) {
        const fx = await market.fx(new Date());
        await pool.query(`INSERT INTO fx_snapshots (at, usdc_usd, sol_usd, source) VALUES ($1,$2,$3,$4)`, [fx.at, fx.usdcUsd?.toString() ?? null, fx.solUsd?.toString() ?? null, fx.source]);
        lastFx = now;
      }
    } else {
      if (now - lastTick >= cfg.polling.position_quote_ms) {
        await engine.tick();
        lastTick = now;
      }
      // new confirmed flow events -> evaluate confluence for their mints
      const since = lastEventCheck;
      lastEventCheck = new Date();
      const rows = await pool.query<{ raw_payload: never; available_at: Date }>(`SELECT raw_payload, available_at FROM raw_events WHERE available_at > $1 AND available_at <= $2`, [since, lastEventCheck]);
      const mints = new Set<string>();
      // only to discover which mints moved; amounts and USD are re-derived with real decimals/FX by DbFlowStore
      for (const r of rows.rows) for (const e of normalizeEnhancedSwap(r.raw_payload, wallets.watched(), r.available_at, () => 0, null).events) mints.add(e.mint);
      for (const m of mints) {
        const res = await engine.onFlow(m);
        log(`flow ${m}: ${res.stage} ${res.reasons.map((x) => x.code).join(",")}`);
      }
    }
  } catch (e) {
    log(`tick error: ${e instanceof Error ? e.message : String(e)}`);
  }
  await systemClock.sleep(1_000);
}
await pool.end();
log("worker stopped");
