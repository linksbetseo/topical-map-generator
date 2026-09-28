/**
 * Wallet bootstrap for a CONFLUENCE session (read-only; Helius + Jupiter + Binance public data).
 *   pnpm --filter @solbot/worker bootstrap-wallets <sessionId>
 * Env: BOOTSTRAP_SEED_MINTS (default 20), BOOTSTRAP_MAX_HELIUS_CALLS (default 3000),
 *      BOOTSTRAP_SEED_HOURS (default 6), BOOTSTRAP_WALLET_PAGES (default 10).
 */
import { readFileSync } from "node:fs";
import { systemClock } from "@solbot/domain";
import { loadRuntimeEnv, parseConfig, redactSecrets } from "@solbot/config";
import { createPool, getSession, migrate } from "@solbot/db";
import { BinanceMinuteFx, HeliusEnhanced, HeliusRpc, JupiterClient, ReadOnlyTransport, SlidingWindowLimiter } from "@solbot/providers";
import { runBootstrap } from "./bootstrap.ts";

const env = loadRuntimeEnv();
if (!env.databaseUrl) throw new Error("DATABASE_URL required");
if (!env.heliusApiKey) throw new Error("HELIUS_API_KEY required for wallet history");
const cfg = parseConfig(process.env.CONFIG_PATH ? JSON.parse(readFileSync(process.env.CONFIG_PATH, "utf8")) : {});
const log = (m: string) => console.log(`${new Date().toISOString()} ${redactSecrets(m, [env.heliusApiKey, env.jupiterApiKey])}`);
const sessionId = process.argv[2];
if (!sessionId) throw new Error("usage: bootstrap-wallets <sessionId>");

const pool = createPool(env.databaseUrl);
await migrate(pool);
const s = await getSession(pool, sessionId);
if (!s) throw new Error("session not found");
if (s.t0) throw new Error("session already started; the wallet list is frozen for the whole session");

const transport = new ReadOnlyTransport(fetch as never, systemClock, 30_000);
const jup = new JupiterClient(transport, new SlidingWindowLimiter(systemClock, 50), env.jupiterApiKey);
// Helius Free: Enhanced API 2 req/s -> stay at 100/min, evenly spaced, 429s retried with back-off
const helius = new HeliusEnhanced(transport, new SlidingWindowLimiter(systemClock, 100, 1), env.heliusApiKey, undefined, {
  minIntervalMs: 600,
  retries429: 4,
  backoffMs: 2_000,
  sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
  nowMs: () => Date.now(),
});
const rpc = new HeliusRpc(transport, new SlidingWindowLimiter(systemClock, 300, 1), env.rpc.url, env.rpc.supportsDas);
const fx = new BinanceMinuteFx(transport);

const seedCount = Number(process.env.BOOTSTRAP_SEED_MINTS ?? 20);
const res = await transport.request("GET", `https://api.jup.ag/tokens/v2/toptraded/24h?limit=${Math.min(100, seedCount)}`, { headers: env.jupiterApiKey ? { "x-api-key": env.jupiterApiKey } : {} });
const seeds = (JSON.parse(res.text) as Array<{ id: string }>).map((t) => t.id).slice(0, seedCount);
log(`seed mints (Jupiter toptraded 24h): ${seeds.length}`);

const summary = await runBootstrap(
  { pool, helius, fx, rpc, jup, cfg, now: () => new Date(), log },
  sessionId,
  {
    seedMints: seeds,
    seedLookbackHours: Number(process.env.BOOTSTRAP_SEED_HOURS ?? 6),
    seedPagesPerMint: 3,
    walletMaxPages: Number(process.env.BOOTSTRAP_WALLET_PAGES ?? 10),
    transferMaxPages: 5,
    maxHeliusCalls: Number(process.env.BOOTSTRAP_MAX_HELIUS_CALLS ?? 3000),
  },
);
console.log(JSON.stringify(summary, null, 2));
await pool.end();
