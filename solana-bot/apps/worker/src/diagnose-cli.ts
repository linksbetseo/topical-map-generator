/**
 * P0 wallet diagnostics (spec v2 §3): read-only, resumable through the durable cache.
 *   pnpm --filter @solbot/worker diagnose-wallets <sessionId>
 * Env: DIAG_WINDOW_DAYS (30), DIAG_MAX_SIGNATURE_PAGES (10 = 10k signatures), DIAG_MAX_TX_PER_WALLET (2000),
 *      DIAG_MAX_RPC_CALLS (40000 ≈ 40k Helius credits), DIAG_CONCURRENCY (4), DIAG_ADDRESSES (comma list, optional).
 */
import { readFileSync } from "node:fs";
import { systemClock } from "@solbot/domain";
import { loadRuntimeEnv, parseConfig, redactSecrets } from "@solbot/config";
import { createPool, getSession, migrate } from "@solbot/db";
import { BinanceMinuteFx, HeliusRpc, JupiterClient, ReadOnlyTransport, SlidingWindowLimiter, SolanaHistory, pacer } from "@solbot/providers";
import { DbTxCache, runDiagnostics } from "./diagnostics.ts";

const env = loadRuntimeEnv();
if (!env.databaseUrl) throw new Error("DATABASE_URL required");
if (!env.heliusApiKey) throw new Error("HELIUS_API_KEY required");
const cfg = parseConfig(process.env.CONFIG_PATH ? JSON.parse(readFileSync(process.env.CONFIG_PATH, "utf8")) : {});
const log = (m: string) => console.log(`${new Date().toISOString()} ${redactSecrets(m, [env.heliusApiKey, env.jupiterApiKey])}`);
const sessionId = process.argv[2];
if (!sessionId) throw new Error("usage: diagnose-wallets <sessionId>");
const num = (k: string, d: number) => (process.env[k] ? Number(process.env[k]) : d);

const pool = createPool(env.databaseUrl);
await migrate(pool);
if (!(await getSession(pool, sessionId))) throw new Error("session not found");

const transport = new ReadOnlyTransport(fetch as never, systemClock, 30_000);
// Helius Free: RPC 10 req/s -> one call every 120 ms (≈ 8/s), shared by all workers of this process
const rpc = new HeliusRpc(transport, new SlidingWindowLimiter(systemClock, 600, 1), env.rpc.url, env.rpc.supportsDas);
const history = new SolanaHistory(rpc, new DbTxCache(pool, () => new Date()), pacer(120, (ms) => new Promise((r) => setTimeout(r, ms)), () => Date.now()));
const jup = new JupiterClient(transport, new SlidingWindowLimiter(systemClock, 30), env.jupiterApiKey);
const fx = new BinanceMinuteFx(transport);

const { runId, summary } = await runDiagnostics({ pool, history, fx, jup, cfg, now: () => new Date(), log }, sessionId, {
  windowDays: num("DIAG_WINDOW_DAYS", 30),
  maxSignaturePages: num("DIAG_MAX_SIGNATURE_PAGES", 10),
  maxTxPerWallet: num("DIAG_MAX_TX_PER_WALLET", 2000),
  maxRpcCalls: num("DIAG_MAX_RPC_CALLS", 40_000),
  concurrency: num("DIAG_CONCURRENCY", 4),
  ...(process.env.DIAG_ADDRESSES ? { addresses: process.env.DIAG_ADDRESSES.split(",").filter(Boolean) } : {}),
});
console.log(JSON.stringify({ runId, summary }, null, 2));
await pool.end();
