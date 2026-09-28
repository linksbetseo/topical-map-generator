/**
 * Owner API process.  OWNER_API_TOKEN=... DATABASE_URL=... pnpm --filter @solbot/api start
 */
import { readFileSync } from "node:fs";
import { SPL_TOKEN_PROGRAM, USDC_MINT, WSOL_MINT, systemClock } from "@solbot/domain";
import { loadRuntimeEnv, parseConfig } from "@solbot/config";
import { createPool, migrate } from "@solbot/db";
import { HeliusRpc, JupiterClient, Priority, ReadOnlyTransport, SlidingWindowLimiter, parseMintAccount } from "@solbot/providers";
import { STRATEGY_NAME, STRATEGY_VERSION } from "@solbot/strategy";
import { readinessChecks } from "@solbot/worker";
import { buildApp } from "./app.ts";

const env = loadRuntimeEnv();
if (!env.databaseUrl) throw new Error("DATABASE_URL required");
const cfg = parseConfig(process.env.CONFIG_PATH ? JSON.parse(readFileSync(process.env.CONFIG_PATH, "utf8")) : {});
const pool = createPool(env.databaseUrl);
await migrate(pool);
const transport = new ReadOnlyTransport(fetch as never, systemClock);
const jup = new JupiterClient(transport, new SlidingWindowLimiter(systemClock, 10), env.jupiterApiKey).withPriority(Priority.RECONCILE);
const rpc = new HeliusRpc(transport, new SlidingWindowLimiter(systemClock, 60), env.rpc.url, env.rpc.supportsDas);

const app = buildApp({
  pool,
  clock: systemClock,
  cfg,
  ownerToken: process.env.OWNER_API_TOKEN ?? null,
  heliusWebhookAuth: env.heliusWebhookAuth,
  allowedOrigins: (process.env.ALLOWED_ORIGINS ?? "").split(",").filter(Boolean),
  codeVersion: process.env.GIT_SHA ?? "unknown",
  strategy: { name: STRATEGY_NAME, version: STRATEGY_VERSION, codeHash: process.env.GIT_SHA ?? "unknown" },
  readiness: (id) =>
    readinessChecks(pool, id, cfg, new Date(), {
      marketSource: "LIVE",
      endpoints: async () => {
        const q = await jup.quote({ inputMint: USDC_MINT, outputMint: WSOL_MINT, amountRaw: 1_000_000n, slippageBps: 100 });
        return q.ok ? { ok: true, detail: `order ok (${q.quote.router}, ${q.quote.feeSemantics})` } : { ok: false, detail: `${q.code}: ${q.detail}` };
      },
      canonicalUsdc: async () => {
        const r = await rpc.getAccountInfo(USDC_MINT, Priority.RECONCILE);
        if (!r.ok || !r.value) return { ok: false, detail: r.ok ? "not found" : r.code };
        const m = parseMintAccount(r.value.owner, r.value.data);
        return m.ok && r.value.owner === SPL_TOKEN_PROGRAM && m.mint.decimals === 6 ? { ok: true, detail: "SPL Token, 6 decimals" } : { ok: false, detail: "unexpected mint data" };
      },
    }),
  bootstrap: async (id) => {
    const q = await pool.query<{ n: number }>(`SELECT count(*)::int AS n FROM wallet_qualification WHERE session_id=$1 AND status='QUALIFIED'`, [id]);
    const n = q.rows[0]!.n;
    return n >= cfg.wallets.min_qualified_for_session
      ? { ready: true, missing: [] }
      : { ready: false, missing: [`qualified wallets ${n}/${cfg.wallets.min_qualified_for_session}`, "wallet history + historical USD prices provider not configured (docs/ACCESS_GAPS.md G4)"] };
  },
  startFx: async () => {
    const r = await jup.usdPrices([WSOL_MINT, USDC_MINT]);
    return r.ok ? { usdcUsd: r.prices.get(USDC_MINT)?.usdPrice ?? null, solUsd: r.prices.get(WSOL_MINT)?.usdPrice ?? null } : { usdcUsd: null, solUsd: null };
  },
});
await app.listen({ host: "0.0.0.0", port: Number(process.env.PORT ?? 8080) });
