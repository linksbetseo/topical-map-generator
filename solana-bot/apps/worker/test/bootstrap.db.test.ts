import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { D, USDC_MINT } from "@solbot/domain";
import { configHash, parseConfig } from "@solbot/config";
import { createSession, type Pool } from "@solbot/db";
import { runBootstrap, type BootstrapDeps } from "../src/index.ts";
import { freshDb } from "../../../packages/db/test/helpers.ts";

let pool: Pool;
beforeAll(async () => {
  pool = await freshDb();
});
afterAll(async () => {
  await pool.end();
});

const cfg = parseConfig();
const NOW = new Date("2026-10-01T12:00:00Z");
const SEED = "SeedMint11111111111111111111111111111111111";
const GOOD = "GoodWallet111111111111111111111111111111111";
const BUSY = "BusyWallet111111111111111111111111111111111";

// Synthetic enhanced transactions (shape verified live 2026-09-28: tokenAmount is a UI decimal)
const swap = (wallet: string, mint: string, usdc: number, tokens: number, buy: boolean, ts: number, sig: string) => ({
  signature: sig,
  timestamp: ts,
  type: "SWAP",
  transactionError: null,
  feePayer: wallet,
  tokenTransfers: buy
    ? [
        { fromUserAccount: wallet, toUserAccount: "Pool", mint: USDC_MINT, tokenAmount: usdc },
        { fromUserAccount: "Pool", toUserAccount: wallet, mint, tokenAmount: tokens },
      ]
    : [
        { fromUserAccount: wallet, toUserAccount: "Pool", mint, tokenAmount: tokens },
        { fromUserAccount: "Pool", toUserAccount: wallet, mint: USDC_MINT, tokenAmount: usdc },
      ],
  nativeTransfers: [],
});

function goodHistory() {
  const txs = [];
  const base = Math.floor(NOW.getTime() / 1000) - 25 * 86_400;
  for (let i = 0; i < 40; i++) {
    const mint = `Tok${String(i % 25).padStart(40, "0")}`;
    const ts = base + (i % 10) * 86_400 + i * 60;
    const loss = i % 5 === 0;
    txs.push(swap(GOOD, mint, 100, 1000, true, ts, `gb${i}`));
    txs.push(swap(GOOD, mint, loss ? 80 : 150, 999.9995, false, ts + 30, `gs${i}`)); // UI float leftover < 0.1%
  }
  return txs.reverse(); // newest first like the API
}

function deps(): BootstrapDeps {
  const helius = {
    calls: 0,
    async history(address: string, q: { type?: string }) {
      this.calls++;
      if (address === SEED) return { ok: true, value: { txs: [swap(GOOD, SEED, 150, 10, true, 1, "seed1"), swap(BUSY, SEED, 150, 10, true, 2, "seed2")], truncated: false } };
      if (q.type === "TRANSFER") return { ok: true, value: { txs: [], truncated: false } };
      if (address === GOOD) return { ok: true, value: { txs: goodHistory(), truncated: false } };
      return { ok: true, value: { txs: [], truncated: true } }; // BUSY: more pages than allowed
    },
  };
  return {
    pool,
    helius: helius as never,
    fx: { calls: 0, fxAt: async () => ({ usdcUsd: new D(1), solUsd: new D(150) }) } as never,
    rpc: { call: async (_m: string, p: [string[]]) => ({ ok: true, value: { value: p[0].map(() => ({ data: [Buffer.from([6]).toString("base64"), "base64"] })) } }) } as never,
    jup: { withPriority: () => ({ usdPrices: async () => ({ ok: true, prices: new Map(), receivedAt: NOW }) }) } as never,
    cfg,
    now: () => NOW,
    log: () => undefined,
  };
}

describe("wallet bootstrap", () => {
  it("qualifies a broad profitable history, rejects a truncated one, freezes results with evidence", async () => {
    const sessionId = await createSession(pool, { kind: "CONFLUENCE", mode: "PAPER", strategyName: "confluence_v1", strategyVersion: "1.0.0", strategyCodeHash: "t", config: cfg, configHash: configHash(cfg) });
    const s = await runBootstrap(deps(), sessionId, { seedMints: [SEED], seedLookbackHours: 6, seedPagesPerMint: 1, walletMaxPages: 5, transferMaxPages: 2, maxHeliusCalls: 100 });
    expect(s.candidates).toBe(2);
    expect(s.qualified).toBe(1);
    const rows = (await pool.query(`SELECT address, status, reasons, metrics FROM wallet_qualification WHERE session_id=$1 ORDER BY address`, [sessionId])).rows;
    const good = rows.find((r) => r.address === GOOD)!;
    const busy = rows.find((r) => r.address === BUSY)!;
    expect(good.status).toBe("QUALIFIED");
    expect(good.metrics.closedEpisodes).toBe(40);
    expect(busy.status).toBe("REJECTED");
    expect(JSON.stringify(busy.reasons)).toMatch(/truncated/);
    const cl = (await pool.query(`SELECT link_check FROM wallet_clusters WHERE session_id=$1 AND address=$2`, [sessionId, GOOD])).rows[0];
    expect(cl.link_check).toBe("CHECKED");
    const src = (await pool.query(`SELECT candidate_source FROM wallets WHERE address=$1`, [GOOD])).rows[0];
    expect(JSON.parse(src.candidate_source).source).toBe("observed_buyer");
    const audit = (await pool.query(`SELECT count(*)::int AS n FROM audit_events WHERE action='WALLET_BOOTSTRAP' AND session_id=$1`, [sessionId])).rows[0];
    expect(audit.n).toBe(1);
  });
});
