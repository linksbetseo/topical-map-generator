import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { D } from "@solbot/domain";
import { configHash, parseConfig } from "@solbot/config";
import { createSession, json, type Pool } from "@solbot/db";
import { SolanaHistory, type RpcTransaction } from "@solbot/providers";
import { DbTxCache, categorize, runDiagnostics, type WalletDiagnostic } from "../src/index.ts";
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
const nowSec = Math.floor(NOW.getTime() / 1000);
const SPL = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const OK = "OkWa11et11111111111111111111111111111111111";
const BUSY = "BusyWa11et111111111111111111111111111111111";
const HUGE = "HugeWa11et111111111111111111111111111111111";
const BROKEN = "BrokenWa11et1111111111111111111111111111111";

/** jsonParsed-shaped getTransaction result: wallet pays SOL for `mint` (buy) or receives SOL (sell). */
function rpcTx(sig: string, t: number, wallet: string, mint: string, buy: boolean, lamports: number): RpcTransaction {
  const pre = [10_000_000_000, 1];
  const post = [pre[0]! + (buy ? -lamports : lamports) - 5000, 1];
  const tok = (amount: string) => ({ accountIndex: 1, mint, owner: wallet, programId: SPL, uiTokenAmount: { amount, decimals: 6 } });
  return {
    slot: t,
    blockTime: t,
    meta: { err: null, fee: 5000, preBalances: pre, postBalances: post, preTokenBalances: [tok(buy ? "0" : "1000")], postTokenBalances: [tok(buy ? "1000" : "0")] },
    transaction: { signatures: [sig], message: { accountKeys: [{ pubkey: wallet }, { pubkey: `ata-${sig}` }], instructions: [{ programId: SPL }] } },
  };
}

function fakeRpc() {
  const txs = new Map<string, RpcTransaction>();
  const sigsOf = new Map<string, Array<{ signature: string; slot: number; blockTime: number; err: null }>>();
  // OK: 3 round trips on different tokens (profitable)
  const okSigs = [];
  for (let i = 0; i < 3; i++) {
    const t = nowSec - (10 - i) * 86_400;
    txs.set(`ob${i}`, rpcTx(`ob${i}`, t, OK, `Mint${i}`.padEnd(44, "1"), true, 1_000_000_000));
    txs.set(`os${i}`, rpcTx(`os${i}`, t + 60, OK, `Mint${i}`.padEnd(44, "1"), false, 1_200_000_000));
    okSigs.push({ signature: `os${i}`, slot: t + 60, blockTime: t + 60, err: null }, { signature: `ob${i}`, slot: t, blockTime: t, err: null });
  }
  sigsOf.set(OK, okSigs.sort((a, b) => b.blockTime - a.blockTime));
  // BUSY: every signature page is full and inside the window -> scan never reaches the window start
  sigsOf.set(BUSY, Array.from({ length: 5000 }, (_, i) => ({ signature: `bz${i}`, slot: nowSec - i, blockTime: nowSec - i, err: null })));
  // HUGE: complete scan but more transactions than the per-wallet budget
  sigsOf.set(HUGE, Array.from({ length: 50 }, (_, i) => ({ signature: `hg${i}`, slot: nowSec - i, blockTime: nowSec - i, err: null })));
  // BROKEN: getTransaction fails
  sigsOf.set(BROKEN, [{ signature: "br0", slot: nowSec - 100, blockTime: nowSec - 100, err: null }]);
  const calls: string[] = [];
  const rpc = {
    async call(method: string, params: unknown[]) {
      calls.push(method);
      if (method === "getSignaturesForAddress") {
        const [addr, opts] = params as [string, { before?: string; limit: number }];
        const all = sigsOf.get(addr) ?? [];
        const start = opts.before ? all.findIndex((s) => s.signature === opts.before) + 1 : 0;
        return { ok: true, value: all.slice(start, start + opts.limit), receivedAt: NOW };
      }
      const sig = (params as [string])[0];
      if (sig === "br0") return { ok: false, code: "PROVIDER_ERROR", detail: "HTTP 500-like" };
      return { ok: true, value: txs.get(sig) ?? null, receivedAt: NOW };
    },
  };
  return { rpc, calls };
}

async function seedSession(): Promise<string> {
  const id = await createSession(pool, { kind: "CONFLUENCE", mode: "PAPER", strategyName: "confluence_v1", strategyVersion: "1.0.0", strategyCodeHash: "t", config: cfg, configHash: configHash(cfg) });
  for (const a of [OK, BUSY, HUGE, BROKEN]) {
    await pool.query(`INSERT INTO wallets (address, candidate_source, first_seen_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, [a, json({ source: "observed_buyer", seed_mints: ["Mint0".padEnd(44, "1")] }), NOW]);
    await pool.query(`INSERT INTO wallet_qualification (session_id, address, status, metrics, coverage_bps, reasons, computed_at) VALUES ($1,$2,'REJECTED',$3,0,$4,$5)`, [
      id,
      a,
      json({ totalPnlUsd: "-5", closedEpisodes: 1 }),
      json([{ code: "WALLET_NOT_QUALIFIED", detail: "legacy" }]),
      NOW,
    ]);
  }
  return id;
}

describe("P0 wallet diagnostics", () => {
  it("attributes every address to exactly one primary reason and never scores an incomplete history", async () => {
    const sessionId = await seedSession();
    const { rpc, calls } = fakeRpc();
    const history = new SolanaHistory(rpc as never, new DbTxCache(pool, () => NOW), async () => undefined, { attempts: 0, backoffMs: 0, sleep: async () => undefined });
    const fx = { calls: 0, fxAt: async () => ({ solUsd: new D(100), usdcUsd: new D(1) }) };
    const jup = { withPriority: () => ({ usdPrices: async () => ({ ok: true, prices: new Map(), receivedAt: NOW }) }) };
    const deps = { pool, history, fx: fx as never, jup: jup as never, cfg, now: () => NOW, log: () => undefined };
    const opts = { windowDays: 30, maxSignaturePages: 2, maxTxPerWallet: 20, maxRpcCalls: 10_000, concurrency: 2 };

    const { runId, summary } = await runDiagnostics(deps, sessionId, opts);
    const rows = (await pool.query<{ record: WalletDiagnostic }>(`SELECT record FROM wallet_diagnostics WHERE run_id=$1`, [runId])).rows.map((r) => r.record);
    const by = new Map(rows.map((r) => [r.wallet_address, r]));

    expect(Object.values(summary.primaryReasons).reduce((a, b) => a + b, 0)).toBe(4);
    // #3: incomplete pagination is HISTORY_INCOMPLETE, not LOW_PF, and costs no transaction fetches
    expect(by.get(BUSY)!.primary_reject_reason).toBe("HISTORY_INCOMPLETE");
    expect(by.get(BUSY)!.all_reject_reasons).toEqual(["HISTORY_INCOMPLETE"]);
    expect(by.get(BUSY)!.pagination_exhausted).toBe(false);
    expect(by.get(BUSY)!.pages_requested).toBe(2);
    expect(by.get(HUGE)!.primary_reject_reason).toBe("HISTORY_INCOMPLETE");
    expect(by.get(HUGE)!.budget_exhausted).toBe(true);
    expect(by.get(HUGE)!.estimated_credits).toBe(50);
    expect(by.get(BROKEN)!.primary_reject_reason).toBe("API_ERROR");

    const ok = by.get(OK)!;
    expect(ok.history_complete).toBe(true);
    expect(ok.normalized_swaps).toBe(6);
    expect(ok.closed_episodes).toBe(3);
    expect(ok.realized_pnl_usd).toBe("60.00"); // 3 × (120 - 100) USD
    expect(ok.network_fees_sol).toBe("0.000030");
    expect(ok.pnl_excluding_discovery_tokens).toBe("40.00");
    expect(ok.primary_reject_reason).toBe("INSUFFICIENT_SAMPLE");
    expect(ok.all_reject_reasons).toContain("LOW_PF"); // no losing episode → PF undefined
    expect(ok.legacy!.total_pnl_usd).toBe("-5");
    expect(summary.legacyComparison.signFlips).toBe(1);
    expect(calls.filter((m) => m === "getTransaction").length).toBe(7); // 6 + the failing one

    // resume/checkpoint: a second run reuses scans and cached transactions
    calls.length = 0;
    const again = await runDiagnostics({ ...deps, now: () => new Date(NOW.getTime() + 1000) }, sessionId, { ...opts });
    expect(calls.filter((m) => m === "getTransaction")).toEqual(["getTransaction"]); // only the uncached failure is retried
    expect(again.summary.primaryReasons).toEqual(summary.primaryReasons);
  });

  it("categorize: first reason is the most basic one, all violated criteria are kept", () => {
    const all = { closed: true, distinct: true, days: true, losing: true, pnl: true, pf: true, concentration: true, infrastructure: true, coverage: true };
    expect(categorize({ apiError: false, historyIncomplete: true, unsupportedShare: 0.5, coverageBps: 0, unknownCostShare: 1, criteria: { ...all, pf: false } }, cfg)).toEqual(["HISTORY_INCOMPLETE"]);
    expect(categorize({ apiError: false, historyIncomplete: false, unsupportedShare: 0.2, coverageBps: 10_000, unknownCostShare: 0, criteria: { ...all, pnl: false, pf: false } }, cfg)).toEqual(["PARSER_UNSUPPORTED", "NEGATIVE_PNL", "LOW_PF"]);
    expect(categorize({ apiError: false, historyIncomplete: false, unsupportedShare: 0, coverageBps: 10_000, unknownCostShare: 0, criteria: all }, cfg)).toEqual([]);
  });
});
