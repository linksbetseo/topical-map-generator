import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { D, FakeClock, NATIVE_SOL, USDC_MINT } from "@solbot/domain";
import { parseConfig } from "@solbot/config";
import { Bucket } from "@solbot/ledger";
import { ScriptedQuoteProvider } from "@solbot/brokers";
import { createAttempt, getSession, loadLedger, reserveEntry, withTx, type Pool } from "@solbot/db";
import type { FlowEvent } from "@solbot/strategy";
import { FixtureMarket, MemoryFlowStore, PaperEngine, StaticWalletBook, nullNotifier } from "../src/index.ts";
import { freshDb, runningSession } from "../../../packages/db/test/helpers.ts";

let pool: Pool;
beforeAll(async () => {
  pool = await freshDb();
});
afterAll(async () => {
  await pool.end();
});

const cfg = parseConfig();
const T0 = new Date("2026-10-01T12:00:00Z");
const MINT = "Mint1111111111111111111111111111111111111111";

async function setup(opts: { t0?: Date } = {}) {
  const t0 = opts.t0 ?? T0;
  const sessionId = await runningSession(pool, { t0 });
  const clock = new FakeClock(new Date(t0.getTime() + 3_600_000));
  const quotes = new ScriptedQuoteProvider(clock);
  const market = new FixtureMarket();
  market.addHealthyToken(MINT, clock.now());
  const flows = new MemoryFlowStore();
  const wallets = new StaticWalletBook(
    new Map([
      ["WA", { qualified: true, clusterId: "c1", linkCheck: "CHECKED" as const }],
      ["WB", { qualified: true, clusterId: "c2", linkCheck: "CHECKED" as const }],
      ["WC", { qualified: true, clusterId: "c3", linkCheck: "CHECKED" as const }],
    ]),
  );
  const engine = new PaperEngine({ pool, clock, cfg, sessionId, quotes, market, flows, wallets, notifier: nullNotifier, workerId: `w-${sessionId}` });
  const buys = (at: Date) => {
    for (const [i, w] of ["WA", "WB", "WC"].entries()) {
      const bt = new Date(at.getTime() - (60 - i * 20) * 1000);
      const e: FlowEvent = { wallet: w, mint: MINT, side: "BUY", tokenRaw: 5_000_000n, usd: new D(150), blockTime: bt, availableAt: new Date(bt.getTime() + 2_000), confirmed: true, signature: `sig-${w}-${bt.getTime()}` };
      flows.add(e);
    }
  };
  return { sessionId, clock, quotes, market, flows, engine, buys };
}

async function ledgerOf(sessionId: string) {
  const c = await pool.connect();
  try {
    return await loadLedger(c, sessionId);
  } finally {
    c.release();
  }
}

async function position(sessionId: string) {
  return (await pool.query(`SELECT * FROM positions WHERE session_id=$1 AND entry_filled_at IS NOT NULL ORDER BY entry_filled_at DESC LIMIT 1`, [sessionId])).rows[0];
}

async function enter(ctx: Awaited<ReturnType<typeof setup>>) {
  ctx.quotes.script(USDC_MINT, MINT, [{ outNetRaw: 1_000_000n }, { outNetRaw: 999_500n }]);
  ctx.quotes.script(MINT, USDC_MINT, [{ outNetRaw: 24_800_000n }]);
  await ctx.engine.tick();
  ctx.buys(ctx.clock.now());
  return ctx.engine.onFlow(MINT);
}

describe("paper engine end-to-end (DEMO fixtures, fake clock, real Postgres)", () => {
  it("signal -> filters -> risk -> paper fill -> take profit -> closed with rent recovered", async () => {
    const ctx = await setup();
    const r = await enter(ctx);
    expect(r).toEqual({ stage: "filled", reasons: [] });
    const p = await position(ctx.sessionId);
    expect(p.status).toBe("OPEN");
    expect(BigInt(p.qty_raw)).toBe(997_501n); // floor(999500 * 0.998)
    const l1 = await ledgerOf(ctx.sessionId);
    expect(l1.balance(Bucket.RENT_LOCKED, NATIVE_SOL)).toBe(2_039_280n);
    expect(l1.verifyIdentity().ok).toBe(true);

    // same episode again: deduplicated, no second position
    expect((await ctx.engine.onFlow(MINT)).stage).toBe("dedupe");

    // price rises: NLR >= +25% => take profit
    ctx.clock.advance(60_000);
    ctx.quotes.script(MINT, USDC_MINT, [{ outNetRaw: 31_500_000n }, { outNetRaw: 31_500_000n }, { outNetRaw: 31_450_000n }]);
    await ctx.engine.tick();
    const closed = (await pool.query(`SELECT * FROM positions WHERE id=$1`, [p.id])).rows[0];
    expect(closed.status).toBe("CLOSED");
    expect(closed.exit_reason).toBe("EXIT_TAKE_PROFIT");
    // notional = 5% of conservative equity 499.99999995 (SOL allocation rounded down) = 24.999999 USDC
    // proceeds floor(31450000*0.998)=31387100 -> 31.3871 - 0.01575 exit fees - 0.00075 close - cost (24.999999 + 0.01575)
    expect(new D(closed.cost_usd).toFixed(6)).toBe("25.015749");
    expect(new D(closed.realized_pnl_usd).toFixed(6)).toBe("6.354851");
    const l2 = await ledgerOf(ctx.sessionId);
    expect(l2.balance(Bucket.RENT_LOCKED, NATIVE_SOL)).toBe(0n);
    expect(l2.balance(Bucket.WALLET, MINT)).toBe(0n);
    expect(l2.verifyIdentity().ok).toBe(true);
    const fills = await pool.query(`SELECT id FROM fills WHERE position_id=$1`, [p.id]);
    expect(fills.rows.every((f: { id: string }) => f.id.startsWith("paper_"))).toBe(true);
    // analytics quotes are scheduled for +5 s / +15 s
    ctx.clock.advance(20_000);
    await ctx.engine.runDueAnalytics();
    const an = await pool.query(`SELECT role FROM quotes WHERE role LIKE 'ANALYTIC%'`);
    expect(an.rowCount).toBeGreaterThanOrEqual(2);
  });

  it("-80% gap: stop-loss exit books the available bad price, never the ideal -10%", async () => {
    const ctx = await setup();
    await enter(ctx);
    ctx.clock.advance(60_000);
    ctx.quotes.script(MINT, USDC_MINT, [{ outNetRaw: 5_000_000n }, { outNetRaw: 5_000_000n }, { outNetRaw: 4_990_000n }]);
    await ctx.engine.tick();
    const p = await position(ctx.sessionId);
    expect(p.status).toBe("CLOSED");
    expect(p.exit_reason).toBe("EXIT_STOP_LOSS");
    expect(new D(p.realized_pnl_usd).lt(-19)).toBe(true); // ~ -20.05 USD, far beyond the -2.50 trigger level
  });

  it("no sell route: position stays visible as UNLIQUIDATABLE, lower bound 0, no fictitious exit", async () => {
    const ctx = await setup();
    await enter(ctx);
    ctx.clock.advance(60_000);
    ctx.quotes.script(MINT, USDC_MINT, [{ fail: "NO_ROUTE" }]);
    await ctx.engine.tick();
    const p = await position(ctx.sessionId);
    expect(p.status).not.toBe("CLOSED");
    expect(p.valuation_status).toBe("UNLIQUIDATABLE");
    const eq = (await pool.query(`SELECT * FROM equity_snapshots WHERE session_id=$1 ORDER BY at DESC LIMIT 1`, [ctx.sessionId])).rows[0];
    expect(new D(eq.breakdown.positions).toString()).toBe("0");
    expect(eq.equity_total_fresh_usd).toBeNull();
    // conservative lower bound dropped by ~25 USD => daily trigger (min(20, 4%)) => EXIT_ONLY
    expect((await getSession(pool, ctx.sessionId))!.state).toBe("EXIT_ONLY");
  });

  it("provider outage: PAUSED_DATA and no loss verdict; resumes when quotes return", async () => {
    const ctx = await setup();
    await enter(ctx);
    ctx.clock.advance(60_000);
    ctx.quotes.script(MINT, USDC_MINT, [{ fail: "PROVIDER_UNAVAILABLE" }]);
    await ctx.engine.tick();
    expect((await getSession(pool, ctx.sessionId))!.state).toBe("PAUSED_DATA");
    expect((await position(ctx.sessionId)).status).not.toBe("CLOSED");
    ctx.clock.advance(5_000);
    ctx.quotes.script(MINT, USDC_MINT, [{ outNetRaw: 25_000_000n }]);
    await ctx.engine.tick();
    expect((await getSession(pool, ctx.sessionId))!.state).toBe("RUNNING");
  });

  it("no entries in the last 4 h; at T_end the session settles and completes; restart does not reset", async () => {
    const ctx = await setup();
    await enter(ctx);
    // jump to 3 h before T_end: management only
    ctx.clock.set(new Date(T0.getTime() + 165 * 3_600_000));
    ctx.quotes.script(MINT, USDC_MINT, [{ outNetRaw: 25_100_000n }]);
    await ctx.engine.tick(); // time stop fires (held > 4 h) and closes the first position
    const MINT2 = "Mint2222222222222222222222222222222222222222";
    ctx.market.addHealthyToken(MINT2, ctx.clock.now());
    for (const [i, w] of ["WA", "WB", "WC"].entries()) {
      const bt = new Date(ctx.clock.now().getTime() - (50 - i * 10) * 1000);
      ctx.flows.add({ wallet: w, mint: MINT2, side: "BUY", tokenRaw: 1n, usd: new D(200), blockTime: bt, availableAt: new Date(bt.getTime() + 1_000), confirmed: true, signature: `late-${w}` });
    }
    const s1 = await ctx.engine.onFlow(MINT2);
    expect(s1.stage).toBe("risk");
    expect(s1.reasons.map((r) => r.code)).toContain("ENTRY_WINDOW_CLOSED");

    // restart: a fresh engine instance continues from the DB, calendar unchanged
    const engine2 = new PaperEngine({ ...(ctx.engine as unknown as { d: ConstructorParameters<typeof PaperEngine>[0] }).d, workerId: "w-restarted" });
    expect(await engine2.recover()).toBe(0);
    ctx.clock.set(new Date(T0.getTime() + 168 * 3_600_000 + 1_000));
    await engine2.tick();
    const s = (await getSession(pool, ctx.sessionId))!;
    expect(["COMPLETED", "INCOMPLETE"]).toContain(s.state);
    expect(s.t_end_snapshot).not.toBeNull();
    expect(new Date(s.t0!).toISOString()).toBe(T0.toISOString());
    const opening = await pool.query(`SELECT count(*)::int AS n FROM ledger_transactions WHERE session_id=$1 AND kind='OPENING'`, [ctx.sessionId]);
    expect(opening.rows[0].n).toBe(1);
    // the long jump without ticks is recorded as a gap, not hidden
    const gaps = await pool.query(`SELECT * FROM data_gaps WHERE session_id=$1`, [ctx.sessionId]);
    expect(gaps.rowCount).toBeGreaterThan(0);
  });

  it("restart in the middle of an order resolves it conservatively and never creates a second fill", async () => {
    const ctx = await setup();
    await ctx.engine.tick();
    const res = await reserveEntry(pool, {
      sessionId: ctx.sessionId,
      idempotencyKey: "crash-1",
      mint: MINT,
      signalId: null,
      inputs: {},
      at: ctx.clock.now(),
      deployerGroup: null,
      decide: async () => ({ approved: true, usdcRaw: 25_000_000n, lamports: 2_200_000n, notionalUsd: new D(25), decision: {} }),
    });
    if (res.status !== "RESERVED") throw new Error();
    await createAttempt(pool, { intentId: res.intentId, attemptNo: 1, model: {}, fencingToken: null, at: ctx.clock.now() }); // crash here
    const recovered = await ctx.engine.recover();
    expect(recovered).toBe(1);
    const p = (await pool.query(`SELECT status FROM positions WHERE id=$1`, [res.positionId])).rows[0];
    expect(p.status).toBe("CLOSED");
    const fills = await pool.query(`SELECT count(*)::int AS n FROM fills WHERE position_id=$1`, [res.positionId]);
    expect(fills.rows[0].n).toBe(0);
    const l = await ledgerOf(ctx.sessionId);
    expect(l.balance(Bucket.RESERVED, USDC_MINT)).toBe(0n);
    expect(l.balance(Bucket.FEE_FAILED_TX, NATIVE_SOL)).toBe(105_000n); // conservative estimated cost of a possibly-sent attempt
    expect(await ctx.engine.recover()).toBe(0);
  });

  it("missing SOL blocks entries instead of a free top-up", async () => {
    const ctx = await setup();
    ctx.market.solUsd = new D(150);
    await ctx.engine.tick();
    // drain SOL by reserving it for a dummy intent
    await withTx(pool, async (c) => {
      const l = await loadLedger(c, ctx.sessionId);
      const lamports = l.balance(Bucket.WALLET, NATIVE_SOL) - 1_000_000n;
      await c.query(`INSERT INTO ledger_transactions (id, session_id, idempotency_key, kind, at) VALUES ('ltx_drain',$1,'drain','COMPENSATION',$2)`, [ctx.sessionId, ctx.clock.now()]);
      await c.query(`INSERT INTO ledger_entries (tx_id, session_id, bucket, asset, amount_raw) VALUES ('ltx_drain',$1,'wallet',$2,$3),('ltx_drain',$1,'equity:external',$2,$4)`, [
        ctx.sessionId,
        NATIVE_SOL,
        (-lamports).toString(),
        lamports.toString(),
      ]);
    });
    ctx.quotes.script(USDC_MINT, MINT, [{ outNetRaw: 1_000_000n }]);
    ctx.buys(ctx.clock.now());
    const r = await ctx.engine.onFlow(MINT);
    expect(r.stage).toBe("risk");
    expect(r.reasons.map((x) => x.code)).toContain("INSUFFICIENT_SOL_RESERVE");
  });
});
