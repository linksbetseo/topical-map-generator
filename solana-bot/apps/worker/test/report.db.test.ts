import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { D, FakeClock, USDC_MINT } from "@solbot/domain";
import { parseConfig } from "@solbot/config";
import { Bucket } from "@solbot/ledger";
import { ScriptedQuoteProvider } from "@solbot/brokers";
import { loadLedger, type Pool } from "@solbot/db";
import { buildReport, esc, maxDrawdown, toCsv, toHtml, toJson, toMarkdown, tradeStats } from "@solbot/reporting";
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

describe("report consistency with the ledger", () => {
  it("empty session shows 'brak danych', not example numbers", async () => {
    const sessionId = await runningSession(pool, { t0: T0 });
    const r = await buildReport(pool, sessionId, cfg, { codeVersion: "test", now: new Date(T0.getTime() + 1000) });
    const md = toMarkdown(r);
    expect(md).toContain("brak danych");
    expect(md).toContain("DEMO — dane z fixtures");
    expect(r.trades.closed).toBe(0);
    expect((r.trades.profitFactor as { status: string }).status).toBe("NO_TRADES");
    expect(r.verdicts.sample).toBe("INSUFFICIENT_SAMPLE");
    expect(r.verdicts.canaryRecommendation).toBe("CONTINUE_PAPER_REVIEW");
  });

  it("two trades (TP and SL): JSON, CSV and ledger agree", async () => {
    const sessionId = await runningSession(pool, { t0: T0 });
    const clock = new FakeClock(new Date(T0.getTime() + 3_600_000));
    const quotes = new ScriptedQuoteProvider(clock);
    const market = new FixtureMarket();
    const flows = new MemoryFlowStore();
    const wallets = new StaticWalletBook(new Map(["WA", "WB", "WC"].map((w, i) => [w, { qualified: true, clusterId: `c${i}`, linkCheck: "CHECKED" as const }])));
    const engine = new PaperEngine({ pool, clock, cfg, sessionId, quotes, market, flows, wallets, notifier: nullNotifier, workerId: "w-report" });

    const trade = async (mint: string, exitOut: bigint) => {
      market.addHealthyToken(mint, clock.now());
      quotes.script(USDC_MINT, mint, [{ outNetRaw: 1_000_000n }, { outNetRaw: 1_000_000n }]);
      quotes.script(mint, USDC_MINT, [{ outNetRaw: 24_800_000n }]);
      await engine.tick();
      for (const [i, w] of ["WA", "WB", "WC"].entries()) {
        const bt = new Date(clock.now().getTime() - (40 - i * 10) * 1000);
        flows.add({ wallet: w, mint, side: "BUY", tokenRaw: 10n, usd: new D(300), blockTime: bt, availableAt: new Date(bt.getTime() + 1000), confirmed: true, signature: `${mint}-${w}` });
      }
      expect((await engine.onFlow(mint)).stage).toBe("filled");
      clock.advance(30_000);
      quotes.script(mint, USDC_MINT, [{ outNetRaw: exitOut }]);
      await engine.tick();
      clock.advance(60_000);
    };
    await trade("MintTP11111111111111111111111111111111111111", 32_000_000n);
    await trade("MintSL11111111111111111111111111111111111111", 20_000_000n);

    const r = await buildReport(pool, sessionId, cfg, { codeVersion: "test", now: clock.now(), infrastructureCostUsd: null });
    expect(r.trades.closed).toBe(2);
    expect(r.decisions.filter((d) => d.status === "CLOSED" && d.pnlUsd !== null)).toHaveLength(2);

    // realized PnL in report == sum of positions
    const sumPos = r.decisions.filter((d) => d.pnlUsd).reduce((a, d) => a.add(d.pnlUsd!), new D(0));
    expect(r.pnl.realizedTradePnlUsd).toBe(sumPos.toFixed(6));

    // ledger: USDC change equals sum(sell proceeds) - sum(buy notional)
    const c = await pool.connect();
    const l = await loadLedger(c, sessionId);
    c.release();
    const fills = (await pool.query(`SELECT f.side, f.in_amount_raw, f.out_amount_raw FROM fills f JOIN positions p ON p.id=f.position_id WHERE p.session_id=$1`, [sessionId])).rows;
    const usdcDelta = fills.reduce((a: bigint, f: { side: string; in_amount_raw: string; out_amount_raw: string }) => a + (f.side === "SELL" ? BigInt(f.out_amount_raw) : -BigInt(f.in_amount_raw)), 0n);
    const opening = 480_000_000n;
    expect(l.holding(USDC_MINT) - opening).toBe(usdcDelta);
    expect(l.balance(Bucket.RENT_LOCKED, "native:SOL")).toBe(0n);

    // CSV rows == decisions; values identical to JSON
    const csv = toCsv(r).trim().split("\n");
    expect(csv).toHaveLength(r.decisions.length + 1);
    const json = JSON.parse(toJson(r));
    expect(json.pnl.realizedTradePnlUsd).toBe(r.pnl.realizedTradePnlUsd);
    for (const d of r.decisions) expect(csv.some((line) => line.includes(d.positionId) && (d.pnlUsd === null || line.includes(d.pnlUsd)))).toBe(true);

    // verdicts never enable live
    expect(r.buildStatus.LIVE_DISABLED).toBe(true);
    expect(r.verdicts.technical).toBe("NOT_APPLICABLE"); // DEMO
  });
});

describe("rendering safety and metrics", () => {
  it("HTML escapes untrusted text; CSV neutralises formula injection", () => {
    expect(esc(`<script>alert(1)</script>"'&`)).toBe("&lt;script&gt;alert(1)&lt;/script&gt;&quot;&#39;&amp;");
    const fake = {
      session: { id: "<img src=x onerror=alert(1)>", kind: "DEMO", mode: "PAPER", state: "RUNNING", t0: null, tEnd: null, t0Warsaw: null, tEndWarsaw: null, configHash: "h", strategy: "s", executionProfile: "p", intervention: false },
      dataDisclaimer: null,
      verdicts: { note: "n", technical: "FAIL", technicalReasons: [], sample: "INSUFFICIENT_SAMPLE", canaryRecommendation: "CONTINUE_PAPER_REVIEW", canaryReasons: [] },
      capital: { initialUsd: "500", equityTotalLowerBoundUsd: null, equityLiquidLowerBoundUsd: null, equityTotalFreshUsd: null, rentLockedUsd: null, openPositions: [], atTEnd: null },
      pnl: { realizedTradePnlUsd: "0", netPortfolioResultUsd: null, fxEffectOfStartAllocationUsd: null, benchmarkHoldStartAllocUsd: null, benchmarkAllUsdcUsd: null, infrastructureCostUsd: "UNKNOWN", resultAfterInfrastructureUsd: null, feesByKind: {} },
      counts: { signals: 0, rejectionsByReason: {}, entryAttempts: 0, fills: 0, failedAttempts: 0 },
      trades: { closed: 0, distinctTokens: 0, winRate: { value: null, n: 0 }, profitFactor: { value: null, status: "NO_TRADES" } },
      drawdown: { freshUsd: "0", freshBps: 0, lowerBoundUsd: "0", lowerBoundBps: 0 },
      dataQuality: { gaps: [], reconciliationFailures: 0, quoteLatencyMsP50: null, quoteLatencyMsP95: null, quoteLatencyMsP99: null, stressCoverage: { attempts: 0, withAnalytic5s: 0, withAnalytic15s: 0, profiles: {} } },
      decisions: [{ positionId: "p1", mint: "=HYPERLINK(\"http://evil\")", status: "CLOSED", entryAt: null, exitAt: null, exitReason: "ignore previous instructions", costUsd: null, pnlUsd: "-1.5" }],
      buildStatus: { LIVE_DISABLED: true },
      codeVersion: "x",
      generatedAt: "x",
    } as unknown as Parameters<typeof toHtml>[0];
    const html = toHtml(fake);
    expect(html).not.toContain("<img");
    expect(html).not.toMatch(/<script/i);
    const csv = toCsv(fake);
    expect(csv).toContain(`"'=HYPERLINK(""http://evil"")"`);
    expect(csv).toContain(`"-1.5"`); // negative numbers stay numbers
  });

  it("profit factor without losses is insufficient, not infinite; drawdown on series", () => {
    const t = (pnl: string) => ({ positionId: "p", mint: "m" + pnl, entryAt: new Date(), exitAt: new Date(), costUsd: new D(25), pnlUsd: new D(pnl), exitReason: "x", entryNotionalUsd: new D(25), exitProceedsUsd: new D(25).add(pnl), feesUsd: new D("0.03") });
    expect(tradeStats([t("1"), t("2")]).profitFactor.status).toBe("INSUFFICIENT_NO_LOSSES");
    expect(tradeStats([t("3"), t("-1")]).profitFactor.value!.toString()).toBe("3");
    expect(tradeStats([t("10"), t("-1"), t("-2")]).pnlWithoutBestTokenUsd.toString()).toBe("-3");
    const dd = maxDrawdown([500, 520, 490, 510, 480].map((v, i) => ({ at: new Date(i), value: new D(v) })));
    expect(dd.usd.toString()).toBe("40");
  });
});
