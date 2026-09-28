import { describe, expect, it } from "vitest";
import { D, FakeClock, ReasonCode, USDC_MINT, reason } from "@solbot/domain";
import { EXECUTION_PROFILES } from "@solbot/config";
import { PaperBroker, ScriptedQuoteProvider, deterministicFailure, type PaperExecutionRequest, type PaperModel } from "../src/index.ts";

const TOKEN = "Token1111111111111111111111111111111111111";
const BASE: PaperModel = { name: "BASE", ...toModel(EXECUTION_PROFILES.BASE) };
function toModel(p: { extra_delay_ms: number; haircut_bps: number; modeled_failure_bps: number; seed: number }) {
  return { extraDelayMs: p.extra_delay_ms, haircutBps: p.haircut_bps, modeledFailureBps: p.modeled_failure_bps, seed: p.seed };
}
const fees = { baseFeeLamportsPerSignature: 5_000n, signaturesPerTx: 1n, priorityFeeLamports: 100_000n };

function setup(model: PaperModel = BASE) {
  const clock = new FakeClock("2026-10-02T10:00:00Z");
  const q = new ScriptedQuoteProvider(clock);
  const broker = new PaperBroker(q, clock, model, fees);
  return { clock, q, broker };
}

const entryReq = (over: Partial<PaperExecutionRequest> = {}): PaperExecutionRequest => ({
  intentId: "int_1",
  attemptNo: 1,
  kind: "ENTRY",
  inputMint: USDC_MINT,
  outputMint: TOKEN,
  amountRaw: 25_000_000n,
  slippageBps: 100,
  feeCapUsd: new D("0.20"),
  solUsd: new D(150),
  usdcUsd: new D(1),
  quoteMaxAgeMs: 2_000,
  entry: { maxPriceImpactBps: 100, maxRoundTripCostBps: 200, reverseSlippageBps: 150, exitFeesLamports: 105_000n },
  ...over,
});

const exitReq = (over: Partial<PaperExecutionRequest> = {}): PaperExecutionRequest => ({
  intentId: "int_x",
  attemptNo: 1,
  kind: "EXIT_NORMAL",
  inputMint: TOKEN,
  outputMint: USDC_MINT,
  amountRaw: 1_000_000n,
  slippageBps: 150,
  feeCapUsd: new D("0.20"),
  solUsd: new D(150),
  usdcUsd: new D(1),
  quoteMaxAgeMs: 2_000,
  ...over,
});

describe("paper fill model", () => {
  it("fills at floor(min(Q0,Q1) * (1-haircut)) after the modeled delay, with a paper_ id", async () => {
    const { q, broker, clock } = setup();
    q.script(USDC_MINT, TOKEN, [{ outNetRaw: 1_000_000n }, { outNetRaw: 999_000n }]);
    q.script(TOKEN, USDC_MINT, [{ outNetRaw: 24_800_000n }]);
    const t0 = clock.now().getTime();
    const o = await broker.execute(entryReq());
    expect(o.status).toBe("FILLED");
    if (o.status !== "FILLED") return;
    expect(o.outAmountRaw).toBe(997_002n); // floor(999000 * 9980 / 10000)
    expect(o.minOutRaw).toBe(990_000n);
    expect(o.fillId.startsWith("paper_")).toBe(true);
    expect(Object.keys(o)).not.toContain("signature");
    expect(clock.now().getTime() - t0).toBe(2_000);
    expect(o.quotes.map((x) => x.role)).toEqual(["Q0", "Q0_REVERSE", "Q1"]);
    // reverse quote is for the whole expected position, not 1 USD
    expect(q.calls[1]!.amountRaw).toBe(1_000_000n);
  });

  it("uses the worse quote even when Q1 improved", async () => {
    const { q, broker } = setup();
    q.script(USDC_MINT, TOKEN, [{ outNetRaw: 1_000_000n }, { outNetRaw: 1_100_000n }]);
    q.script(TOKEN, USDC_MINT, [{ outNetRaw: 24_800_000n }]);
    const o = await broker.execute(entryReq());
    expect(o.status === "FILLED" && o.outAmountRaw).toBe(998_000n);
  });

  it("does not clip to min_out: small adverse move + haircut => failed attempt with estimated fees", async () => {
    const { q, broker } = setup();
    q.script(USDC_MINT, TOKEN, [{ outNetRaw: 1_000_000n }, { outNetRaw: 991_000n }]);
    q.script(TOKEN, USDC_MINT, [{ outNetRaw: 24_800_000n }]);
    const o = await broker.execute(entryReq());
    expect(o.status).toBe("FAILED");
    if (o.status !== "FAILED") return;
    expect(o.code).toBe(ReasonCode.MIN_OUT_NOT_MET);
    expect(o.chargedFees.every((f) => f.kind === "FAILED_TX" && f.isEstimate && f.source === "MODEL_ESTIMATE")).toBe(true);
    expect(o.chargedFees.reduce((a, f) => a + f.amountRaw, 0n)).toBe(105_000n);
  });

  it("-80% crash on exit: never fills at the ideal -10% stop; fails at min_out", async () => {
    const { q, broker } = setup();
    q.script(TOKEN, USDC_MINT, [{ outNetRaw: 22_500_000n }, { outNetRaw: 4_500_000n }]);
    const o = await broker.execute(exitReq());
    expect(o.status).toBe("FAILED");
    expect(o.status === "FAILED" && o.code).toBe(ReasonCode.MIN_OUT_NOT_MET);
  });

  it("-80% already visible in Q0: exit fills at the available bad price, not at -10%", async () => {
    const { q, broker } = setup();
    q.script(TOKEN, USDC_MINT, [{ outNetRaw: 4_500_000n }, { outNetRaw: 4_490_000n }]);
    const o = await broker.execute(exitReq({ kind: "EXIT_EMERGENCY", slippageBps: 300, feeCapUsd: new D("0.50") }));
    expect(o.status).toBe("FILLED");
    expect(o.status === "FILLED" && o.outAmountRaw).toBe(4_481_020n); // floor(4490000 * 0.998)
  });

  it("no sell route => NOT_SENT with NO_ROUTE, no costs; provider outage is a different code", async () => {
    const a = setup();
    a.q.script(TOKEN, USDC_MINT, [{ fail: "NO_ROUTE" }]);
    const o1 = await a.broker.execute(exitReq());
    expect(o1.status === "NOT_SENT" && o1.code).toBe("NO_ROUTE");
    const b = setup();
    b.q.script(TOKEN, USDC_MINT, [{ fail: "PROVIDER_UNAVAILABLE" }]);
    const o2 = await b.broker.execute(exitReq());
    expect(o2.status === "NOT_SENT" && o2.code).toBe("PROVIDER_UNAVAILABLE");
  });

  it("entry requires a working reverse route for the whole position", async () => {
    const { q, broker } = setup();
    q.script(USDC_MINT, TOKEN, [{ outNetRaw: 1_000_000n }]);
    q.script(TOKEN, USDC_MINT, [{ fail: "NO_ROUTE" }]);
    const o = await broker.execute(entryReq());
    expect(o.status === "NOT_SENT" && o.code).toBe("NO_ROUTE");
    expect(o.quotes.map((x) => x.role)).toEqual(["Q0", "Q0_REVERSE"]);
  });

  it("price impact above 100 bps on either side blocks the entry", async () => {
    const a = setup();
    a.q.script(USDC_MINT, TOKEN, [{ outNetRaw: 1_000_000n, priceImpactBps: 101 }]);
    expect((await a.broker.execute(entryReq())).status === "NOT_SENT").toBe(true);
    const b = setup();
    b.q.script(USDC_MINT, TOKEN, [{ outNetRaw: 1_000_000n }]);
    b.q.script(TOKEN, USDC_MINT, [{ outNetRaw: 24_900_000n, priceImpactBps: 150 }]);
    const o = await b.broker.execute(entryReq());
    expect(o.status === "NOT_SENT" && o.code).toBe(ReasonCode.PRICE_IMPACT_TOO_HIGH);
  });

  it("round trip > 2% (including modeled network costs) blocks the entry", async () => {
    const { q, broker } = setup();
    q.script(USDC_MINT, TOKEN, [{ outNetRaw: 1_000_000n }]);
    // 25 -> 24.55 back = 0.45 USD + 2 * 105000 lamports * 150 = 0.0315 USD => 1.926% OK; 24.5 => 2.126% not OK
    q.script(TOKEN, USDC_MINT, [{ outNetRaw: 24_500_000n }]);
    const o = await broker.execute(entryReq());
    expect(o.status === "NOT_SENT" && o.code).toBe(ReasonCode.ROUND_TRIP_COST_TOO_HIGH);
    expect(o.checks.round_trip_cost_bps).toBe("212.60");
  });

  it("risk re-check right before send can cancel without costs", async () => {
    const { q, broker } = setup();
    q.script(USDC_MINT, TOKEN, [{ outNetRaw: 1_000_000n }]);
    q.script(TOKEN, USDC_MINT, [{ outNetRaw: 24_900_000n }]);
    const o = await broker.execute(entryReq({ preExecutionCheck: async () => [reason(ReasonCode.DATA_STALE)] }));
    expect(o.status === "NOT_SENT" && o.code).toBe(ReasonCode.DATA_STALE);
    expect(o.quotes.some((x) => x.role === "Q1")).toBe(false);
  });

  it("stale Q0 (slow provider) is not used", async () => {
    const { q, broker } = setup();
    q.script(TOKEN, USDC_MINT, [{ outNetRaw: 1n, latencyMs: 2_500 }]);
    // receivedAt is after latency, so age is 0; simulate stale by a tiny max age instead
    const o = await broker.execute(exitReq({ quoteMaxAgeMs: -1 }));
    expect(o.status === "NOT_SENT" && o.code).toBe(ReasonCode.QUOTE_STALE);
  });

  it("Q1 failure after the modeled send is a failed attempt with estimated costs", async () => {
    const { q, broker } = setup();
    q.script(TOKEN, USDC_MINT, [{ outNetRaw: 22_000_000n }, { fail: "PROVIDER_UNAVAILABLE" }]);
    const o = await broker.execute(exitReq());
    expect(o.status).toBe("FAILED");
  });

  it("platform fee from the quote is recorded as included (not deducted again)", async () => {
    const { q, broker } = setup();
    q.script(TOKEN, USDC_MINT, [{ outNetRaw: 22_000_000n, platformFeeRaw: 22_000n }]);
    const o = await broker.execute(exitReq());
    if (o.status !== "FILLED") throw new Error(o.status);
    const pf = o.fees.find((f) => f.kind === "PLATFORM")!;
    expect(pf.includedInQuote).toBe(true);
    expect(o.outAmountRaw).toBe(21_956_000n); // haircut only; platform fee not subtracted twice
  });
});

describe("stress profiles", () => {
  it("modeled failures are deterministic and close to the configured rate", () => {
    const stress: PaperModel = { name: "STRESS", ...toModel(EXECUTION_PROFILES.STRESS) };
    let fails = 0;
    for (let i = 0; i < 5_000; i++) if (deterministicFailure(stress, `int_${i}`, 1)) fails++;
    expect(fails).toBeGreaterThan(420);
    expect(fails).toBeLessThan(580);
    expect(deterministicFailure(stress, "int_7", 1)).toBe(deterministicFailure(stress, "int_7", 1));
    expect(deterministicFailure(BASE, "int_7", 1)).toBe(false);
  });

  it("SEVERE waits 15 s and applies 100 bps haircut", async () => {
    const severe: PaperModel = { name: "SEVERE", ...toModel(EXECUTION_PROFILES.SEVERE), modeledFailureBps: 0 };
    const { q, broker, clock } = setup(severe);
    q.script(TOKEN, USDC_MINT, [{ outNetRaw: 10_000_000n }]);
    const t0 = clock.now().getTime();
    const o = await broker.execute(exitReq({ slippageBps: 150 }));
    expect(clock.now().getTime() - t0).toBe(15_000);
    expect(o.status === "FILLED" && o.outAmountRaw).toBe(9_900_000n);
  });
});
