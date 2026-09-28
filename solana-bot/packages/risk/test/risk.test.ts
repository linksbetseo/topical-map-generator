import { describe, expect, it } from "vitest";
import { D, ReasonCode, SessionState } from "@solbot/domain";
import { parseConfig } from "@solbot/config";
import { evaluateEntry, evaluateLossTriggers, exposureUsd, type EntryCandidate, type RiskSnapshot } from "../src/index.ts";

// Exposure/sizing scenarios use the brief's 4 positions; the default (3) is tested separately.
const cfg = parseConfig({ sizing: { max_open_positions: 4 } });
const now = new Date("2026-10-02T10:00:00Z");

function snap(over: Partial<RiskSnapshot> = {}): RiskSnapshot {
  return {
    now,
    sessionState: SessionState.RUNNING,
    tEnd: new Date("2026-10-08T12:00:00Z"),
    entriesPausedByOwner: false,
    equityUsd: new D(500),
    equityHasProviderUncertainty: false,
    equityAtUtcDayStartUsd: new D(500),
    peakEquityUsd: new D(500),
    initialEquityUsd: new D(500),
    usdcUsd: new D(1),
    solUsd: new D(150),
    freeUsdcRaw: 480_000_000n,
    freeLamports: 133_000_000n, // ~19.95 USD
    positions: [],
    pendingEntries: [],
    unresolvedOrderMints: new Set(),
    anyStatusUnknown: false,
    entryAttemptsToday: 0,
    entryNotionalTodayUsd: new D(0),
    lastClosedAtByMint: new Map(),
    staleData: [],
    reconciliationOk: true,
    ...over,
  };
}

const cand: EntryCandidate = { mint: "MintA", deployerGroup: null, entryFeesLamports: 105_000n, exitFeesLamports: 105_000n, rentLamports: 2_039_280n };
const codes = (d: { reasons: { code: string }[] }) => d.reasons.map((r) => r.code);
const pos = (mint: string, cost: string, liq: string, dg: string | null = null) => ({
  mint,
  remainingCostUsd: new D(cost),
  conservativeLiquidationUsd: new D(liq),
  deployerGroup: dg,
});

describe("sizing", () => {
  it("500 USD equity => 25 USD entry", () => {
    const d = evaluateEntry(cand, snap(), cfg);
    expect(d.approved).toBe(true);
    expect(d.notionalUsd!.toString()).toBe("25");
    expect(d.usdcRaw).toBe(25_000_000n);
    expect(d.reserveLamports).toBe(105_000n + 2_039_280n);
  });

  it("5% of equity binds below 500 USD", () => {
    const d = evaluateEntry(cand, snap({ equityUsd: new D(400), equityAtUtcDayStartUsd: new D(400), peakEquityUsd: new D(400) }), cfg);
    expect(d.notionalUsd!.toString()).toBe("20");
  });

  it("exposure uses max(cost, conservative liquidation) and caps at 20% of equity", () => {
    const s = snap({ positions: [pos("a", "25", "30"), pos("b", "25", "0"), pos("c", "25", "10")] });
    expect(exposureUsd(s).toString()).toBe("80");
    const d = evaluateEntry(cand, s, cfg);
    expect(d.notionalUsd!.toString()).toBe("20"); // room 100 - 80
  });

  it("below 10 USD minimum => no entry", () => {
    // exposure 95 of 100 USD allowed -> 5 USD room < 10 USD minimum
    const d = evaluateEntry(cand, snap({ positions: [pos("a", "45", "30"), pos("b", "25", "25"), pos("c", "25", "25")] }), cfg);
    expect(d.approved).toBe(false);
    expect(codes(d)).toContain(ReasonCode.EXPOSURE_LIMIT);
  });

  it("default config allows at most 3 open or reserved positions", () => {
    const d = evaluateEntry(cand, snap({ positions: [pos("a", "1", "1"), pos("b", "1", "1")], pendingEntries: [{ mint: "d", notionalUsd: new D(1), deployerGroup: null }] }), parseConfig());
    expect(codes(d)).toContain(ReasonCode.MAX_POSITIONS_REACHED);
  });

  it("max 4 open or reserved positions", () => {
    const d = evaluateEntry(cand, snap({ positions: [pos("a", "1", "1"), pos("b", "1", "1"), pos("c", "1", "1")], pendingEntries: [{ mint: "d", notionalUsd: new D(1), deployerGroup: null }] }), cfg);
    expect(codes(d)).toContain(ReasonCode.MAX_POSITIONS_REACHED);
  });

  it("daily attempt and notional limits", () => {
    expect(codes(evaluateEntry(cand, snap({ entryAttemptsToday: 8 }), cfg))).toContain(ReasonCode.DAILY_ATTEMPT_LIMIT);
    const d = evaluateEntry(cand, snap({ entryNotionalTodayUsd: new D(95) }), cfg);
    expect(codes(d)).toContain(ReasonCode.DAILY_NOTIONAL_LIMIT);
    expect(evaluateEntry(cand, snap({ entryNotionalTodayUsd: new D(80) }), cfg).notionalUsd!.toString()).toBe("20");
  });
});

describe("gates", () => {
  it("no SOL => no entry (no free top-up)", () => {
    const d = evaluateEntry(cand, snap({ freeLamports: 2_000_000n }), cfg);
    expect(codes(d)).toContain(ReasonCode.INSUFFICIENT_SOL_RESERVE);
  });

  it("stale data, unknown order, reconciliation mismatch and owner pause block entries", () => {
    const d = evaluateEntry(cand, snap({ staleData: ["quote"], anyStatusUnknown: true, reconciliationOk: false, entriesPausedByOwner: true }), cfg);
    expect(codes(d)).toEqual(
      expect.arrayContaining([ReasonCode.DATA_STALE, ReasonCode.UNRESOLVED_ORDER, ReasonCode.RECONCILIATION_MISMATCH, ReasonCode.ENTRIES_PAUSED]),
    );
  });

  it("last 4 hours: management only", () => {
    const d = evaluateEntry(cand, snap({ now: new Date("2026-10-08T08:00:00Z") }), cfg);
    expect(codes(d)).toContain(ReasonCode.ENTRY_WINDOW_CLOSED);
    expect(evaluateEntry(cand, snap({ now: new Date("2026-10-08T07:59:59Z") }), cfg).approved).toBe(true);
  });

  it("24h cooldown, duplicate mint and deployer group", () => {
    expect(codes(evaluateEntry(cand, snap({ lastClosedAtByMint: new Map([["MintA", new Date(now.getTime() - 23 * 3_600_000)]]) }), cfg))).toContain(ReasonCode.MINT_COOLDOWN);
    expect(evaluateEntry(cand, snap({ lastClosedAtByMint: new Map([["MintA", new Date(now.getTime() - 24 * 3_600_000)]]) }), cfg).approved).toBe(true);
    expect(codes(evaluateEntry(cand, snap({ positions: [pos("MintA", "1", "1")] }), cfg))).toContain(ReasonCode.POSITION_ALREADY_OPEN);
    expect(codes(evaluateEntry({ ...cand, deployerGroup: "g1" }, snap({ positions: [pos("x", "1", "1", "g1")] }), cfg))).toContain(ReasonCode.DEPLOYER_GROUP_OVERLAP);
  });

  it("session not RUNNING blocks entries (EXIT_ONLY, PAUSED_DATA, HALTED_RISK)", () => {
    for (const st of [SessionState.EXIT_ONLY, SessionState.PAUSED_DATA, SessionState.HALTED_RISK, SessionState.READY]) {
      expect(codes(evaluateEntry(cand, snap({ sessionState: st }), cfg))).toContain(ReasonCode.SESSION_NOT_RUNNING);
    }
  });

  it("fee and rent caps", () => {
    expect(codes(evaluateEntry({ ...cand, entryFeesLamports: 2_000_000n }, snap(), cfg))).toContain(ReasonCode.FEE_CAP_EXCEEDED); // 0.30 USD
    expect(codes(evaluateEntry({ ...cand, rentLamports: 7_000_000n }, snap(), cfg))).toContain(ReasonCode.RENT_CAP_EXCEEDED); // 1.05 USD
  });
});

describe("loss triggers", () => {
  it("daily trigger = min(20, 4% of day-start equity) => EXIT_ONLY", () => {
    const e = evaluateLossTriggers(snap({ equityAtUtcDayStartUsd: new D(480), equityUsd: new D("460.8") }), cfg);
    expect(e.dailyThresholdUsd.toString()).toBe("19.2");
    expect(e.action).toBe("EXIT_ONLY");
  });

  it("session loss 50 USD => HALTED_RISK", () => {
    const e = evaluateLossTriggers(snap({ equityUsd: new D(450), equityAtUtcDayStartUsd: new D(455), peakEquityUsd: new D(455) }), cfg);
    expect(e.action).toBe("HALTED_RISK");
    expect(codes(e)).toContain(ReasonCode.SESSION_LOSS_TRIGGER);
  });

  it("high-water drawdown min(50, 10% peak)", () => {
    const e = evaluateLossTriggers(snap({ peakEquityUsd: new D(560), equityUsd: new D(510), equityAtUtcDayStartUsd: new D(515) }), cfg);
    expect(e.drawdownThresholdUsd.toString()).toBe("50");
    expect(e.action).toBe("HALTED_RISK");
  });

  it("provider outage => PAUSED_DATA, never a loss verdict", () => {
    const e = evaluateLossTriggers(snap({ equityUsd: new D(300), equityHasProviderUncertainty: true }), cfg);
    expect(e.action).toBe("PAUSED_DATA");
    expect(codes(e)).not.toContain(ReasonCode.SESSION_LOSS_TRIGGER);
  });

  it("no loss => NONE", () => {
    expect(evaluateLossTriggers(snap(), cfg).action).toBe("NONE");
  });
});
