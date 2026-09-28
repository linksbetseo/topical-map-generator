import { describe, expect, it } from "vitest";
import { D, ReasonCode } from "@solbot/domain";
import { parseConfig } from "@solbot/config";
import {
  buildClusters,
  deriveEdges,
  evaluateConfluence,
  evaluateExit,
  evaluateTokenFilters,
  qualifyWallet,
  reconstructEpisodes,
  walletMetrics,
  type FlowEvent,
  type TokenView,
  type WalletEvent,
  type WalletStatus,
} from "../src/index.ts";

const cfg = parseConfig();
const T0 = new Date("2026-10-01T12:00:00Z");
const day = 86_400_000;

function trades(wallet: string, n: number, opts: { lossEvery?: number; tokens?: number; days?: number; pnlWin?: string; pnlLoss?: string } = {}): WalletEvent[] {
  const out: WalletEvent[] = [];
  for (let i = 0; i < n; i++) {
    const mint = `M${i % (opts.tokens ?? n)}`;
    const t = new Date(T0.getTime() - 25 * day + (i % (opts.days ?? 10)) * day + i * 60_000);
    const loss = opts.lossEvery !== undefined && i % opts.lossEvery === 0;
    out.push({ wallet, mint, blockTime: t, availableAt: t, kind: "SWAP_BUY", tokenRaw: 1000n, usd: new D(100), signature: `b${wallet}${i}` });
    const t2 = new Date(t.getTime() + 30_000);
    out.push({ wallet, mint, blockTime: t2, availableAt: t2, kind: "SWAP_SELL", tokenRaw: 1000n, usd: new D(loss ? opts.pnlLoss ?? "80" : opts.pnlWin ?? "150"), signature: `s${wallet}${i}` });
  }
  return out;
}

describe("wallet qualification", () => {
  it("100% winners with three trades do not qualify", () => {
    const ev = trades("W", 3);
    const q = qualifyWallet(walletMetrics(reconstructEpisodes(ev, T0, new Map()), ev, T0), { infrastructure: false, deployerOfObservedToken: false }, cfg);
    expect(q.status).toBe("REJECTED");
    expect(q.metrics.profitFactor).toBeNull(); // no losses: PF undefined, not infinite
  });

  it("a broad, profitable, loss-including history qualifies", () => {
    const ev = trades("W", 40, { lossEvery: 5, tokens: 25, days: 10 });
    const q = qualifyWallet(walletMetrics(reconstructEpisodes(ev, T0, new Map()), ev, T0), { infrastructure: false, deployerOfObservedToken: false }, cfg);
    expect(q.reasons).toEqual([]);
    expect(q.status).toBe("QUALIFIED");
  });

  it("airdrops/transfers are never purchases at zero cost and do not inflate PnL", () => {
    const t = new Date(T0.getTime() - 2 * day);
    const ev: WalletEvent[] = [
      { wallet: "W", mint: "AIR", blockTime: t, availableAt: t, kind: "TRANSFER_IN", tokenRaw: 1000n, usd: null, signature: "a1" },
      { wallet: "W", mint: "AIR", blockTime: new Date(t.getTime() + 1000), availableAt: t, kind: "SWAP_SELL", tokenRaw: 1000n, usd: new D(5000), signature: "a2" },
    ];
    const r = reconstructEpisodes(ev, T0, new Map());
    expect(r.episodes[0]!.status).toBe("UNKNOWN_COST_BASIS");
    expect(r.episodes[0]!.pnlUsd).toBeNull();
    expect(walletMetrics(r, ev, T0).totalPnlUsd.toString()).toBe("0");
  });

  it("unsold losing positions count with a zero lower bound", () => {
    const t = new Date(T0.getTime() - 3 * day);
    const ev: WalletEvent[] = [
      ...trades("W", 2, { pnlWin: "120" }),
      { wallet: "W", mint: "BAG", blockTime: t, availableAt: t, kind: "SWAP_BUY", tokenRaw: 1n, usd: new D(500), signature: "bag" },
    ];
    const r = reconstructEpisodes(ev, T0, new Map());
    const open = r.episodes.find((e) => e.mint === "BAG")!;
    expect(open.status).toBe("OPEN_ZERO_LOWER_BOUND");
    expect(open.pnlUsd!.toString()).toBe("-500");
    expect(walletMetrics(r, ev, T0).totalPnlUsd.toString()).toBe("-460");
  });

  it("history after T0 (or known only after T0) is not used", () => {
    const late = new Date(T0.getTime() + 1000);
    const ev: WalletEvent[] = [{ wallet: "W", mint: "X", blockTime: new Date(T0.getTime() - 1000), availableAt: late, kind: "SWAP_BUY", tokenRaw: 1n, usd: new D(1), signature: "x" }];
    expect(reconstructEpisodes(ev, T0, new Map()).episodes).toEqual([]);
  });
});

describe("clusters", () => {
  const watched = new Set(["A", "B", "C", "D"]);
  const at = T0;
  const tr = (from: string, to: string, usd: string, h: number) => ({ from, to, usd: new D(usd), at: new Date(at.getTime() - h * 3_600_000), signature: `${from}${to}${h}` });

  it("two direct transfers >= 20 USD link wallets; one small spam transfer does not", () => {
    const edges = deriveEdges(watched, [tr("A", "B", "25", 5), tr("B", "A", "30", 4), tr("C", "D", "1", 3)], [], new Set(), at, cfg);
    expect(edges.map((e) => `${e.a}-${e.b}:${e.kind}`)).toEqual(["A-B:DIRECT_TRANSFERS"]);
  });

  it("common exchange funder does not link; common private funder alone does not link", () => {
    const funds = [tr("CEX", "A", "100", 10), tr("CEX", "B", "100", 10), tr("F", "C", "100", 10), tr("F", "D", "100", 10)];
    expect(deriveEdges(watched, funds, [], new Set(["CEX"]), at, cfg)).toEqual([]);
  });

  it("common private funder + 3 shared mints bought within 60 s links", () => {
    const funds = [tr("F", "C", "100", 10), tr("F", "D", "100", 10)];
    const buys = ["m1", "m2", "m3"].flatMap((m, i) => [
      { wallet: "C", mint: m, at: new Date(at.getTime() - (i + 1) * day) },
      { wallet: "D", mint: m, at: new Date(at.getTime() - (i + 1) * day + 45_000) },
    ]);
    const edges = deriveEdges(watched, funds, buys, new Set(), at, cfg);
    expect(edges).toHaveLength(1);
    expect(edges[0]!.kind).toBe("COMMON_FUNDER");
    const cl = buildClusters(["A", "B", "C", "D"], new Set(["A", "B", "C", "D"]), edges, at);
    expect(cl.get("C")!.clusterId).toBe(cl.get("D")!.clusterId);
    expect(cl.get("A")!.clusterId).not.toBe(cl.get("C")!.clusterId);
  });
});

describe("confluence_v1", () => {
  const now = new Date("2026-10-02T10:00:00Z");
  const status = (cluster: string, q = true, link: "CHECKED" | "UNKNOWN" = "CHECKED"): WalletStatus => ({ qualified: q, clusterId: cluster, linkCheck: link });
  const buy = (wallet: string, secAgo: number, lagMs = 2_000, usd = "150", raw = 1000n): FlowEvent => {
    const bt = new Date(now.getTime() - secAgo * 1000);
    return { wallet, mint: "MINT", side: "BUY", tokenRaw: raw, usd: new D(usd), blockTime: bt, availableAt: new Date(bt.getTime() + lagMs), confirmed: true, signature: `sig-${wallet}-${secAgo}` };
  };

  it("3 qualified wallets from 3 clusters within 180 s => signal with cautious wording", () => {
    const w = new Map([["A", status("c1")], ["B", status("c2")], ["C", status("c3")]]);
    const r = evaluateConfluence("MINT", now, [buy("A", 100), buy("B", 60), buy("C", 10)], w, cfg);
    expect(r.signal).not.toBeNull();
    expect(r.signal!.summary).toContain("bez wykrytego powiązania");
    expect(r.signal!.summary).not.toMatch(/niezależn/);
    expect(r.signal!.ttlUntil.getTime() - now.getTime()).toBe(30_000);
  });

  it("three wallets of the same cluster are not three confirmations", () => {
    const w = new Map([["A", status("c1")], ["B", status("c1")], ["C", status("c1")]]);
    const r = evaluateConfluence("MINT", now, [buy("A", 100), buy("B", 60), buy("C", 10)], w, cfg);
    expect(r.signal).toBeNull();
    expect(r.reasons[0]!.code).toBe(ReasonCode.CONFLUENCE_NOT_MET);
  });

  it("UNKNOWN link check, unqualified wallet, small buy, late delivery and round trips are excluded", () => {
    const w = new Map([["A", status("c1")], ["B", status("c2", true, "UNKNOWN")], ["C", status("c3", false)], ["D", status("c4")], ["E", status("c5")], ["F", status("c6")]]);
    const sellF: FlowEvent = { ...buy("F", 5), side: "SELL", tokenRaw: 500n, signature: "sellF" };
    const r = evaluateConfluence("MINT", now, [buy("A", 100), buy("B", 90), buy("C", 80), buy("D", 70, 16_000), buy("E", 60, 1_000, "99.99"), buy("F", 50), sellF], w, cfg);
    expect(r.signal).toBeNull();
    const reasons = Object.fromEntries(r.excluded.map((x) => [x.wallet, x.reason]));
    expect(reasons.B).toMatch(/UNKNOWN/);
    expect(reasons.C).toMatch(/not qualified/);
    expect(reasons.D).toMatch(/delivered 16000 ms/);
    expect(reasons.E).toMatch(/< 100/);
    expect(reasons.F).toMatch(/retained 5000 bps/);
  });

  it("events not yet available at decision time do not create a signal (no look-ahead)", () => {
    const w = new Map([["A", status("c1")], ["B", status("c2")], ["C", status("c3")]]);
    const late = buy("C", 1, 3_000); // available 2 s after `now`
    expect(evaluateConfluence("MINT", now, [buy("A", 100), buy("B", 60), late], w, cfg).signal).toBeNull();
    // the same data re-evaluated later does not rewrite the earlier decision; it is a new evaluation
    expect(evaluateConfluence("MINT", new Date(now.getTime() + 3_000), [buy("A", 100), buy("B", 60), late], w, cfg).signal).not.toBeNull();
  });

  it("buys older than 180 s do not count", () => {
    const w = new Map([["A", status("c1")], ["B", status("c2")], ["C", status("c3")]]);
    expect(evaluateConfluence("MINT", now, [buy("A", 181), buy("B", 60), buy("C", 10)], w, cfg).signal).toBeNull();
  });
});

describe("token filters", () => {
  const now = new Date("2026-10-02T10:00:00Z");
  const view = (over: Partial<TokenView> = {}): TokenView => ({
    mint: "MINT",
    firstPoolId: "pool",
    firstPoolCreatedAt: new Date(now.getTime() - 5 * 3_600_000),
    liquidityUsd: new D(250_000),
    volume5mUsd: new D(40_000),
    sells5m: 40,
    priceChange5mPct: new D(5),
    launchpad: null,
    graduatedAt: null,
    availableAt: new Date(now.getTime() - 5_000),
    ...over,
  });
  const risk = { passed: true, reasons: [], availableAt: new Date(now.getTime() - 10_000) };
  const holders = { ok: true, reasons: [], holderCount: 800, top10Bps: 2_200, largestBps: 600, availableAt: new Date(now.getTime() - 30_000) };

  it("passes a healthy token and reports every check", () => {
    const r = evaluateTokenFilters(now, view(), risk, holders, cfg);
    expect(r.reasons).toEqual([]);
    expect(r.checks.length).toBeGreaterThan(8);
  });

  it("missing holder data, freeze authority and unknown metrics block (never assumed ok)", () => {
    expect(evaluateTokenFilters(now, view(), risk, null, cfg).reasons.map((x) => x.code)).toContain(ReasonCode.HOLDER_DATA_UNAVAILABLE);
    const frozen = { passed: false, reasons: [{ code: ReasonCode.FREEZE_AUTHORITY_ACTIVE }], availableAt: risk.availableAt };
    expect(evaluateTokenFilters(now, view(), frozen, holders, cfg).reasons.map((x) => x.code)).toContain(ReasonCode.FREEZE_AUTHORITY_ACTIVE);
    expect(evaluateTokenFilters(now, view({ volume5mUsd: null }), risk, holders, cfg).reasons.map((x) => x.code)).toContain(ReasonCode.DATA_REQUIREMENT_NOT_MET);
  });

  it("thresholds: age, liquidity, pump, concentration, pre-migration", () => {
    const codes = (v: TokenView, h = holders) => evaluateTokenFilters(now, v, risk, h, cfg).reasons.map((x) => x.code);
    expect(codes(view({ firstPoolCreatedAt: new Date(now.getTime() - 5 * 60_000) }))).toContain(ReasonCode.TOKEN_AGE_OUT_OF_RANGE);
    expect(codes(view({ firstPoolCreatedAt: new Date(now.getTime() - 73 * 3_600_000) }))).toContain(ReasonCode.TOKEN_AGE_OUT_OF_RANGE);
    expect(codes(view({ liquidityUsd: new D(99_999) }))).toContain(ReasonCode.LIQUIDITY_TOO_LOW);
    expect(codes(view({ priceChange5mPct: new D("30.01") }))).toContain(ReasonCode.PRICE_PUMP_5M_TOO_HIGH);
    expect(codes(view(), { ...holders, top10Bps: 3_001 })).toContain(ReasonCode.TOP10_CONCENTRATION_TOO_HIGH);
    expect(codes(view(), { ...holders, largestBps: 1_001 })).toContain(ReasonCode.LARGEST_HOLDER_TOO_HIGH);
    expect(codes(view({ launchpad: "pump.fun" }))).toContain(ReasonCode.PRE_MIGRATION_BONDING_CURVE);
  });

  it("stale data is rejected and future data is a look-ahead error", () => {
    expect(evaluateTokenFilters(now, view({ availableAt: new Date(now.getTime() - 31_000) }), risk, holders, cfg).reasons.map((x) => x.code)).toContain(ReasonCode.DATA_STALE);
    expect(() => evaluateTokenFilters(now, view({ availableAt: new Date(now.getTime() + 1) }), risk, holders, cfg)).toThrow(/look-ahead/);
  });
});

describe("exit rules", () => {
  const entry = new Date("2026-10-02T10:00:00Z");
  const p = { costUsd: new D(25), entryFilledAt: entry, trailingPeakUsd: null, entryWallets: [] as { wallet: string; qtyAtEntryRaw: bigint; qtyNowRaw: bigint | null }[] };
  const ctx = (over: Partial<Parameters<typeof evaluateExit>[1]> = {}) => ({ now: new Date(entry.getTime() + 60_000), tEnd: new Date(entry.getTime() + 100 * 3_600_000), policyViolation: null, emergencyWindDown: false, netLiquidationUsd: new D(25), ...over });

  it("stop loss at -10% NLR (trigger only)", () => {
    expect(evaluateExit(p, ctx({ netLiquidationUsd: new D("22.51") }), cfg).exit).toBe(false);
    const d = evaluateExit(p, ctx({ netLiquidationUsd: new D("22.50") }), cfg);
    expect(d.exit && d.code).toBe(ReasonCode.EXIT_STOP_LOSS);
  });

  it("priority: policy violation beats stop loss; emergency beats take profit", () => {
    const a = evaluateExit(p, ctx({ netLiquidationUsd: new D(1), policyViolation: "freeze authority enabled" }), cfg);
    expect(a.exit && a.code).toBe(ReasonCode.EXIT_POLICY_VIOLATION);
    expect(a.exit && a.kind).toBe("EMERGENCY");
    const b = evaluateExit(p, ctx({ netLiquidationUsd: new D(40), emergencyWindDown: true }), cfg);
    expect(b.exit && b.code).toBe(ReasonCode.EXIT_EMERGENCY);
  });

  it("trailing activates at +15% and fires 8% below the fresh peak; take profit at +25%", () => {
    const d1 = evaluateExit(p, ctx({ netLiquidationUsd: new D("29") }), cfg); // +16%
    expect(d1.exit).toBe(false);
    expect(d1.trailingPeakUsd!.toString()).toBe("29");
    const d2 = evaluateExit({ ...p, trailingPeakUsd: new D(30) }, ctx({ netLiquidationUsd: new D("27.6") }), cfg);
    expect(d2.exit && d2.code).toBe(ReasonCode.EXIT_TRAILING_STOP);
    const d3 = evaluateExit(p, ctx({ netLiquidationUsd: new D("31.25") }), cfg);
    expect(d3.exit && d3.code).toBe(ReasonCode.EXIT_TAKE_PROFIT);
  });

  it("distribution: 2 entry wallets reduced by >= 50% (transfers not counted upstream)", () => {
    const wallets = [
      { wallet: "A", qtyAtEntryRaw: 100n, qtyNowRaw: 50n },
      { wallet: "B", qtyAtEntryRaw: 100n, qtyNowRaw: 10n },
      { wallet: "C", qtyAtEntryRaw: 100n, qtyNowRaw: null },
    ];
    const d = evaluateExit({ ...p, entryWallets: wallets }, ctx(), cfg);
    expect(d.exit && d.code).toBe(ReasonCode.EXIT_DISTRIBUTION);
  });

  it("time stop after 4 h and session end, even without a fresh mark", () => {
    const d = evaluateExit(p, ctx({ now: new Date(entry.getTime() + 4 * 3_600_000), netLiquidationUsd: null }), cfg);
    expect(d.exit && d.code).toBe(ReasonCode.EXIT_TIME_STOP);
    const e = evaluateExit(p, ctx({ tEnd: new Date(entry.getTime() + 60_000), netLiquidationUsd: null }), cfg);
    expect(e.exit && e.code).toBe(ReasonCode.EXIT_SESSION_END);
  });
});

describe("episode dust (provider UI float amounts)", () => {
  it("a sell leaving <= 0.1% of the peak closes the episode", () => {
    const t = new Date(T0.getTime() - 3 * day);
    const ev: WalletEvent[] = [
      { wallet: "W", mint: "F", blockTime: t, availableAt: t, kind: "SWAP_BUY", tokenRaw: 1_000_000_000n, usd: new D(100), signature: "b" },
      { wallet: "W", mint: "F", blockTime: new Date(t.getTime() + 60_000), availableAt: t, kind: "SWAP_SELL", tokenRaw: 999_999_999n, usd: new D(130), signature: "s" },
    ];
    const r = reconstructEpisodes(ev, T0, new Map());
    expect(r.episodes).toHaveLength(1);
    expect(r.episodes[0]!.status).toBe("CLOSED");
    expect(r.episodes[0]!.pnlUsd!.toString()).toBe("30");
  });

  it("a partial sell (> 0.1% left) keeps the episode open and counts it at the zero lower bound", () => {
    const t = new Date(T0.getTime() - 3 * day);
    const ev: WalletEvent[] = [
      { wallet: "W", mint: "F", blockTime: t, availableAt: t, kind: "SWAP_BUY", tokenRaw: 1_000n, usd: new D(100), signature: "b" },
      { wallet: "W", mint: "F", blockTime: new Date(t.getTime() + 60_000), availableAt: t, kind: "SWAP_SELL", tokenRaw: 500n, usd: new D(70), signature: "s" },
    ];
    const r = reconstructEpisodes(ev, T0, new Map());
    expect(r.episodes[0]!.status).toBe("OPEN_ZERO_LOWER_BOUND");
    expect(r.episodes[0]!.pnlUsd!.toString()).toBe("-30");
  });
});
