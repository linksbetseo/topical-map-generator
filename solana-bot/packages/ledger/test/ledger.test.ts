import { describe, expect, it } from "vitest";
import fc from "fast-check";
import { D, NATIVE_SOL, USDC_MINT, ValuationStatus } from "@solbot/domain";
import {
  Bucket,
  Ledger,
  LedgerError,
  buyFillTx,
  computeEquity,
  openingAllocation,
  openingTx,
  releaseTx,
  rentRecoveryTx,
  reserveTx,
  sellFillTx,
  type FeeItem,
  type PositionMark,
} from "../src/index.ts";

const S = "ses_test";
const T0 = new Date("2026-10-01T12:00:00Z");
const MINT = "Token1111111111111111111111111111111111111";
let n = 0;
const base = (k?: string) => ({ id: `ltx_${++n}`, sessionId: S, idempotencyKey: k ?? `k${n}`, at: T0 });

const fx = { usdcUsd: new D("0.9999"), solUsd: new D("150.25"), at: T0, source: "test" };

function opened(): Ledger {
  const l = new Ledger(S);
  const a = openingAllocation(new D(500), new D(20), fx.usdcUsd, fx.solUsd);
  l.post(openingTx(base("open"), a.usdcRaw, a.lamports));
  return l;
}

const modelFees = (lamports: bigint): FeeItem[] => [
  { kind: "BASE_NETWORK", asset: NATIVE_SOL, amountRaw: 5_000n, usdFx: fx.solUsd, source: "MODEL_ESTIMATE", includedInQuote: false, isEstimate: true },
  { kind: "PRIORITY", asset: NATIVE_SOL, amountRaw: lamports, usdFx: fx.solUsd, source: "MODEL_ESTIMATE", includedInQuote: false, isEstimate: true },
];
const platformFee: FeeItem = { kind: "PLATFORM", asset: MINT, amountRaw: 12_345n, usdFx: null, source: "QUOTE", includedInQuote: true, isEstimate: false };

describe("opening balance", () => {
  it("is 500 USD of value at T0 FX, SOL allocation included (not additional)", () => {
    const a = openingAllocation(new D(500), new D(20), fx.usdcUsd, fx.solUsd);
    // 480 USD / 0.9999 = 480.048004.. USDC -> floor to 6 dp
    expect(a.usdcRaw).toBe(480_048_004n);
    // 20 / 150.25 SOL -> floor to lamports
    expect(a.lamports).toBe(133_111_480n);
    expect(a.valueUsd.lte(500)).toBe(true);
    // rounding shortfall below one raw unit of each asset
    expect(a.roundingShortfallUsd.lt(new D("0.000001").add(new D("0.000000001").mul(fx.solUsd)))).toBe(true);
    const l = opened();
    const e = computeEquity(l, fx, new Map(), { closeAccountFeeLamportsPerPosition: 5_000n });
    if (!e.ok) throw new Error("fx");
    expect(e.equityTotalLowerBoundUsd.toFixed(6)).toBe(a.valueUsd.toFixed(6));
    expect(e.equityTotalLowerBoundUsd.sub(500).abs().lt("0.000002")).toBe(true);
  });
});

describe("ledger invariants", () => {
  it("rejects unbalanced, zero and cross-session transactions", () => {
    const l = new Ledger(S);
    expect(() =>
      l.post({ ...base(), kind: "COMPENSATION", entries: [{ bucket: Bucket.WALLET, asset: USDC_MINT, amountRaw: 1n }] }),
    ).toThrow(LedgerError);
    expect(() => l.post({ ...base(), kind: "COMPENSATION", entries: [] })).toThrow(LedgerError);
    expect(() => l.post({ ...base(), sessionId: "other", kind: "OPENING", entries: [] })).toThrow(/another session/);
  });

  it("never lets holdings go negative (reservation beyond balance)", () => {
    const l = opened();
    expect(() => l.post(reserveTx(base(), { usdcRaw: 10n ** 12n, lamports: 0n }))).toThrow(/would become/);
  });

  it("is idempotent under duplicate delivery and refuses conflicting payloads", () => {
    const l = opened();
    const t = reserveTx(base("res-1"), { usdcRaw: 25_000_000n, lamports: 3_000_000n });
    expect(l.post(t)).toBe(true);
    expect(l.post({ ...t, id: "ltx_dup" })).toBe(false);
    expect(l.balance(Bucket.RESERVED, USDC_MINT)).toBe(25_000_000n);
    expect(() => l.post(reserveTx(base("res-1"), { usdcRaw: 1n, lamports: 0n }))).toThrow(/IDEMPOTENCY|different payload/);
  });

  it("stored transactions are frozen (append-only)", () => {
    const l = opened();
    const stored = l.transactions()[0]!;
    expect(() => {
      (stored.entries as unknown as { amountRaw: bigint }[])[0]!.amountRaw = 1n;
    }).toThrow();
  });

  it("random valid operation sequences keep the accounting identity (property)", () => {
    fc.assert(
      fc.property(
        fc.array(
          fc.record({
            op: fc.constantFrom("reserve", "release", "buy", "sell"),
            usdc: fc.bigInt({ min: 1n, max: 30_000_000n }),
            lam: fc.bigInt({ min: 0n, max: 5_000_000n }),
            tok: fc.bigInt({ min: 1n, max: 10n ** 12n }),
          }),
          { maxLength: 40 },
        ),
        (ops) => {
          const l = opened();
          for (const o of ops) {
            try {
              if (o.op === "reserve") l.post(reserveTx(base(), { usdcRaw: o.usdc, lamports: o.lam }));
              if (o.op === "release") l.post(releaseTx(base(), { usdcRaw: o.usdc, lamports: o.lam }));
              if (o.op === "buy") l.post(buyFillTx(base(), { usdcInRaw: o.usdc, tokenMint: MINT, tokenOutRaw: o.tok, fees: modelFees(o.lam), rentLamports: 0n }));
              if (o.op === "sell") l.post(sellFillTx(base(), { tokenMint: MINT, tokenInRaw: o.tok, usdcOutRaw: o.usdc, fees: [] }));
            } catch (e) {
              if (!(e instanceof LedgerError) || e.code !== "NEGATIVE_BALANCE") throw e;
            }
          }
          const id = l.verifyIdentity();
          const noNegative = l.allBalances().every((b) => !["wallet", "reserved", "rent_locked"].includes(b.bucket) || b.amountRaw >= 0n);
          return id.ok && noNegative;
        },
      ),
      { numRuns: 200 },
    );
  });
});

describe("fees and rent", () => {
  it("fees included in the quote never move balances (no double deduction)", () => {
    const l = opened();
    l.post(reserveTx(base(), { usdcRaw: 25_000_000n, lamports: 3_000_000n }));
    const before = l.holding(NATIVE_SOL);
    l.post(buyFillTx(base(), { usdcInRaw: 25_000_000n, tokenMint: MINT, tokenOutRaw: 1_000_000n, fees: [platformFee], rentLamports: 0n }));
    expect(l.holding(NATIVE_SOL)).toBe(before);
    expect(l.balance(Bucket.WALLET, MINT)).toBe(1_000_000n);
  });

  it("rent reduces liquid funds, is not an expense, and recovery is not trading profit", () => {
    const l = opened();
    const rent = 2_039_280n;
    l.post(reserveTx(base(), { usdcRaw: 25_000_000n, lamports: 105_000n + rent }));
    l.post(buyFillTx(base(), { usdcInRaw: 25_000_000n, tokenMint: MINT, tokenOutRaw: 1_000n, fees: modelFees(100_000n), rentLamports: rent }));
    expect(l.balance(Bucket.RENT_LOCKED, NATIVE_SOL)).toBe(rent);
    expect(l.balance(Bucket.FEE_NETWORK, NATIVE_SOL) + l.balance(Bucket.FEE_PRIORITY, NATIVE_SOL)).toBe(105_000n);

    const mark: PositionMark = { mint: MINT, status: ValuationStatus.FRESH, netUsdcRaw: 25_000_000n, exitFeesLamports: 105_000n, quoteAt: T0 };
    const e1 = computeEquity(l, fx, new Map([[MINT, mark]]), { closeAccountFeeLamportsPerPosition: 5_000n });
    if (!e1.ok) throw new Error();
    expect(e1.rentLockedUsd.gt(0)).toBe(true);
    expect(e1.equityTotalLowerBoundUsd.sub(e1.equityLiquidLowerBoundUsd).toFixed(9)).toBe(e1.rentRecoverableUsd.toFixed(9));

    // full exit, then close the account: total equity changes only by the close fee
    l.post(sellFillTx(base(), { tokenMint: MINT, tokenInRaw: 1_000n, usdcOutRaw: 25_000_000n, fees: modelFees(100_000n) }));
    const e2 = computeEquity(l, fx, new Map(), { closeAccountFeeLamportsPerPosition: 5_000n });
    l.post(rentRecoveryTx(base(), rent, [{ kind: "CLOSE_ACCOUNT", asset: NATIVE_SOL, amountRaw: 5_000n, usdFx: fx.solUsd, source: "MODEL_ESTIMATE", includedInQuote: false, isEstimate: true }]));
    const e3 = computeEquity(l, fx, new Map(), { closeAccountFeeLamportsPerPosition: 5_000n });
    if (!e2.ok || !e3.ok) throw new Error();
    // e2 counts no recoverable rent (no open holdings -> no close cost reserved), so recovery moves rent into liquid SOL minus fee
    expect(e3.equityLiquidLowerBoundUsd.gt(e2.equityLiquidLowerBoundUsd)).toBe(true);
    expect(e3.equityTotalLowerBoundUsd.lt(e2.equityTotalLowerBoundUsd)).toBe(true); // only the close fee is lost
    expect(l.verifyIdentity().ok).toBe(true);
  });
});

describe("conservative valuation", () => {
  it("no route => position lower bound 0, fresh equity unavailable, last mark not used", () => {
    const l = opened();
    l.post(reserveTx(base(), { usdcRaw: 25_000_000n, lamports: 105_000n }));
    l.post(buyFillTx(base(), { usdcInRaw: 25_000_000n, tokenMint: MINT, tokenOutRaw: 1_000n, fees: modelFees(100_000n), rentLamports: 0n }));
    const noRoute: PositionMark = { mint: MINT, status: ValuationStatus.UNLIQUIDATABLE, netUsdcRaw: null, exitFeesLamports: 0n, quoteAt: null };
    const e = computeEquity(l, fx, new Map([[MINT, noRoute]]), { closeAccountFeeLamportsPerPosition: 5_000n });
    if (!e.ok) throw new Error();
    expect(e.positionsLowerBoundUsd.toString()).toBe("0");
    expect(e.equityTotalFreshUsd).toBeNull();
    expect(e.hasProviderUncertainty).toBe(false);
    expect(e.uncertain).toEqual([{ mint: MINT, status: "UNLIQUIDATABLE" }]);
  });

  it("provider outage is flagged as uncertainty, not as liquidation", () => {
    const l = opened();
    l.post(reserveTx(base(), { usdcRaw: 25_000_000n, lamports: 105_000n }));
    l.post(buyFillTx(base(), { usdcInRaw: 25_000_000n, tokenMint: MINT, tokenOutRaw: 1_000n, fees: modelFees(100_000n), rentLamports: 0n }));
    const e = computeEquity(l, fx, new Map(), { closeAccountFeeLamportsPerPosition: 0n });
    if (!e.ok) throw new Error();
    expect(e.hasProviderUncertainty).toBe(true);
  });

  it("missing FX is reported, never defaulted to 1.0", () => {
    const e = computeEquity(opened(), { ...fx, usdcUsd: null }, new Map(), { closeAccountFeeLamportsPerPosition: 0n });
    expect(e).toEqual({ ok: false, reason: "FX_MISSING", missing: ["USDC/USD"] });
  });
});
