import { describe, expect, it } from "vitest";
import fc from "fast-check";
import {
  D,
  bps,
  percentPointsToBpsCeil,
  rawToUi,
  reduceByBpsFloor,
  uiToRawFloor,
  usdToRawFloor,
  parseRaw,
  assertSessionTransition,
  assertOrderTransition,
  SessionState,
  OrderState,
  FakeClock,
  utcDayKey,
  canonicalJson,
} from "../src/index.ts";

describe("units", () => {
  it("bps rejects non-integers and out-of-range values", () => {
    expect(() => bps(1.5)).toThrow();
    expect(() => bps(-1)).toThrow();
    expect(() => bps(10_001)).toThrow();
    expect(bps(0)).toBe(0);
    expect(bps(10_000)).toBe(10_000);
  });

  it("reduceByBpsFloor matches the brief formula at edges", () => {
    expect(reduceByBpsFloor(0n, bps(20))).toBe(0n);
    expect(reduceByBpsFloor(1n, bps(20))).toBe(0n); // floor(0.998)
    expect(reduceByBpsFloor(10_000n, bps(20))).toBe(9_980n);
    expect(reduceByBpsFloor(9_999n, bps(100))).toBe(9_899n); // floor(9899.01)
    expect(reduceByBpsFloor(123n, bps(10_000))).toBe(0n);
    // u64::MAX stays exact in bigint arithmetic
    const u64max = 18_446_744_073_709_551_615n;
    expect(reduceByBpsFloor(u64max, bps(0))).toBe(u64max);
    expect(reduceByBpsFloor(u64max, bps(1))).toBe((u64max * 9_999n) / 10_000n);
  });

  it("reduceByBpsFloor never increases and never goes negative (property)", () => {
    fc.assert(
      fc.property(fc.bigInt({ min: 0n, max: 2n ** 80n }), fc.integer({ min: 0, max: 10_000 }), (a, b) => {
        const r = reduceByBpsFloor(a, bps(b));
        return r <= a && r >= 0n;
      }),
    );
  });

  it("raw <-> ui round trip is exact for integer raw values and all decimals 0..18", () => {
    fc.assert(
      fc.property(fc.bigInt({ min: 0n, max: 2n ** 64n }), fc.integer({ min: 0, max: 18 }), (raw, dec) => {
        return uiToRawFloor(rawToUi(raw, dec), dec) === raw;
      }),
    );
  });

  it("uiToRawFloor rounds toward zero", () => {
    expect(uiToRawFloor(new D("1.0000009"), 6)).toBe(1_000_000n);
    expect(usdToRawFloor(new D("20"), 9, new D("150"))).toBe(133_333_333n);
  });

  it("rejects unsupported decimals", () => {
    expect(() => rawToUi(1n, 19)).toThrow();
    expect(() => rawToUi(1n, -1)).toThrow();
    expect(() => rawToUi(1n, 1.5)).toThrow();
  });

  it("parseRaw refuses floats, negatives, numbers and empty values", () => {
    expect(parseRaw("123", "x")).toBe(123n);
    for (const bad of ["1.5", "-1", "", " 1", 1, null, undefined, "1e6"]) {
      expect(() => parseRaw(bad, "x")).toThrow();
    }
  });

  it("price impact percentage points convert to bps rounding up", () => {
    expect(percentPointsToBpsCeil(-0.1)).toBe(10);
    expect(percentPointsToBpsCeil(0.1)).toBe(10);
    expect(percentPointsToBpsCeil(1.001)).toBe(101);
    expect(percentPointsToBpsCeil(0)).toBe(0);
    expect(() => percentPointsToBpsCeil(Number.NaN)).toThrow();
  });
});

describe("state machines", () => {
  it("HALTED_RISK can only settle", () => {
    expect(() => assertSessionTransition(SessionState.HALTED_RISK, SessionState.RUNNING)).toThrow();
    assertSessionTransition(SessionState.HALTED_RISK, SessionState.SETTLING);
  });

  it("session cannot skip validation/bootstrap", () => {
    expect(() => assertSessionTransition(SessionState.DRAFT, SessionState.RUNNING)).toThrow();
    expect(() => assertSessionTransition(SessionState.BOOTSTRAP, SessionState.RUNNING)).toThrow();
  });

  it("order STATUS_UNKNOWN cannot restart a new trade", () => {
    expect(() => assertOrderTransition(OrderState.STATUS_UNKNOWN, OrderState.RESERVED)).toThrow();
    expect(() => assertOrderTransition(OrderState.STATUS_UNKNOWN, OrderState.SIGNED)).toThrow();
    assertOrderTransition(OrderState.STATUS_UNKNOWN, OrderState.CONFIRMED);
  });

  it("paper path never passes through SIGNED/SUBMITTED", () => {
    assertOrderTransition(OrderState.QUOTED, OrderState.PAPER_FILLED);
    expect(() => assertOrderTransition(OrderState.PAPER_FILLED, OrderState.SUBMITTED)).toThrow();
  });
});

describe("clock and hashing", () => {
  it("fake clock never goes backwards and uses UTC day keys", async () => {
    const c = new FakeClock("2026-03-29T00:30:00Z"); // DST change day in Europe/Warsaw
    await c.sleep(2_000);
    expect(c.now().toISOString()).toBe("2026-03-29T00:30:02.000Z");
    expect(() => c.advance(-1)).toThrow();
    expect(utcDayKey(c.now())).toBe("2026-03-29");
  });

  it("canonical json is key-order independent and serializes bigint", () => {
    expect(canonicalJson({ b: 1n, a: [{ d: 2, c: 1 }] })).toBe(canonicalJson({ a: [{ c: 1, d: 2 }], b: 1n }));
    expect(canonicalJson({ x: 10n ** 30n })).toBe('{"x":"1000000000000000000000000000000"}');
  });
});
