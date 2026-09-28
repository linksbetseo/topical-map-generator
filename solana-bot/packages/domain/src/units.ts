import { Decimal } from "decimal.js";

/**
 * Money and unit helpers.
 *
 * Conventions (ASSUMPTIONS A3/A4):
 * - on-chain amounts are `bigint` in raw (smallest) units,
 * - USD values and FX rates are `Decimal`,
 * - percentages always carry the unit in the name: *_bps (integer), *_frac (0..1), *_pct (0..100).
 */

export const D = Decimal.clone({ precision: 40, rounding: Decimal.ROUND_HALF_EVEN, toExpNeg: -40, toExpPos: 40 });
export type Dec = InstanceType<typeof D>;

export const BPS_DENOMINATOR = 10_000n;

export type Bps = number & { readonly __unit: "bps" };

export function bps(value: number): Bps {
  if (!Number.isInteger(value) || value < 0 || value > 10_000) {
    throw new RangeError(`bps must be an integer in [0, 10000], got ${value}`);
  }
  return value as Bps;
}

/** floor(amount * (10000 - cutBps) / 10000) — used for haircut and min_out. */
export function reduceByBpsFloor(amountRaw: bigint, cutBps: Bps): bigint {
  if (amountRaw < 0n) throw new RangeError("amountRaw must be non-negative");
  return (amountRaw * (BPS_DENOMINATOR - BigInt(cutBps))) / BPS_DENOMINATOR;
}

export function parseRaw(value: unknown, field: string): bigint {
  if (typeof value !== "string" || !/^[0-9]+$/.test(value)) {
    throw new TypeError(`${field}: expected non-negative integer string, got ${JSON.stringify(value)}`);
  }
  return BigInt(value);
}

export function rawToUi(amountRaw: bigint, decimals: number): Dec {
  assertDecimals(decimals);
  return new D(amountRaw.toString()).div(new D(10).pow(decimals));
}

/** Converts a UI amount to raw units, rounding toward zero (never overstates holdings). */
export function uiToRawFloor(ui: Dec, decimals: number): bigint {
  assertDecimals(decimals);
  const scaled = ui.mul(new D(10).pow(decimals)).toDecimalPlaces(0, Decimal.ROUND_DOWN);
  return BigInt(scaled.toFixed(0));
}

export function rawToUsd(amountRaw: bigint, decimals: number, usdPerUnit: Dec): Dec {
  return rawToUi(amountRaw, decimals).mul(usdPerUnit);
}

/** Converts USD to raw units of an asset, rounding down (conservative for amounts we receive/hold). */
export function usdToRawFloor(usd: Dec, decimals: number, usdPerUnit: Dec): bigint {
  if (usdPerUnit.lte(0)) throw new RangeError("usdPerUnit must be positive");
  return uiToRawFloor(usd.div(usdPerUnit), decimals);
}

export function assertDecimals(decimals: number): void {
  if (!Number.isInteger(decimals) || decimals < 0 || decimals > 18) {
    throw new RangeError(`unsupported decimals ${decimals}`);
  }
}

/** Jupiter `priceImpact` is in percentage points (-0.1 = -0.1%). Returns ceil(|pp| * 100) bps. */
export function percentPointsToBpsCeil(pp: number): number {
  if (!Number.isFinite(pp)) throw new TypeError("priceImpact must be finite");
  return new D(pp).abs().mul(100).toDecimalPlaces(0, Decimal.ROUND_UP).toNumber();
}

export function usd(value: string | number | Dec): Dec {
  return new D(value);
}

export function minBig(...xs: bigint[]): bigint {
  if (xs.length === 0) throw new RangeError("minBig of empty list");
  return xs.reduce((a, b) => (b < a ? b : a));
}

export function maxBig(...xs: bigint[]): bigint {
  if (xs.length === 0) throw new RangeError("maxBig of empty list");
  return xs.reduce((a, b) => (b > a ? b : a));
}

export function decMin(...xs: Dec[]): Dec {
  return D.min(...xs);
}

export function decMax(...xs: Dec[]): Dec {
  return D.max(...xs);
}

export const LAMPORTS_PER_SOL = 1_000_000_000n;
export const SOL_DECIMALS = 9;
export const USDC_DECIMALS = 6;
