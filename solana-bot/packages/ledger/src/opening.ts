import { D, SOL_DECIMALS, USDC_DECIMALS, rawToUsd, usdToRawFloor, type Dec } from "@solbot/domain";

export interface OpeningAllocation {
  usdcRaw: bigint;
  lamports: bigint;
  usdcUsd: Dec;
  solUsd: Dec;
  /** Value at T0 FX; equals initial_total minus sub-unit rounding (< 1 raw unit per asset). */
  valueUsd: Dec;
  roundingShortfallUsd: Dec;
}

/**
 * T0 allocation (brief §8): total USD split into USDC and SOL *by value* at recorded T0 FX.
 * Unit counts follow from the rates; SOL is part of the 500 USD, not an addition.
 */
export function openingAllocation(totalUsd: Dec, solValueUsd: Dec, usdcUsd: Dec, solUsd: Dec): OpeningAllocation {
  if (solValueUsd.gt(totalUsd) || solValueUsd.lt(0)) throw new RangeError("SOL allocation must be within total");
  const usdcValueUsd = totalUsd.sub(solValueUsd);
  const usdcRaw = usdToRawFloor(usdcValueUsd, USDC_DECIMALS, usdcUsd);
  const lamports = usdToRawFloor(solValueUsd, SOL_DECIMALS, solUsd);
  const valueUsd = rawToUsd(usdcRaw, USDC_DECIMALS, usdcUsd).add(rawToUsd(lamports, SOL_DECIMALS, solUsd));
  return { usdcRaw, lamports, usdcUsd, solUsd, valueUsd, roundingShortfallUsd: new D(totalUsd).sub(valueUsd) };
}
