/**
 * Base assets. Values come from the official Jupiter OpenAPI examples and the
 * official `@solana-program/token-2022` package. They are still confirmed on-chain
 * (owner program + decimals) in the VALIDATING state before a session may start.
 */
export const USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
/** Wrapped SOL mint. Native SOL (lamports on the wallet) is a different asset. */
export const WSOL_MINT = "So11111111111111111111111111111111111111112";
/** Ledger asset id for native lamports (never confused with wSOL). */
export const NATIVE_SOL = "native:SOL";

export const SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
export const TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/** Base assets are explicit exceptions, never speculation candidates. */
export const BASE_ASSETS: ReadonlySet<string> = new Set([USDC_MINT, WSOL_MINT, NATIVE_SOL]);

const BASE58 = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;

export function isPlausibleAddress(value: string): boolean {
  return BASE58.test(value);
}

export function assertAddress(value: string, field = "address"): string {
  if (!isPlausibleAddress(value)) throw new TypeError(`${field}: not a base58 Solana address`);
  return value;
}
