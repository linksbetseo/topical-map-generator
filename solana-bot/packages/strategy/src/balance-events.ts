import { D, USDC_MINT, WSOL_MINT, type Dec } from "@solbot/domain";
import type { WalletEvent } from "./wallets.ts";

/**
 * Economic effect of one transaction on one wallet, read from balance changes (not transfer lists).
 * Structural copy of providers' CompactTx so strategy keeps no provider dependency.
 *
 * Rules (spec v2 §5):
 *  - identity of an asset = mint address; amounts are raw integers (bigint) with their decimals;
 *  - SOL leg = wallet lamport change + network fee (when the wallet paid it) + WSOL owned by the wallet
 *    ± rent of token accounts the wallet opened/closed in this transaction, read from those accounts'
 *    own lamport changes (exact for SPL Token and Token-2022; rent is a refundable deposit, reported
 *    apart, not a trading cost). Tips paid inside the tx stay in the leg;
 *  - base assets (SOL/WSOL, USDC, USDT) never count as risk tokens;
 *  - a risk-token change with no material base leg is a transfer, never a trade at price zero;
 *  - anything else (several risk tokens, token and base moving the same way) is UNSUPPORTED — counted,
 *    never guessed.
 */
export const BALANCE_NORMALIZER_VERSION = 1;
export const USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
/** rent-exempt minimum of a 165-byte SPL Token account (lamports) — for tests and documentation */
export const SPL_TOKEN_ACCOUNT_RENT = 2_039_280n;
/**
 * With fee and rent removed, a SOL leg below this is noise (e.g. a tip on a plain transfer), not a
 * payment. Real micro-buys of 0.0005 SOL exist (observed 2026-09-28) and stay trades; the report
 * counts sub-1-USD ("dust") episodes separately.
 */
export const MIN_BASE_LEG_LAMPORTS = 100_000n;
export const MIN_STABLE_LEG_RAW = 10_000n; // 0.01 USDC/USDT

export interface TokenBalanceLike {
  accountIndex: number;
  mint: string;
  owner: string | null;
  programId: string | null;
  amount: string;
  decimals: number;
}

export interface CompactTxLike {
  signature: string;
  slot: number;
  blockTime: number | null;
  err: unknown;
  fee: number;
  accountKeys: string[];
  preBalances: number[];
  postBalances: number[];
  preTokenBalances: TokenBalanceLike[];
  postTokenBalances: TokenBalanceLike[];
  programIds: string[];
}

export type TxClass = "BUY" | "SELL" | "TRANSFER_IN" | "TRANSFER_OUT" | "BASE_ONLY" | "NO_CHANGE" | "UNSUPPORTED" | "FAILED" | "NOT_INVOLVED";

export interface BaseLegs {
  /** economic SOL (incl. WSOL) change in lamports, fee and rent deposits excluded */
  solLamports: bigint;
  usdcRaw: bigint;
  usdtRaw: bigint;
}

export interface WalletTxEffect {
  signature: string;
  blockTime: Date | null;
  cls: TxClass;
  /** risk-token net change per mint (raw) */
  tokens: Array<{ mint: string; deltaRaw: bigint; decimals: number; programId: string | null }>;
  base: BaseLegs;
  /** network fee paid by this wallet (lamports), 0 when someone else paid */
  feeLamports: bigint;
  /** rent deposited (+) / refunded (-) for token accounts opened/closed here; null = an account's lamports are unknown */
  rentLamports: bigint | null;
  note?: string;
}

const isBase = (mint: string) => mint === WSOL_MINT || mint === USDC_MINT || mint === USDT_MINT;

export function walletTxEffect(tx: CompactTxLike, wallet: string): WalletTxEffect {
  const blockTime = tx.blockTime === null ? null : new Date(tx.blockTime * 1000);
  const idx = tx.accountKeys.indexOf(wallet);
  const feePayer = tx.accountKeys[0] === wallet;
  const empty: BaseLegs = { solLamports: 0n, usdcRaw: 0n, usdtRaw: 0n };
  const owned = (l: TokenBalanceLike[]) => l.filter((b) => b.owner === wallet);
  const pre = owned(tx.preTokenBalances);
  const post = owned(tx.postTokenBalances);
  if (idx < 0 && pre.length === 0 && post.length === 0) return { signature: tx.signature, blockTime, cls: "NOT_INVOLVED", tokens: [], base: empty, feeLamports: 0n, rentLamports: 0n };
  const feeLamports = feePayer ? BigInt(tx.fee) : 0n;
  if (tx.err !== null && tx.err !== undefined) return { signature: tx.signature, blockTime, cls: "FAILED", tokens: [], base: empty, feeLamports, rentLamports: 0n };

  const native = idx >= 0 ? BigInt(tx.postBalances[idx]!) - BigInt(tx.preBalances[idx]!) : 0n;
  // per token account: pre/post raw amount (absent = account did not exist)
  const accounts = new Map<number, { mint: string; decimals: number; programId: string | null; pre: bigint | null; post: bigint | null }>();
  for (const b of pre) accounts.set(b.accountIndex, { mint: b.mint, decimals: b.decimals, programId: b.programId, pre: BigInt(b.amount), post: null });
  for (const b of post) {
    const a = accounts.get(b.accountIndex);
    if (a) a.post = BigInt(b.amount);
    else accounts.set(b.accountIndex, { mint: b.mint, decimals: b.decimals, programId: b.programId, pre: null, post: BigInt(b.amount) });
  }
  let rent: bigint | null = 0n;
  const perMint = new Map<string, { deltaRaw: bigint; decimals: number; programId: string | null }>();
  for (const [i, a] of accounts) {
    if (a.pre === null || a.post === null) {
      // opened (pre absent) or closed (post absent) here: its lamport change is the rent deposit/refund
      // (for WSOL minus the wrapped amount, which is counted as SOL through the token delta)
      // A new account is funded by the transaction's payer: it is this wallet's deposit only when the
      // wallet paid (otherwise the sender funded it). A closed account refunds its owner (this wallet).
      const pl = tx.preBalances[i];
      const ql = tx.postBalances[i];
      if (a.pre !== null || feePayer) {
        if (pl === undefined || ql === undefined || rent === null) rent = null;
        else rent += BigInt(ql) - BigInt(pl) - (a.mint === WSOL_MINT ? (a.post ?? 0n) - (a.pre ?? 0n) : 0n);
      }
    }
    const d = (a.post ?? 0n) - (a.pre ?? 0n);
    const m = perMint.get(a.mint) ?? { deltaRaw: 0n, decimals: a.decimals, programId: a.programId };
    m.deltaRaw += d;
    perMint.set(a.mint, m);
  }
  const base: BaseLegs = {
    solLamports: native + feeLamports + (perMint.get(WSOL_MINT)?.deltaRaw ?? 0n) + (rent ?? 0n),
    usdcRaw: perMint.get(USDC_MINT)?.deltaRaw ?? 0n,
    usdtRaw: perMint.get(USDT_MINT)?.deltaRaw ?? 0n,
  };
  const tokens = [...perMint.entries()].filter(([m, v]) => !isBase(m) && v.deltaRaw !== 0n).map(([mint, v]) => ({ mint, ...v }));
  const res = (cls: TxClass, note?: string): WalletTxEffect => ({ signature: tx.signature, blockTime, cls, tokens, base, feeLamports, rentLamports: rent, ...(note ? { note } : {}) });

  if (tokens.length === 0) return res(base.solLamports !== 0n || base.usdcRaw !== 0n || base.usdtRaw !== 0n ? "BASE_ONLY" : "NO_CHANGE");
  if (tokens.length > 1) return res("UNSUPPORTED", "several risk tokens changed (token-to-token or batch)");
  const t = tokens[0]!;
  const signs = [base.solLamports, base.usdcRaw, base.usdtRaw].filter((v) => v !== 0n).map((v) => (v > 0n ? 1 : -1));
  if (new Set(signs).size > 1) return res("UNSUPPORTED", "base legs move in opposite directions");
  const baseSign = signs[0] ?? 0;
  if (!materialBase(base)) return res(t.deltaRaw > 0n ? "TRANSFER_IN" : "TRANSFER_OUT");
  if (t.deltaRaw > 0n && baseSign < 0) return res("BUY");
  if (t.deltaRaw < 0n && baseSign > 0) return res("SELL");
  return res("UNSUPPORTED", "token and base move the same way (liquidity or unknown program)");
}

export interface BaseFx {
  solUsd: Dec;
  usdcUsd: Dec;
  /** USD per USDT; the Binance anchor is USDT itself, so parity (1) is an explicit assumption */
  usdtUsd: Dec;
}

/** USD value of the base legs (signed), null without FX when a SOL or USDC leg exists. */
export function baseUsd(b: BaseLegs, fx: BaseFx | null): Dec | null {
  if (!fx && (b.solLamports !== 0n || b.usdcRaw !== 0n)) return null;
  const sol = new D(b.solLamports.toString()).div(1e9).mul(fx?.solUsd ?? 0);
  const usdc = new D(b.usdcRaw.toString()).div(1e6).mul(fx?.usdcUsd ?? 0);
  const usdt = new D(b.usdtRaw.toString()).div(1e6).mul(fx?.usdtUsd ?? 1);
  return sol.add(usdc).add(usdt);
}

/** Material base leg (fee and rent already removed): SOL ≥ 0.0001 or a stable leg ≥ 0.01. */
export function materialBase(b: BaseLegs): boolean {
  const abs = (v: bigint) => (v < 0n ? -v : v);
  return abs(b.solLamports) >= MIN_BASE_LEG_LAMPORTS || abs(b.usdcRaw) >= MIN_STABLE_LEG_RAW || abs(b.usdtRaw) >= MIN_STABLE_LEG_RAW;
}

/**
 * Wallet events for episode reconstruction. Swaps carry the USD value of their base legs at the time
 * of the transaction (null = no FX → unpriced, never zero). Transfers carry no price.
 */
export function effectToEvents(e: WalletTxEffect, wallet: string, fx: BaseFx | null, availableAt: Date): WalletEvent[] {
  if (!e.blockTime) return [];
  const t = e.tokens[0];
  if (!t) return [];
  const usd = baseUsd(e.base, fx);
  const common = { wallet, mint: t.mint, blockTime: e.blockTime, availableAt, signature: e.signature, tokenRaw: t.deltaRaw < 0n ? -t.deltaRaw : t.deltaRaw };
  switch (e.cls) {
    case "BUY":
      return [{ ...common, kind: "SWAP_BUY", usd: usd ? usd.abs() : null }];
    case "SELL":
      return [{ ...common, kind: "SWAP_SELL", usd: usd ? usd.abs() : null }];
    case "TRANSFER_IN":
      return [{ ...common, kind: "TRANSFER_IN", usd: null }];
    case "TRANSFER_OUT":
      return [{ ...common, kind: "TRANSFER_OUT", usd: null }];
    default:
      return [];
  }
}
