import { D, NATIVE_SOL, USDC_MINT, WSOL_MINT, uiToRawFloor, type Dec } from "@solbot/domain";
import type { FlowEvent } from "@solbot/strategy";

/**
 * Normalizes a Helius Enhanced Transaction (SDK types, UNVERIFIED against live payloads) into
 * confirmed BUY/SELL flow events for watched wallets. Anything ambiguous is dropped with a reason,
 * never guessed: token-to-token swaps, unknown decimals, multiple non-base mints, failed tx.
 */
export interface EnhancedTxLike {
  signature: string;
  timestamp?: number;
  type?: string;
  transactionError?: unknown;
  tokenTransfers?: Array<{ fromUserAccount?: string; toUserAccount?: string; mint?: string; tokenAmount?: number | string }>;
  nativeTransfers?: Array<{ fromUserAccount?: string; toUserAccount?: string; amount?: number }>;
}

export interface NormalizeResult {
  events: FlowEvent[];
  dropped: Array<{ wallet: string; reason: string }>;
}

const SOL_DUST = new D("0.001"); // ignore fee-sized native movements

export function normalizeEnhancedSwap(
  tx: EnhancedTxLike,
  watched: ReadonlySet<string>,
  availableAt: Date,
  decimalsOf: (mint: string) => number | null,
  fx: { usdcUsd: Dec; solUsd: Dec } | null,
): NormalizeResult {
  const out: NormalizeResult = { events: [], dropped: [] };
  if (tx.type !== "SWAP" || (tx.transactionError !== null && tx.transactionError !== undefined) || typeof tx.timestamp !== "number") return out;
  const blockTime = new Date(tx.timestamp * 1000);
  const wallets = new Set<string>();
  for (const t of tx.tokenTransfers ?? []) {
    if (t.fromUserAccount && watched.has(t.fromUserAccount)) wallets.add(t.fromUserAccount);
    if (t.toUserAccount && watched.has(t.toUserAccount)) wallets.add(t.toUserAccount);
  }
  for (const w of wallets) {
    const delta = new Map<string, Dec>();
    const add = (mint: string, v: Dec) => delta.set(mint, (delta.get(mint) ?? new D(0)).add(v));
    for (const t of tx.tokenTransfers ?? []) {
      if (!t.mint || t.tokenAmount === undefined) continue;
      const amt = new D(String(t.tokenAmount));
      const mint = t.mint === WSOL_MINT ? NATIVE_SOL : t.mint;
      if (t.toUserAccount === w) add(mint, amt);
      if (t.fromUserAccount === w) add(mint, amt.neg());
    }
    for (const n of tx.nativeTransfers ?? []) {
      if (typeof n.amount !== "number") continue;
      const sol = new D(n.amount).div(1e9);
      if (n.toUserAccount === w) add(NATIVE_SOL, sol);
      if (n.fromUserAccount === w) add(NATIVE_SOL, sol.neg());
    }
    const sol = delta.get(NATIVE_SOL) ?? new D(0);
    const usdc = delta.get(USDC_MINT) ?? new D(0);
    const others = [...delta.entries()].filter(([m, v]) => m !== NATIVE_SOL && m !== USDC_MINT && !v.eq(0));
    if (others.length !== 1) {
      out.dropped.push({ wallet: w, reason: others.length === 0 ? "no non-base token change" : "multiple non-base tokens (token-to-token not priced)" });
      continue;
    }
    const [mint, tokDelta] = others[0]!;
    const dec = decimalsOf(mint);
    if (dec === null) {
      out.dropped.push({ wallet: w, reason: `AMOUNT_UNIT_AMBIGUOUS: decimals of ${mint} unknown` });
      continue;
    }
    const solBase = sol.abs().gt(SOL_DUST) ? sol : new D(0);
    const baseUsd = fx ? usdc.mul(fx.usdcUsd).add(solBase.mul(fx.solUsd)) : null;
    const side = tokDelta.gt(0) ? "BUY" : "SELL";
    // a buy must spend base, a sell must receive base; otherwise it is a transfer-like movement
    if (baseUsd !== null && ((side === "BUY" && !baseUsd.lt(0)) || (side === "SELL" && !baseUsd.gt(0)))) {
      out.dropped.push({ wallet: w, reason: "no base asset leg" });
      continue;
    }
    out.events.push({
      wallet: w,
      mint,
      side,
      tokenRaw: uiToRawFloor(tokDelta.abs(), dec),
      usd: baseUsd ? baseUsd.abs() : null,
      blockTime,
      availableAt,
      confirmed: true,
      signature: tx.signature,
    });
  }
  return out;
}
