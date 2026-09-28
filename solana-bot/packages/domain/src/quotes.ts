import type { ReasonCode } from "./reasons.ts";

export type ExecutionFidelity =
  /** Quote requested without taker: no transaction, not specific to any wallet. */
  | "QUOTE_ONLY_NO_TAKER"
  /** Taker-specific order built for a real wallet (SHADOW/LIVE). */
  | "TAKER_SPECIFIC_ORDER"
  | "FIXTURE";

export type FeeSemantics =
  /** No fee in quote, or fee collected in input mint (output already reflects it). */
  | "NO_OUTPUT_MINT_FEE"
  /** outAmount already excludes the platform fee collected in the output mint. */
  | "OUTPUT_NET_OF_FEE"
  /** outAmount was gross; fee subtracted exactly once during normalization. */
  | "OUTPUT_GROSS_ADJUSTED";

export interface QuoteRequest {
  inputMint: string;
  outputMint: string;
  /** exact input amount, raw units */
  amountRaw: bigint;
  slippageBps: number;
}

export interface NormalizedQuote {
  provider: string;
  profile: string;
  inputMint: string;
  outputMint: string;
  inAmountRaw: bigint;
  /** Output net of every fee that is included in the quote. Use this and only this for fills. */
  outAmountNetRaw: bigint;
  /** Output of the route as reported per step (before output-mint fee), for audit. */
  routeOutRaw: bigint;
  otherAmountThresholdRaw: bigint | null;
  feeSemantics: FeeSemantics;
  feeMint: string;
  feeBpsTotal: number;
  platformFeeBps: number | null;
  platformFeeRaw: bigint | null;
  priceImpactBps: number;
  slippageBps: number;
  router: string;
  mode: string;
  requestId: string;
  /** Hints from the provider; for quote-only they are not specific to our wallet. */
  signatureFeeLamports: bigint | null;
  prioritizationFeeLamports: bigint | null;
  rentFeeLamports: bigint | null;
  requestedAt: Date;
  receivedAt: Date;
  executionFidelity: ExecutionFidelity;
  rawPayload: unknown;
  rawPayloadHash: string;
}

export type QuoteFailureCode = Extract<
  ReasonCode,
  "NO_ROUTE" | "PROVIDER_UNAVAILABLE" | "RATE_LIMITED" | "PROVIDER_ERROR" | "QUOTE_FIELD_MISSING" | "QUOTE_SEMANTICS_UNRESOLVED"
>;

export type QuoteResult =
  | { ok: true; quote: NormalizedQuote }
  | { ok: false; code: QuoteFailureCode; detail: string; requestedAt: Date; receivedAt: Date; rawPayload?: unknown };

export interface QuoteProvider {
  readonly name: string;
  readonly profile: string;
  quote(req: QuoteRequest): Promise<QuoteResult>;
}

/**
 * Two quotes are comparable only when they describe the identical request under the same
 * profile and fee semantics. Router may differ inside the same profile (recorded, allowed).
 */
export function comparabilityKey(q: NormalizedQuote): string {
  return [q.provider, q.profile, q.mode, q.inputMint, q.outputMint, q.inAmountRaw.toString(), q.slippageBps, q.executionFidelity].join("|");
}
