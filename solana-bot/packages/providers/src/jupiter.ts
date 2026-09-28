import {
  D,
  ReasonCode,
  percentPointsToBpsCeil,
  sha256Hex,
  type Dec,
  type FeeSemantics,
  type NormalizedQuote,
  type QuoteFailureCode,
  type QuoteProvider,
  type QuoteRequest,
  type QuoteResult,
} from "@solbot/domain";
import { NetworkError, SendBlockedError, type HttpResult, type ReadOnlyTransport } from "./transport.ts";
import { Priority, type SlidingWindowLimiter } from "./rate-limit.ts";

/**
 * Jupiter Swap API v2 `/order` without `taker` (quote only), profile jupiter_order_manual_norfq_v1.
 * Contract: jup-ag/docs openapi-spec/swap/v2/swap.yaml @ 956fe05 (2026-09-28). See docs/provider-contracts.md.
 */
export const JUPITER_BASE_URL = "https://api.jup.ag";
export const JUPITER_PROFILE = "jupiter_order_manual_norfq_v1";

type Json = Record<string, unknown>;

const isIntString = (v: unknown): v is string => typeof v === "string" && /^[0-9]+$/.test(v);

function fail(code: QuoteFailureCode, detail: string, t: { requestedAt: Date; receivedAt: Date }, rawPayload?: unknown): QuoteResult {
  const r: QuoteResult = { ok: false, code, detail, requestedAt: t.requestedAt, receivedAt: t.receivedAt };
  if (rawPayload !== undefined) r.rawPayload = rawPayload;
  return r;
}

/**
 * Normalizes a /order response. Missing critical fields block (no default zeros).
 * Fee semantics are resolved per response by reconciling top-level amounts with routePlan
 * (docs do not state whether outAmount is before or after an output-mint fee).
 */
export function normalizeJupiterOrder(body: unknown, req: QuoteRequest, t: { requestedAt: Date; receivedAt: Date }, rawText?: string): QuoteResult {
  if (!body || typeof body !== "object" || Array.isArray(body)) return fail(ReasonCode.QUOTE_FIELD_MISSING, "body is not an object", t, body);
  const b = body as Json;
  const missing: string[] = [];
  const need = (k: string, ok: boolean) => {
    if (!ok) missing.push(k);
  };
  need("inputMint", typeof b.inputMint === "string");
  need("outputMint", typeof b.outputMint === "string");
  need("inAmount", isIntString(b.inAmount));
  need("outAmount", isIntString(b.outAmount));
  need("router", typeof b.router === "string");
  need("swapMode", b.swapMode === "ExactIn");
  need("priceImpact", typeof b.priceImpact === "number" && Number.isFinite(b.priceImpact));
  need("feeBps", typeof b.feeBps === "number" && Number.isInteger(b.feeBps) && b.feeBps >= 0);
  need("feeMint", typeof b.feeMint === "string");
  need("mode", typeof b.mode === "string");
  need("requestId", typeof b.requestId === "string");
  const plan = Array.isArray(b.routePlan) ? (b.routePlan as Json[]) : null;
  need("routePlan", plan !== null && plan.length > 0);
  if (plan) {
    plan.forEach((s, i) => {
      const si = (s?.swapInfo ?? null) as Json | null;
      need(`routePlan[${i}].swapInfo`, !!si && typeof si.inputMint === "string" && typeof si.outputMint === "string" && isIntString(si.inAmount) && isIntString(si.outAmount));
    });
  }
  if (missing.length > 0) return fail(ReasonCode.QUOTE_FIELD_MISSING, `missing/invalid: ${missing.join(", ")}`, t, body);

  const inputMint = b.inputMint as string;
  const outputMint = b.outputMint as string;
  const inAmount = BigInt(b.inAmount as string);
  const outAmount = BigInt(b.outAmount as string);
  const feeMint = b.feeMint as string;
  const feeBps = b.feeBps as number;

  if (inputMint !== req.inputMint || outputMint !== req.outputMint || inAmount !== req.amountRaw) {
    return fail(ReasonCode.QUOTE_SEMANTICS_UNRESOLVED, "response does not echo the requested mints/amount", t, body);
  }
  if (typeof b.slippageBps === "number" && b.slippageBps !== req.slippageBps) {
    return fail(ReasonCode.QUOTE_SEMANTICS_UNRESOLVED, `slippageBps ${b.slippageBps} != requested ${req.slippageBps}`, t, body);
  }

  let routeIn = 0n;
  let routeOut = 0n;
  for (const s of plan!) {
    const si = s.swapInfo as Json;
    if (si.inputMint === inputMint) routeIn += BigInt(si.inAmount as string);
    if (si.outputMint === outputMint) routeOut += BigInt(si.outAmount as string);
  }

  let platformFeeRaw: bigint | null = null;
  let platformFeeBps: number | null = null;
  if (b.platformFee !== undefined && b.platformFee !== null) {
    const pf = b.platformFee as Json;
    if (pf.amount !== undefined && !isIntString(pf.amount)) return fail(ReasonCode.QUOTE_FIELD_MISSING, "platformFee.amount invalid", t, body);
    platformFeeRaw = pf.amount !== undefined ? BigInt(pf.amount as string) : null;
    platformFeeBps = typeof pf.feeBps === "number" ? pf.feeBps : null;
  }

  let semantics: FeeSemantics;
  let outNet: bigint;
  if (feeMint !== inputMint && feeMint !== outputMint) {
    return fail(ReasonCode.QUOTE_SEMANTICS_UNRESOLVED, `feeMint ${feeMint} is neither input nor output`, t, body);
  }
  const inputDeduction = inAmount - routeIn;
  const outputDeduction = routeOut - outAmount;
  if (inputDeduction < 0n || outputDeduction < 0n) {
    return fail(ReasonCode.QUOTE_SEMANTICS_UNRESOLVED, `route amounts exceed top-level amounts (in ${inputDeduction}, out ${outputDeduction})`, t, body);
  }
  if (inputDeduction > 0n && outputDeduction > 0n) {
    return fail(ReasonCode.QUOTE_SEMANTICS_UNRESOLVED, "fee deducted on both sides", t, body);
  }
  // platformFee.amount is documented but was absent in live responses (2026-09-28); honour it when present.
  const expectedOnOutput = (routeOut * BigInt(feeBps)) / 10_000n;
  const withinRounding = (x: bigint, y: bigint) => (x > y ? x - y : y - x) <= 2n;
  if (outputDeduction > 0n) {
    // Verified live (buy and sell, metis/dflow): outAmount = route output x (1 - feeBps), whatever feeMint says.
    const ok = platformFeeRaw !== null ? outputDeduction === platformFeeRaw : withinRounding(outputDeduction, expectedOnOutput);
    if (!ok) return fail(ReasonCode.QUOTE_SEMANTICS_UNRESOLVED, `route_out-outAmount=${outputDeduction} != fee ${platformFeeRaw ?? expectedOnOutput} (feeBps ${feeBps})`, t, body);
    semantics = "OUTPUT_NET_OF_FEE";
    outNet = outAmount;
    platformFeeRaw = outputDeduction;
  } else if (inputDeduction > 0n) {
    // Documented variant: fee taken from the input before routing; output already reflects it.
    const expectedOnInput = (inAmount * BigInt(feeBps)) / 10_000n;
    const ok = platformFeeRaw !== null ? inputDeduction === platformFeeRaw : withinRounding(inputDeduction, expectedOnInput);
    if (!ok) return fail(ReasonCode.QUOTE_SEMANTICS_UNRESOLVED, `inAmount-route_in=${inputDeduction} != fee (feeBps ${feeBps})`, t, body);
    semantics = "NO_OUTPUT_MINT_FEE";
    outNet = outAmount;
    platformFeeRaw = inputDeduction;
  } else if (feeBps === 0) {
    semantics = "NO_OUTPUT_MINT_FEE";
    outNet = outAmount;
  } else if (platformFeeRaw !== null && platformFeeRaw > 0n && feeMint === outputMint) {
    // Gross outAmount with an explicit fee amount: subtract exactly once.
    semantics = "OUTPUT_GROSS_ADJUSTED";
    outNet = outAmount - platformFeeRaw;
  } else {
    // e.g. RFQ firm quotes: feeBps > 0 but no deduction visible in the route
    return fail(ReasonCode.QUOTE_SEMANTICS_UNRESOLVED, `feeBps ${feeBps} but no deduction visible in routePlan`, t, body);
  }

  const lam = (k: string): bigint | null => {
    const v = b[k];
    return typeof v === "number" && Number.isInteger(v) && v >= 0 ? BigInt(v) : null;
  };
  const threshold = isIntString(b.otherAmountThreshold) ? BigInt(b.otherAmountThreshold) : null;
  const quote: NormalizedQuote = {
    provider: "jupiter",
    profile: JUPITER_PROFILE,
    inputMint,
    outputMint,
    inAmountRaw: inAmount,
    outAmountNetRaw: outNet,
    routeOutRaw: routeOut,
    otherAmountThresholdRaw: threshold,
    feeSemantics: semantics,
    feeMint,
    feeBpsTotal: feeBps,
    platformFeeBps,
    platformFeeRaw,
    priceImpactBps: percentPointsToBpsCeil(b.priceImpact as number),
    slippageBps: req.slippageBps,
    router: b.router as string,
    mode: b.mode as string,
    requestId: b.requestId as string,
    signatureFeeLamports: lam("signatureFeeLamports"),
    prioritizationFeeLamports: lam("prioritizationFeeLamports"),
    rentFeeLamports: lam("rentFeeLamports"),
    requestedAt: t.requestedAt,
    receivedAt: t.receivedAt,
    executionFidelity: b.transaction === null || b.transaction === undefined ? "QUOTE_ONLY_NO_TAKER" : "TAKER_SPECIFIC_ORDER",
    rawPayload: body,
    rawPayloadHash: sha256Hex(rawText ?? JSON.stringify(body)),
  };
  if (quote.mode !== "manual") return fail(ReasonCode.QUOTE_NOT_COMPARABLE, `mode ${quote.mode} != manual (profile requires slippageBps)`, t, body);
  return { ok: true, quote };
}

const NO_ROUTE_TEXT = /no route|routes? not found|no routes found|could not find any route/i;

export function classifyHttpFailure(res: HttpResult): { code: QuoteFailureCode; detail: string } {
  let err = "";
  try {
    const j = JSON.parse(res.text) as { error?: unknown };
    err = typeof j.error === "string" ? j.error : "";
  } catch {
    err = "";
  }
  if (res.status === 429) return { code: ReasonCode.RATE_LIMITED, detail: "HTTP 429" };
  if (res.status >= 500) return { code: ReasonCode.PROVIDER_UNAVAILABLE, detail: `HTTP ${res.status}` };
  if (res.status === 400 && NO_ROUTE_TEXT.test(err)) return { code: ReasonCode.NO_ROUTE, detail: err };
  return { code: ReasonCode.PROVIDER_ERROR, detail: `HTTP ${res.status} ${err}`.trim() };
}

export class JupiterClient implements QuoteProvider {
  readonly name = "jupiter";
  readonly profile = JUPITER_PROFILE;

  constructor(
    private readonly transport: ReadOnlyTransport,
    private readonly limiter: SlidingWindowLimiter,
    private readonly apiKey: string | null,
    private readonly baseUrl = JUPITER_BASE_URL,
    private readonly priority: Priority = Priority.ENTRY,
  ) {}

  private headers(): Record<string, string> {
    return this.apiKey ? { "x-api-key": this.apiKey } : {};
  }

  withPriority(p: Priority): JupiterClient {
    return new JupiterClient(this.transport, this.limiter, this.apiKey, this.baseUrl, p);
  }

  private async get(path: string, params: Record<string, string>): Promise<HttpResult | { error: QuoteFailureCode; detail: string; at: Date }> {
    const ok = await this.limiter.acquire(this.priority, this.priority <= Priority.ENTRY ? 10_000 : 0);
    const url = `${this.baseUrl}${path}?${new URLSearchParams(params)}`;
    if (!ok) return { error: ReasonCode.RATE_LIMITED, detail: "local budget exhausted", at: new Date() };
    try {
      const res = await this.transport.request("GET", url, { headers: this.headers() });
      this.limiter.observeHeaders(res.headers);
      return res;
    } catch (e) {
      if (e instanceof SendBlockedError) throw e;
      if (e instanceof NetworkError) return { error: ReasonCode.PROVIDER_UNAVAILABLE, detail: e.message, at: new Date() };
      throw e;
    }
  }

  async quote(req: QuoteRequest): Promise<QuoteResult> {
    const res = await this.get("/swap/v2/order", {
      inputMint: req.inputMint,
      outputMint: req.outputMint,
      amount: req.amountRaw.toString(),
      swapMode: "ExactIn",
      slippageBps: String(req.slippageBps),
      // RFQ (jupiterz) quotes are firm prices without a route breakdown (slippageBps 0,
      // threshold == outAmount; verified live 2026-09-28), so fees cannot be reconciled.
      excludeRouters: "jupiterz",
    });
    if ("error" in res) return fail(res.error, res.detail, { requestedAt: res.at, receivedAt: res.at });
    const t = { requestedAt: res.requestedAt, receivedAt: res.receivedAt };
    if (res.status !== 200) {
      const c = classifyHttpFailure(res);
      return fail(c.code, c.detail, t, res.text.slice(0, 2_000));
    }
    let body: unknown;
    try {
      body = JSON.parse(res.text);
    } catch {
      return fail(ReasonCode.PROVIDER_ERROR, "invalid JSON", t);
    }
    return normalizeJupiterOrder(body, req, t, res.text);
  }

  async recentTokens(): Promise<{ ok: true; tokens: TokenInfo[]; receivedAt: Date; rejected: number } | { ok: false; code: QuoteFailureCode; detail: string }> {
    return this.tokenList("/tokens/v2/recent", {});
  }

  async searchTokens(mints: string[]): Promise<{ ok: true; tokens: TokenInfo[]; receivedAt: Date; rejected: number } | { ok: false; code: QuoteFailureCode; detail: string }> {
    if (mints.length === 0 || mints.length > 100) throw new RangeError("search takes 1..100 mints");
    return this.tokenList("/tokens/v2/search", { query: mints.join(",") });
  }

  private async tokenList(path: string, params: Record<string, string>) {
    const res = await this.get(path, params);
    if ("error" in res) return { ok: false as const, code: res.error, detail: res.detail };
    if (res.status !== 200) return { ok: false as const, ...classifyHttpFailure(res) };
    let arr: unknown;
    try {
      arr = JSON.parse(res.text);
    } catch {
      return { ok: false as const, code: ReasonCode.PROVIDER_ERROR as QuoteFailureCode, detail: "invalid JSON" };
    }
    if (!Array.isArray(arr)) return { ok: false as const, code: ReasonCode.PROVIDER_ERROR as QuoteFailureCode, detail: "expected array" };
    const tokens: TokenInfo[] = [];
    let rejected = 0;
    for (const item of arr) {
      const t = parseTokenInfo(item, res.receivedAt);
      if (t) tokens.push(t);
      else rejected++;
    }
    return { ok: true as const, tokens, receivedAt: res.receivedAt, rejected };
  }

  async usdPrices(ids: string[]): Promise<{ ok: true; prices: Map<string, { usdPrice: Dec; blockId: number | null }>; receivedAt: Date } | { ok: false; code: QuoteFailureCode; detail: string }> {
    if (ids.length === 0 || ids.length > 50) throw new RangeError("price takes 1..50 ids");
    const res = await this.get("/price/v3", { ids: ids.join(",") });
    if ("error" in res) return { ok: false, code: res.error, detail: res.detail };
    if (res.status !== 200) return { ok: false, ...classifyHttpFailure(res) };
    const body = JSON.parse(res.text) as Record<string, { usdPrice?: unknown; blockId?: unknown }>;
    const prices = new Map<string, { usdPrice: Dec; blockId: number | null }>();
    for (const id of ids) {
      const p = body[id];
      // Omitted tokens have no reliable price: they stay absent (UNKNOWN_VALUATION), never 0 or 1.
      if (p && typeof p.usdPrice === "number" && Number.isFinite(p.usdPrice) && p.usdPrice > 0) {
        prices.set(id, { usdPrice: new D(String(p.usdPrice)), blockId: typeof p.blockId === "number" ? p.blockId : null });
      }
    }
    return { ok: true, prices, receivedAt: res.receivedAt };
  }
}

/** Subset of Jupiter MintInformation that we use. Provider numbers become Decimal via their string form. */
export interface TokenInfo {
  mint: string;
  decimals: number;
  tokenProgram: string | null;
  firstPoolId: string | null;
  firstPoolCreatedAt: Date | null;
  liquidityUsd: Dec | null;
  stats5m: { buyVolumeUsd: Dec | null; sellVolumeUsd: Dec | null; numSells: number | null; priceChangePct: Dec | null } | null;
  providerHolderCount: number | null;
  providerTopHoldersPct: Dec | null;
  mintAuthorityDisabled: boolean | null;
  freezeAuthorityDisabled: boolean | null;
  dev: string | null;
  launchpad: string | null;
  graduatedAt: Date | null;
  mcapUsd: Dec | null;
  fdvUsd: Dec | null;
  providerUpdatedAt: Date | null;
  receivedAt: Date;
  /** Untrusted display strings: escape on render, never interpret. */
  untrusted: { name: string | null; symbol: string | null; icon: string | null };
  raw: unknown;
}

const num = (v: unknown): Dec | null => (typeof v === "number" && Number.isFinite(v) ? new D(String(v)) : null);
const date = (v: unknown): Date | null => {
  if (typeof v !== "string") return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d;
};
const str = (v: unknown, max = 200): string | null => (typeof v === "string" ? v.slice(0, max) : null);

export function parseTokenInfo(v: unknown, receivedAt: Date): TokenInfo | null {
  if (!v || typeof v !== "object") return null;
  const o = v as Json;
  if (typeof o.id !== "string" || typeof o.decimals !== "number" || !Number.isInteger(o.decimals)) return null;
  const fp = (o.firstPool ?? null) as Json | null;
  const s5 = (o.stats5m ?? null) as Json | null;
  const audit = (o.audit ?? null) as Json | null;
  return {
    mint: o.id,
    decimals: o.decimals,
    tokenProgram: str(o.tokenProgram, 64),
    firstPoolId: fp ? str(fp.id, 64) : null,
    firstPoolCreatedAt: fp ? date(fp.createdAt) : null,
    liquidityUsd: num(o.liquidity),
    stats5m: s5
      ? { buyVolumeUsd: num(s5.buyVolume), sellVolumeUsd: num(s5.sellVolume), numSells: typeof s5.numSells === "number" ? s5.numSells : null, priceChangePct: num(s5.priceChange) }
      : null,
    providerHolderCount: typeof o.holderCount === "number" ? o.holderCount : null,
    providerTopHoldersPct: audit ? num(audit.topHoldersPercentage) : null,
    mintAuthorityDisabled: audit && typeof audit.mintAuthorityDisabled === "boolean" ? audit.mintAuthorityDisabled : null,
    freezeAuthorityDisabled: audit && typeof audit.freezeAuthorityDisabled === "boolean" ? audit.freezeAuthorityDisabled : null,
    dev: str(o.dev, 64),
    launchpad: str(o.launchpad, 100),
    graduatedAt: date(o.graduatedAt),
    mcapUsd: num(o.mcap),
    fdvUsd: num(o.fdv),
    providerUpdatedAt: date(o.updatedAt),
    receivedAt,
    untrusted: { name: str(o.name), symbol: str(o.symbol, 50), icon: str(o.icon, 500) },
    raw: v,
  };
}
