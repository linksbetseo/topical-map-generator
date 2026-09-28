import {
  sha256Hex,
  type Clock,
  type NormalizedQuote,
  type QuoteFailureCode,
  type QuoteProvider,
  type QuoteRequest,
  type QuoteResult,
} from "@solbot/domain";

/**
 * Quote provider driven by an explicit script. For tests and DEMO mode only;
 * every quote is marked `executionFidelity: "FIXTURE"` and provider "fixture".
 */
export type ScriptStep =
  | { outNetRaw: bigint; priceImpactBps?: number; router?: string; platformFeeRaw?: bigint; latencyMs?: number }
  | { fail: QuoteFailureCode; detail?: string; latencyMs?: number };

export class ScriptedQuoteProvider implements QuoteProvider {
  readonly name = "fixture";
  readonly profile = "jupiter_order_manual_norfq_v1";
  readonly calls: QuoteRequest[] = [];
  private readonly scripts = new Map<string, ScriptStep[]>();

  constructor(private readonly clock: Clock) {}

  static key(inputMint: string, outputMint: string): string {
    return `${inputMint}->${outputMint}`;
  }

  /** Steps are consumed in order per direction; the last step repeats. */
  script(inputMint: string, outputMint: string, steps: ScriptStep[]): this {
    this.scripts.set(ScriptedQuoteProvider.key(inputMint, outputMint), [...steps]);
    return this;
  }

  async quote(req: QuoteRequest): Promise<QuoteResult> {
    this.calls.push(req);
    const k = ScriptedQuoteProvider.key(req.inputMint, req.outputMint);
    const steps = this.scripts.get(k);
    if (!steps || steps.length === 0) throw new Error(`no script for ${k}`);
    const step = steps.length > 1 ? steps.shift()! : steps[0]!;
    const requestedAt = this.clock.now();
    if (step.latencyMs) await this.clock.sleep(step.latencyMs);
    const receivedAt = this.clock.now();
    if ("fail" in step) return { ok: false, code: step.fail, detail: step.detail ?? step.fail, requestedAt, receivedAt };
    const payload = { fixture_origin: "synthetic-test", req: { ...req, amountRaw: req.amountRaw.toString() }, out: step.outNetRaw.toString() };
    const quote: NormalizedQuote = {
      provider: this.name,
      profile: this.profile,
      inputMint: req.inputMint,
      outputMint: req.outputMint,
      inAmountRaw: req.amountRaw,
      outAmountNetRaw: step.outNetRaw,
      routeOutRaw: step.outNetRaw,
      otherAmountThresholdRaw: null,
      feeSemantics: "NO_OUTPUT_MINT_FEE",
      feeMint: req.inputMint,
      feeBpsTotal: 0,
      platformFeeBps: null,
      platformFeeRaw: step.platformFeeRaw ?? null,
      priceImpactBps: step.priceImpactBps ?? 10,
      slippageBps: req.slippageBps,
      router: step.router ?? "metis",
      mode: "manual",
      requestId: `fixture-${this.calls.length}`,
      signatureFeeLamports: null,
      prioritizationFeeLamports: null,
      rentFeeLamports: null,
      requestedAt,
      receivedAt,
      executionFidelity: "FIXTURE",
      rawPayload: payload,
      rawPayloadHash: sha256Hex(JSON.stringify(payload)),
    };
    return { ok: true, quote };
  }
}
