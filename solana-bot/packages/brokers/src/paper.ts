import {
  D,
  NATIVE_SOL,
  ReasonCode,
  SOL_DECIMALS,
  USDC_DECIMALS,
  bps,
  comparabilityKey,
  minBig,
  newId,
  rawToUsd,
  reason,
  reduceByBpsFloor,
  sha256Hex,
  type Clock,
  type Dec,
  type NormalizedQuote,
  type QuoteProvider,
  type QuoteResult,
  type Reason,
  type ReasonCode as RC,
} from "@solbot/domain";
import type { FeeItem } from "@solbot/ledger";

/**
 * Paper execution (brief §9). The broker never books anything: it returns an outcome that the
 * engine persists (fill + ledger) in one DB transaction. It never signs, never sends.
 *
 * Model per attempt:
 *   Q0 (exact input) -> [entry: reverse quote of the whole expected output, impact + round-trip checks]
 *   -> fee cap + pre-execution risk check -> modeled "send" -> sleep(extra delay) -> Q1 (same request)
 *   -> candidate = floor(min(Q0,Q1) * (1 - haircut)); min_out = floor(Q0 * (1 - slippage cap))
 *   -> candidate < min_out  => failed attempt (estimated network costs charged, no clipping)
 */

export type AttemptKind = "ENTRY" | "EXIT_NORMAL" | "EXIT_EMERGENCY";

export interface PaperModel {
  name: "BASE" | "STRESS" | "SEVERE";
  extraDelayMs: number;
  haircutBps: number;
  modeledFailureBps: number;
  seed: number;
}

export interface FeeModel {
  baseFeeLamportsPerSignature: bigint;
  signaturesPerTx: bigint;
  priorityFeeLamports: bigint;
}

export interface PaperExecutionRequest {
  intentId: string;
  attemptNo: number;
  kind: AttemptKind;
  inputMint: string;
  outputMint: string;
  amountRaw: bigint;
  slippageBps: number;
  feeCapUsd: Dec;
  solUsd: Dec;
  usdcUsd: Dec;
  quoteMaxAgeMs: number;
  /** Entry-only guards. */
  entry?: {
    maxPriceImpactBps: number;
    maxRoundTripCostBps: number;
    reverseSlippageBps: number;
    /** Modeled costs of a later normal exit, used in the round-trip estimate. */
    exitFeesLamports: bigint;
  };
  /** Risk re-evaluation immediately before the modeled send. Non-empty => cancelled before send. */
  preExecutionCheck?: () => Promise<Reason[]>;
}

export interface QuoteRecord {
  role: "Q0" | "Q0_REVERSE" | "Q1";
  result: QuoteResult;
}

interface OutcomeBase {
  intentId: string;
  attemptNo: number;
  model: PaperModel;
  quotes: QuoteRecord[];
  assumptions: string[];
  timings: { startedAt: Date; q0Ms: number | null; q1Ms: number | null; finishedAt: Date };
  checks: Record<string, string>;
}

export interface PaperFilled extends OutcomeBase {
  status: "FILLED";
  fillId: string; // paper_… never looks like a chain signature
  inAmountRaw: bigint;
  outAmountRaw: bigint;
  minOutRaw: bigint;
  q0OutNetRaw: bigint;
  q1OutNetRaw: bigint;
  fees: FeeItem[];
  executionFidelity: NormalizedQuote["executionFidelity"];
  router: { q0: string; q1: string };
}

export interface PaperFailed extends OutcomeBase {
  status: "FAILED";
  code: RC;
  detail: string;
  /** Estimated costs of a failed transaction after the modeled send (not an on-chain fact). */
  chargedFees: FeeItem[];
}

export interface PaperNotSent extends OutcomeBase {
  status: "NOT_SENT";
  code: RC;
  reasons: Reason[];
}

export type PaperOutcome = PaperFilled | PaperFailed | PaperNotSent;

export function deterministicFailure(model: PaperModel, intentId: string, attemptNo: number): boolean {
  if (model.modeledFailureBps <= 0) return false;
  const h = sha256Hex(`${model.seed}|${intentId}|${attemptNo}`);
  const x = BigInt(`0x${h.slice(0, 16)}`); // uniform in [0, 2^64)
  return x * 10_000n < BigInt(model.modeledFailureBps) * 2n ** 64n;
}

export class PaperBroker {
  readonly kind = "PAPER" as const;

  constructor(
    private readonly quotes: QuoteProvider,
    private readonly clock: Clock,
    private readonly model: PaperModel,
    private readonly fees: FeeModel,
  ) {}

  networkFees(solUsd: Dec, kind: "SUCCESS" | "FAILED"): FeeItem[] {
    const base = this.fees.baseFeeLamportsPerSignature * this.fees.signaturesPerTx;
    const item = (k: FeeItem["kind"], amountRaw: bigint): FeeItem => ({
      kind: kind === "FAILED" ? "FAILED_TX" : k,
      asset: NATIVE_SOL,
      amountRaw,
      usdFx: solUsd,
      source: "MODEL_ESTIMATE",
      includedInQuote: false,
      isEstimate: true,
    });
    return [item("BASE_NETWORK", base), item("PRIORITY", this.fees.priorityFeeLamports)].filter((f) => f.amountRaw > 0n);
  }

  modeledNetworkFeeLamports(): bigint {
    return this.fees.baseFeeLamportsPerSignature * this.fees.signaturesPerTx + this.fees.priorityFeeLamports;
  }

  async execute(req: PaperExecutionRequest): Promise<PaperOutcome> {
    const startedAt = this.clock.now();
    const quotes: QuoteRecord[] = [];
    const checks: Record<string, string> = {};
    const assumptions = [
      `model=${this.model.name}`,
      `extra_delay_ms=${this.model.extraDelayMs}`,
      `haircut_bps=${this.model.haircutBps} (assumption, not measured slippage)`,
      `modeled_failure_bps=${this.model.modeledFailureBps} seed=${this.model.seed}`,
      "network fees: model estimate (quote without taker is not wallet-specific)",
      "fill is a paper simulation from quotes, not an on-chain swap",
    ];
    let q0Ms: number | null = null;
    let q1Ms: number | null = null;
    const base = (): OutcomeBase => ({
      intentId: req.intentId,
      attemptNo: req.attemptNo,
      model: this.model,
      quotes,
      assumptions,
      timings: { startedAt, q0Ms, q1Ms, finishedAt: this.clock.now() },
      checks,
    });
    const notSent = (code: RC, reasons: Reason[] = [reason(code)]): PaperNotSent => ({ ...base(), status: "NOT_SENT", code, reasons });
    const failed = (code: RC, detail: string): PaperFailed => ({ ...base(), status: "FAILED", code, detail, chargedFees: this.networkFees(req.solUsd, "FAILED") });

    // ---- Q0
    const r0 = await this.quotes.quote({ inputMint: req.inputMint, outputMint: req.outputMint, amountRaw: req.amountRaw, slippageBps: req.slippageBps });
    quotes.push({ role: "Q0", result: r0 });
    if (!r0.ok) return notSent(r0.code, [reason(r0.code, r0.detail)]);
    const q0 = r0.quote;
    q0Ms = q0.receivedAt.getTime() - q0.requestedAt.getTime();
    if (q0.inputMint !== req.inputMint || q0.outputMint !== req.outputMint || q0.inAmountRaw !== req.amountRaw || q0.slippageBps !== req.slippageBps) {
      return notSent(ReasonCode.QUOTE_NOT_COMPARABLE, [reason(ReasonCode.QUOTE_NOT_COMPARABLE, "Q0 does not match the request")]);
    }
    const q0Age = this.clock.now().getTime() - q0.receivedAt.getTime();
    checks.q0_age_ms = String(q0Age);
    if (q0Age > req.quoteMaxAgeMs) return notSent(ReasonCode.QUOTE_STALE);
    checks.q0_price_impact_bps = String(q0.priceImpactBps);

    // ---- entry guards: impact + reverse quote of the whole expected position + round trip
    if (req.entry) {
      if (q0.priceImpactBps > req.entry.maxPriceImpactBps) {
        return notSent(ReasonCode.PRICE_IMPACT_TOO_HIGH, [
          reason(ReasonCode.PRICE_IMPACT_TOO_HIGH, "buy side", { observed_bps: q0.priceImpactBps, max_bps: req.entry.maxPriceImpactBps }),
        ]);
      }
      const rr = await this.quotes.quote({ inputMint: req.outputMint, outputMint: req.inputMint, amountRaw: q0.outAmountNetRaw, slippageBps: req.entry.reverseSlippageBps });
      quotes.push({ role: "Q0_REVERSE", result: rr });
      if (!rr.ok) return notSent(rr.code, [reason(rr.code, `reverse quote: ${rr.detail}`)]);
      const rev = rr.quote;
      checks.reverse_price_impact_bps = String(rev.priceImpactBps);
      if (rev.priceImpactBps > req.entry.maxPriceImpactBps) {
        return notSent(ReasonCode.PRICE_IMPACT_TOO_HIGH, [
          reason(ReasonCode.PRICE_IMPACT_TOO_HIGH, "sell side (reverse)", { observed_bps: rev.priceImpactBps, max_bps: req.entry.maxPriceImpactBps }),
        ]);
      }
      // round-trip economic cost: lost USDC through both routes + modeled network costs of entry and exit
      const inUsd = rawToUsd(req.amountRaw, USDC_DECIMALS, req.usdcUsd);
      const backUsd = rawToUsd(rev.outAmountNetRaw, USDC_DECIMALS, req.usdcUsd);
      const netFeesUsd = rawToUsd(this.modeledNetworkFeeLamports() + req.entry.exitFeesLamports, SOL_DECIMALS, req.solUsd);
      const costUsd = inUsd.sub(backUsd).add(netFeesUsd);
      const costBps = inUsd.gt(0) ? costUsd.div(inUsd).mul(10_000) : new D(Infinity);
      checks.round_trip_cost_usd = costUsd.toFixed(6);
      checks.round_trip_cost_bps = costBps.toFixed(2);
      if (costBps.gt(req.entry.maxRoundTripCostBps)) {
        return notSent(ReasonCode.ROUND_TRIP_COST_TOO_HIGH, [
          reason(ReasonCode.ROUND_TRIP_COST_TOO_HIGH, undefined, { observed_bps: costBps.toFixed(2), max_bps: req.entry.maxRoundTripCostBps }),
        ]);
      }
    }

    // ---- fee cap (modeled, not in quote)
    const feesUsd = rawToUsd(this.modeledNetworkFeeLamports(), SOL_DECIMALS, req.solUsd);
    checks.modeled_network_fees_usd = feesUsd.toFixed(6);
    if (feesUsd.gt(req.feeCapUsd)) return notSent(ReasonCode.FEE_CAP_EXCEEDED);

    // ---- risk check #2, right before the modeled send
    if (req.preExecutionCheck) {
      const rs = await req.preExecutionCheck();
      if (rs.length > 0) return notSent(rs[0]!.code, rs);
    }

    // ---- modeled send: from here on, failures carry estimated transaction costs
    await this.clock.sleep(this.model.extraDelayMs);
    const r1 = await this.quotes.quote({ inputMint: req.inputMint, outputMint: req.outputMint, amountRaw: req.amountRaw, slippageBps: req.slippageBps });
    quotes.push({ role: "Q1", result: r1 });
    if (!r1.ok) return failed(r1.code, `Q1 unavailable after modeled send: ${r1.detail}`);
    const q1 = r1.quote;
    q1Ms = q1.receivedAt.getTime() - q1.requestedAt.getTime();
    if (comparabilityKey(q0) !== comparabilityKey(q1) || q0.feeSemantics !== q1.feeSemantics) {
      return failed(ReasonCode.QUOTE_NOT_COMPARABLE, `Q0/Q1 differ: ${comparabilityKey(q0)} vs ${comparabilityKey(q1)}`);
    }
    if (q0.router !== q1.router) assumptions.push(`router changed within profile: ${q0.router} -> ${q1.router}`);

    if (deterministicFailure(this.model, req.intentId, req.attemptNo)) {
      return failed(ReasonCode.MODELED_EXECUTION_FAILURE, `modeled failure (${this.model.modeledFailureBps} bps, seed ${this.model.seed})`);
    }

    const candidate = reduceByBpsFloor(minBig(q0.outAmountNetRaw, q1.outAmountNetRaw), bps(this.model.haircutBps));
    const minOut = reduceByBpsFloor(q0.outAmountNetRaw, bps(req.slippageBps));
    checks.q0_out_net_raw = q0.outAmountNetRaw.toString();
    checks.q1_out_net_raw = q1.outAmountNetRaw.toString();
    checks.candidate_raw = candidate.toString();
    checks.min_out_raw = minOut.toString();
    if (candidate < minOut) {
      // No clipping to min_out: the swap would have reverted.
      return failed(ReasonCode.MIN_OUT_NOT_MET, `candidate ${candidate} < min_out ${minOut}`);
    }

    const worse = q1.outAmountNetRaw <= q0.outAmountNetRaw ? q1 : q0;
    const fees: FeeItem[] = [...this.networkFees(req.solUsd, "SUCCESS")];
    if (worse.platformFeeRaw !== null && worse.platformFeeRaw > 0n) {
      fees.push({ kind: "PLATFORM", asset: worse.feeMint, amountRaw: worse.platformFeeRaw, usdFx: null, source: "QUOTE", includedInQuote: true, isEstimate: false });
    }

    return {
      ...base(),
      status: "FILLED",
      fillId: newId("paper"),
      inAmountRaw: req.amountRaw,
      outAmountRaw: candidate,
      minOutRaw: minOut,
      q0OutNetRaw: q0.outAmountNetRaw,
      q1OutNetRaw: q1.outAmountNetRaw,
      fees,
      executionFidelity: q1.executionFidelity,
      router: { q0: q0.router, q1: q1.router },
    };
  }
}
