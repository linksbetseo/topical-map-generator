import {
  D,
  MINUTE_MS,
  NATIVE_SOL,
  ReasonCode,
  SECOND_MS,
  SOL_DECIMALS,
  SessionState,
  USDC_DECIMALS,
  USDC_MINT,
  ValuationStatus,
  newId,
  rawToUsd,
  reason,
  sessionManagesPositions,
  utcDayKey,
  utcDayStart,
  type Clock,
  type Dec,
  type QuoteProvider,
  type Reason,
} from "@solbot/domain";
import { EXECUTION_PROFILES, type Config } from "@solbot/config";
import { Bucket, computeEquity, type EquityBreakdown, type FeeItem, type FxSnapshot, type PositionMark } from "@solbot/ledger";
import { evaluateEntry, evaluateLossTriggers, exitLimits, lamportsToUsd, type RiskSnapshot } from "@solbot/risk";
import { PaperBroker, type PaperModel, type PaperOutcome } from "@solbot/brokers";
import { evaluateConfluence, evaluateExit, evaluateTokenFilters } from "@solbot/strategy";
import {
  bookBuyFill,
  bookSellFill,
  createAttempt,
  createExitIntent,
  enqueueJob,
  getSession,
  loadLedger,
  reserveEntry,
  resolveFailedEntry,
  resolveFailedExit,
  transitionSession,
  withTx,
  json,
  JobPriority,
  type Client,
  type Pool,
  type SessionRow,
} from "@solbot/db";
import type { FlowStore, MarketData, Notifier, WalletBook } from "./ports.ts";

interface OpenPositionRow {
  id: string;
  mint: string;
  status: string;
  qty_raw: string;
  cost_usd: string | null;
  rent_lamports: string;
  entry_filled_at: Date | null;
  peak_net_value_usd: string | null;
  deployer_group: string | null;
  signal_wallets: Array<{ wallet: string; qtyAtEntryRaw: string }> | null;
  exit_attempts: number;
  next_exit_at: Date | null;
}

export interface EngineDeps {
  pool: Pool;
  clock: Clock;
  cfg: Config;
  sessionId: string;
  quotes: QuoteProvider;
  market: MarketData;
  flows: FlowStore;
  wallets: WalletBook;
  notifier: Notifier;
  workerId: string;
}

export class PaperEngine {
  private readonly broker: PaperBroker;
  private readonly model: PaperModel;
  private marks = new Map<string, PositionMark & { at: Date }>();
  private lastFx: FxSnapshot | null = null;
  private lastReconcileAt = 0;
  private lastMintCheck = new Map<string, number>();

  constructor(private readonly d: EngineDeps) {
    const p = EXECUTION_PROFILES[d.cfg.execution.profile];
    this.model = { name: d.cfg.execution.profile, extraDelayMs: p.extra_delay_ms, haircutBps: p.haircut_bps, modeledFailureBps: p.modeled_failure_bps, seed: p.seed };
    this.broker = new PaperBroker(d.quotes, d.clock, this.model, {
      baseFeeLamportsPerSignature: BigInt(d.cfg.execution.base_fee_lamports_per_signature),
      signaturesPerTx: BigInt(d.cfg.execution.signatures_per_tx),
      priorityFeeLamports: BigInt(d.cfg.execution.priority_fee_lamports_estimate),
    });
  }

  private get cfg(): Config {
    return this.d.cfg;
  }

  private networkFeeLamports(): bigint {
    return this.broker.modeledNetworkFeeLamports();
  }

  private async session(): Promise<SessionRow> {
    const s = await getSession(this.d.pool, this.d.sessionId);
    if (!s) throw new Error("session not found");
    return s;
  }

  private async reject(stage: string, mint: string | null, reasons: Reason[], refId: string | null = null): Promise<void> {
    const now = this.d.clock.now();
    for (const r of reasons) {
      await this.d.pool.query(`INSERT INTO rejection_reasons (session_id, stage, reason_code, mint, ref_id, detail, at) VALUES ($1,$2,$3,$4,$5,$6,$7)`, [
        this.d.sessionId,
        stage,
        r.code,
        mint,
        refId,
        json({ detail: r.detail ?? null, evidence: r.evidence ?? null }),
        now,
      ]);
    }
  }

  private async transition(to: SessionState, reasonCode: string | null, detail?: string): Promise<void> {
    const s = await this.session();
    if (s.state === to) return;
    await withTx(this.d.pool, (c) => transitionSession(c, this.d.sessionId, to, this.d.clock.now(), reasonCode, detail));
    await this.d.notifier.notify({ kind: "STATE", from: s.state, to, reason: reasonCode });
  }

  // ------------------------------------------------------------------ valuation
  private async openPositions(c?: Client): Promise<OpenPositionRow[]> {
    const q = c ?? this.d.pool;
    const r = await q.query<OpenPositionRow>(`SELECT * FROM positions WHERE session_id=$1 AND status IN ('OPEN','EXITING') ORDER BY id`, [this.d.sessionId]);
    return r.rows;
  }

  private async refreshMarks(now: Date): Promise<void> {
    for (const p of await this.openPositions()) {
      const qty = BigInt(p.qty_raw);
      const res = await this.d.quotes.quote({ inputMint: p.mint, outputMint: USDC_MINT, amountRaw: qty, slippageBps: this.cfg.risk.exit_slippage_bps });
      let mark: PositionMark;
      if (res.ok) {
        mark = { mint: p.mint, status: ValuationStatus.FRESH, netUsdcRaw: res.quote.outAmountNetRaw, exitFeesLamports: this.networkFeeLamports(), quoteAt: res.quote.receivedAt };
      } else if (res.code === "NO_ROUTE") {
        mark = { mint: p.mint, status: ValuationStatus.UNLIQUIDATABLE, netUsdcRaw: null, exitFeesLamports: this.networkFeeLamports(), quoteAt: null };
      } else {
        mark = { mint: p.mint, status: ValuationStatus.UNKNOWN_VALUATION, netUsdcRaw: null, exitFeesLamports: this.networkFeeLamports(), quoteAt: null };
      }
      this.marks.set(p.mint, { ...mark, at: now });
      await this.d.pool.query(
        `INSERT INTO quotes (id, position_id, role, provider, profile, ok, failure_code, input_mint, output_mint, in_amount_raw, out_amount_net_raw, price_impact_bps, router, requested_at, received_at, raw_payload, raw_payload_hash)
         VALUES ($1,$2,'MARK',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
        [
          newId("q"),
          p.id,
          this.d.quotes.name,
          this.d.quotes.profile,
          res.ok,
          res.ok ? null : res.code,
          p.mint,
          USDC_MINT,
          qty.toString(),
          res.ok ? res.quote.outAmountNetRaw.toString() : null,
          res.ok ? res.quote.priceImpactBps : null,
          res.ok ? res.quote.router : null,
          res.ok ? res.quote.requestedAt : res.requestedAt,
          res.ok ? res.quote.receivedAt : res.receivedAt,
          res.ok ? json(res.quote.rawPayload) : null,
          res.ok ? res.quote.rawPayloadHash : null,
        ],
      );
      const lastUsd = mark.netUsdcRaw !== null && this.lastFx?.usdcUsd ? rawToUsd(mark.netUsdcRaw, USDC_DECIMALS, this.lastFx.usdcUsd).toString() : null;
      await this.d.pool.query(
        `UPDATE positions SET valuation_status=$2, last_mark_usd=COALESCE($3, last_mark_usd), last_mark_at=CASE WHEN $3::numeric IS NULL THEN last_mark_at ELSE $4 END WHERE id=$1`,
        [p.id, mark.status, lastUsd, now],
      );
    }
  }

  /** Marks older than the quote freshness limit are STALE (not reused as fresh from cache). */
  private currentMarks(now: Date): Map<string, PositionMark> {
    const out = new Map<string, PositionMark>();
    for (const [mint, m] of this.marks) {
      const stale = m.status === ValuationStatus.FRESH && m.quoteAt !== null && now.getTime() - m.quoteAt.getTime() > Math.max(this.cfg.freshness.quote_ms, this.cfg.polling.position_quote_ms * 2);
      out.set(mint, stale ? { ...m, status: ValuationStatus.STALE, netUsdcRaw: null } : m);
    }
    return out;
  }

  async equity(now: Date): Promise<EquityBreakdown | null> {
    if (!this.lastFx) return null;
    const c = await this.d.pool.connect();
    try {
      const l = await loadLedger(c, this.d.sessionId);
      const e = computeEquity(l, this.lastFx, this.currentMarks(now), { closeAccountFeeLamportsPerPosition: BigInt(this.cfg.execution.base_fee_lamports_per_signature) });
      return e.ok ? e : null;
    } finally {
      c.release();
    }
  }

  // ------------------------------------------------------------------ risk snapshot
  private async riskSnapshot(c: Client | null, s: SessionRow, now: Date, equity: EquityBreakdown, free: { usdc: bigint; lamports: bigint } | null): Promise<RiskSnapshot> {
    const q = c ?? this.d.pool;
    const day = utcDayStart(now);
    const positions = await q.query<{ mint: string; cost_usd: string | null; deployer_group: string | null; status: string }>(
      `SELECT mint, cost_usd, deployer_group, status FROM positions WHERE session_id=$1 AND status IN ('OPEN','EXITING','RESERVED')`,
      [this.d.sessionId],
    );
    const pending = await q.query<{ mint: string; notional_usd: string; deployer_group: string | null }>(
      `SELECT p.mint, r.notional_usd, p.deployer_group FROM balance_reservations r JOIN trade_intents i ON i.id=r.intent_id JOIN positions p ON p.entry_intent_id=i.id
       WHERE r.session_id=$1 AND r.status='ACTIVE'`,
      [this.d.sessionId],
    );
    const attempts = await q.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM order_attempts a JOIN trade_intents i ON i.id=a.intent_id
       WHERE i.session_id=$1 AND i.kind='ENTRY' AND a.counts_toward_daily_attempts AND a.started_at >= $2`,
      [this.d.sessionId, day],
    );
    const filledToday = await q.query<{ s: string | null }>(
      `SELECT sum(f.in_amount_raw * f.usdc_usd / 1000000)::text AS s FROM fills f JOIN positions p ON p.id=f.position_id WHERE p.session_id=$1 AND f.side='BUY' AND f.filled_at >= $2`,
      [this.d.sessionId, day],
    );
    const reservedToday = pending.rows.reduce((a, r) => a.add(r.notional_usd), new D(0));
    const closed = await q.query<{ mint: string; closed_at: Date }>(
      `SELECT mint, max(closed_at) AS closed_at FROM positions WHERE session_id=$1 AND status='CLOSED' AND entry_filled_at IS NOT NULL GROUP BY mint`,
      [this.d.sessionId],
    );
    const unresolved = await q.query<{ mint: string }>(
      `SELECT i.mint FROM order_attempts a JOIN trade_intents i ON i.id=a.intent_id WHERE i.session_id=$1 AND a.state NOT IN ('PAPER_FILLED','FAILED_PAPER','CANCELLED_BEFORE_SEND','REJECTED','EXPIRED','FAILED_ONCHAIN','RECONCILED')`,
      [this.d.sessionId],
    );
    const marks = this.currentMarks(now);
    const openExposure = positions.rows
      .filter((p) => p.status !== "RESERVED")
      .map((p) => {
        const m = marks.get(p.mint);
        const liq = m && m.status === ValuationStatus.FRESH && m.netUsdcRaw !== null ? rawToUsd(m.netUsdcRaw, USDC_DECIMALS, equity.usdcUsd) : new D(0);
        return { mint: p.mint, remainingCostUsd: new D(p.cost_usd ?? 0), conservativeLiquidationUsd: liq, deployerGroup: p.deployer_group };
      });
    const dayKey = utcDayKey(now);
    const dayStart = s.day_start_equity[dayKey];
    const c2 = c ?? (await this.d.pool.connect());
    let freeUsdc: bigint;
    let freeLamports: bigint;
    try {
      const l = free ? null : await loadLedger(c2 as Client, this.d.sessionId);
      freeUsdc = free ? free.usdc : l!.balance(Bucket.WALLET, USDC_MINT);
      freeLamports = free ? free.lamports : l!.balance(Bucket.WALLET, NATIVE_SOL);
    } finally {
      if (!c) (c2 as Client).release();
    }
    const staleData: string[] = [];
    if (!this.lastFx || now.getTime() - this.lastFx.at.getTime() > this.cfg.freshness.fx_ms) staleData.push("fx");
    return {
      now,
      sessionState: s.state,
      tEnd: s.t_end!,
      entriesPausedByOwner: s.entries_paused_by_owner || s.flatten_requested,
      equityUsd: equity.equityTotalLowerBoundUsd,
      equityHasProviderUncertainty: equity.hasProviderUncertainty,
      equityAtUtcDayStartUsd: new D(dayStart ?? equity.equityTotalLowerBoundUsd),
      peakEquityUsd: new D(s.peak_equity_usd ?? equity.equityTotalLowerBoundUsd),
      initialEquityUsd: new D(this.cfg.capital.initial_total_usd),
      usdcUsd: equity.usdcUsd,
      solUsd: equity.solUsd,
      freeUsdcRaw: freeUsdc,
      freeLamports,
      positions: openExposure,
      pendingEntries: pending.rows.map((p) => ({ mint: p.mint, notionalUsd: new D(p.notional_usd), deployerGroup: p.deployer_group })),
      unresolvedOrderMints: new Set(unresolved.rows.map((r) => r.mint)),
      anyStatusUnknown: false,
      entryAttemptsToday: attempts.rows[0]!.n,
      entryNotionalTodayUsd: new D(filledToday.rows[0]!.s ?? 0).add(reservedToday),
      lastClosedAtByMint: new Map(closed.rows.map((r) => [r.mint, r.closed_at])),
      staleData,
      reconciliationOk: true,
    };
  }

  // ------------------------------------------------------------------ entries
  /** Evaluates confluence for a mint at `now` and, if all gates pass, runs one paper entry attempt. */
  async onFlow(mint: string): Promise<{ stage: string; reasons: Reason[] }> {
    const now = this.d.clock.now();
    const s = await this.session();
    if (s.kind === "CONFLUENCE" && this.d.wallets.qualifiedCount() < this.cfg.wallets.min_qualified_for_session) {
      return { stage: "readiness", reasons: [reason(ReasonCode.INSUFFICIENT_QUALIFIED_WALLETS)] };
    }
    const events = await this.d.flows.events(mint, new Date(now.getTime() - this.cfg.signal.window_seconds * SECOND_MS), now);
    const conf = evaluateConfluence(mint, now, events, this.d.wallets.statuses(), this.cfg);
    if (!conf.signal) return { stage: "confluence", reasons: conf.reasons };
    const sig = conf.signal;

    const ins = await this.d.pool.query<{ id: string }>(
      `INSERT INTO signals (id, session_id, strategy_version, config_hash, mint, episode_key, first_detected_at, ttl_until, status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'DETECTED') ON CONFLICT (session_id, mint, episode_key) DO NOTHING RETURNING id`,
      [newId("sig"), this.d.sessionId, s.strategy_version, s.config_hash, mint, sig.episodeKey, sig.detectedAt, sig.ttlUntil],
    );
    if (!ins.rows[0]) return { stage: "dedupe", reasons: [] }; // same episode already handled
    const signalId = ins.rows[0].id;
    await this.d.pool.query(`INSERT INTO signal_evidence (signal_id, kind, data) VALUES ($1,'CONFLUENCE',$2),($1,'EXCLUDED',$3)`, [signalId, json(sig), json(conf.excluded)]);
    const fail = async (stage: string, reasons: Reason[]) => {
      await this.reject(stage, mint, reasons, signalId);
      await this.d.pool.query(`UPDATE signals SET status='REJECTED' WHERE id=$1`, [signalId]);
      return { stage, reasons };
    };

    // token filters (data available at `now` only)
    const [view, risk, holders] = await Promise.all([this.d.market.tokenView(mint, now), this.d.market.mintRisk(mint, now), this.d.market.holders(mint, now)]);
    const filters = evaluateTokenFilters(now, view, risk, holders, this.cfg);
    await this.d.pool.query(`INSERT INTO signal_evidence (signal_id, kind, data) VALUES ($1,'FILTERS',$2)`, [signalId, json(filters)]);
    if (!filters.passed) return fail("token_filters", filters.reasons);

    const rent = await this.d.market.rentLamports(mint);
    if (rent === null) return fail("token_filters", [reason(ReasonCode.RENT_UNKNOWN)]);
    const deployerGroup = await this.d.market.deployerGroup(mint);
    const equity = await this.equity(now);
    if (!equity) return fail("risk", [reason(ReasonCode.FX_MISSING)]);

    // risk check #1 + atomic reservation under the session lock
    let decisionReasons: Reason[] = [];
    const res = await reserveEntry(this.d.pool, {
      sessionId: this.d.sessionId,
      idempotencyKey: `entry:${sig.episodeKey}`,
      mint,
      signalId,
      at: now,
      deployerGroup,
      inputs: { signal: sig, filters: filters.checks, equity: equity.equityTotalLowerBoundUsd.toString() },
      decide: async ({ c, session, freeUsdcRaw, freeLamports }) => {
        const snap = await this.riskSnapshot(c, session, now, equity, { usdc: freeUsdcRaw, lamports: freeLamports });
        const dec = evaluateEntry({ mint, deployerGroup, entryFeesLamports: this.networkFeeLamports(), exitFeesLamports: this.networkFeeLamports(), rentLamports: rent }, snap, this.cfg);
        decisionReasons = dec.reasons;
        return dec.approved
          ? { approved: true, usdcRaw: dec.usdcRaw!, lamports: dec.reserveLamports!, notionalUsd: dec.notionalUsd!, decision: dec }
          : { approved: false, decision: dec };
      },
    });
    if (res.status === "DUPLICATE") return { stage: "dedupe", reasons: [] };
    if (res.status === "REJECTED") return fail("risk", decisionReasons);

    await this.d.pool.query(`UPDATE signals SET status='ACTED' WHERE id=$1`, [signalId]);
    await this.d.pool.query(`UPDATE positions SET signal_id=$2, signal_wallets=$3 WHERE id=$1`, [
      res.positionId,
      signalId,
      json(await Promise.all(sig.wallets.map(async (w) => ({ wallet: w.wallet, qtyAtEntryRaw: ((await this.d.flows.walletQty(w.wallet, mint, now)) ?? BigInt(w.netBoughtRaw)).toString() })))),
    ]);
    const intent = await this.d.pool.query<{ amount_raw: string }>(`SELECT amount_raw FROM trade_intents WHERE id=$1`, [res.intentId]);
    const amountRaw = BigInt(intent.rows[0]!.amount_raw);
    const attemptId = await createAttempt(this.d.pool, { intentId: res.intentId, attemptNo: 1, model: this.model, fencingToken: null, at: now });

    const outcome = await this.broker.execute({
      intentId: res.intentId,
      attemptNo: 1,
      kind: "ENTRY",
      inputMint: USDC_MINT,
      outputMint: mint,
      amountRaw,
      slippageBps: this.cfg.risk.entry_slippage_bps,
      feeCapUsd: new D(this.cfg.risk.entry_fee_cap_usd),
      solUsd: equity.solUsd,
      usdcUsd: equity.usdcUsd,
      quoteMaxAgeMs: this.cfg.freshness.quote_ms,
      entry: {
        maxPriceImpactBps: this.cfg.universe.max_price_impact_bps_per_side,
        maxRoundTripCostBps: this.cfg.universe.max_round_trip_cost_bps,
        reverseSlippageBps: this.cfg.risk.exit_slippage_bps,
        exitFeesLamports: this.networkFeeLamports(),
      },
      // risk check #2 right before the modeled send: TTL, session state, owner pause
      preExecutionCheck: async () => {
        const t = this.d.clock.now();
        const cur = await this.session();
        const rs: Reason[] = [];
        if (t > sig.ttlUntil) rs.push(reason(ReasonCode.SIGNAL_EXPIRED, `ttl ${sig.ttlUntil.toISOString()}`));
        if (cur.state !== SessionState.RUNNING) rs.push(reason(ReasonCode.SESSION_NOT_RUNNING, cur.state));
        if (cur.entries_paused_by_owner || cur.flatten_requested) rs.push(reason(ReasonCode.ENTRIES_PAUSED));
        if (t.getTime() >= cur.t_end!.getTime() - this.cfg.experiment.entry_cutoff_before_end_hours * 3_600_000) rs.push(reason(ReasonCode.ENTRY_WINDOW_CLOSED));
        return rs;
      },
    });
    await this.storeAttemptQuotes(attemptId, outcome);
    await this.scheduleAnalytics(attemptId, USDC_MINT, mint, amountRaw, this.cfg.risk.entry_slippage_bps);

    if (outcome.status === "FILLED") {
      const feesUsd = this.feesUsd(outcome.fees, equity);
      const costUsd = rawToUsd(amountRaw, USDC_DECIMALS, equity.usdcUsd).add(feesUsd);
      await withTx(this.d.pool, (c) =>
        bookBuyFill(c, {
          sessionId: this.d.sessionId,
          intentId: res.intentId,
          attemptId,
          positionId: res.positionId,
          fillId: outcome.fillId,
          tokenMint: mint,
          usdcInRaw: amountRaw,
          tokenOutRaw: outcome.outAmountRaw,
          minOutRaw: outcome.minOutRaw,
          fees: outcome.fees,
          rentLamports: rent,
          usdcUsd: equity.usdcUsd,
          solUsd: equity.solUsd,
          costUsd,
          executionFidelity: outcome.executionFidelity,
          at: this.d.clock.now(),
          outcome: serializeOutcome(outcome),
        }),
      );
      await this.d.notifier.notify({ kind: "ENTRY", mint, notionalUsd: rawToUsd(amountRaw, USDC_DECIMALS, equity.usdcUsd), fillId: outcome.fillId });
      return { stage: "filled", reasons: [] };
    }
    const failed = outcome.status === "FAILED";
    await withTx(this.d.pool, (c) =>
      resolveFailedEntry(c, {
        sessionId: this.d.sessionId,
        intentId: res.intentId,
        attemptId,
        positionId: res.positionId,
        state: failed ? "FAILED_PAPER" : "CANCELLED_BEFORE_SEND",
        chargedFees: failed ? outcome.chargedFees : [],
        reasonCode: outcome.code,
        outcome: serializeOutcome(outcome),
        at: this.d.clock.now(),
        releaseReservation: true,
      }),
    );
    await this.reject("execution", mint, failed ? [reason(outcome.code, outcome.detail)] : outcome.reasons, attemptId);
    return { stage: "execution", reasons: failed ? [reason(outcome.code, outcome.detail)] : outcome.reasons };
  }

  private feesUsd(fees: readonly FeeItem[], equity: { solUsd: Dec }): Dec {
    return fees
      .filter((f) => !f.includedInQuote && f.asset === NATIVE_SOL)
      .reduce((a, f) => a.add(rawToUsd(f.amountRaw, SOL_DECIMALS, f.usdFx ?? equity.solUsd)), new D(0));
  }

  private async storeAttemptQuotes(attemptId: string, o: PaperOutcome): Promise<void> {
    for (const q of o.quotes) {
      const r = q.result;
      await this.d.pool.query(
        `INSERT INTO quotes (id, attempt_id, role, provider, profile, ok, failure_code, input_mint, output_mint, in_amount_raw, out_amount_net_raw, price_impact_bps, router, requested_at, received_at, raw_payload, raw_payload_hash)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
        [
          newId("q"),
          attemptId,
          q.role,
          this.d.quotes.name,
          this.d.quotes.profile,
          r.ok,
          r.ok ? null : r.code,
          r.ok ? r.quote.inputMint : "",
          r.ok ? r.quote.outputMint : "",
          r.ok ? r.quote.inAmountRaw.toString() : "0",
          r.ok ? r.quote.outAmountNetRaw.toString() : null,
          r.ok ? r.quote.priceImpactBps : null,
          r.ok ? r.quote.router : null,
          r.ok ? r.quote.requestedAt : r.requestedAt,
          r.ok ? r.quote.receivedAt : r.receivedAt,
          r.ok ? json(r.quote.rawPayload) : null,
          r.ok ? r.quote.rawPayloadHash : null,
        ],
      );
    }
  }

  /** +5 s / +15 s analytical quotes (lower priority than exits) for later stress replay. */
  private async scheduleAnalytics(attemptId: string, inputMint: string, outputMint: string, amountRaw: bigint, slippageBps: number): Promise<void> {
    const now = this.d.clock.now().getTime();
    for (const [role, delay] of [
      ["ANALYTIC_5S", 5_000],
      ["ANALYTIC_15S", 15_000],
    ] as const) {
      await enqueueJob(this.d.pool, {
        kind: "ANALYTIC_QUOTE",
        dedupeKey: `${attemptId}:${role}`,
        payload: { attemptId, role, inputMint, outputMint, amountRaw: amountRaw.toString(), slippageBps },
        priority: JobPriority.ANALYTICS,
        runAt: new Date(now + delay),
        maxRetries: 1,
      });
    }
  }

  async runDueAnalytics(): Promise<number> {
    const now = this.d.clock.now();
    const due = await this.d.pool.query<{ id: string; payload: { attemptId: string; role: string; inputMint: string; outputMint: string; amountRaw: string; slippageBps: number } }>(
      `UPDATE jobs SET status='DONE' WHERE kind='ANALYTIC_QUOTE' AND status='READY' AND next_run_at <= $1 RETURNING id, payload`,
      [now],
    );
    for (const j of due.rows) {
      const p = j.payload;
      const r = await this.d.quotes.quote({ inputMint: p.inputMint, outputMint: p.outputMint, amountRaw: BigInt(p.amountRaw), slippageBps: p.slippageBps });
      await this.storeAttemptQuotes(p.attemptId, { quotes: [{ role: p.role as "Q1", result: r }] } as unknown as PaperOutcome);
    }
    return due.rowCount ?? 0;
  }

  // ------------------------------------------------------------------ exits
  private async exitPosition(p: OpenPositionRow, code: string, kind: "NORMAL" | "EMERGENCY", equity: EquityBreakdown): Promise<void> {
    const now = this.d.clock.now();
    if (p.next_exit_at && p.next_exit_at > now) return;
    const qty = BigInt(p.qty_raw);
    const limits = exitLimits(this.cfg, kind);
    const walletLamports = (await this.d.pool.query<{ s: string | null }>(
      `SELECT sum(amount_raw)::text AS s FROM ledger_entries WHERE session_id=$1 AND bucket='wallet' AND asset=$2`,
      [this.d.sessionId, NATIVE_SOL],
    )).rows[0]!.s;
    if (BigInt(walletLamports ?? "0") < this.networkFeeLamports() * 2n) {
      await this.alert("CRITICAL", `insufficient SOL to exit ${p.mint}; position stays visible`);
      return;
    }
    const intentKey = `exit:${p.id}:${Math.floor(p.exit_attempts / this.cfg.execution.max_execution_attempts_per_intent)}`;
    const intentId = await withTx(this.d.pool, (c) =>
      createExitIntent(c, {
        sessionId: this.d.sessionId,
        positionId: p.id,
        mint: p.mint,
        qtyRaw: qty,
        kind: kind === "EMERGENCY" ? "EXIT_EMERGENCY" : "EXIT_NORMAL",
        exitReason: code,
        idempotencyKey: intentKey,
        decision: { code, kind, limits: { slippageBps: limits.slippageBps, feeCapUsd: limits.feeCapUsd.toString() } },
        inputs: { mark: this.marks.get(p.mint) ? { status: this.marks.get(p.mint)!.status } : null },
        at: now,
      }),
    );
    const attemptNo = (p.exit_attempts % this.cfg.execution.max_execution_attempts_per_intent) + 1;
    const attemptId = await createAttempt(this.d.pool, { intentId, attemptNo, model: this.model, fencingToken: null, at: now });
    const outcome = await this.broker.execute({
      intentId,
      attemptNo,
      kind: kind === "EMERGENCY" ? "EXIT_EMERGENCY" : "EXIT_NORMAL",
      inputMint: p.mint,
      outputMint: USDC_MINT,
      amountRaw: qty,
      slippageBps: limits.slippageBps,
      feeCapUsd: limits.feeCapUsd,
      solUsd: equity.solUsd,
      usdcUsd: equity.usdcUsd,
      quoteMaxAgeMs: this.cfg.freshness.quote_ms,
    });
    await this.storeAttemptQuotes(attemptId, outcome);
    if (outcome.status === "FILLED") {
      const exitFeesUsd = this.feesUsd(outcome.fees, equity);
      const closeFee: FeeItem[] = this.cfg.execution.model_close_token_account
        ? [{ kind: "CLOSE_ACCOUNT", asset: NATIVE_SOL, amountRaw: BigInt(this.cfg.execution.base_fee_lamports_per_signature), usdFx: equity.solUsd, source: "MODEL_ESTIMATE", includedInQuote: false, isEstimate: true }]
        : [];
      const closeFeeUsd = this.feesUsd(closeFee, equity);
      const proceedsUsd = rawToUsd(outcome.outAmountRaw, USDC_DECIMALS, equity.usdcUsd);
      const pnl = proceedsUsd.sub(exitFeesUsd).sub(closeFeeUsd).sub(new D(p.cost_usd ?? 0));
      await withTx(this.d.pool, (c) =>
        bookSellFill(c, {
          sessionId: this.d.sessionId,
          intentId,
          attemptId,
          positionId: p.id,
          fillId: outcome.fillId,
          tokenMint: p.mint,
          tokenInRaw: qty,
          usdcOutRaw: outcome.outAmountRaw,
          minOutRaw: outcome.minOutRaw,
          fees: outcome.fees,
          usdcUsd: equity.usdcUsd,
          solUsd: equity.solUsd,
          executionFidelity: outcome.executionFidelity,
          closeAccount: this.cfg.execution.model_close_token_account ? { rentLamports: BigInt(p.rent_lamports), closeFees: closeFee } : null,
          exitReason: code,
          realizedPnlUsd: pnl,
          at: this.d.clock.now(),
          outcome: serializeOutcome(outcome),
        }),
      );
      this.marks.delete(p.mint);
      await this.d.notifier.notify({ kind: "EXIT", mint: p.mint, reason: code, pnlUsd: pnl, fillId: outcome.fillId });
      return;
    }
    const failed = outcome.status === "FAILED";
    const backoffMs = Math.min(5 * MINUTE_MS, 5_000 * 2 ** Math.min(p.exit_attempts, 6));
    const valuation = !failed && outcome.code === "NO_ROUTE" ? ValuationStatus.UNLIQUIDATABLE : !failed && outcome.code === "PROVIDER_UNAVAILABLE" ? ValuationStatus.UNKNOWN_VALUATION : null;
    await withTx(this.d.pool, (c) =>
      resolveFailedExit(c, {
        sessionId: this.d.sessionId,
        attemptId,
        positionId: p.id,
        state: failed ? "FAILED_PAPER" : "CANCELLED_BEFORE_SEND",
        chargedFees: failed ? outcome.chargedFees : [],
        reasonCode: outcome.code,
        outcome: serializeOutcome(outcome),
        at: this.d.clock.now(),
        nextExitAt: new Date(this.d.clock.now().getTime() + backoffMs),
        valuationStatus: valuation,
      }),
    );
    await this.reject("exit", p.mint, failed ? [reason(outcome.code, outcome.detail)] : outcome.reasons, attemptId);
  }

  private async alert(severity: "WARN" | "CRITICAL", message: string): Promise<void> {
    await this.d.pool.query(`INSERT INTO alerts (session_id, severity, code, message, at) VALUES ($1,$2,'ENGINE',$3,$4)`, [this.d.sessionId, severity, message, this.d.clock.now()]);
    await this.d.notifier.notify({ kind: "ALERT", severity, message });
  }

  // ------------------------------------------------------------------ controller tick
  async tick(): Promise<void> {
    const now = this.d.clock.now();
    let s = await this.session();
    if (!sessionManagesPositions(s.state) || !s.t0 || !s.t_end) return;
    const tEnd = s.t_end;

    // worker gap detection (restart does not stop the calendar)
    if (s.last_tick_at && now.getTime() - new Date(s.last_tick_at).getTime() > 60_000) {
      const open = (await this.openPositions()).length > 0;
      await this.d.pool.query(`INSERT INTO data_gaps (session_id, kind, started_at, ended_at, positions_open, explained, detail) VALUES ($1,'WORKER_DOWN',$2,$3,$4,false,'no ticks')`, [
        this.d.sessionId,
        s.last_tick_at,
        now,
        open,
      ]);
    }
    await this.d.pool.query(`UPDATE sessions SET last_tick_at=$2 WHERE id=$1`, [this.d.sessionId, now]);
    await this.d.pool.query(
      `INSERT INTO heartbeats (worker_id, session_id, last_beat_at, started_at) VALUES ($1,$2,$3,$3) ON CONFLICT (worker_id) DO UPDATE SET last_beat_at=$3, session_id=$2`,
      [this.d.workerId, this.d.sessionId, now],
    );

    // FX + marks + equity
    const fx = await this.d.market.fx(now);
    await this.d.pool.query(`INSERT INTO fx_snapshots (at, usdc_usd, sol_usd, source) VALUES ($1,$2,$3,$4)`, [fx.at, fx.usdcUsd?.toString() ?? null, fx.solUsd?.toString() ?? null, fx.source]);
    if (fx.usdcUsd && fx.solUsd) this.lastFx = fx;
    await this.refreshMarks(now);
    const equity = await this.equity(now);
    if (!equity) {
      if (s.state === SessionState.RUNNING) await this.transition(SessionState.PAUSED_DATA, ReasonCode.FX_MISSING);
      return;
    }
    await this.recordEquity(s, now, equity);
    s = await this.session();

    // T_end handling first: the calendar never stops
    if (now >= tEnd && s.state !== SessionState.SETTLING) {
      await this.d.pool.query(`UPDATE sessions SET t_end_snapshot=$2 WHERE id=$1 AND t_end_snapshot IS NULL`, [
        this.d.sessionId,
        json({ observed_at: now.toISOString(), t_end: tEnd.toISOString(), equity_total_lower_bound_usd: equity.equityTotalLowerBoundUsd.toString(), equity_total_fresh_usd: equity.equityTotalFreshUsd?.toString() ?? null, uncertain: equity.uncertain }),
      ]);
      await this.transition(SessionState.SETTLING, ReasonCode.EXIT_SESSION_END);
      s = await this.session();
    }

    if (s.state !== SessionState.SETTLING) await this.applyLossTriggers(s, now, equity);
    s = await this.session();

    // exits
    const settling = s.state === SessionState.SETTLING;
    const windDown = s.flatten_requested || s.state === SessionState.HALTED_RISK;
    for (const p of await this.openPositions()) {
      const mark = this.currentMarks(now).get(p.mint);
      const netLiq = mark && mark.status === ValuationStatus.FRESH && mark.netUsdcRaw !== null
        ? rawToUsd(mark.netUsdcRaw, USDC_DECIMALS, equity.usdcUsd).sub(lamportsToUsd(mark.exitFeesLamports, equity.solUsd))
        : null;
      const policy = await this.policyCheck(p.mint, now);
      const wallets = await Promise.all(
        (p.signal_wallets ?? []).map(async (w) => ({ wallet: w.wallet, qtyAtEntryRaw: BigInt(w.qtyAtEntryRaw), qtyNowRaw: await this.d.flows.walletQty(w.wallet, p.mint, now) })),
      );
      const dec = evaluateExit(
        { costUsd: new D(p.cost_usd ?? 0), entryFilledAt: p.entry_filled_at!, trailingPeakUsd: p.peak_net_value_usd ? new D(p.peak_net_value_usd) : null, entryWallets: wallets },
        { now, tEnd, policyViolation: policy, emergencyWindDown: windDown, netLiquidationUsd: netLiq },
        this.cfg,
      );
      if (dec.trailingPeakUsd && (!p.peak_net_value_usd || !dec.trailingPeakUsd.eq(p.peak_net_value_usd))) {
        await this.d.pool.query(`UPDATE positions SET peak_net_value_usd=$2, trailing_active=true WHERE id=$1`, [p.id, dec.trailingPeakUsd.toString()]);
      }
      if (dec.exit) await this.exitPosition(p, dec.code, dec.kind, equity);
      else if (settling) await this.exitPosition(p, ReasonCode.EXIT_SESSION_END, "NORMAL", equity);
    }

    if (settling) {
      const remaining = (await this.openPositions()).length;
      const deadline = tEnd.getTime() + this.cfg.experiment.settlement_max_minutes * MINUTE_MS;
      if (remaining === 0 || now.getTime() >= deadline) {
        const gaps = await this.d.pool.query<{ n: number }>(`SELECT count(*)::int AS n FROM data_gaps WHERE session_id=$1 AND positions_open AND NOT explained AND ended_at - started_at > interval '60 seconds'`, [this.d.sessionId]);
        const complete = remaining === 0 && gaps.rows[0]!.n === 0;
        await this.transition(complete ? SessionState.COMPLETED : SessionState.INCOMPLETE, null, complete ? "settled" : `${remaining} open positions, ${gaps.rows[0]!.n} unexplained gaps`);
      }
    }
    await this.reconcile(now);
    await this.runDueAnalytics();
  }

  private async policyCheck(mint: string, now: Date): Promise<string | null> {
    const last = this.lastMintCheck.get(mint) ?? 0;
    if (now.getTime() - last < this.cfg.freshness.mint_risk_ms) return null;
    this.lastMintCheck.set(mint, now.getTime());
    const r = await this.d.market.mintRisk(mint, now);
    if (r && !r.passed) return r.reasons.map((x) => x.code).join(",");
    return null;
  }

  private async recordEquity(s: SessionRow, now: Date, e: EquityBreakdown): Promise<void> {
    await this.d.pool.query(
      `INSERT INTO equity_snapshots (session_id, at, equity_total_lower_bound_usd, equity_liquid_lower_bound_usd, equity_total_fresh_usd, usdc_usd, sol_usd, uncertain, breakdown) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [
        this.d.sessionId,
        now,
        e.equityTotalLowerBoundUsd.toString(),
        e.equityLiquidLowerBoundUsd.toString(),
        e.equityTotalFreshUsd?.toString() ?? null,
        e.usdcUsd.toString(),
        e.solUsd.toString(),
        json(e.uncertain),
        json({ usdc: e.usdcHeldUsd.toString(), sol: e.solSpendableUsd.toString(), positions: e.positionsLowerBoundUsd.toString(), rent_locked: e.rentLockedUsd.toString(), liabilities: e.pendingLiabilitiesUsd.toString() }),
      ],
    );
    const dayKey = utcDayKey(now);
    const days = s.day_start_equity ?? {};
    const updates: string[] = [];
    if (!days[dayKey] && e.equityTotalFreshUsd) {
      days[dayKey] = e.equityTotalFreshUsd.toString();
      updates.push("day");
    }
    const peak = s.peak_equity_usd ? new D(s.peak_equity_usd) : null;
    const newPeak = e.equityTotalFreshUsd && (!peak || e.equityTotalFreshUsd.gt(peak)) ? e.equityTotalFreshUsd : peak;
    await this.d.pool.query(`UPDATE sessions SET day_start_equity=$2, peak_equity_usd=$3 WHERE id=$1`, [this.d.sessionId, json(days), newPeak?.toString() ?? null]);
    // benchmarks (analytical capital only)
    const t0 = await this.d.pool.query<{ refs: { usdc_usd: string; sol_usd: string } }>(`SELECT refs FROM ledger_transactions WHERE session_id=$1 AND kind='OPENING'`, [this.d.sessionId]);
    const opening = await this.d.pool.query<{ asset: string; amount_raw: string }>(
      `SELECT e.asset, e.amount_raw FROM ledger_entries e JOIN ledger_transactions t ON t.id=e.tx_id WHERE t.session_id=$1 AND t.kind='OPENING' AND e.bucket='wallet'`,
      [this.d.sessionId],
    );
    if (t0.rows[0]) {
      const usdc0 = BigInt(opening.rows.find((r) => r.asset === USDC_MINT)?.amount_raw ?? "0");
      const sol0 = BigInt(opening.rows.find((r) => r.asset === NATIVE_SOL)?.amount_raw ?? "0");
      const hold = rawToUsd(usdc0, USDC_DECIMALS, e.usdcUsd).add(rawToUsd(sol0, SOL_DECIMALS, e.solUsd));
      const allUsdc = new D(this.cfg.capital.initial_total_usd).div(t0.rows[0].refs.usdc_usd).mul(e.usdcUsd);
      await this.d.pool.query(`INSERT INTO benchmark_snapshots (session_id, at, hold_start_alloc_usd, all_usdc_usd) VALUES ($1,$2,$3,$4)`, [this.d.sessionId, now, hold.toString(), allUsdc.toString()]);
    }
  }

  private async applyLossTriggers(s: SessionRow, now: Date, equity: EquityBreakdown): Promise<void> {
    const snap = await this.riskSnapshot(null, s, now, equity, null);
    const ev = evaluateLossTriggers(snap, this.cfg);
    if (ev.action === "HALTED_RISK" && s.state !== SessionState.HALTED_RISK) {
      await this.reject("loss_trigger", null, ev.reasons);
      if (s.state === SessionState.PAUSED_DATA || s.state === SessionState.RUNNING || s.state === SessionState.EXIT_ONLY) await this.transition(SessionState.HALTED_RISK, ev.reasons[0]!.code);
      return;
    }
    if (ev.action === "EXIT_ONLY" && s.state === SessionState.RUNNING) {
      await this.reject("loss_trigger", null, ev.reasons);
      await this.transition(SessionState.EXIT_ONLY, ReasonCode.DAILY_LOSS_TRIGGER);
      return;
    }
    if (ev.action === "PAUSED_DATA" && s.state === SessionState.RUNNING) {
      await this.transition(SessionState.PAUSED_DATA, ReasonCode.UNKNOWN_VALUATION);
      return;
    }
    if (ev.action === "NONE" && s.state === SessionState.PAUSED_DATA) {
      await this.transition(SessionState.RUNNING, null, "valuations fresh again");
      return;
    }
    if (s.state === SessionState.EXIT_ONLY && !s.flatten_requested) {
      // re-arm only on a new UTC day, after reconciliation and with fresh data
      const since = await this.d.pool.query<{ at: Date }>(`SELECT at FROM session_transitions WHERE session_id=$1 AND to_state='EXIT_ONLY' ORDER BY at DESC LIMIT 1`, [this.d.sessionId]);
      const at = since.rows[0]?.at;
      if (at && utcDayKey(at) !== utcDayKey(now) && ev.action === "NONE" && (await this.reconcile(now, true))) {
        await this.transition(SessionState.RUNNING, null, "new UTC day");
      }
    }
  }

  /** Ledger identity + SQL sums; a mismatch pauses entries. Runs at most every reconciliation_ms. */
  async reconcile(now: Date, force = false): Promise<boolean> {
    if (!force && now.getTime() - this.lastReconcileAt < this.cfg.polling.reconciliation_ms) return true;
    this.lastReconcileAt = now.getTime();
    const c = await this.d.pool.connect();
    try {
      const l = await loadLedger(c, this.d.sessionId);
      const id = l.verifyIdentity();
      const sql = await c.query<{ bucket: string; asset: string; s: string }>(`SELECT bucket, asset, sum(amount_raw)::text AS s FROM ledger_entries WHERE session_id=$1 GROUP BY bucket, asset`, [this.d.sessionId]);
      const mismatches = [...id.mismatches];
      for (const r of sql.rows) if (l.balance(r.bucket, r.asset) !== BigInt(r.s)) mismatches.push(`${r.bucket}|${r.asset}`);
      const pos = await c.query<{ mint: string; qty_raw: string }>(`SELECT mint, qty_raw FROM positions WHERE session_id=$1 AND status IN ('OPEN','EXITING')`, [this.d.sessionId]);
      for (const p of pos.rows) if (l.balance(Bucket.WALLET, p.mint) !== BigInt(p.qty_raw)) mismatches.push(`position|${p.mint}`);
      await c.query(`INSERT INTO reconciliation_runs (session_id, at, ok, mismatches) VALUES ($1,$2,$3,$4)`, [this.d.sessionId, now, mismatches.length === 0, json(mismatches)]);
      if (mismatches.length > 0) {
        await this.alert("CRITICAL", `reconciliation mismatch: ${mismatches.join(",")}`);
        await c.query(`UPDATE sessions SET entries_paused_by_owner=true, intervention=true WHERE id=$1`, [this.d.sessionId]);
      }
      return mismatches.length === 0;
    } finally {
      c.release();
    }
  }

  /**
   * Restart recovery: attempts left non-terminal by a crash are resolved conservatively
   * (treated as failed after send, with estimated costs) — never re-executed as a new trade.
   */
  async recover(): Promise<number> {
    const now = this.d.clock.now();
    const stuck = await this.d.pool.query<{ id: string; intent_id: string; kind: string; position_id: string }>(
      `SELECT a.id, a.intent_id, i.kind, i.position_id FROM order_attempts a JOIN trade_intents i ON i.id=a.intent_id
       WHERE i.session_id=$1 AND a.state='QUOTED'`,
      [this.d.sessionId],
    );
    const fx = this.lastFx?.solUsd ?? null;
    for (const a of stuck.rows) {
      const fees = this.broker.networkFees(fx ?? new D(0), "FAILED").map((f) => ({ ...f, usdFx: fx }));
      if (a.kind === "ENTRY") {
        await withTx(this.d.pool, (c) =>
          resolveFailedEntry(c, { sessionId: this.d.sessionId, intentId: a.intent_id, attemptId: a.id, positionId: a.position_id, state: "FAILED_PAPER", chargedFees: fees, reasonCode: "RECOVERED_AFTER_RESTART", outcome: { recovered: true }, at: now, releaseReservation: true }),
        );
      } else {
        await withTx(this.d.pool, (c) =>
          resolveFailedExit(c, { sessionId: this.d.sessionId, attemptId: a.id, positionId: a.position_id, state: "FAILED_PAPER", chargedFees: fees, reasonCode: "RECOVERED_AFTER_RESTART", outcome: { recovered: true }, at: now, nextExitAt: now, valuationStatus: null }),
        );
      }
    }
    // reservations without an attempt (crash between reservation and attempt)
    const orphan = await this.d.pool.query<{ intent_id: string; position_id: string }>(
      `SELECT r.intent_id, i.position_id FROM balance_reservations r JOIN trade_intents i ON i.id=r.intent_id
       WHERE r.session_id=$1 AND r.status='ACTIVE' AND NOT EXISTS (SELECT 1 FROM order_attempts a WHERE a.intent_id=r.intent_id)`,
      [this.d.sessionId],
    );
    for (const o of orphan.rows) {
      const attemptId = await createAttempt(this.d.pool, { intentId: o.intent_id, attemptNo: 1, model: this.model, fencingToken: null, at: now });
      await withTx(this.d.pool, (c) =>
        resolveFailedEntry(c, { sessionId: this.d.sessionId, intentId: o.intent_id, attemptId, positionId: o.position_id, state: "CANCELLED_BEFORE_SEND", chargedFees: [], reasonCode: "RECOVERED_AFTER_RESTART", outcome: { recovered: true }, at: now, releaseReservation: true }),
      );
    }
    if (stuck.rowCount || orphan.rowCount) await this.alert("WARN", `recovered ${stuck.rowCount} attempts and ${orphan.rowCount} reservations after restart`);
    return (stuck.rowCount ?? 0) + (orphan.rowCount ?? 0);
  }
}

function serializeOutcome(o: PaperOutcome): unknown {
  return JSON.parse(
    json({ ...o, quotes: o.quotes.map((q) => ({ role: q.role, ok: q.result.ok, code: q.result.ok ? null : q.result.code })) }),
  );
}
