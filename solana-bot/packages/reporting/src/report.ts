import { D, NATIVE_SOL, SOL_DECIMALS, formatWarsaw, rawToUsd, reduceByBpsFloor, bps, type Dec } from "@solbot/domain";
import { EXECUTION_PROFILES, type Config } from "@solbot/config";
import { loadLedger, getSession, type Pool } from "@solbot/db";
import { maxDrawdown, percentile, tradeStats, type ClosedTrade, type TradeStats } from "./metrics.ts";

/**
 * Final/interim report built only from the database (ledger, fills, snapshots). Nothing here is an
 * example value: before the first measurement every metric is "brak danych".
 */

export const BUILD_STATUS = {
  IMPLEMENTED: true,
  TESTED_WITH_FIXTURES: true,
  VERIFIED_READ_ONLY_MAINNET: true,
  FORWARD_TEST_COMPLETED: false,
  LIVE_DISABLED: true,
} as const;

export interface Report {
  generatedAt: string;
  codeVersion: string;
  session: { id: string; kind: string; mode: string; state: string; t0: string | null; tEnd: string | null; t0Warsaw: string | null; tEndWarsaw: string | null; configHash: string; strategy: string; executionProfile: string; intervention: boolean };
  dataDisclaimer: string | null;
  capital: {
    initialUsd: string;
    equityTotalLowerBoundUsd: string | null;
    equityLiquidLowerBoundUsd: string | null;
    equityTotalFreshUsd: string | null;
    rentLockedUsd: string | null;
    openPositions: Array<{ mint: string; status: string; valuation: string | null; costUsd: string | null; lastMarkUsd: string | null }>;
    atTEnd: unknown;
  };
  pnl: {
    realizedTradePnlUsd: string;
    netPortfolioResultUsd: string | null;
    fxEffectOfStartAllocationUsd: string | null;
    benchmarkHoldStartAllocUsd: string | null;
    benchmarkAllUsdcUsd: string | null;
    infrastructureCostUsd: string;
    resultAfterInfrastructureUsd: string | null;
    feesByKind: Record<string, { usd: string; estimate: boolean }>;
  };
  counts: { signals: number; rejectionsByReason: Record<string, number>; entryAttempts: number; fills: number; failedAttempts: number };
  trades: TradeStatsJson;
  drawdown: { freshUsd: string; freshBps: number; lowerBoundUsd: string; lowerBoundBps: number };
  dataQuality: { gaps: Array<{ kind: string; startedAt: string; endedAt: string | null; positionsOpen: boolean; explained: boolean }>; reconciliationFailures: number; quoteLatencyMsP50: number | null; quoteLatencyMsP95: number | null; quoteLatencyMsP99: number | null; stressCoverage: StressCoverage };
  verdicts: { technical: "PASS" | "FAIL" | "NOT_APPLICABLE"; technicalReasons: string[]; sample: "INSUFFICIENT_SAMPLE" | "SAMPLE_OK_NOT_PROOF"; canaryRecommendation: "CONSIDER_TECHNICAL_CANARY" | "CONTINUE_PAPER_REVIEW"; canaryReasons: string[]; note: string };
  decisions: Array<{ positionId: string; mint: string; status: string; entryAt: string | null; exitAt: string | null; exitReason: string | null; costUsd: string | null; pnlUsd: string | null }>;
  buildStatus: typeof BUILD_STATUS;
}

type TradeStatsJson = Record<keyof TradeStats, unknown>;

export interface StressCoverage {
  attempts: number;
  withAnalytic5s: number;
  withAnalytic15s: number;
  profiles: Record<string, { replayable: number; wouldFail: number; outputDeltaRawSum: string; note: string }>;
}

const s = (x: Dec | null | undefined) => (x === null || x === undefined ? null : x.toFixed(6));

export async function buildReport(pool: Pool, sessionId: string, cfg: Config, opts: { codeVersion: string; infrastructureCostUsd?: Dec | null; now: Date }): Promise<Report> {
  const session = await getSession(pool, sessionId);
  if (!session) throw new Error("session not found");
  const q = <T extends Record<string, unknown>>(sql: string, params: unknown[] = [sessionId]) => pool.query<T>(sql, params).then((r) => r.rows);

  const lastEq = (await q<{ equity_total_lower_bound_usd: string; equity_liquid_lower_bound_usd: string; equity_total_fresh_usd: string | null; breakdown: { rent_locked: string } }>(
    `SELECT * FROM equity_snapshots WHERE session_id=$1 ORDER BY at DESC LIMIT 1`,
  ))[0];
  const bench = (await q<{ hold_start_alloc_usd: string; all_usdc_usd: string }>(`SELECT * FROM benchmark_snapshots WHERE session_id=$1 ORDER BY at DESC LIMIT 1`))[0];
  const positions = await q<{ id: string; mint: string; status: string; valuation_status: string | null; cost_usd: string | null; last_mark_usd: string | null; entry_filled_at: Date | null; closed_at: Date | null; exit_reason: string | null; realized_pnl_usd: string | null }>(
    `SELECT * FROM positions WHERE session_id=$1 ORDER BY entry_filled_at NULLS LAST, id`,
  );
  const fills = await q<{ position_id: string; side: string; in_amount_raw: string; out_amount_raw: string; usdc_usd: string }>(
    `SELECT f.* FROM fills f JOIN positions p ON p.id=f.position_id WHERE p.session_id=$1`,
  );
  const feeRows = await q<{ kind: string; asset: string; amount_raw: string; usd_fx: string | null; is_estimate: boolean; included_in_quote: boolean; fill_id: string | null; position_id: string | null }>(
    `SELECT fi.*, f.position_id FROM fee_items fi JOIN order_attempts a ON a.id=fi.attempt_id JOIN trade_intents i ON i.id=a.intent_id LEFT JOIN fills f ON f.id=fi.fill_id WHERE i.session_id=$1`,
  );

  // trades
  const trades: ClosedTrade[] = [];
  for (const p of positions) {
    if (p.status !== "CLOSED" || !p.entry_filled_at || p.realized_pnl_usd === null) continue;
    const buy = fills.find((f) => f.position_id === p.id && f.side === "BUY");
    const sell = fills.find((f) => f.position_id === p.id && f.side === "SELL");
    if (!buy || !sell) continue;
    const fees = feeRows
      .filter((f) => f.position_id === p.id && !f.included_in_quote && f.asset === NATIVE_SOL && f.usd_fx !== null)
      .reduce((a, f) => a.add(rawToUsd(BigInt(f.amount_raw), SOL_DECIMALS, new D(f.usd_fx!))), new D(0));
    trades.push({
      positionId: p.id,
      mint: p.mint,
      entryAt: p.entry_filled_at,
      exitAt: p.closed_at!,
      costUsd: new D(p.cost_usd!),
      pnlUsd: new D(p.realized_pnl_usd),
      exitReason: p.exit_reason ?? "",
      entryNotionalUsd: new D(buy.in_amount_raw).div(1e6).mul(buy.usdc_usd),
      exitProceedsUsd: new D(sell.out_amount_raw).div(1e6).mul(sell.usdc_usd),
      feesUsd: fees,
    });
  }
  const stats = tradeStats(trades);

  const feesByKind: Report["pnl"]["feesByKind"] = {};
  for (const f of feeRows) {
    if (f.included_in_quote || f.usd_fx === null || f.asset !== NATIVE_SOL) continue;
    const cur = feesByKind[f.kind] ?? { usd: "0", estimate: false };
    feesByKind[f.kind] = { usd: new D(cur.usd).add(rawToUsd(BigInt(f.amount_raw), SOL_DECIMALS, new D(f.usd_fx))).toFixed(6), estimate: cur.estimate || f.is_estimate };
  }

  const eqSeries = await q<{ at: Date; equity_total_lower_bound_usd: string; equity_total_fresh_usd: string | null }>(`SELECT at, equity_total_lower_bound_usd, equity_total_fresh_usd FROM equity_snapshots WHERE session_id=$1 ORDER BY at`);
  const ddFresh = maxDrawdown(eqSeries.filter((e) => e.equity_total_fresh_usd !== null).map((e) => ({ at: e.at, value: new D(e.equity_total_fresh_usd!) })));
  const ddLower = maxDrawdown(eqSeries.map((e) => ({ at: e.at, value: new D(e.equity_total_lower_bound_usd) })));

  const signals = (await q<{ n: number }>(`SELECT count(*)::int AS n FROM signals WHERE session_id=$1`))[0]!.n;
  const rej = await q<{ reason_code: string; n: number }>(`SELECT reason_code, count(*)::int AS n FROM rejection_reasons WHERE session_id=$1 GROUP BY reason_code ORDER BY n DESC`);
  const att = (await q<{ entry: number; failed: number }>(
    `SELECT count(*) FILTER (WHERE i.kind='ENTRY')::int AS entry, count(*) FILTER (WHERE a.state='FAILED_PAPER')::int AS failed FROM order_attempts a JOIN trade_intents i ON i.id=a.intent_id WHERE i.session_id=$1`,
  ))[0]!;
  const gaps = await q<{ kind: string; started_at: Date; ended_at: Date | null; positions_open: boolean; explained: boolean }>(`SELECT * FROM data_gaps WHERE session_id=$1 ORDER BY started_at`);
  const recFail = (await q<{ n: number }>(`SELECT count(*)::int AS n FROM reconciliation_runs WHERE session_id=$1 AND NOT ok`))[0]!.n;
  const lat = (await q<{ ms: number }>(
    `SELECT extract(epoch from (received_at - requested_at))*1000 AS ms FROM quotes WHERE (attempt_id IN (SELECT a.id FROM order_attempts a JOIN trade_intents i ON i.id=a.intent_id WHERE i.session_id=$1)) OR position_id IN (SELECT id FROM positions WHERE session_id=$1)`,
  )).map((r) => Number(r.ms));

  const stress = await stressCoverage(pool, sessionId);
  const initial = new D(cfg.capital.initial_total_usd);
  const equityTotal = lastEq ? new D(lastEq.equity_total_lower_bound_usd) : null;
  const infra = opts.infrastructureCostUsd ?? null;

  // verdicts (never enable anything)
  const techReasons: string[] = [];
  const durationOk = session.t0 && session.t_end && ["COMPLETED", "INCOMPLETE"].includes(session.state);
  if (!durationOk) techReasons.push("sesja 168 h nie została zakończona");
  if (gaps.some((g) => g.positions_open && !g.explained && g.ended_at && g.ended_at.getTime() - g.started_at.getTime() > 60_000)) techReasons.push("niewyjaśnione luki > 60 s przy otwartych pozycjach");
  if (recFail > 0) techReasons.push(`${recFail} nieudanych uzgodnień salda`);
  const dupFills = (await q<{ n: number }>(`SELECT count(*)::int AS n FROM (SELECT attempt_id FROM fills GROUP BY attempt_id HAVING count(*) > 1) x`, []))[0]!.n;
  if (dupFills > 0) techReasons.push("zduplikowane fille");
  const unresolved = (await q<{ n: number }>(`SELECT count(*)::int AS n FROM order_attempts a JOIN trade_intents i ON i.id=a.intent_id WHERE i.session_id=$1 AND a.state='QUOTED'`))[0]!.n;
  if (unresolved > 0) techReasons.push(`${unresolved} nierozstrzygniętych prób`);
  const c = await pool.connect();
  let ledgerOk = false;
  try {
    ledgerOk = (await loadLedger(c, sessionId)).verifyIdentity().ok;
  } finally {
    c.release();
  }
  if (!ledgerOk) techReasons.push("rozjazd księgi");
  const technical = session.kind === "DEMO" ? "NOT_APPLICABLE" : techReasons.length === 0 ? "PASS" : "FAIL";
  const sample = stats.closed < 30 || stats.distinctTokens < 15 ? "INSUFFICIENT_SAMPLE" : "SAMPLE_OK_NOT_PROOF";
  const canaryReasons: string[] = [];
  if (technical !== "PASS") canaryReasons.push("brak technicznego PASS");
  if (!stats.totalPnlUsd.gt(0)) canaryReasons.push("wynik transakcyjny netto nie jest dodatni");
  if (infra === null) canaryReasons.push("nieujawniony koszt infrastruktury");
  else if (!stats.totalPnlUsd.sub(infra).gt(0)) canaryReasons.push("wynik po kosztach infrastruktury nie jest dodatni");
  if (stats.pnlWithoutBestTokenUsd.lt(0)) canaryReasons.push("wynik bez najlepszego tokena ujemny");
  if (stress.attempts === 0 || stress.withAnalytic15s < stress.attempts) canaryReasons.push("niepełne pokrycie stress replay");

  return {
    generatedAt: opts.now.toISOString(),
    codeVersion: opts.codeVersion,
    session: {
      id: session.id,
      kind: session.kind,
      mode: session.mode,
      state: session.state,
      t0: session.t0?.toISOString() ?? null,
      tEnd: session.t_end?.toISOString() ?? null,
      t0Warsaw: session.t0 ? formatWarsaw(session.t0) : null,
      tEndWarsaw: session.t_end ? formatWarsaw(session.t_end) : null,
      configHash: session.config_hash,
      strategy: `${session.strategy_name}@${session.strategy_version}`,
      executionProfile: `${cfg.experiment.execution_profile} / ${cfg.execution.profile}`,
      intervention: session.intervention,
    },
    dataDisclaimer: session.kind === "DEMO" ? "DEMO — dane z fixtures, nie są wynikiem rynkowym" : session.kind === "INFRA_TEST" ? "INFRA_TEST — raport nie ocenia skuteczności confluence" : null,
    capital: {
      initialUsd: initial.toFixed(2),
      equityTotalLowerBoundUsd: s(equityTotal),
      equityLiquidLowerBoundUsd: lastEq ? new D(lastEq.equity_liquid_lower_bound_usd).toFixed(6) : null,
      equityTotalFreshUsd: lastEq?.equity_total_fresh_usd ? new D(lastEq.equity_total_fresh_usd).toFixed(6) : null,
      rentLockedUsd: lastEq ? new D(lastEq.breakdown.rent_locked).toFixed(6) : null,
      openPositions: positions.filter((p) => p.status !== "CLOSED").map((p) => ({ mint: p.mint, status: p.status, valuation: p.valuation_status, costUsd: p.cost_usd, lastMarkUsd: p.last_mark_usd })),
      atTEnd: session.t_end_snapshot ?? null,
    },
    pnl: {
      realizedTradePnlUsd: stats.totalPnlUsd.toFixed(6),
      netPortfolioResultUsd: s(equityTotal?.sub(initial)),
      fxEffectOfStartAllocationUsd: bench ? new D(bench.hold_start_alloc_usd).sub(initial).toFixed(6) : null,
      benchmarkHoldStartAllocUsd: bench ? new D(bench.hold_start_alloc_usd).toFixed(6) : null,
      benchmarkAllUsdcUsd: bench ? new D(bench.all_usdc_usd).toFixed(6) : null,
      infrastructureCostUsd: infra === null ? "UNKNOWN (brak danych billingowych)" : infra.toFixed(2),
      resultAfterInfrastructureUsd: infra === null ? null : stats.totalPnlUsd.sub(infra).toFixed(6),
      feesByKind,
    },
    counts: { signals, rejectionsByReason: Object.fromEntries(rej.map((r) => [r.reason_code, r.n])), entryAttempts: att.entry, fills: fills.length, failedAttempts: att.failed },
    trades: jsonStats(stats),
    drawdown: { freshUsd: ddFresh.usd.toFixed(6), freshBps: ddFresh.bps, lowerBoundUsd: ddLower.usd.toFixed(6), lowerBoundBps: ddLower.bps },
    dataQuality: {
      gaps: gaps.map((g) => ({ kind: g.kind, startedAt: g.started_at.toISOString(), endedAt: g.ended_at?.toISOString() ?? null, positionsOpen: g.positions_open, explained: g.explained })),
      reconciliationFailures: recFail,
      quoteLatencyMsP50: percentile(lat, 50),
      quoteLatencyMsP95: percentile(lat, 95),
      quoteLatencyMsP99: percentile(lat, 99),
      stressCoverage: stress,
    },
    verdicts: {
      technical,
      technicalReasons: techReasons,
      sample,
      canaryRecommendation: canaryReasons.length === 0 ? "CONSIDER_TECHNICAL_CANARY" : "CONTINUE_PAPER_REVIEW",
      canaryReasons,
      note: "Raport nie włącza trybu LIVE. Siedem dni nie dowodzi stabilnej przewagi; brak annualizacji.",
    },
    decisions: positions.map((p) => ({
      positionId: p.id,
      mint: p.mint,
      status: p.status,
      entryAt: p.entry_filled_at?.toISOString() ?? null,
      exitAt: p.closed_at?.toISOString() ?? null,
      exitReason: p.exit_reason,
      costUsd: p.cost_usd,
      pnlUsd: p.realized_pnl_usd,
    })),
    buildStatus: BUILD_STATUS,
  };
}

function jsonStats(st: TradeStats): TradeStatsJson {
  return JSON.parse(JSON.stringify(st, (_k, v) => (v && typeof v === "object" && "toFixed" in v && typeof v.toFixed === "function" && !(v instanceof Date) ? (v as Dec).toFixed(6) : v)));
}

/**
 * Stress replay coverage from stored quotes only (brief §13): STRESS uses Q0 vs +5 s quote, SEVERE Q0 vs +15 s.
 * Output deltas are for the recorded BASE quantity; downstream PnL for a different quantity is REPLAY_UNSUPPORTED.
 */
async function stressCoverage(pool: Pool, sessionId: string): Promise<StressCoverage> {
  const rows = (
    await pool.query<{ attempt_id: string; role: string; out_amount_net_raw: string | null; ok: boolean; kind: string }>(
      `SELECT q.attempt_id, q.role, q.out_amount_net_raw, q.ok, i.kind FROM quotes q JOIN order_attempts a ON a.id=q.attempt_id JOIN trade_intents i ON i.id=a.intent_id
       WHERE i.session_id=$1 AND q.role IN ('Q0','ANALYTIC_5S','ANALYTIC_15S')`,
      [sessionId],
    )
  ).rows;
  const byAttempt = new Map<string, Record<string, bigint | null>>();
  const kindOf = new Map<string, string>();
  for (const r of rows) {
    kindOf.set(r.attempt_id, r.kind);
    const m = byAttempt.get(r.attempt_id) ?? {};
    m[r.role] = r.ok && r.out_amount_net_raw ? BigInt(r.out_amount_net_raw) : null;
    byAttempt.set(r.attempt_id, m);
  }
  const profiles: StressCoverage["profiles"] = {};
  for (const [name, role] of [
    ["STRESS", "ANALYTIC_5S"],
    ["SEVERE", "ANALYTIC_15S"],
  ] as const) {
    const p = EXECUTION_PROFILES[name];
    let replayable = 0;
    let wouldFail = 0;
    let delta = 0n;
    for (const [attemptId, m] of byAttempt) {
      const q0 = m.Q0;
      const cap = kindOf.get(attemptId) === "ENTRY" ? 100 : kindOf.get(attemptId) === "EXIT_EMERGENCY" ? 300 : 150;
      if (q0 === undefined || q0 === null || !(role in m)) continue;
      replayable++;
      const later = m[role];
      if (later === null || later === undefined) {
        wouldFail++;
        continue;
      }
      const cand = reduceByBpsFloor(later < q0 ? later : q0, bps(p.haircut_bps));
      const minOut = reduceByBpsFloor(q0, bps(cap));
      if (cand < minOut) wouldFail++;
      else delta += cand - reduceByBpsFloor(q0, bps(EXECUTION_PROFILES.BASE.haircut_bps));
    }
    profiles[name] = { replayable, wouldFail, outputDeltaRawSum: delta.toString(), note: `${p.extra_delay_ms} ms / ${p.haircut_bps} bps haircut; scenario, not a forecast; separate analytical ledger; failure-rate component not replayed` };
  }
  return {
    attempts: byAttempt.size,
    withAnalytic5s: [...byAttempt.values()].filter((m) => "ANALYTIC_5S" in m).length,
    withAnalytic15s: [...byAttempt.values()].filter((m) => "ANALYTIC_15S" in m).length,
    profiles,
  };
}
