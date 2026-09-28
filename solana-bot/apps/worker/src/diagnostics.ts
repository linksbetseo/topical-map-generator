import { D, type Dec } from "@solbot/domain";
import type { Config } from "@solbot/config";
import { json, type Pool } from "@solbot/db";
import { Priority, TX_EXTRACTOR_VERSION, type BinanceMinuteFx, type CompactTx, type JupiterClient, type SignatureInfo, type SignatureScan, type SolanaHistory, type TxCache } from "@solbot/providers";
import {
  BALANCE_NORMALIZER_VERSION,
  effectToEvents,
  qualifyWallet,
  reconstructEpisodes,
  walletMetrics,
  walletTxEffect,
  type BaseFx,
  type Episode,
  type TxClass,
  type WalletEvent,
} from "@solbot/strategy";

/**
 * P0 diagnostics of bootstrap candidates (spec v2 §3). Read-only. For every address it records how much
 * history was actually obtained, how it was parsed and priced, the reconstructed result and every
 * violated criterion. The qualification thresholds are the configured ones (unchanged); the extra
 * diagnostic cut-offs below only decide which *category* explains a rejection, never qualification.
 */
export const DIAGNOSTICS_VERSION = 1;
export const PRICING_VERSION = "binance-1m-open:SOLUSDT,USDCUSDT;USDT=USD parity;v1";

export const REJECT_CATEGORIES = [
  "API_ERROR",
  "HISTORY_INCOMPLETE",
  "PARSER_UNSUPPORTED",
  "MISSING_PRICE",
  "UNKNOWN_COST_BASIS",
  "INSUFFICIENT_SAMPLE",
  "NEGATIVE_PNL",
  "LOW_PF",
  "RISK_OR_COPYABILITY_FAIL",
] as const;
export type Category = (typeof REJECT_CATEGORIES)[number] | "QUALIFIED_PROVISIONAL";

/** diagnostic cut-offs (category attribution only) */
export const DIAG_UNSUPPORTED_SHARE = 0.1;
export const DIAG_UNKNOWN_COST_SHARE = 0.2;
/** a signature scan younger than this is reused instead of paid again */
export const SCAN_REUSE_HOURS = 6;

export interface DiagnosticsOptions {
  windowDays: number;
  maxSignaturePages: number;
  maxTxPerWallet: number;
  maxRpcCalls: number;
  concurrency: number;
  addresses?: string[];
}

export interface DiagnosticsDeps {
  pool: Pool;
  history: SolanaHistory;
  fx: BinanceMinuteFx;
  jup: JupiterClient;
  cfg: Config;
  now: () => Date;
  log: (m: string) => void;
}

export interface WalletDiagnostic {
  wallet_address: string;
  candidate_discovered_at: string | null;
  candidate_source: string | null;
  discovery_token_mints: string[];
  history_requested_from: string;
  history_requested_to: string;
  oldest_observed_time: string | null;
  newest_observed_time: string | null;
  signatures_in_window: number;
  pagination_exhausted: boolean;
  history_complete: boolean;
  budget_exhausted: boolean;
  pages_requested: number;
  provider_requests: number;
  estimated_credits: number;
  cache_hits: number;
  successful_transactions: number;
  failed_transactions: number;
  fetch_errors: number;
  parser_errors: number;
  class_counts: Partial<Record<TxClass, number>>;
  normalized_swaps: number;
  transfers: number;
  unknown_events: number;
  priced_swap_count: number;
  priced_notional_usd: string | null;
  unpriced_material_events: number;
  unknown_opening_inventory: number;
  unresolved_external_transfers: number;
  transfer_only_mints: number;
  position_episodes: number;
  closed_episodes: number;
  unknown_cost_episodes: number;
  /** closed episodes that cost less than 1 USD (test buys / dust) — counted in closed_episodes, shown apart */
  dust_closed_episodes: number;
  median_closed_episode_cost_usd: string | null;
  distinct_risk_tokens: number;
  active_utc_days: number;
  losing_episodes: number;
  realized_pnl_usd: string | null;
  open_pnl_mark_usd: string | null;
  open_pnl_zero_floor_usd: string | null;
  total_pnl_usd: string | null;
  profit_factor: string | null;
  largest_winner_share_of_gross_profit: string | null;
  largest_token_share_bps: number | null;
  pnl_excluding_discovery_tokens: string | null;
  pnl_excluding_best_token: string | null;
  network_fees_sol: string;
  rent_deposit_net_sol: string | null;
  primary_reject_reason: Category;
  all_reject_reasons: Category[];
  criteria_failed: string[];
  legacy: { status: string | null; reasons: string[]; total_pnl_usd: string | null; closed_episodes: number | null } | null;
  parser_version: string;
  pricing_version: string;
  raw_evidence: { signature_scan_id: number | null; cached_transactions: number };
}

// ------------------------------------------------------------------ durable cache

export class DbTxCache implements TxCache {
  constructor(private readonly pool: Pool, private readonly now: () => Date) {}
  async get(signatures: string[]): Promise<Map<string, CompactTx>> {
    const out = new Map<string, CompactTx>();
    for (let i = 0; i < signatures.length; i += 1000) {
      const r = await this.pool.query<{ signature: string; compact: CompactTx }>(
        `SELECT signature, compact FROM rpc_tx_cache WHERE signature = ANY($1) AND extractor_version = $2`,
        [signatures.slice(i, i + 1000), TX_EXTRACTOR_VERSION],
      );
      for (const row of r.rows) out.set(row.signature, row.compact);
    }
    return out;
  }
  async put(txs: CompactTx[]): Promise<void> {
    for (const t of txs)
      await this.pool.query(
        `INSERT INTO rpc_tx_cache (signature, slot, block_time, extractor_version, compact, source, fetched_at) VALUES ($1,$2,$3,$4,$5,'helius-rpc:getTransaction',$6) ON CONFLICT (signature) DO NOTHING`,
        [t.signature, t.slot, t.blockTime === null ? null : new Date(t.blockTime * 1000), t.v, json(t), this.now()],
      );
  }
}

async function cachedScan(d: DiagnosticsDeps, address: string, gte: number, lte: number, maxPages: number): Promise<{ scan: SignatureScan; id: number; fresh: boolean } | { error: string }> {
  const prev = await d.pool.query<{ id: number; complete: boolean; calls: number; oldest_seen: Date | null; newest_seen: Date | null; signatures: Array<[string, number, number | null, boolean]> }>(
    // checkpoint: reuse a scan of this address made in the last SCAN_REUSE_HOURS that starts at or before
    // the requested window (signatures newer than that scan are not seen — stated in the report params)
    `SELECT id, complete, calls, oldest_seen, newest_seen, signatures FROM rpc_signature_scans
      WHERE address=$1 AND gte_time <= $2 AND scanned_at > $3 ORDER BY scanned_at DESC LIMIT 1`,
    [address, new Date(gte * 1000), new Date(d.now().getTime() - SCAN_REUSE_HOURS * 3_600_000)],
  );
  const p = prev.rows[0];
  if (p && (p.complete || p.calls >= maxPages)) {
    const sigs: SignatureInfo[] = p.signatures
      .map(([signature, slot, blockTime, failed]) => ({ signature, slot, blockTime, failed }))
      .filter((x) => x.blockTime === null || (x.blockTime >= gte && x.blockTime <= lte));
    const sec = (x: Date | null) => (x ? Math.floor(new Date(x).getTime() / 1000) : null);
    return { scan: { signatures: sigs, complete: p.complete, calls: 0, oldestSeen: sec(p.oldest_seen), newestSeen: sec(p.newest_seen) }, id: p.id, fresh: false };
  }
  const r = await d.history.signatures(address, { gteTime: gte, lteTime: lte, maxPages });
  if (!r.ok) return { error: `${r.code} ${r.detail}` };
  const s = r.value;
  const ins = await d.pool.query<{ id: number }>(
    `INSERT INTO rpc_signature_scans (address, gte_time, lte_time, complete, calls, oldest_seen, newest_seen, signatures, scanned_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
    [
      address,
      new Date(gte * 1000),
      new Date(lte * 1000),
      s.complete,
      s.calls,
      s.oldestSeen === null ? null : new Date(s.oldestSeen * 1000),
      s.newestSeen === null ? null : new Date(s.newestSeen * 1000),
      json(s.signatures.map((x) => [x.signature, x.slot, x.blockTime, x.failed])),
      d.now(),
    ],
  );
  return { scan: s, id: ins.rows[0]!.id, fresh: true };
}

// ------------------------------------------------------------------ classification

export function categorize(x: {
  apiError: boolean;
  historyIncomplete: boolean;
  unsupportedShare: number;
  coverageBps: number;
  unknownCostShare: number;
  criteria: { closed: boolean; distinct: boolean; days: boolean; losing: boolean; pnl: boolean; pf: boolean; concentration: boolean; infrastructure: boolean; coverage: boolean };
}, cfg: Config): Category[] {
  const out: Category[] = [];
  if (x.apiError) out.push("API_ERROR");
  if (x.historyIncomplete) out.push("HISTORY_INCOMPLETE");
  if (x.apiError || x.historyIncomplete) return out; // metrics of an unevaluated history are not reported as findings
  if (x.unsupportedShare >= DIAG_UNSUPPORTED_SHARE) out.push("PARSER_UNSUPPORTED");
  if (!x.criteria.coverage || x.coverageBps < cfg.wallets.min_volume_coverage_bps) out.push("MISSING_PRICE");
  if (x.unknownCostShare >= DIAG_UNKNOWN_COST_SHARE) out.push("UNKNOWN_COST_BASIS");
  if (!x.criteria.closed || !x.criteria.distinct || !x.criteria.days || !x.criteria.losing) out.push("INSUFFICIENT_SAMPLE");
  if (!x.criteria.pnl) out.push("NEGATIVE_PNL");
  if (!x.criteria.pf) out.push("LOW_PF");
  if (!x.criteria.concentration || !x.criteria.infrastructure) out.push("RISK_OR_COPYABILITY_FAIL");
  return out;
}

const s = (d: Dec | null | undefined) => (d === null || d === undefined ? null : d.toFixed(2));
const iso = (sec: number | null) => (sec === null ? null : new Date(sec * 1000).toISOString());

function pnlOf(eps: Episode[], exclude: ReadonlySet<string>): Dec | null {
  const scored = eps.filter((e) => e.pnlUsd !== null && !exclude.has(e.mint));
  return scored.reduce((a, e) => a.add(e.pnlUsd!), new D(0));
}

// ------------------------------------------------------------------ run

export async function runDiagnostics(d: DiagnosticsDeps, sessionId: string, o: DiagnosticsOptions): Promise<{ runId: string; summary: DiagnosticsSummary }> {
  const now = d.now();
  const lte = Math.floor(now.getTime() / 1000);
  const gte = lte - o.windowDays * 86_400;
  const runId = `diag_${now.toISOString().replace(/[-:.TZ]/g, "")}`;
  await d.pool.query(`INSERT INTO wallet_diagnostic_runs (run_id, session_id, params, started_at) VALUES ($1,$2,$3,$4)`, [
    runId,
    sessionId,
    json({ ...o, gte: iso(gte), lte: iso(lte), diagnosticsVersion: DIAGNOSTICS_VERSION, extractorVersion: TX_EXTRACTOR_VERSION, normalizerVersion: BALANCE_NORMALIZER_VERSION, pricingVersion: PRICING_VERSION, thresholds: d.cfg.wallets }),
    now,
  ]);

  const cands = (
    await d.pool.query<{ address: string; candidate_source: string | null; first_seen_at: Date | null; status: string | null; reasons: Array<{ detail?: string }> | null; metrics: Record<string, unknown> | null }>(
      `SELECT q.address, w.candidate_source, w.first_seen_at, q.status, q.reasons, q.metrics
         FROM wallet_qualification q LEFT JOIN wallets w ON w.address=q.address
        WHERE q.session_id=$1 ${o.addresses ? "AND q.address = ANY($2)" : ""} ORDER BY q.address`,
      o.addresses ? [sessionId, o.addresses] : [sessionId],
    )
  ).rows;
  d.log(`diagnostics ${runId}: ${cands.length} addresses, window ${iso(gte)} .. ${iso(lte)}`);

  // stage A: signature scans (1 credit per 1000 signatures) for everyone
  const scans = new Map<string, { scan: SignatureScan; id: number } | { error: string }>();
  for (const c of cands) {
    const r = await cachedScan(d, c.address, gte, lte, o.maxSignaturePages);
    scans.set(c.address, r);
  }
  const sigCalls = d.history.calls;
  d.log(`signature scans done: ${sigCalls} rpc calls`);

  // stage B: cheapest complete histories first, within the per-wallet and global budgets
  const order = [...cands].sort((a, b) => {
    const n = (x: string) => {
      const r = scans.get(x)!;
      return "error" in r ? Number.MAX_SAFE_INTEGER : r.scan.signatures.filter((q) => !q.failed).length;
    };
    return n(a.address) - n(b.address) || a.address.localeCompare(b.address);
  });

  const marks = new Map<string, Dec>();
  const records: WalletDiagnostic[] = [];
  for (const c of order) {
    const sc = scans.get(c.address)!;
    let source: { seed_mints?: string[]; source?: string } = {};
    try {
      source = c.candidate_source ? JSON.parse(c.candidate_source) : {};
    } catch {
      source = {};
    }
    const legacy = c.status
      ? {
          status: c.status,
          reasons: (c.reasons ?? []).map((r) => String(r.detail ?? "")),
          total_pnl_usd: typeof c.metrics?.totalPnlUsd === "string" ? (c.metrics.totalPnlUsd as string) : c.metrics?.totalPnlUsd ? String(c.metrics.totalPnlUsd) : null,
          closed_episodes: typeof c.metrics?.closedEpisodes === "number" ? (c.metrics.closedEpisodes as number) : null,
        }
      : null;
    const base = {
      wallet_address: c.address,
      candidate_discovered_at: c.first_seen_at ? new Date(c.first_seen_at).toISOString() : null,
      candidate_source: source.source ?? null,
      discovery_token_mints: source.seed_mints ?? [],
      history_requested_from: iso(gte)!,
      history_requested_to: iso(lte)!,
      legacy,
      parser_version: `extractor v${TX_EXTRACTOR_VERSION}; balance-normalizer v${BALANCE_NORMALIZER_VERSION}`,
      pricing_version: PRICING_VERSION,
    };
    const empty = {
      oldest_observed_time: null,
      newest_observed_time: null,
      signatures_in_window: 0,
      pagination_exhausted: false,
      history_complete: false,
      budget_exhausted: false,
      pages_requested: 0,
      provider_requests: 0,
      estimated_credits: 0,
      cache_hits: 0,
      successful_transactions: 0,
      failed_transactions: 0,
      fetch_errors: 0,
      parser_errors: 0,
      class_counts: {},
      normalized_swaps: 0,
      transfers: 0,
      unknown_events: 0,
      priced_swap_count: 0,
      priced_notional_usd: null,
      unpriced_material_events: 0,
      unknown_opening_inventory: 0,
      unresolved_external_transfers: 0,
      transfer_only_mints: 0,
      position_episodes: 0,
      closed_episodes: 0,
      unknown_cost_episodes: 0,
      dust_closed_episodes: 0,
      median_closed_episode_cost_usd: null,
      distinct_risk_tokens: 0,
      active_utc_days: 0,
      losing_episodes: 0,
      realized_pnl_usd: null,
      open_pnl_mark_usd: null,
      open_pnl_zero_floor_usd: null,
      total_pnl_usd: null,
      profit_factor: null,
      largest_winner_share_of_gross_profit: null,
      largest_token_share_bps: null,
      pnl_excluding_discovery_tokens: null,
      pnl_excluding_best_token: null,
      network_fees_sol: "0",
      rent_deposit_net_sol: "0",
      criteria_failed: [] as string[],
      raw_evidence: { signature_scan_id: null as number | null, cached_transactions: 0 },
    };
    if ("error" in sc) {
      records.push({ ...base, ...empty, criteria_failed: [`signature scan: ${sc.error}`], primary_reject_reason: "API_ERROR", all_reject_reasons: ["API_ERROR"] });
      continue;
    }
    const { scan } = sc;
    const ok = scan.signatures.filter((x) => !x.failed);
    const rec = {
      ...base,
      ...empty,
      oldest_observed_time: iso(scan.oldestSeen),
      newest_observed_time: iso(scan.newestSeen),
      signatures_in_window: scan.signatures.length,
      pagination_exhausted: scan.complete,
      pages_requested: scan.calls,
      successful_transactions: ok.length,
      failed_transactions: scan.signatures.length - ok.length,
      raw_evidence: { signature_scan_id: sc.id, cached_transactions: 0 },
    };
    const remaining = o.maxRpcCalls - d.history.calls;
    if (!scan.complete || ok.length > o.maxTxPerWallet || ok.length > remaining) {
      const why = !scan.complete
        ? `signature scan stopped after ${o.maxSignaturePages} pages before reaching the window start`
        : ok.length > o.maxTxPerWallet
          ? `${ok.length} transactions in window > per-wallet budget ${o.maxTxPerWallet} (deferred, not scored)`
          : `global budget left ${remaining} < ${ok.length} (deferred, not scored)`;
      records.push({ ...rec, budget_exhausted: scan.complete, estimated_credits: ok.length, criteria_failed: [why], primary_reject_reason: "HISTORY_INCOMPLETE", all_reject_reasons: ["HISTORY_INCOMPLETE"] });
      continue;
    }

    const before = d.history.calls;
    const hitsBefore = d.history.cacheHits;
    const { txs, errors } = await d.history.transactions(ok.map((x) => x.signature), o.concurrency);
    const requests = d.history.calls - before;
    rec.provider_requests = requests;
    rec.estimated_credits = requests;
    rec.cache_hits = d.history.cacheHits - hitsBefore;
    rec.fetch_errors = errors.size;
    rec.raw_evidence.cached_transactions = txs.size;
    rec.history_complete = errors.size === 0;
    if (errors.size > 0) {
      records.push({ ...rec, criteria_failed: [`${errors.size} transactions could not be fetched: ${[...errors.values()].slice(0, 3).join("; ")}`], primary_reject_reason: "API_ERROR", all_reject_reasons: ["API_ERROR"] });
      continue;
    }

    // normalize every transaction from balance changes
    const counts: Partial<Record<TxClass, number>> = {};
    const events: WalletEvent[] = [];
    let fees = 0n;
    let rent: bigint | null = 0n;
    let unpriced = 0;
    let parserErrors = 0;
    let notional = new D(0);
    for (const x of ok) {
      const tx = txs.get(x.signature)!;
      let e;
      try {
        e = walletTxEffect(tx, c.address);
      } catch {
        parserErrors++;
        continue;
      }
      counts[e.cls] = (counts[e.cls] ?? 0) + 1;
      fees += e.feeLamports;
      rent = rent === null || e.rentLamports === null ? null : rent + e.rentLamports;
      let fx: BaseFx | null = null;
      if ((e.cls === "BUY" || e.cls === "SELL") && e.blockTime) {
        const f = await d.fx.fxAt(e.blockTime);
        fx = f ? { solUsd: f.solUsd, usdcUsd: f.usdcUsd, usdtUsd: new D(1) } : null;
      }
      for (const ev of effectToEvents(e, c.address, fx, now)) {
        if (ev.kind === "SWAP_BUY" || ev.kind === "SWAP_SELL") {
          if (ev.usd === null) unpriced++;
          else notional = notional.add(ev.usd);
        }
        events.push(ev);
      }
    }
    // mints that were only ever transferred (airdrops/spam) are not positions of this wallet
    const swapped = new Set(events.filter((e) => e.kind === "SWAP_BUY" || e.kind === "SWAP_SELL").map((e) => e.mint));
    const allMints = new Set(events.map((e) => e.mint));
    const scoredEvents = events.filter((e) => swapped.has(e.mint));
    const swapEvents = scoredEvents.filter((e) => e.kind === "SWAP_BUY" || e.kind === "SWAP_SELL");
    // unknown opening inventory: first observed event of a mint takes tokens out
    const firstByMint = new Map<string, WalletEvent>();
    for (const e of [...scoredEvents].sort((a, b) => a.blockTime.getTime() - b.blockTime.getTime())) if (!firstByMint.has(e.mint)) firstByMint.set(e.mint, e);
    const unknownOpening = [...firstByMint.values()].filter((e) => e.kind === "SWAP_SELL" || e.kind === "TRANSFER_OUT").length;

    // conservative marks for open positions: current Jupiter price (not a liquidation quote)
    const r0 = reconstructEpisodes(scoredEvents, new Date(now.getTime() + 1), new Map());
    const need = [...new Set(r0.episodes.filter((e) => e.end === null).map((e) => e.mint))].filter((m) => !marks.has(m));
    for (let i = 0; i < need.length; i += 50) {
      const pr = await d.jup.withPriority(Priority.DISCOVERY).usdPrices(need.slice(i, i + 50));
      if (!pr.ok) continue;
      const decs = new Map<string, number>();
      for (const x of ok) {
        const tx = txs.get(x.signature)!;
        for (const b of [...tx.preTokenBalances, ...tx.postTokenBalances]) decs.set(b.mint, b.decimals);
      }
      for (const [m, p] of pr.prices) {
        const dec = decs.get(m);
        if (dec !== undefined) marks.set(m, p.usdPrice.div(new D(10).pow(dec)));
      }
    }
    const recon = reconstructEpisodes(scoredEvents, new Date(now.getTime() + 1), marks);
    const m = walletMetrics(recon, swapEvents, new Date(now.getTime() + 1));
    const q = qualifyWallet(m, { infrastructure: allMints.size > 400, deployerOfObservedToken: false }, d.cfg);
    const reconZero = reconstructEpisodes(scoredEvents, new Date(now.getTime() + 1), new Map());

    const closed = recon.episodes.filter((e) => e.status === "CLOSED");
    const realized = closed.reduce((a, e) => a.add(e.pnlUsd!), new D(0));
    const open = recon.episodes.filter((e) => e.end === null && e.pnlUsd !== null);
    const openMark = open.reduce((a, e) => a.add(e.pnlUsd!), new D(0));
    const openZero = reconZero.episodes.filter((e) => e.end === null && e.pnlUsd !== null).reduce((a, e) => a.add(e.pnlUsd!), new D(0));
    const winners = recon.episodes.filter((e) => e.pnlUsd !== null && e.pnlUsd.gt(0));
    const maxWin = winners.reduce((a, e) => (e.pnlUsd!.gt(a) ? e.pnlUsd! : a), new D(0));
    const byToken = new Map<string, Dec>();
    for (const e of recon.episodes) if (e.pnlUsd !== null) byToken.set(e.mint, (byToken.get(e.mint) ?? new D(0)).add(e.pnlUsd));
    const best = [...byToken.entries()].sort((a, b) => b[1].cmp(a[1]))[0]?.[0];

    const tradeLike = (counts.BUY ?? 0) + (counts.SELL ?? 0) + (counts.TRANSFER_IN ?? 0) + (counts.TRANSFER_OUT ?? 0) + (counts.UNSUPPORTED ?? 0);
    const unsupportedShare = tradeLike ? (counts.UNSUPPORTED ?? 0) / tradeLike : 0;
    const unknownCostShare = recon.episodes.length ? m.unknownCostBasisEpisodes / recon.episodes.length : 0;
    const w = d.cfg.wallets;
    const criteria = {
      closed: m.closedEpisodes >= w.min_episodes,
      distinct: m.distinctTokens >= w.min_distinct_tokens,
      days: m.activeDays >= w.min_active_days,
      losing: m.losingEpisodes >= w.min_losing_episodes,
      coverage: m.coverageBps >= w.min_volume_coverage_bps,
      pnl: m.totalPnlUsd.gt(0),
      pf: m.profitFactor !== null && m.profitFactor.gte(w.min_profit_factor),
      concentration: m.maxTokenShareOfPositivePnlBps !== null && m.maxTokenShareOfPositivePnlBps <= w.max_single_token_positive_pnl_bps,
      infrastructure: !(allMints.size > 400),
    };
    const cats = categorize({ apiError: false, historyIncomplete: false, unsupportedShare, coverageBps: m.coverageBps, unknownCostShare, criteria }, d.cfg);
    records.push({
      ...rec,
      parser_errors: parserErrors,
      class_counts: counts,
      normalized_swaps: (counts.BUY ?? 0) + (counts.SELL ?? 0),
      transfers: (counts.TRANSFER_IN ?? 0) + (counts.TRANSFER_OUT ?? 0),
      unknown_events: counts.UNSUPPORTED ?? 0,
      priced_swap_count: swapEvents.length - unpriced,
      priced_notional_usd: s(notional),
      unpriced_material_events: unpriced + (counts.UNSUPPORTED ?? 0),
      unknown_opening_inventory: unknownOpening,
      unresolved_external_transfers: scoredEvents.filter((e) => e.kind === "TRANSFER_IN" || e.kind === "TRANSFER_OUT").length,
      transfer_only_mints: allMints.size - swapped.size,
      position_episodes: recon.episodes.length,
      closed_episodes: m.closedEpisodes,
      unknown_cost_episodes: m.unknownCostBasisEpisodes,
      dust_closed_episodes: closed.filter((e) => e.costUsd.lt(1)).length,
      median_closed_episode_cost_usd: closed.length ? s(closed.map((e) => e.costUsd).sort((a, b) => a.cmp(b))[Math.floor((closed.length - 1) / 2)]!) : null,
      distinct_risk_tokens: m.distinctTokens,
      active_utc_days: m.activeDays,
      losing_episodes: m.losingEpisodes,
      realized_pnl_usd: s(realized),
      open_pnl_mark_usd: s(openMark),
      open_pnl_zero_floor_usd: s(openZero),
      total_pnl_usd: s(m.totalPnlUsd),
      profit_factor: m.profitFactor ? m.profitFactor.toFixed(3) : null,
      largest_winner_share_of_gross_profit: m.grossProfitUsd.gt(0) ? maxWin.div(m.grossProfitUsd).toFixed(4) : null,
      largest_token_share_bps: m.maxTokenShareOfPositivePnlBps,
      pnl_excluding_discovery_tokens: s(pnlOf(recon.episodes, new Set(base.discovery_token_mints))),
      pnl_excluding_best_token: s(pnlOf(recon.episodes, new Set(best ? [best] : []))),
      network_fees_sol: new D(fees.toString()).div(1e9).toFixed(6),
      rent_deposit_net_sol: rent === null ? null : new D(rent.toString()).div(1e9).toFixed(6),
      criteria_failed: q.reasons.map((r) => r.detail ?? r.code),
      primary_reject_reason: cats[0] ?? "QUALIFIED_PROVISIONAL",
      all_reject_reasons: cats.length ? cats : ["QUALIFIED_PROVISIONAL"],
    });
    d.log(`${c.address}: ${ok.length} tx, ${requests} requests, ${cats[0] ?? "QUALIFIED_PROVISIONAL"}`);
  }

  for (const r of records)
    await d.pool.query(`INSERT INTO wallet_diagnostics (run_id, address, primary_reason, record, computed_at) VALUES ($1,$2,$3,$4,$5)`, [runId, r.wallet_address, r.primary_reject_reason, json(r), d.now()]);
  const summary = summarize(records, { rpcCalls: d.history.calls, signatureCalls: sigCalls, cacheHits: d.history.cacheHits, binanceCalls: d.fx.calls });
  await d.pool.query(`UPDATE wallet_diagnostic_runs SET summary=$2, finished_at=$3 WHERE run_id=$1`, [runId, json(summary), d.now()]);
  await d.pool.query(`INSERT INTO audit_events (session_id, actor, action, data, at) VALUES ($1,'worker','WALLET_DIAGNOSTICS',$2,$3)`, [sessionId, json({ runId, summary }), d.now()]);
  return { runId, summary };
}

// ------------------------------------------------------------------ summary

export interface DiagnosticsSummary {
  addresses: number;
  primaryReasons: Record<string, number>;
  allReasons: Record<string, number>;
  criteriaFailed: Record<string, number>;
  evaluated: number;
  distributions: Record<string, { n: number; p10: number; p25: number; p50: number; p75: number; p90: number; max: number } | null>;
  legacyComparison: { legacyPnlPositive: number; newPnlPositive: number; signFlips: number; compared: number };
  providerUsage: { heliusRpcCalls: number; signatureScanCalls: number; txCacheHits: number; estimatedHeliusCredits: number; binanceCalls: number };
}

export function quantiles(xs: number[]) {
  if (xs.length === 0) return null;
  const v = [...xs].sort((a, b) => a - b);
  const q = (p: number) => v[Math.min(v.length - 1, Math.floor(p * (v.length - 1)))]!;
  return { n: v.length, p10: q(0.1), p25: q(0.25), p50: q(0.5), p75: q(0.75), p90: q(0.9), max: v[v.length - 1]! };
}

export function summarize(rs: WalletDiagnostic[], usage: { rpcCalls: number; signatureCalls: number; cacheHits: number; binanceCalls: number }): DiagnosticsSummary {
  const inc = (o: Record<string, number>, k: string) => (o[k] = (o[k] ?? 0) + 1);
  const primary: Record<string, number> = {};
  const all: Record<string, number> = {};
  const crit: Record<string, number> = {};
  for (const r of rs) {
    inc(primary, r.primary_reject_reason);
    for (const c of r.all_reject_reasons) inc(all, c);
    for (const c of r.criteria_failed) inc(crit, c.replace(/-?[0-9]+(\.[0-9]+)?/g, "#"));
  }
  const ev = rs.filter((r) => r.primary_reject_reason !== "API_ERROR" && r.primary_reject_reason !== "HISTORY_INCOMPLETE");
  const num = (f: (r: WalletDiagnostic) => number | string | null) => ev.map(f).filter((x): x is number | string => x !== null).map(Number);
  const distributions = {
    signatures_in_window_all: quantiles(rs.map((r) => r.signatures_in_window)),
    successful_transactions: quantiles(num((r) => r.successful_transactions)),
    normalized_swaps: quantiles(num((r) => r.normalized_swaps)),
    closed_episodes: quantiles(num((r) => r.closed_episodes)),
    dust_closed_episodes: quantiles(num((r) => r.dust_closed_episodes)),
    median_closed_episode_cost_usd: quantiles(num((r) => r.median_closed_episode_cost_usd)),
    distinct_risk_tokens: quantiles(num((r) => r.distinct_risk_tokens)),
    active_utc_days: quantiles(num((r) => r.active_utc_days)),
    losing_episodes: quantiles(num((r) => r.losing_episodes)),
    unknown_share_of_trade_events: quantiles(ev.map((r) => (r.normalized_swaps + r.transfers + r.unknown_events ? r.unknown_events / (r.normalized_swaps + r.transfers + r.unknown_events) : 0))),
    total_pnl_usd: quantiles(num((r) => r.total_pnl_usd)),
    realized_pnl_usd: quantiles(num((r) => r.realized_pnl_usd)),
    profit_factor: quantiles(num((r) => r.profit_factor)),
  };
  const cmp = ev.filter((r) => r.legacy?.total_pnl_usd != null && r.total_pnl_usd != null);
  const pos = (x: string | null | undefined) => x != null && Number(x) > 0;
  return {
    addresses: rs.length,
    primaryReasons: primary,
    allReasons: all,
    criteriaFailed: crit,
    evaluated: ev.length,
    distributions,
    legacyComparison: {
      compared: cmp.length,
      legacyPnlPositive: cmp.filter((r) => pos(r.legacy!.total_pnl_usd)).length,
      newPnlPositive: cmp.filter((r) => pos(r.total_pnl_usd)).length,
      signFlips: cmp.filter((r) => pos(r.legacy!.total_pnl_usd) !== pos(r.total_pnl_usd)).length,
    },
    providerUsage: { heliusRpcCalls: usage.rpcCalls, signatureScanCalls: usage.signatureCalls, txCacheHits: usage.cacheHits, estimatedHeliusCredits: usage.rpcCalls, binanceCalls: usage.binanceCalls },
  };
}

export const DIAGNOSTIC_CSV_COLUMNS: Array<keyof WalletDiagnostic> = [
  "wallet_address", "primary_reject_reason", "all_reject_reasons", "signatures_in_window", "pagination_exhausted", "history_complete", "budget_exhausted",
  "oldest_observed_time", "newest_observed_time", "successful_transactions", "failed_transactions", "provider_requests", "estimated_credits", "cache_hits",
  "fetch_errors", "parser_errors", "normalized_swaps", "transfers", "unknown_events", "priced_swap_count", "priced_notional_usd", "unpriced_material_events",
  "unknown_opening_inventory", "unresolved_external_transfers", "transfer_only_mints", "position_episodes", "closed_episodes", "unknown_cost_episodes", "dust_closed_episodes", "median_closed_episode_cost_usd",
  "distinct_risk_tokens", "active_utc_days", "losing_episodes", "realized_pnl_usd", "open_pnl_mark_usd", "open_pnl_zero_floor_usd", "total_pnl_usd",
  "profit_factor", "largest_winner_share_of_gross_profit", "largest_token_share_bps", "pnl_excluding_discovery_tokens", "pnl_excluding_best_token",
  "network_fees_sol", "rent_deposit_net_sol", "criteria_failed", "legacy", "discovery_token_mints", "candidate_discovered_at", "candidate_source",
  "history_requested_from", "history_requested_to", "parser_version", "pricing_version",
];

export function diagnosticsCsv(rs: WalletDiagnostic[]): string {
  const cell = (v: unknown) => {
    const t = v === null || v === undefined ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
    return /[",\n]/.test(t) ? `"${t.replace(/"/g, '""')}"` : t;
  };
  return [DIAGNOSTIC_CSV_COLUMNS.join(","), ...rs.map((r) => DIAGNOSTIC_CSV_COLUMNS.map((c) => cell(r[c])).join(","))].join("\n") + "\n";
}
