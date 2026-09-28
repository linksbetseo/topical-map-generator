import { D, NATIVE_SOL, USDC_MINT, WSOL_MINT, type Dec } from "@solbot/domain";
import type { Config } from "@solbot/config";
import { json, type Pool } from "@solbot/db";
import { Priority, type BinanceMinuteFx, type HeliusEnhanced, type HeliusRpc, type JupiterClient } from "@solbot/providers";
import { buildClusters, deriveEdges, qualifyWallet, reconstructEpisodes, walletMetrics, type Buy, type Transfer, type WalletEvent } from "@solbot/strategy";
import { normalizeEnhancedSwap, type EnhancedTxLike } from "./helius-flow.ts";

/**
 * Wallet bootstrap (brief §6). Candidates come from *observed buyers* of recognised tokens, ranked by
 * breadth of activity — never by later profits (no survivorship selection). Statistics are a
 * reconstruction with stated coverage, not an audit. Everything is read-only.
 */
export interface BootstrapDeps {
  pool: Pool;
  helius: HeliusEnhanced;
  fx: BinanceMinuteFx;
  rpc: HeliusRpc;
  jup: JupiterClient;
  cfg: Config;
  now: () => Date;
  log: (m: string) => void;
}

export interface BootstrapOptions {
  seedMints: string[];
  seedLookbackHours: number;
  seedPagesPerMint: number;
  walletMaxPages: number;
  transferMaxPages: number;
  maxHeliusCalls: number;
}

export interface BootstrapSummary {
  computedAt: string;
  windowStart: string;
  seedMints: number;
  candidates: number;
  processed: number;
  qualified: number;
  rejectedByReason: Record<string, number>;
  linkChecked: number;
  clusters: number;
  edges: number;
  heliusCalls: number;
  binanceCalls: number;
  stoppedByBudget: boolean;
  populationNote: string;
}

async function decimalsOf(rpc: HeliusRpc, mints: string[], cache: Map<string, number>): Promise<void> {
  const todo = mints.filter((m) => !cache.has(m) && m !== NATIVE_SOL);
  for (let i = 0; i < todo.length; i += 100) {
    const chunk = todo.slice(i, i + 100);
    const r = await rpc.call<{ value: Array<{ data: [string, string] } | null> }>("getMultipleAccounts", [chunk, { encoding: "base64", dataSlice: { offset: 44, length: 1 } }], Priority.DISCOVERY);
    if (!r.ok) continue;
    r.value.value.forEach((acc, j) => {
      if (acc && Array.isArray(acc.data)) {
        const b = Buffer.from(acc.data[0], "base64");
        if (b.length === 1) cache.set(chunk[j]!, b[0]!);
      }
    });
  }
  cache.set(USDC_MINT, 6);
  cache.set(WSOL_MINT, 9);
}

function mintsIn(txs: EnhancedTxLike[]): string[] {
  const s = new Set<string>();
  for (const t of txs) for (const x of t.tokenTransfers ?? []) if (x.mint) s.add(x.mint);
  return [...s];
}

/** Buyers (fee payers with a confirmed BUY of the seed mint) in the lookback window. */
export async function discoverCandidates(d: BootstrapDeps, o: BootstrapOptions, decimals: Map<string, number>): Promise<Map<string, { mints: Set<string>; buys: number }>> {
  const now = d.now();
  const out = new Map<string, { mints: Set<string>; buys: number }>();
  for (const mint of o.seedMints) {
    if (d.helius.calls >= o.maxHeliusCalls) break;
    const r = await d.helius.history(mint, { type: "SWAP", gteTime: Math.floor(now.getTime() / 1000) - o.seedLookbackHours * 3600, lteTime: Math.floor(now.getTime() / 1000), maxPages: o.seedPagesPerMint });
    if (!r.ok) {
      d.log(`seed ${mint}: ${r.code} ${r.detail}`);
      continue;
    }
    const txs = r.value.txs as Array<EnhancedTxLike & { feePayer?: string }>;
    await decimalsOf(d.rpc, mintsIn(txs), decimals);
    for (const tx of txs) {
      const w = tx.feePayer;
      if (!w) continue;
      const ev = normalizeEnhancedSwap(tx, new Set([w]), now, (m) => decimals.get(m) ?? null, null).events;
      if (!ev.some((e) => e.side === "BUY" && e.mint === mint)) continue;
      const c = out.get(w) ?? { mints: new Set<string>(), buys: 0 };
      c.mints.add(mint);
      c.buys++;
      out.set(w, c);
    }
  }
  return out;
}

async function walletEvents(d: BootstrapDeps, wallet: string, txs: EnhancedTxLike[], decimals: Map<string, number>, availableAt: Date): Promise<{ events: WalletEvent[]; buys: Buy[]; unpriced: number }> {
  await decimalsOf(d.rpc, mintsIn(txs), decimals);
  const events: WalletEvent[] = [];
  const buys: Buy[] = [];
  let unpriced = 0;
  for (const tx of txs) {
    if (typeof tx.timestamp !== "number") continue;
    const fx = await d.fx.fxAt(new Date(tx.timestamp * 1000));
    const r = normalizeEnhancedSwap(tx, new Set([wallet]), availableAt, (m) => decimals.get(m) ?? null, fx);
    for (const e of r.events) {
      if (e.usd === null) unpriced++;
      events.push({ wallet, mint: e.mint, blockTime: e.blockTime, availableAt, kind: e.side === "BUY" ? "SWAP_BUY" : "SWAP_SELL", tokenRaw: e.tokenRaw, usd: e.usd, signature: e.signature });
      if (e.side === "BUY") buys.push({ wallet, mint: e.mint, at: e.blockTime });
    }
    // swaps the normalizer could not price/attribute still count against coverage
    for (const x of r.dropped) if (x.wallet === wallet) {
      unpriced++;
      events.push({ wallet, mint: `unattributed:${tx.signature}`, blockTime: new Date(tx.timestamp * 1000), availableAt, kind: "SWAP_BUY", tokenRaw: 0n, usd: null, signature: tx.signature });
    }
  }
  return { events, buys, unpriced };
}

async function transfersOf(d: BootstrapDeps, wallet: string, o: BootstrapOptions, window: { gte: number; lte: number }): Promise<{ ok: boolean; transfers: Transfer[] }> {
  const r = await d.helius.history(wallet, { type: "TRANSFER", gteTime: window.gte, lteTime: window.lte, maxPages: o.transferMaxPages });
  if (!r.ok || r.value.truncated) return { ok: false, transfers: [] };
  const out: Transfer[] = [];
  for (const tx of r.value.txs as Array<EnhancedTxLike & { nativeTransfers?: Array<{ fromUserAccount?: string; toUserAccount?: string; amount?: number }> }>) {
    if (typeof tx.timestamp !== "number") continue;
    const at = new Date(tx.timestamp * 1000);
    const fx = await d.fx.fxAt(at);
    if (!fx) continue;
    for (const n of tx.nativeTransfers ?? []) {
      if (!n.fromUserAccount || !n.toUserAccount || typeof n.amount !== "number") continue;
      out.push({ from: n.fromUserAccount, to: n.toUserAccount, usd: new D(n.amount).div(1e9).mul(fx.solUsd), at, signature: tx.signature });
    }
    for (const t of tx.tokenTransfers ?? []) {
      if (t.mint !== USDC_MINT || !t.fromUserAccount || !t.toUserAccount || t.tokenAmount === undefined) continue;
      out.push({ from: t.fromUserAccount, to: t.toUserAccount, usd: new D(String(t.tokenAmount)).mul(fx.usdcUsd), at, signature: tx.signature });
    }
  }
  return { ok: true, transfers: out };
}

export async function runBootstrap(d: BootstrapDeps, sessionId: string, o: BootstrapOptions): Promise<BootstrapSummary> {
  const computedAt = d.now();
  const w = d.cfg.wallets;
  const window = { gte: Math.floor(computedAt.getTime() / 1000) - w.qualification_window_days * 86_400, lte: Math.floor(computedAt.getTime() / 1000) };
  const decimals = new Map<string, number>();

  const cands = await discoverCandidates(d, o, decimals);
  const ranked = [...cands.entries()].sort((a, b) => b[1].mints.size - a[1].mints.size || b[1].buys - a[1].buys || a[0].localeCompare(b[0])).slice(0, w.max_candidates);
  d.log(`candidates: ${cands.size} found, ${ranked.length} selected (by breadth of observed buys, not by profit)`);

  const rejected: Record<string, number> = {};
  const qualified: string[] = [];
  const allBuys: Buy[] = [];
  let processed = 0;
  let stoppedByBudget = false;
  const openMints = new Set<string>();
  const perWallet = new Map<string, { events: WalletEvent[]; truncated: boolean; unpriced: number }>();

  for (const [wallet, info] of ranked) {
    if (d.helius.calls >= o.maxHeliusCalls) {
      stoppedByBudget = true;
      break;
    }
    await d.pool.query(`INSERT INTO wallets (address, candidate_source, first_seen_at) VALUES ($1,$2,$3) ON CONFLICT (address) DO NOTHING`, [
      wallet,
      json({ source: "observed_buyer", seed_mints: [...info.mints], buys: info.buys }),
      computedAt,
    ]);
    const h = await d.helius.history(wallet, { type: "SWAP", gteTime: window.gte, lteTime: window.lte, maxPages: o.walletMaxPages });
    processed++;
    if (!h.ok) {
      perWallet.set(wallet, { events: [], truncated: true, unpriced: 0 });
      continue;
    }
    const ev = await walletEvents(d, wallet, h.value.txs as EnhancedTxLike[], decimals, computedAt);
    perWallet.set(wallet, { events: ev.events, truncated: h.value.truncated, unpriced: ev.unpriced });
    allBuys.push(...ev.buys);
  }

  // conservative marks for positions still open at computation time: current Jupiter price, else zero lower bound
  for (const v of perWallet.values()) for (const e of v.events) if (!e.mint.startsWith("unattributed:")) openMints.add(e.mint);
  const marks = new Map<string, Dec>();
  const mintList = [...openMints];
  for (let i = 0; i < mintList.length; i += 50) {
    const r = await d.jup.withPriority(Priority.DISCOVERY).usdPrices(mintList.slice(i, i + 50));
    if (!r.ok) continue;
    for (const [m, p] of r.prices) {
      const dec = decimals.get(m);
      if (dec !== undefined) marks.set(m, p.usdPrice.div(new D(10).pow(dec)));
    }
  }

  const infraish = (wallet: string, v: { events: WalletEvent[] }) => v.events.length > 0 && new Set(v.events.map((e) => e.mint)).size > 400; // routers/market makers touching hundreds of mints
  for (const [wallet, v] of perWallet) {
    const recon = reconstructEpisodes(v.events, new Date(computedAt.getTime() + 1), marks);
    const m = walletMetrics(recon, v.events, new Date(computedAt.getTime() + 1));
    let q = qualifyWallet(m, { infrastructure: infraish(wallet, v), deployerOfObservedToken: false }, d.cfg);
    if (v.truncated) q = { ...q, status: "REJECTED", reasons: [...q.reasons, { code: "DATA_REQUIREMENT_NOT_MET", detail: "history truncated or unavailable (coverage unknown)" }] };
    for (const r of q.reasons) {
      const k = (r.detail ?? r.code).replace(/[0-9.]+/g, "#");
      rejected[k] = (rejected[k] ?? 0) + 1;
    }
    if (q.status === "QUALIFIED") qualified.push(wallet);
    await d.pool.query(
      `INSERT INTO wallet_qualification (session_id, address, status, metrics, coverage_bps, reasons, computed_at) VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (session_id, address) DO UPDATE SET status=EXCLUDED.status, metrics=EXCLUDED.metrics, coverage_bps=EXCLUDED.coverage_bps, reasons=EXCLUDED.reasons, computed_at=EXCLUDED.computed_at`,
      [sessionId, wallet, q.status, json({ ...m, truncated: v.truncated, unpriced: v.unpriced, events: v.events.length }), m.coverageBps, json(q.reasons), computedAt],
    );
  }
  const finalQualified = qualified.slice(0, w.max_qualified);

  // links only for qualified wallets (budgeted); unchecked => UNKNOWN and cannot count toward a signal
  const transfers: Transfer[] = [];
  const checked = new Set<string>();
  for (const wallet of finalQualified) {
    if (d.helius.calls >= o.maxHeliusCalls) {
      stoppedByBudget = true;
      break;
    }
    const t = await transfersOf(d, wallet, o, window);
    if (t.ok) {
      checked.add(wallet);
      transfers.push(...t.transfers);
    }
  }
  const infra = new Set((await d.pool.query<{ address: string }>(`SELECT address FROM infra_registry`)).rows.map((r) => r.address));
  const edges = deriveEdges(new Set(finalQualified), transfers, allBuys, infra, computedAt, d.cfg);
  const clusters = buildClusters(finalQualified, checked, edges, computedAt);
  for (const e of edges) {
    await d.pool.query(`INSERT INTO wallet_edges (a, b, kind, source, evidence, confidence, valid_from, valid_to) VALUES ($1,$2,$3,'helius-transfers',$4,$5,$6,$7)`, [
      e.a,
      e.b,
      e.kind,
      json(e.evidence),
      e.confidence,
      e.validFrom,
      e.validTo,
    ]);
  }
  for (const [wallet, c] of clusters) {
    await d.pool.query(
      `INSERT INTO wallet_clusters (session_id, address, cluster_id, link_check) VALUES ($1,$2,$3,$4) ON CONFLICT (session_id, address) DO UPDATE SET cluster_id=EXCLUDED.cluster_id, link_check=EXCLUDED.link_check`,
      [sessionId, wallet, c.clusterId, c.linkCheck],
    );
  }
  const summary: BootstrapSummary = {
    computedAt: computedAt.toISOString(),
    windowStart: new Date(window.gte * 1000).toISOString(),
    seedMints: o.seedMints.length,
    candidates: cands.size,
    processed,
    qualified: finalQualified.length,
    rejectedByReason: rejected,
    linkChecked: checked.size,
    clusters: new Set([...clusters.values()].map((c) => c.clusterId)).size,
    edges: edges.length,
    heliusCalls: d.helius.calls,
    binanceCalls: d.fx.calls,
    stoppedByBudget,
    populationNote: "Kandydaci = płacący opłatę kupujący obserwowanych tokenów z ostatnich godzin; populacja nie jest reprezentatywna dla całego rynku. Historia tylko swapów (type=SWAP); przelewy tylko dla powiązań.",
  };
  await d.pool.query(`INSERT INTO audit_events (session_id, actor, action, data, at) VALUES ($1,'worker','WALLET_BOOTSTRAP',$2,$3)`, [sessionId, json(summary), d.now()]);
  return summary;
}
