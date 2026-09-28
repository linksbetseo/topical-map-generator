import { D, SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM, USDC_MINT, WSOL_MINT, reason, ReasonCode, type Dec } from "@solbot/domain";
import type { Config } from "@solbot/config";
import type { FxSnapshot } from "@solbot/ledger";
import { evaluateMintRisk, holderConcentration, parseMintAccount, Priority, type HeliusRpc, type JupiterClient, type TokenInfo } from "@solbot/providers";
import type { FlowEvent, HolderView, TokenView, WalletStatus } from "@solbot/strategy";
import { json, type Pool } from "@solbot/db";
import { normalizeEnhancedSwap, type EnhancedTxLike } from "./helius-flow.ts";
import type { FlowStore, MarketData, WalletBook } from "./ports.ts";

/**
 * PAPER adapters over read-only mainnet providers. Not verified live in this build (egress + keys,
 * see docs/ACCESS_GAPS.md). Every snapshot is persisted with its receive time before use.
 */
export class LiveMarketData implements MarketData {
  readonly source = "LIVE" as const;
  private tokenCache = new Map<string, { info: TokenInfo; at: number }>();
  private mintCache = new Map<string, { supply: bigint; program: string; decimals: number }>();

  constructor(
    private readonly pool: Pool,
    private readonly jup: JupiterClient,
    private readonly rpc: HeliusRpc | null,
    private readonly cfg: Config,
  ) {}

  async fx(now: Date): Promise<FxSnapshot> {
    const r = await this.jup.withPriority(Priority.RECONCILE).usdPrices([WSOL_MINT, USDC_MINT]);
    if (!r.ok) return { usdcUsd: null, solUsd: null, at: now, source: `jupiter.price.v3:${r.code}` };
    return { usdcUsd: r.prices.get(USDC_MINT)?.usdPrice ?? null, solUsd: r.prices.get(WSOL_MINT)?.usdPrice ?? null, at: r.receivedAt, source: "jupiter.price.v3" };
  }

  private async info(mint: string, now: Date): Promise<TokenInfo | null> {
    const c = this.tokenCache.get(mint);
    if (c && now.getTime() - c.at <= this.cfg.freshness.market_metadata_ms) return c.info;
    const r = await this.jup.withPriority(Priority.ENTRY).searchTokens([mint]);
    if (!r.ok) return null;
    const t = r.tokens.find((x) => x.mint === mint) ?? null;
    if (!t) return null;
    await this.pool.query(`INSERT INTO tokens (mint, decimals, token_program, first_seen_at) VALUES ($1,$2,$3,$4) ON CONFLICT (mint) DO NOTHING`, [mint, t.decimals, t.tokenProgram ?? "unknown", now]);
    await this.pool.query(`INSERT INTO token_snapshots (mint, provider, received_at, available_at, data, raw_payload_hash) VALUES ($1,'jupiter.tokens.v2',$2,$3,$4,'')`, [mint, t.receivedAt, now, json(t.raw)]);
    this.tokenCache.set(mint, { info: t, at: t.receivedAt.getTime() });
    return t;
  }

  async tokenView(mint: string, now: Date): Promise<TokenView | null> {
    const t = await this.info(mint, now);
    if (!t) return null;
    const vol = t.stats5m && t.stats5m.buyVolumeUsd !== null && t.stats5m.sellVolumeUsd !== null ? t.stats5m.buyVolumeUsd.add(t.stats5m.sellVolumeUsd) : null;
    return {
      mint,
      firstPoolId: t.firstPoolId,
      firstPoolCreatedAt: t.firstPoolCreatedAt,
      liquidityUsd: t.liquidityUsd,
      volume5mUsd: vol,
      sells5m: t.stats5m?.numSells ?? null,
      priceChange5mPct: t.stats5m?.priceChangePct ?? null,
      launchpad: t.launchpad,
      graduatedAt: t.graduatedAt,
      availableAt: t.receivedAt,
    };
  }

  async mintRisk(mint: string, now: Date) {
    if (!this.rpc) return null;
    const r = await this.rpc.getAccountInfo(mint, Priority.ENTRY);
    if (!r.ok || !r.value) return null;
    const parsed = parseMintAccount(r.value.owner, r.value.data);
    if (!parsed.ok) return { passed: false, reasons: [parsed.reason], availableAt: now, decimals: -1, tokenProgram: r.value.owner };
    this.mintCache.set(mint, { supply: parsed.mint.supplyRaw, program: parsed.mint.tokenProgram, decimals: parsed.mint.decimals });
    const risk = evaluateMintRisk(mint, parsed.mint, this.cfg.universe.allowed_token2022_extensions);
    await this.pool.query(`INSERT INTO token_risk_checks (mint, checked_at, passed, results, inputs) SELECT $1,$2,$3,$4,$5 WHERE EXISTS (SELECT 1 FROM tokens WHERE mint=$1)`, [
      mint,
      now,
      risk.verdict !== "REJECTED",
      json(risk),
      json({ slot: r.value.slot, extensions: parsed.mint.extensions }),
    ]);
    return { passed: risk.verdict !== "REJECTED", reasons: risk.reasons, availableAt: r.receivedAt, decimals: parsed.mint.decimals, tokenProgram: parsed.mint.tokenProgram };
  }

  async holders(mint: string, now: Date): Promise<HolderView | null> {
    if (!this.rpc) return null;
    const m = this.mintCache.get(mint);
    if (!m) return { ok: false, reasons: [reason(ReasonCode.HOLDER_DATA_UNAVAILABLE, "mint not read")], holderCount: 0, top10Bps: null, largestBps: null, availableAt: now };
    const r = await this.rpc.getAllTokenAccounts(mint, { priority: Priority.ENTRY });
    if (!r.ok) return { ok: false, reasons: [reason(ReasonCode.HOLDER_DATA_UNAVAILABLE, r.code)], holderCount: 0, top10Bps: null, largestBps: null, availableAt: now };
    const infra = new Set((await this.pool.query<{ address: string }>(`SELECT address FROM infra_registry`)).rows.map((x) => x.address));
    const h = holderConcentration(r.value.accounts, m.supply, r.value.complete, infra);
    return { ok: h.ok, reasons: h.reasons, holderCount: h.holderCount, top10Bps: h.top10Bps, largestBps: h.largestBps, availableAt: r.receivedAt };
  }

  async rentLamports(mint: string): Promise<bigint | null> {
    const m = this.mintCache.get(mint);
    if (!m) return null;
    if (m.program === SPL_TOKEN_PROGRAM) return BigInt(this.cfg.execution.spl_token_account_rent_lamports);
    if (m.program !== TOKEN_2022_PROGRAM || !this.rpc) return null;
    // Token-2022 account for an allowlisted (metadata-only) mint: 165 + account type + ImmutableOwner TLV = 170 bytes (estimate)
    const r = await this.rpc.call<number>("getMinimumBalanceForRentExemption", [170], Priority.ENTRY);
    return r.ok && Number.isInteger(r.value) ? BigInt(r.value) : null;
  }

  async deployerGroup(mint: string): Promise<string | null> {
    const t = this.tokenCache.get(mint)?.info;
    // Provider-reported developer wallet; treated as a grouping key, not as proof of identity.
    return t?.dev ? `dev:${t.dev}` : null;
  }
}

/** Flow events from durably stored webhook payloads (normalized on read; available_at from ingest). */
export class DbFlowStore implements FlowStore {
  private decimals = new Map<string, number>();
  constructor(
    private readonly pool: Pool,
    private readonly watched: () => ReadonlySet<string>,
  ) {}

  private async fxAt(t: Date): Promise<{ usdcUsd: Dec; solUsd: Dec } | null> {
    const r = await this.pool.query<{ usdc_usd: string | null; sol_usd: string | null; at: Date }>(
      `SELECT usdc_usd, sol_usd, at FROM fx_snapshots WHERE at <= $1 AND usdc_usd IS NOT NULL AND sol_usd IS NOT NULL ORDER BY at DESC LIMIT 1`,
      [t],
    );
    const row = r.rows[0];
    // FX known at the transaction time (no later data); older than 2 min => unknown
    if (!row || t.getTime() - row.at.getTime() > 120_000) return null;
    return { usdcUsd: new D(row.usdc_usd!), solUsd: new D(row.sol_usd!) };
  }

  private async load(from: Date, to: Date): Promise<FlowEvent[]> {
    const rows = await this.pool.query<{ raw_payload: EnhancedTxLike; available_at: Date; block_time: Date | null }>(
      `SELECT raw_payload, available_at, block_time FROM raw_events WHERE provider='helius-webhook' AND block_time BETWEEN $1 AND $2 AND available_at <= $2`,
      [from, to],
    );
    if (this.decimals.size === 0 || rows.rowCount) {
      for (const t of (await this.pool.query<{ mint: string; decimals: number }>(`SELECT mint, decimals FROM tokens`)).rows) this.decimals.set(t.mint, t.decimals);
    }
    const out: FlowEvent[] = [];
    for (const r of rows.rows) {
      const fx = r.block_time ? await this.fxAt(r.block_time) : null;
      out.push(...normalizeEnhancedSwap(r.raw_payload, this.watched(), r.available_at, (m) => this.decimals.get(m) ?? null, fx).events);
    }
    return out;
  }

  async events(mint: string, from: Date, to: Date): Promise<FlowEvent[]> {
    return (await this.load(from, to)).filter((e) => e.mint === mint);
  }

  async walletQty(wallet: string, mint: string, at: Date): Promise<bigint | null> {
    // Quantity from observed confirmed swaps since session start only (partial view, documented).
    const evs = (await this.load(new Date(at.getTime() - 8 * 86_400_000), at)).filter((e) => e.wallet === wallet && e.mint === mint);
    if (evs.length === 0) return null;
    return evs.reduce((q, e) => q + (e.side === "BUY" ? e.tokenRaw : -e.tokenRaw), 0n);
  }
}

/** Frozen wallet list from wallet_qualification + wallet_clusters computed before T0. */
export class DbWalletBook implements WalletBook {
  private map = new Map<string, WalletStatus>();
  constructor(private readonly pool: Pool, private readonly sessionId: string) {}
  async load(): Promise<this> {
    const r = await this.pool.query<{ address: string; status: string; cluster_id: string | null; link_check: string | null }>(
      `SELECT q.address, q.status, c.cluster_id, c.link_check FROM wallet_qualification q LEFT JOIN wallet_clusters c ON c.session_id=q.session_id AND c.address=q.address WHERE q.session_id=$1`,
      [this.sessionId],
    );
    this.map = new Map(r.rows.map((w) => [w.address, { qualified: w.status === "QUALIFIED", clusterId: w.cluster_id ?? `solo:${w.address}`, linkCheck: w.link_check === "CHECKED" ? "CHECKED" : "UNKNOWN" }]));
    return this;
  }
  statuses(): ReadonlyMap<string, WalletStatus> {
    return this.map;
  }
  qualifiedCount(): number {
    return [...this.map.values()].filter((w) => w.qualified).length;
  }
  watched(): ReadonlySet<string> {
    return new Set(this.map.keys());
  }
}
