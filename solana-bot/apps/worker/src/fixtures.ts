import { D, type Dec } from "@solbot/domain";
import type { FxSnapshot } from "@solbot/ledger";
import type { FlowEvent, HolderView, MintRiskView, TokenView, WalletStatus } from "@solbot/strategy";
import type { FlowStore, MarketData, WalletBook } from "./ports.ts";

/**
 * DEMO / test adapters. Everything they return is fixture data; the market source is "FIXTURE"
 * and sessions using them must be of kind DEMO. Never used as market evidence.
 */
export class FixtureMarket implements MarketData {
  readonly source = "FIXTURE" as const;
  usdcUsd: Dec | null = new D(1);
  solUsd: Dec | null = new D(150);
  tokens = new Map<string, Omit<TokenView, "availableAt">>();
  risk = new Map<string, { passed: boolean; reasons: MintRiskView["reasons"] }>();
  holderViews = new Map<string, Omit<HolderView, "availableAt">>();
  rent = new Map<string, bigint>();
  deployers = new Map<string, string>();

  async fx(now: Date): Promise<FxSnapshot> {
    return { usdcUsd: this.usdcUsd, solUsd: this.solUsd, at: now, source: "fixture" };
  }
  async tokenView(mint: string, now: Date): Promise<TokenView | null> {
    const t = this.tokens.get(mint);
    return t ? { ...t, availableAt: now } : null;
  }
  async mintRisk(mint: string, now: Date) {
    const r = this.risk.get(mint);
    return r ? { ...r, availableAt: now, decimals: 6, tokenProgram: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" } : null;
  }
  async holders(mint: string, now: Date): Promise<HolderView | null> {
    const h = this.holderViews.get(mint);
    return h ? { ...h, availableAt: now } : null;
  }
  async rentLamports(mint: string): Promise<bigint | null> {
    return this.rent.get(mint) ?? null;
  }
  async deployerGroup(mint: string): Promise<string | null> {
    return this.deployers.get(mint) ?? null;
  }

  /** A token that passes every v1 filter (fixture). */
  addHealthyToken(mint: string, now: Date): void {
    this.tokens.set(mint, {
      mint,
      firstPoolId: `pool-${mint}`,
      firstPoolCreatedAt: new Date(now.getTime() - 6 * 3_600_000),
      liquidityUsd: new D(300_000),
      volume5mUsd: new D(50_000),
      sells5m: 60,
      priceChange5mPct: new D(4),
      launchpad: null,
      graduatedAt: null,
    });
    this.risk.set(mint, { passed: true, reasons: [] });
    this.holderViews.set(mint, { ok: true, reasons: [], holderCount: 900, top10Bps: 2_100, largestBps: 500 });
    this.rent.set(mint, 2_039_280n);
  }
}

export class MemoryFlowStore implements FlowStore {
  readonly list: FlowEvent[] = [];
  add(e: FlowEvent): void {
    this.list.push(e);
  }
  async events(mint: string, from: Date, to: Date): Promise<FlowEvent[]> {
    return this.list.filter((e) => e.mint === mint && e.blockTime >= from && e.blockTime <= to && e.availableAt <= to);
  }
  async walletQty(wallet: string, mint: string, at: Date): Promise<bigint | null> {
    let q = 0n;
    let seen = false;
    for (const e of this.list) {
      if (e.wallet !== wallet || e.mint !== mint || e.availableAt > at || !e.confirmed) continue;
      seen = true;
      q += e.side === "BUY" ? e.tokenRaw : -e.tokenRaw;
    }
    return seen ? q : null;
  }
}

export class StaticWalletBook implements WalletBook {
  constructor(private readonly map: Map<string, WalletStatus>) {}
  statuses(): ReadonlyMap<string, WalletStatus> {
    return this.map;
  }
  qualifiedCount(): number {
    return [...this.map.values()].filter((w) => w.qualified).length;
  }
}
