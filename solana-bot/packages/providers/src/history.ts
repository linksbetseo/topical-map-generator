import { D, type Dec } from "@solbot/domain";
import { NetworkError, type ReadOnlyTransport } from "./transport.ts";
import { Priority, type SlidingWindowLimiter } from "./rate-limit.ts";

/**
 * Historical data for the wallet bootstrap (brief §6):
 *  - Helius Enhanced Transactions by address (swaps / transfers), query names from helius-sdk
 *    src/enhanced/client.eager.ts @ b76a792: before-signature, gte-time, lte-time, type, limit;
 *    verified live 2026-09-28 (tokenAmount is a UI decimal number).
 *  - Binance public market data 1-minute klines (no key): SOLUSDT, USDCUSDT. USD is approximated by
 *    USDT — an explicit, documented assumption. The *open* of the minute containing t is used, so the
 *    price is one that was already known at t.
 */

export type HistoryResult<T> = { ok: true; value: T } | { ok: false; code: "PROVIDER_UNAVAILABLE" | "PROVIDER_ERROR" | "RATE_LIMITED"; detail: string };

export class HeliusEnhanced {
  calls = 0;
  constructor(
    private readonly transport: ReadOnlyTransport,
    private readonly limiter: SlidingWindowLimiter,
    private readonly apiKey: string,
    private readonly base = "https://api-mainnet.helius-rpc.com/v0",
  ) {}

  /** One page (newest first). */
  async page(address: string, q: { type?: "SWAP" | "TRANSFER"; gteTime?: number; lteTime?: number; beforeSignature?: string; limit?: number }): Promise<HistoryResult<unknown[]>> {
    if (!(await this.limiter.acquire(Priority.DISCOVERY, 120_000))) return { ok: false, code: "RATE_LIMITED", detail: "local budget" };
    const p = new URLSearchParams({ "api-key": this.apiKey, limit: String(q.limit ?? 100), commitment: "confirmed" });
    if (q.type) p.set("type", q.type);
    if (q.gteTime !== undefined) p.set("gte-time", String(q.gteTime));
    if (q.lteTime !== undefined) p.set("lte-time", String(q.lteTime));
    if (q.beforeSignature) p.set("before-signature", q.beforeSignature);
    let res;
    try {
      this.calls++;
      res = await this.transport.request("GET", `${this.base}/addresses/${address}/transactions?${p}`);
    } catch (e) {
      if (e instanceof NetworkError) return { ok: false, code: "PROVIDER_UNAVAILABLE", detail: e.message };
      throw e;
    }
    if (res.status === 429) return { ok: false, code: "RATE_LIMITED", detail: "HTTP 429" };
    if (res.status >= 500) return { ok: false, code: "PROVIDER_UNAVAILABLE", detail: `HTTP ${res.status}` };
    if (res.status !== 200) return { ok: false, code: "PROVIDER_ERROR", detail: `HTTP ${res.status} ${res.text.slice(0, 200)}` };
    const body = JSON.parse(res.text) as unknown;
    if (!Array.isArray(body)) return { ok: false, code: "PROVIDER_ERROR", detail: "expected array" };
    return { ok: true, value: body };
  }

  /** All pages in [gteTime, lteTime]; `truncated` when maxPages was hit (coverage unknown => caller must not qualify). */
  async history(address: string, q: { type?: "SWAP" | "TRANSFER"; gteTime: number; lteTime: number; maxPages: number }): Promise<HistoryResult<{ txs: unknown[]; truncated: boolean }>> {
    const txs: unknown[] = [];
    let before: string | undefined;
    for (let i = 0; i < q.maxPages; i++) {
      const r = await this.page(address, { type: q.type, gteTime: q.gteTime, lteTime: q.lteTime, beforeSignature: before, limit: 100 });
      if (!r.ok) return r;
      txs.push(...r.value);
      if (r.value.length < 100) return { ok: true, value: { txs, truncated: false } };
      const last = r.value[r.value.length - 1] as { signature?: string };
      if (!last.signature) return { ok: false, code: "PROVIDER_ERROR", detail: "missing signature for pagination" };
      before = last.signature;
    }
    return { ok: true, value: { txs, truncated: true } };
  }
}

export class BinanceMinuteFx {
  private cache = new Map<string, Map<number, Dec>>();
  calls = 0;
  constructor(
    private readonly transport: ReadOnlyTransport,
    private readonly base = "https://data-api.binance.vision/api/v3/klines",
  ) {}

  private async load(symbol: string, minute: number): Promise<void> {
    const start = minute - 500 * 60_000;
    let res;
    try {
      this.calls++;
      res = await this.transport.request("GET", `${this.base}?symbol=${symbol}&interval=1m&startTime=${start}&limit=1000`);
    } catch {
      return;
    }
    if (res.status !== 200) return;
    const rows = JSON.parse(res.text) as Array<[number, string]>;
    const m = this.cache.get(symbol) ?? new Map<number, Dec>();
    for (const r of rows) if (typeof r[0] === "number" && typeof r[1] === "string") m.set(r[0], new D(r[1]));
    this.cache.set(symbol, m);
  }

  /** Open price of the minute that contains t (USDT quote); null when unavailable. */
  async at(symbol: string, t: Date): Promise<Dec | null> {
    const minute = Math.floor(t.getTime() / 60_000) * 60_000;
    let m = this.cache.get(symbol);
    if (!m || !m.has(minute)) {
      await this.load(symbol, minute);
      m = this.cache.get(symbol);
    }
    return m?.get(minute) ?? null;
  }

  async fxAt(t: Date): Promise<{ usdcUsd: Dec; solUsd: Dec } | null> {
    const [sol, usdc] = await Promise.all([this.at("SOLUSDT", t), this.at("USDCUSDT", t)]);
    return sol && usdc ? { solUsd: sol, usdcUsd: usdc } : null;
  }
}
