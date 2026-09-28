import { NetworkError, type ReadOnlyTransport } from "./transport.ts";
import type { Pacer } from "./solana-tx.ts";

/**
 * Birdeye Data Services (read-only), contracts from data.birdeye.so/docs (read 2026-09-28) and live
 * probes with a Standard-package key the same day:
 *  - GET /trader/txs/seek_by_time   10 CU, ≤100 per page, offset+limit ≤ 10 000, before_time/after_time (s)
 *  - GET /defi/v3/ohlcv             12–100 CU by size, ≤5000 candles, types 1s..1M, sparse (no padding)
 *  - GET /trader/gainers-losers     25 CU (rankings: raw lists are dominated by unrealized marks, see docs/)
 *  - GET /defi/v2/tokens/top_traders, /wallet/v2/pnl/summary — 20–25 CU
 * Standard package: 60 req/min, 30k CU. Wallet endpoints are capped at 5 rps / 75 rpm on every package.
 * Responses are cached by request key so a replay never pays twice for the same page.
 */
export const BIRDEYE_BASE = "https://public-api.birdeye.so";

export interface JsonCache {
  get(key: string): Promise<unknown | undefined>;
  put(key: string, value: unknown): Promise<void>;
}

export type BirdeyeResult<T> = { ok: true; value: T; cached: boolean } | { ok: false; code: "RATE_LIMITED" | "FORBIDDEN" | "PROVIDER_ERROR" | "PROVIDER_UNAVAILABLE"; detail: string };

export interface BirdeyeTrade {
  tx_hash: string;
  block_unix_time: number;
  owner: string;
  source?: string;
  tx_type?: string;
  volume_usd?: number;
  base: BirdeyeLeg;
  quote: BirdeyeLeg;
}
export interface BirdeyeLeg {
  address: string;
  symbol?: string;
  decimals: number;
  /** raw integer amount (may be a JS number from the API; kept for evidence only) */
  amount?: number | string;
  ui_amount?: number;
  /** "from" = the owner gave this asset, "to" = the owner received it */
  type_swap: "from" | "to";
  price?: number | null;
  nearest_price?: number | null;
}

export interface Candle {
  unix_time: number;
  o: number;
  h: number;
  l: number;
  c: number;
  v?: number;
  v_usd?: number;
}

/** Documented CU cost per request (data.birdeye.so/docs/guides/what-is-compute-unit-cost, 2026-09-28). */
export function birdeyeCu(path: string, returnedItems: number): number {
  if (path === "/trader/txs/seek_by_time") return 10;
  if (path === "/trader/gainers-losers" || path === "/defi/v2/tokens/top_traders") return 25;
  if (path === "/defi/v3/ohlcv") return returnedItems <= 100 ? 12 : returnedItems <= 300 ? 25 : returnedItems <= 1000 ? 40 : returnedItems <= 2000 ? 75 : 100;
  return 30;
}

export class BudgetExceededError extends Error {}

export interface GainerRow {
  address: string;
  pnl: number;
  realized_pnl: number;
  unrealized_pnl: number;
  volume: number;
  trade_count: number;
}

export class BirdeyeClient {
  calls = 0;
  cacheHits = 0;
  /** estimated compute units spent by this client (cache hits are free) */
  cu = 0;
  /** hard cap on estimated CU; a request that would start above it throws BudgetExceededError */
  cuBudget = Number.POSITIVE_INFINITY;
  constructor(
    private readonly transport: ReadOnlyTransport,
    private readonly apiKey: string,
    private readonly cache: JsonCache,
    private readonly pace: Pacer = async () => undefined,
    private readonly retry = { attempts: 3, backoffMs: 2_000, sleep: (ms: number) => new Promise<void>((r) => setTimeout(r, ms)) },
    private readonly base = BIRDEYE_BASE,
  ) {}

  private async get<T>(path: string, params: Record<string, string | number | undefined>): Promise<BirdeyeResult<T>> {
    const q = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) if (v !== undefined) q.set(k, String(v));
    const key = `${path}?${q}`;
    const hit = await this.cache.get(key);
    if (hit !== undefined) {
      this.cacheHits++;
      return { ok: true, value: hit as T, cached: true };
    }
    if (this.cu >= this.cuBudget) throw new BudgetExceededError(`Birdeye CU budget ${this.cuBudget} reached`);
    let last: BirdeyeResult<T> = { ok: false, code: "PROVIDER_ERROR", detail: "not called" };
    for (let i = 0; i <= this.retry.attempts; i++) {
      if (i > 0) await this.retry.sleep(this.retry.backoffMs * 2 ** (i - 1));
      await this.pace();
      this.calls++;
      let res;
      try {
        res = await this.transport.request("GET", `${this.base}${key}`, { headers: { "X-API-KEY": this.apiKey, "x-chain": "solana" } });
      } catch (e) {
        if (e instanceof NetworkError) {
          last = { ok: false, code: "PROVIDER_UNAVAILABLE", detail: e.message };
          continue;
        }
        throw e;
      }
      if (res.status === 429) {
        last = { ok: false, code: "RATE_LIMITED", detail: "HTTP 429" };
        continue;
      }
      if (res.status >= 500) {
        last = { ok: false, code: "PROVIDER_UNAVAILABLE", detail: `HTTP ${res.status}` };
        continue;
      }
      if (res.status === 401 || res.status === 403) return { ok: false, code: "FORBIDDEN", detail: `HTTP ${res.status} ${res.text.slice(0, 160)}` };
      if (res.status !== 200) return { ok: false, code: "PROVIDER_ERROR", detail: `HTTP ${res.status} ${res.text.slice(0, 160)}` };
      let body: { success?: boolean; data?: T; message?: string };
      try {
        body = JSON.parse(res.text);
      } catch {
        return { ok: false, code: "PROVIDER_ERROR", detail: "bad JSON" };
      }
      if (body.success === false || body.data === undefined) return { ok: false, code: "PROVIDER_ERROR", detail: String(body.message ?? "no data").slice(0, 160) };
      const items = (body.data as { items?: unknown[] } | undefined)?.items;
      this.cu += birdeyeCu(path, Array.isArray(items) ? items.length : 0);
      await this.cache.put(key, body.data);
      return { ok: true, value: body.data, cached: false };
    }
    return last;
  }

  /**
   * Swaps of a trader in [afterTime, beforeTime] (seconds), newest first, paged by 100 up to maxItems.
   * The API accepts only ONE of before_time / after_time (HTTP 422 otherwise, verified live), so we
   * page backwards from before_time and stop once trades are older than afterTime.
   */
  async traderSwaps(address: string, q: { afterTime: number; beforeTime: number; maxItems: number }): Promise<BirdeyeResult<{ trades: BirdeyeTrade[]; complete: boolean }>> {
    const trades: BirdeyeTrade[] = [];
    let cachedAll = true;
    for (let offset = 0; offset < q.maxItems && offset + 100 <= 10_000; offset += 100) {
      const r = await this.get<{ items: BirdeyeTrade[]; has_next?: boolean }>("/trader/txs/seek_by_time", {
        address,
        tx_type: "swap",
        offset,
        limit: 100,
        before_time: q.beforeTime,
      });
      if (!r.ok) return r;
      cachedAll &&= r.cached;
      const items = r.value.items ?? [];
      trades.push(...items.filter((t) => t.block_unix_time >= q.afterTime));
      const reachedStart = items.some((t) => t.block_unix_time < q.afterTime);
      if (reachedStart || !r.value.has_next || items.length === 0) return { ok: true, value: { trades, complete: true }, cached: cachedAll };
    }
    return { ok: true, value: { trades, complete: false }, cached: cachedAll };
  }

  /** Ranked traders for a window; raw lists are dominated by unrealized marks — filter before use. */
  async gainers(q: { type: "today" | "yesterday" | "1W" | "30d"; sortBy: "PnL" | "realized_pnl" | "unrealized_pnl" | "trader_score"; offset: number; limit: number; minTrade?: number }): Promise<BirdeyeResult<GainerRow[]>> {
    const r = await this.get<{ items: GainerRow[] }>("/trader/gainers-losers", { type: q.type, sort_by: q.sortBy, sort_type: "desc", offset: q.offset, limit: q.limit, min_trade: q.minTrade });
    return r.ok ? { ok: true, value: r.value.items ?? [], cached: r.cached } : r;
  }

  /** USD candles of a token (sparse: seconds without trades are absent). */
  async candles(address: string, type: "1s" | "15s" | "1m" | "5m", timeFrom: number, timeTo: number): Promise<BirdeyeResult<Candle[]>> {
    const r = await this.get<{ items: Candle[] }>("/defi/v3/ohlcv", { address, type, currency: "usd", time_from: timeFrom, time_to: timeTo });
    return r.ok ? { ok: true, value: r.value.items ?? [], cached: r.cached } : r;
  }
}
