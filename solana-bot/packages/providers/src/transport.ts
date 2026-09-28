import { ReasonCode, type Clock } from "@solbot/domain";

/**
 * Read-only HTTP transport. The only transport available to PAPER/DEMO. It refuses, before any
 * network I/O, every request that could submit a transaction — regardless of UI flags (brief §2).
 */

export const BLOCKED_RPC_METHODS: ReadonlySet<string> = new Set([
  "sendTransaction",
  "sendRawTransaction",
  "sendBundle",
  "simulateBundle",
  "sendTransactionBatch",
  "requestAirdrop",
  // simulateTransaction is permitted only for SHADOW (Stage F), never in PAPER
  "simulateTransaction",
]);

const BLOCKED_PATHS: readonly RegExp[] = [/\/execute(?:[/?#]|$)/i, /\/submit(?:[/?#]|$)/i, /\/sendBundle/i, /\/transactions\/send/i];

export class SendBlockedError extends Error {
  readonly code = ReasonCode.SEND_BLOCKED_BY_TRANSPORT;
  constructor(what: string) {
    super(`blocked by read-only transport: ${what}`);
  }
}

export interface HttpResult {
  status: number;
  headers: Record<string, string>;
  text: string;
  requestedAt: Date;
  receivedAt: Date;
}

export type FetchLike = (url: string, init: { method: string; headers: Record<string, string>; body?: string; signal?: AbortSignal }) => Promise<{
  status: number;
  headers: { forEach(cb: (value: string, key: string) => void): void };
  text(): Promise<string>;
}>;

export class NetworkError extends Error {}

export function assertReadOnlyRequest(url: string, body: unknown): void {
  let path: string;
  try {
    path = new URL(url).pathname;
  } catch {
    throw new SendBlockedError("unparseable URL");
  }
  for (const p of BLOCKED_PATHS) if (p.test(path)) throw new SendBlockedError(`path ${path}`);
  const calls = Array.isArray(body) ? body : body !== undefined ? [body] : [];
  for (const c of calls) {
    if (c && typeof c === "object" && "method" in c) {
      const m = String((c as { method: unknown }).method);
      if (BLOCKED_RPC_METHODS.has(m)) throw new SendBlockedError(`RPC method ${m}`);
    }
  }
}

export class ReadOnlyTransport {
  readonly readOnly = true as const;

  constructor(
    private readonly fetchImpl: FetchLike,
    private readonly clock: Clock,
    private readonly timeoutMs = 10_000,
  ) {}

  async request(method: "GET" | "POST", url: string, opts: { headers?: Record<string, string>; json?: unknown } = {}): Promise<HttpResult> {
    assertReadOnlyRequest(url, opts.json);
    const headers: Record<string, string> = { accept: "application/json", ...(opts.headers ?? {}) };
    let body: string | undefined;
    if (opts.json !== undefined) {
      headers["content-type"] = "application/json";
      body = JSON.stringify(opts.json);
    }
    const requestedAt = this.clock.now();
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), this.timeoutMs);
    try {
      const res = await this.fetchImpl(url, { method, headers, ...(body !== undefined ? { body } : {}), signal: ac.signal });
      const text = await res.text();
      const h: Record<string, string> = {};
      res.headers.forEach((v, k) => (h[k.toLowerCase()] = v));
      return { status: res.status, headers: h, text, requestedAt, receivedAt: this.clock.now() };
    } catch (e) {
      throw new NetworkError(e instanceof Error ? e.name : "network error"); // no URL in message (keys)
    } finally {
      clearTimeout(timer);
    }
  }
}

/**
 * JSON.parse that keeps selected numeric fields exact (as bigint) using the source-text access
 * reviver (Node >= 21). Provider numbers for token amounts must never pass through float64.
 */
export function parseJsonExact(text: string, bigintKeys: ReadonlySet<string>): unknown {
  return JSON.parse(text, function (this: unknown, key: string, value: unknown, context?: { source?: string }) {
    if (bigintKeys.has(key) && typeof value === "number") {
      const src = context?.source;
      if (src === undefined || !/^-?[0-9]+$/.test(src)) throw new TypeError(`field ${key}: expected integer, got ${src ?? value}`);
      return BigInt(src);
    }
    return value;
  } as (key: string, value: unknown) => unknown);
}
