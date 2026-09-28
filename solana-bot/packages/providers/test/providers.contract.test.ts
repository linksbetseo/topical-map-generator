import { describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { FakeClock, ReasonCode } from "@solbot/domain";
import {
  HeliusEnhanced,
  HeliusRpc,
  JupiterClient,
  Priority,
  ReadOnlyTransport,
  SendBlockedError,
  SlidingWindowLimiter,
  holderConcentration,
  tokenAccountsViaProgramAccounts,
  normalizeJupiterOrder,
  parseJsonExact,
  parseTokenInfo,
  type FetchLike,
} from "../src/index.ts";

const fx = (name: string) => JSON.parse(readFileSync(new URL(`./fixtures/${name}`, import.meta.url), "utf8"));
const t = { requestedAt: new Date("2026-10-02T10:00:00Z"), receivedAt: new Date("2026-10-02T10:00:00.150Z") };
const reqOf = (f: { request: { inputMint: string; outputMint: string; amount: string; slippageBps: number } }) => ({
  inputMint: f.request.inputMint,
  outputMint: f.request.outputMint,
  amountRaw: BigInt(f.request.amount),
  slippageBps: f.request.slippageBps,
});

function fakeFetch(handler: (url: string, init: { method: string; headers: Record<string, string>; body?: string }) => { status: number; body: unknown; headers?: Record<string, string> }) {
  const calls: Array<{ url: string; method: string; headers: Record<string, string>; body?: string }> = [];
  const f: FetchLike = async (url, init) => {
    calls.push({ url, ...init });
    const r = handler(url, init);
    const h = new Map(Object.entries(r.headers ?? {}));
    return { status: r.status, headers: { forEach: (cb) => h.forEach((v, k) => cb(v, k)) }, text: async () => (typeof r.body === "string" ? r.body : JSON.stringify(r.body)) };
  };
  return { f, calls };
}

describe("read-only transport", () => {
  const clock = new FakeClock("2026-10-02T10:00:00Z");
  it("blocks sending paths and RPC methods before any network I/O", async () => {
    const spy = vi.fn();
    const tr = new ReadOnlyTransport(spy as unknown as FetchLike, clock);
    await expect(tr.request("POST", "https://api.jup.ag/swap/v2/execute", { json: { signedTransaction: "x", requestId: "y" } })).rejects.toBeInstanceOf(SendBlockedError);
    await expect(tr.request("POST", "https://api.jup.ag/swap/v2/execute?x=1")).rejects.toBeInstanceOf(SendBlockedError);
    for (const method of ["sendTransaction", "sendRawTransaction", "sendBundle", "simulateTransaction", "requestAirdrop"]) {
      await expect(tr.request("POST", "https://mainnet.helius-rpc.com/?api-key=k", { json: { jsonrpc: "2.0", id: 1, method, params: [] } })).rejects.toBeInstanceOf(SendBlockedError);
    }
    await expect(
      tr.request("POST", "https://mainnet.helius-rpc.com/", { json: [{ method: "getSlot" }, { method: "sendTransaction" }] }),
    ).rejects.toBeInstanceOf(SendBlockedError);
    expect(spy).not.toHaveBeenCalled();
  });

  it("exact JSON parsing keeps u64 token amounts", () => {
    const v = parseJsonExact('{"amount": 18446744073709551615, "x": 1.5}', new Set(["amount"])) as { amount: bigint; x: number };
    expect(v.amount).toBe(18_446_744_073_709_551_615n);
    expect(() => parseJsonExact('{"amount": 1.5}', new Set(["amount"]))).toThrow();
  });
});

describe("rate limiter", () => {
  it("reserves capacity for exits over discovery and honours provider reset headers", () => {
    const clock = new FakeClock("2026-10-02T10:00:00Z");
    const l = new SlidingWindowLimiter(clock, 10, 0.6);
    let disc = 0;
    while (l.tryAcquire(Priority.DISCOVERY)) disc++;
    expect(disc).toBe(6);
    let exits = 0;
    while (l.tryAcquire(Priority.EXIT)) exits++;
    expect(exits).toBe(4);
    clock.advance(60_001);
    expect(l.tryAcquire(Priority.DISCOVERY)).toBe(true);
    l.observeHeaders({ "x-ratelimit-remaining": "0", "x-ratelimit-reset": String(clock.now().getTime() / 1000 + 5) });
    expect(l.tryAcquire(Priority.EXIT)).toBe(false);
    clock.advance(5_001);
    expect(l.tryAcquire(Priority.EXIT)).toBe(true);
  });
});

describe("Jupiter /order normalization (fixtures derived from OpenAPI schema, not live)", () => {
  it("fee collected in input mint: output is already net", () => {
    const f = fx("jupiter-order-buy-fee-input.json");
    const r = normalizeJupiterOrder(f.response, reqOf(f), t);
    if (!r.ok) throw new Error(r.detail);
    expect(r.quote.outAmountNetRaw).toBe(1_234_567_890n);
    expect(r.quote.platformFeeRaw).toBe(25_000n);
    expect(r.quote.feeSemantics).toBe("NO_OUTPUT_MINT_FEE");
    expect(r.quote.priceImpactBps).toBe(12);
    expect(r.quote.executionFidelity).toBe("QUOTE_ONLY_NO_TAKER");
    expect(r.quote.router).toBe("metis");
  });

  it("fee collected in output mint and already deducted: not deducted again", () => {
    const f = fx("jupiter-order-sell-fee-output.json");
    const r = normalizeJupiterOrder(f.response, reqOf(f), t);
    if (!r.ok) throw new Error(r.detail);
    expect(r.quote.feeSemantics).toBe("OUTPUT_NET_OF_FEE");
    expect(r.quote.outAmountNetRaw).toBe(24_850_150n);
  });

  it("gross outAmount with output-mint fee: fee subtracted exactly once", () => {
    const f = fx("jupiter-order-sell-fee-output.json");
    const r = normalizeJupiterOrder({ ...f.response, outAmount: "24875025" }, reqOf(f), t);
    if (!r.ok) throw new Error(r.detail);
    expect(r.quote.feeSemantics).toBe("OUTPUT_GROSS_ADJUSTED");
    expect(r.quote.outAmountNetRaw).toBe(24_850_150n);
  });

  it("inconsistent fee arithmetic is unresolved (blocks), never guessed", () => {
    const f = fx("jupiter-order-sell-fee-output.json");
    const r = normalizeJupiterOrder({ ...f.response, outAmount: "24860000" }, reqOf(f), t);
    expect(r.ok === false && r.code).toBe(ReasonCode.QUOTE_SEMANTICS_UNRESOLVED);
  });

  it("missing critical fields block instead of defaulting to zero", () => {
    const f = fx("jupiter-order-buy-fee-input.json");
    for (const k of ["outAmount", "priceImpact", "feeBps", "feeMint", "router", "routePlan", "requestId", "swapMode"]) {
      const body = { ...f.response };
      delete body[k];
      const r = normalizeJupiterOrder(body, reqOf(f), t);
      expect(r.ok === false && r.code).toBe(ReasonCode.QUOTE_FIELD_MISSING);
    }
    const r = normalizeJupiterOrder({ ...f.response, outAmount: 1234567890 }, reqOf(f), t);
    expect(r.ok === false && r.code).toBe(ReasonCode.QUOTE_FIELD_MISSING); // number instead of string
  });

  it("ultra mode or echo mismatch is not comparable with the profile", () => {
    const f = fx("jupiter-order-buy-fee-input.json");
    expect(normalizeJupiterOrder({ ...f.response, mode: "ultra" }, reqOf(f), t)).toMatchObject({ ok: false, code: "QUOTE_NOT_COMPARABLE" });
    expect(normalizeJupiterOrder({ ...f.response, inAmount: "24999999" }, reqOf(f), t)).toMatchObject({ ok: false, code: "QUOTE_SEMANTICS_UNRESOLVED" });
  });
});

describe("Jupiter client over HTTP (fake fetch)", () => {
  const f = fx("jupiter-order-buy-fee-input.json");
  const make = (handler: Parameters<typeof fakeFetch>[0], key: string | null = "test-key") => {
    const clock = new FakeClock("2026-10-02T10:00:00Z");
    const { f: ff, calls } = fakeFetch(handler);
    const c = new JupiterClient(new ReadOnlyTransport(ff, clock), new SlidingWindowLimiter(clock, 60), key);
    return { c, calls };
  };

  it("requests /swap/v2/order without taker, with slippageBps and the api key header", async () => {
    const { c, calls } = make(() => ({ status: 200, body: f.response }));
    const r = await c.quote(reqOf(f));
    expect(r.ok).toBe(true);
    const u = new URL(calls[0]!.url);
    expect(u.pathname).toBe("/swap/v2/order");
    expect(u.searchParams.get("taker")).toBeNull();
    expect(u.searchParams.get("slippageBps")).toBe("100");
    expect(u.searchParams.get("amount")).toBe("25000000");
    expect(calls[0]!.headers["x-api-key"]).toBe("test-key");
  });

  it("classifies NO_ROUTE, provider errors, outages and rate limits distinctly", async () => {
    const cases: Array<[number | "throw", unknown, string]> = [
      [400, { error: "No routes found" }, "NO_ROUTE"],
      [400, { error: "Invalid inputMint" }, "PROVIDER_ERROR"],
      [401, { error: "unauthorized" }, "PROVIDER_ERROR"],
      [429, {}, "RATE_LIMITED"],
      [503, "bad gateway", "PROVIDER_UNAVAILABLE"],
      ["throw", null, "PROVIDER_UNAVAILABLE"],
    ];
    for (const [status, body, code] of cases) {
      const { c } = make(() => {
        if (status === "throw") throw new Error("ECONNRESET");
        return { status, body };
      });
      const r = await c.quote(reqOf(f));
      expect(r.ok === false && r.code, `${status}`).toBe(code);
    }
  });

  it("price v3: tokens omitted by the provider stay unknown (never 0 or 1)", async () => {
    const { c } = make(() => ({ status: 200, body: { So11111111111111111111111111111111111111112: { usdPrice: 147.48, blockId: 1, decimals: 9, liquidity: 1, createdAt: "x", priceChange24h: 0 } } }));
    const r = await c.usdPrices(["So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]);
    if (!r.ok) throw new Error();
    expect(r.prices.get("So11111111111111111111111111111111111111112")!.usdPrice.toString()).toBe("147.48");
    expect(r.prices.has("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")).toBe(false);
  });

  it("parses the official /tokens/v2/search example", () => {
    const tok = parseTokenInfo(fx("jupiter-tokens-search-jup.json").response[0], t.receivedAt)!;
    expect(tok.mint).toBe("JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN");
    expect(tok.firstPoolCreatedAt!.toISOString()).toBe("2024-01-29T17:33:29.000Z");
    expect(tok.providerTopHoldersPct!.toString()).toBe("15.45"); // 0-100 scale
    expect(tok.liquidityUsd!.toString()).toBe("2992387.89");
    expect(tok.stats5m).toBeNull(); // not in the example: absent, not zero
    expect(tok.untrusted.symbol).toBe("JUP");
  });
});

describe("holders (owner-consolidated)", () => {
  const supply = 1_000_000n;
  const acc = (owner: string, amountRaw: bigint) => ({ owner, amountRaw });

  it("consolidates token accounts by owner and excludes only documented infra", () => {
    const accounts = [acc("pool", 500_000n), acc("A", 50_000n), acc("A", 50_000n), ...Array.from({ length: 400 }, (_, i) => acc(`h${i}`, 1_000n))];
    const r = holderConcentration(accounts, supply, true, new Set(["pool"]));
    expect(r.ok).toBe(true);
    expect(r.holderCount).toBe(401);
    expect(r.largestBps).toBe(1_000); // A = 100k = 10%
    expect(r.top10Bps).toBe(1_090);
  });

  it("an undocumented large holder is never excluded", () => {
    const r = holderConcentration([acc("whale", 600_000n), acc("x", 400_000n)], supply, true, new Set());
    expect(r.largestBps).toBe(6_000);
  });

  it("incomplete pagination or partial coverage => filter not met", () => {
    expect(holderConcentration([acc("a", 1_000_000n)], supply, false, new Set()).ok).toBe(false);
    const r = holderConcentration([acc("a", 900_000n)], supply, true, new Set());
    expect(r.ok).toBe(false);
    expect(r.reasons[0]!.code).toBe("HOLDER_DATA_UNAVAILABLE");
  });

  it("DAS pagination keeps exact u64 amounts", async () => {
    const clock = new FakeClock("2026-10-02T10:00:00Z");
    const pages = [
      `{"jsonrpc":"2.0","id":1,"result":{"last_indexed_slot":5,"cursor":"c1","token_accounts":[{"owner":"o1","amount":18446744073709551615},{"owner":"o2","amount":1}]}}`,
      `{"jsonrpc":"2.0","id":2,"result":{"last_indexed_slot":6,"token_accounts":[{"owner":"o3","amount":2}]}}`,
    ];
    let i = 0;
    const { f: ff } = fakeFetch(() => ({ status: 200, body: pages[i++]! }));
    const rpc = new HeliusRpc(new ReadOnlyTransport(ff, clock), new SlidingWindowLimiter(clock, 600), "https://mainnet.helius-rpc.com/?api-key=k");
    const r = await rpc.getAllTokenAccounts("mint", { pageLimit: 2 });
    if (!r.ok) throw new Error(r.detail);
    expect(r.value.complete).toBe(true);
    expect(r.value.accounts.map((a) => a.amountRaw)).toEqual([18_446_744_073_709_551_615n, 1n, 2n]);
    expect(r.value.lastIndexedSlot).toBe(6);
  });
});

describe("keyless holders via getProgramAccounts (public RPC)", () => {
  it("decodes owner + u64 amount from a 40-byte data slice and sends the right filters", async () => {
    const clock = new FakeClock("2026-10-02T10:00:00Z");
    const slice = (ownerByte: number, amount: bigint) => {
      const b = new Uint8Array(40);
      b.fill(ownerByte, 0, 32);
      new DataView(b.buffer).setBigUint64(32, amount, true);
      return Buffer.from(b).toString("base64");
    };
    let sent: { method: string; params: unknown[] } | null = null;
    const { f: ff } = fakeFetch((_u, init) => {
      sent = JSON.parse(init.body!);
      return { status: 200, body: { jsonrpc: "2.0", id: 1, result: { context: { slot: 42 }, value: [{ pubkey: "a", account: { data: [slice(7, 18_446_744_073_709_551_615n), "base64"] } }, { pubkey: "b", account: { data: [slice(0, 5n), "base64"] } }] } } };
    });
    const rpc = new HeliusRpc(new ReadOnlyTransport(ff, clock), new SlidingWindowLimiter(clock, 120), "https://api.mainnet-beta.solana.com", false);
    const r = await tokenAccountsViaProgramAccounts(rpc, "MintAddr", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
    if (!r.ok) throw new Error(r.detail);
    expect(r.value.accounts[0]!.amountRaw).toBe(18_446_744_073_709_551_615n);
    expect(r.value.accounts[1]!.owner).toBe("11111111111111111111111111111111");
    expect(r.value.lastIndexedSlot).toBe(42);
    expect(sent!.method).toBe("getProgramAccounts");
    expect(JSON.stringify(sent!.params)).toContain('"dataSize":165');
    expect(JSON.stringify(sent!.params)).toContain('"dataSlice":{"offset":32,"length":40}');
  });

  it("a refusal from the public RPC is reported, not replaced by partial data", async () => {
    const clock = new FakeClock("2026-10-02T10:00:00Z");
    const { f: ff } = fakeFetch(() => ({ status: 200, body: { jsonrpc: "2.0", id: 1, error: { code: -32010, message: "excluded from account secondary indexes" } } }));
    const rpc = new HeliusRpc(new ReadOnlyTransport(ff, clock), new SlidingWindowLimiter(clock, 120), "https://api.mainnet-beta.solana.com", false);
    const r = await tokenAccountsViaProgramAccounts(rpc, "MintAddr", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
    expect(r.ok).toBe(false);
  });
});

describe("Jupiter /order — recorded live responses (2026-09-28, no taker)", () => {
  it("buy USDC->SOL: platformFee has no amount; outAmount is already net of feeBps", () => {
    for (const name of ["jupiter-order-live-usdc-sol-1usdc.json", "jupiter-order-live-usdc-sol-25usdc.json"]) {
      const f = fx(name);
      expect(f.fixture_origin).toBe("recorded-live");
      expect(f.response.platformFee.amount).toBeUndefined();
      const r = normalizeJupiterOrder(f.response, reqOf(f), t);
      if (!r.ok) throw new Error(`${name}: ${r.detail}`);
      expect(r.quote.feeSemantics).toBe("OUTPUT_NET_OF_FEE");
      expect(r.quote.outAmountNetRaw).toBe(BigInt(f.response.outAmount));
      expect(r.quote.mode).toBe("manual");
      expect(r.quote.executionFidelity).toBe("QUOTE_ONLY_NO_TAKER");
      const feeBps = Number((r.quote.platformFeeRaw! * 10_000n) / r.quote.routeOutRaw);
      expect(feeBps).toBeLessThanOrEqual(f.response.feeBps);
    }
  });

  it("sell SOL->USDC: feeMint is the input mint, yet the fee is deducted from the output (multi-hop split)", () => {
    const f = fx("jupiter-order-live-sol-usdc-0.2sol.json");
    expect(f.response.feeMint).toBe(f.response.inputMint);
    const r = normalizeJupiterOrder(f.response, reqOf(f), t);
    if (!r.ok) throw new Error(r.detail);
    expect(r.quote.feeSemantics).toBe("OUTPUT_NET_OF_FEE");
    expect(r.quote.routeOutRaw - r.quote.outAmountNetRaw).toBe(r.quote.platformFeeRaw);
  });

  it("RFQ (jupiterz) firm quote is rejected: slippage not applied and fee not reconcilable", () => {
    const f = fx("jupiter-order-live-rfq-100usdc.json");
    expect(f.response.router).toBe("jupiterz");
    const r = normalizeJupiterOrder(f.response, reqOf(f), t);
    expect(r.ok).toBe(false);
    expect(r.ok === false && r.code).toBe(ReasonCode.QUOTE_SEMANTICS_UNRESOLVED);
  });

  it("client excludes the RFQ router in the profile request", async () => {
    const clock = new FakeClock("2026-10-02T10:00:00Z");
    const { f: ff, calls } = fakeFetch(() => ({ status: 200, body: fx("jupiter-order-live-usdc-sol-25usdc.json").response }));
    const c = new JupiterClient(new ReadOnlyTransport(ff, clock), new SlidingWindowLimiter(clock, 60), "k");
    const f = fx("jupiter-order-live-usdc-sol-25usdc.json");
    expect((await c.quote(reqOf(f))).ok).toBe(true);
    expect(new URL(calls[0]!.url).searchParams.get("excludeRouters")).toBe("jupiterz");
  });
});

describe("Helius Enhanced history pagination", () => {
  // type-filtered pages come back short while older matches still exist; only an empty page ends the history
  const make = (pages: unknown[][]) => {
    const clock = new FakeClock("2026-10-02T10:00:00Z");
    const { f, calls } = fakeFetch((url) => {
      const before = new URL(url).searchParams.get("before-signature");
      const i = before ? Number(before.slice(1)) + 1 : 0;
      return { status: 200, body: pages[i] ?? [] };
    });
    return { h: new HeliusEnhanced(new ReadOnlyTransport(f, clock), new SlidingWindowLimiter(clock, 100), "k"), calls };
  };
  const page = (i: number, n: number) => Array.from({ length: n }, (_, j) => ({ signature: j === n - 1 ? `p${i}` : `x${i}-${j}` }));
  const q = { type: "SWAP" as const, gteTime: 0, lteTime: 1, maxPages: 5 };

  it("keeps paging past short pages until an empty page", async () => {
    const { h, calls } = make([page(0, 48), page(1, 25), page(2, 7)]);
    const r = await h.history("W", q);
    expect(r.ok && r.value).toEqual({ txs: [...page(0, 48), ...page(1, 25), ...page(2, 7)], truncated: false });
    expect(calls).toHaveLength(4);
  });

  it("marks the history truncated when maxPages is hit", async () => {
    const { h } = make([page(0, 50), page(1, 50), page(2, 50)]);
    const r = await h.history("W", { ...q, maxPages: 2 });
    expect(r.ok && r.value.truncated).toBe(true);
    expect(r.ok && r.value.txs).toHaveLength(100);
  });
});
