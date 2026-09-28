import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { D, USDC_MINT, WSOL_MINT } from "@solbot/domain";
import { effectToEvents, reconstructEpisodes, walletMetrics, walletTxEffect, SPL_TOKEN_ACCOUNT_RENT, type CompactTxLike, type BaseFx } from "@solbot/strategy";
import { normalizeEnhancedSwap } from "../src/helius-flow.ts";

/**
 * P0 ledger tests (spec v2 §16). The first block reproduces, on a real mainnet transaction, the SOL
 * double count of the legacy transfer-list normalizer; the rest are acceptance tests for the
 * balance-change normalizer.
 */
const fx = (name: string) => JSON.parse(readFileSync(new URL(`./fixtures/${name}`, import.meta.url), "utf8"));
const FX: BaseFx = { solUsd: new D(100), usdcUsd: new D(1), usdtUsd: new D(1) };
const T0 = new Date("2026-10-01T00:00:00Z");
const SPL = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const W = "Wa11et1111111111111111111111111111111111111";
const W2 = "Wa11et2222222222222222222222222222222222222";
const POOL = "Poo1111111111111111111111111111111111111111";
const MEME = "Meme111111111111111111111111111111111111111";
const MEME2 = "Meme222222222222222222222222222222222222222";

/** Synthetic transaction: wallets' lamport deltas and token-account pre/post (null = account absent). */
function tx(
  sig: string,
  t: number,
  opts: {
    payer?: string;
    fee?: number;
    lamports?: Record<string, number>;
    tokens?: Array<{ owner: string; mint: string; pre: string | null; post: string | null; decimals?: number; programId?: string }>;
    err?: unknown;
  },
): CompactTxLike {
  const payer = opts.payer ?? W;
  const keys = [payer, ...Object.keys(opts.lamports ?? {}).filter((k) => k !== payer)];
  const pre = keys.map(() => 10_000_000_000);
  const post = keys.map((k, i) => pre[i]! + (opts.lamports?.[k] ?? 0));
  const tokens = opts.tokens ?? [];
  const accIdx = tokens.map((a, i) => {
    // token accounts are real keys: rent-exempt lamports (+ wrapped SOL for WSOL), 0 when absent
    const lam = (amt: string | null) => (amt === null ? 0 : Number(SPL_TOKEN_ACCOUNT_RENT) + (a.mint === WSOL_MINT ? Number(amt) : 0));
    keys.push(`acct-${i}`);
    pre.push(lam(a.pre));
    post.push(lam(a.post));
    return keys.length - 1;
  });
  const tb = (which: "pre" | "post") =>
    tokens.flatMap((a, i) =>
      a[which] === null ? [] : [{ accountIndex: accIdx[i]!, mint: a.mint, owner: a.owner, programId: a.programId ?? SPL, amount: a[which]!, decimals: a.decimals ?? 6 }],
    );
  return { signature: sig, slot: 1, blockTime: t, err: opts.err ?? null, fee: opts.fee ?? 5000, accountKeys: keys, preBalances: pre, postBalances: post, preTokenBalances: tb("pre"), postTokenBalances: tb("post"), programIds: [] };
}
const t = (min: number) => Math.floor(Date.parse("2026-09-20T00:00:00Z") / 1000) + min * 60;

describe("REPRODUCTION: legacy transfer-list normalizer vs balance changes (mainnet tx 3RjgA7Vr…, OKX route)", () => {
  const enhanced = fx("enhanced-okx-sell-unwrap.json");
  const rpc = fx("rpc-okx-sell-unwrap.json") as CompactTxLike;
  const wallet = enhanced.feePayer as string;

  it("legacy counts the same SOL twice (WSOL transfer + unwrap native transfer)", () => {
    const decimals = () => 6;
    const legacy = normalizeEnhancedSwap(enhanced, new Set([wallet]), new Date(), decimals, { usdcUsd: FX.usdcUsd, solUsd: FX.solUsd });
    expect(legacy.events).toHaveLength(1);
    // 0.270505481 (WSOL) + 0.270169956 (native) - 0.001855569 (tip) = 0.538819868 SOL → 53.88 USD at 100
    expect(legacy.events[0]!.usd!.toFixed(2)).toBe("53.88");
  });

  it("balance changes give the real proceeds: lamport delta + fee = 0.268314387 SOL", () => {
    const e = walletTxEffect(rpc, wallet);
    expect(e.cls).toBe("SELL");
    expect(e.feeLamports).toBe(6577n);
    expect(e.base.solLamports).toBe(268_307_810n + 6577n);
    expect(e.tokens).toEqual([{ mint: "8RVBk8vxLiUHueLUW1f4izFVqN3nWippLhkohKg6EGkS", deltaRaw: -5_000_000_000n, decimals: 6, programId: "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" }]);
    const [ev] = effectToEvents(e, wallet, FX, new Date());
    expect(ev!.usd!.toFixed(4)).toBe("26.8314");
  });

  it("a real 0.0005 SOL buy into a new Token-2022 account: exact rent from the account's lamports, still a BUY", () => {
    const x = fx("rpc-token2022-microbuy.json") as CompactTxLike;
    const e = walletTxEffect(x, "7PCjgWHgh9V37aEusHPjGq4jSfMNZaH5SXAHifke1osg");
    expect(e.cls).toBe("BUY");
    expect(e.rentLamports).toBe(1_944_231n);
    expect(e.feeLamports).toBe(10_350n);
    expect(e.base.solLamports).toBe(-500_000n);
  });

  it("a version-1 transaction (new format, requires maxSupportedTransactionVersion 1) is read like any other", () => {
    const x = fx("rpc-v1-transaction.json") as CompactTxLike;
    expect(x.accountKeys[0]).toBe("2BgzoYwUn36BWjnF9R3qpZwYvMnmurwRx8Y6gkhS4835");
    expect(x.accountKeys).toHaveLength(x.preBalances.length);
    expect(walletTxEffect(x, x.accountKeys[0]!).cls).not.toBe("NOT_INVOLVED");
  });

  it("a real Jupiter USDC buy is one BUY with integer amounts", () => {
    const buy = fx("rpc-jupiter-usdc-buy.json") as CompactTxLike;
    const e = walletTxEffect(buy, buy.accountKeys[0]!);
    expect(e.cls).toBe("BUY");
    expect(e.base.usdcRaw).toBe(-49_999_292n);
    expect(e.tokens).toHaveLength(1);
    expect(e.tokens[0]!.deltaRaw).toBe(202_013_459_666n);
    expect(e.tokens[0]!.decimals).toBe(9);
  });
});

describe("balance-change normalizer — acceptance (spec v2 §16)", () => {
  it("#1 token not listed anywhere, bought for SOL: cost = SOL spent × SOL/USD; fee and rent excluded", () => {
    const e = walletTxEffect(tx("b1", t(0), { fee: 5000, lamports: { [W]: -2_000_000_000 - 5000 - Number(SPL_TOKEN_ACCOUNT_RENT) }, tokens: [{ owner: W, mint: MEME, pre: null, post: "1000000" }] }), W);
    expect(e.cls).toBe("BUY");
    expect(e.rentLamports).toBe(SPL_TOKEN_ACCOUNT_RENT);
    expect(e.base.solLamports).toBe(-2_000_000_000n);
    const [ev] = effectToEvents(e, W, { ...FX, solUsd: new D(150) }, new Date());
    expect(ev!.usd!.toString()).toBe("300");
  });

  it("#4 raw amounts beyond 2^53 stay exact; decimals 6 and 9; blockTime is seconds", () => {
    const big = "123456789123456789123";
    const e = walletTxEffect(tx("u1", t(1), { lamports: { [W]: -1_000_000_000 - 5000 }, tokens: [{ owner: W, mint: MEME, pre: "0", post: big, decimals: 9 }] }), W);
    expect(e.tokens[0]!.deltaRaw).toBe(BigInt(big));
    expect(e.tokens[0]!.decimals).toBe(9);
    expect(e.blockTime!.toISOString()).toBe("2026-09-20T00:01:00.000Z");
    const u = walletTxEffect(tx("u2", t(2), { lamports: { [W]: -5000 }, tokens: [{ owner: W, mint: USDC_MINT, pre: "5000000", post: "0" }, { owner: W, mint: MEME, pre: "0", post: "7", decimals: 6 }] }), W);
    expect(u.base.usdcRaw).toBe(-5_000_000n);
    expect(u.cls).toBe("BUY");
  });

  it("#5 a multi-hop route (SOL → USDC → token through the wallet's own USDC account) is one purchase", () => {
    const e = walletTxEffect(
      tx("m1", t(3), {
        lamports: { [W]: -1_000_000_000 - 5000 },
        tokens: [
          { owner: W, mint: USDC_MINT, pre: "0", post: "0" }, // intermediate hop nets to zero
          { owner: W, mint: MEME, pre: "0", post: "500" },
        ],
      }),
      W,
    );
    expect(e.cls).toBe("BUY");
    expect(effectToEvents(e, W, FX, new Date())).toHaveLength(1);
  });

  it("#7 one transaction touching two watched wallets keeps both economic events", () => {
    const x = tx("two", t(4), {
      lamports: { [W]: -1_000_000_000 - 5000, [W2]: -2_000_000_000 },
      tokens: [
        { owner: W, mint: MEME, pre: "0", post: "100" },
        { owner: W2, mint: MEME, pre: "0", post: "200" },
      ],
    });
    expect(walletTxEffect(x, W).cls).toBe("BUY");
    expect(walletTxEffect(x, W2).cls).toBe("BUY");
    expect(walletTxEffect(x, W2).feeLamports).toBe(0n);
  });

  it("#9 incoming transfer is not a buy, outgoing transfer is not a sale", () => {
    const tin = walletTxEffect(tx("ti", t(5), { payer: POOL, lamports: { [W]: 0 }, tokens: [{ owner: W, mint: MEME, pre: null, post: "100" }] }), W);
    expect(tin.cls).toBe("TRANSFER_IN");
    const tout = walletTxEffect(tx("to", t(6), { lamports: { [W]: -5000 }, tokens: [{ owner: W, mint: MEME, pre: "100", post: "0" }] }), W);
    expect(tout.cls).toBe("TRANSFER_OUT");
    // bought for 100 USD, then moved out: no proceeds are invented
    const buy = walletTxEffect(tx("bb", t(4), { lamports: { [W]: -1_000_000_000 - 5000 }, tokens: [{ owner: W, mint: MEME, pre: "0", post: "100" }] }), W);
    const events = [buy, tout].flatMap((e) => effectToEvents(e, W, FX, new Date(0)));
    const r = reconstructEpisodes(events, T0, new Map());
    expect(r.episodes).toHaveLength(1);
    expect(r.episodes[0]!.status).toBe("UNKNOWN_COST_BASIS");
    expect(r.episodes[0]!.pnlUsd).toBeNull();
  });

  it("#10 airdropped / unknown opening inventory never gets cost basis zero", () => {
    const air = walletTxEffect(tx("air", t(7), { payer: POOL, lamports: { [W]: 0 }, tokens: [{ owner: W, mint: MEME, pre: null, post: "100" }] }), W);
    const sell = walletTxEffect(tx("sl", t(8), { lamports: { [W]: 5_000_000_000 - 5000 }, tokens: [{ owner: W, mint: MEME, pre: "100", post: "0" }] }), W);
    const events = [air, sell].flatMap((e) => effectToEvents(e, W, FX, new Date(0)));
    const r = reconstructEpisodes(events, T0, new Map());
    expect(r.episodes[0]!.status).toBe("UNKNOWN_COST_BASIS");
    expect(r.episodes[0]!.pnlUsd).toBeNull(); // not +500 USD
  });

  it("#11 thirty partial sells of one position are one closed episode, not thirty", () => {
    const evs = [walletTxEffect(tx("p0", t(10), { lamports: { [W]: -3_000_000_000 - 5000 }, tokens: [{ owner: W, mint: MEME, pre: "0", post: "3000" }] }), W)];
    for (let i = 0; i < 30; i++)
      evs.push(walletTxEffect(tx(`p${i + 1}`, t(11 + i), { lamports: { [W]: 110_000_000 - 5000 }, tokens: [{ owner: W, mint: MEME, pre: String(3000 - i * 100), post: String(3000 - (i + 1) * 100) }] }), W));
    const events = evs.flatMap((e) => effectToEvents(e, W, FX, new Date(0)));
    const r = reconstructEpisodes(events, T0, new Map());
    const m = walletMetrics(r, events, T0);
    expect(m.closedEpisodes).toBe(1);
    expect(r.episodes[0]!.pnlUsd!.toFixed(2)).toBe("30.00"); // 30 × 11 - 300
  });

  it("#12 unsold losers stay in the score (conservative mark)", () => {
    const evs: ReturnType<typeof walletTxEffect>[] = [];
    // one small realized win …
    evs.push(walletTxEffect(tx("w1", t(20), { lamports: { [W]: -1_000_000_000 - 5000 }, tokens: [{ owner: W, mint: MEME, pre: "0", post: "10" }] }), W));
    evs.push(walletTxEffect(tx("w2", t(21), { lamports: { [W]: 1_100_000_000 - 5000 }, tokens: [{ owner: W, mint: MEME, pre: "10", post: "0" }] }), W));
    // … and a large loser still held
    evs.push(walletTxEffect(tx("l1", t(22), { lamports: { [W]: -5_000_000_000 - 5000 }, tokens: [{ owner: W, mint: MEME2, pre: "0", post: "1000" }] }), W));
    const events = evs.flatMap((e) => effectToEvents(e, W, FX, new Date(0)));
    const marks = new Map([[MEME2, new D("0.05")]]); // 1000 raw × 0.05 = 50 USD left of 500
    const m = walletMetrics(reconstructEpisodes(events, T0, marks), events, T0);
    expect(m.totalPnlUsd.toFixed(2)).toBe("-440.00");
  });

  it("WSOL kept in a persistent account counts as SOL; closing a token account refunds rent, not proceeds", () => {
    const e = walletTxEffect(
      tx("ws", t(30), {
        lamports: { [W]: -5000 + Number(SPL_TOKEN_ACCOUNT_RENT) },
        tokens: [
          { owner: W, mint: WSOL_MINT, pre: "0", post: "750000000", decimals: 9 },
          { owner: W, mint: MEME, pre: "100", post: null }, // sold everything and closed the account
        ],
      }),
      W,
    );
    expect(e.cls).toBe("SELL");
    expect(e.rentLamports).toBe(-SPL_TOKEN_ACCOUNT_RENT);
    expect(e.base.solLamports).toBe(750_000_000n);
  });

  it("failed transactions cost the fee and move nothing else", () => {
    const e = walletTxEffect(tx("f", t(31), { err: { InstructionError: [2, "Custom"] }, lamports: { [W]: -5000 } }), W);
    expect(e.cls).toBe("FAILED");
    expect(e.feeLamports).toBe(5000n);
    expect(effectToEvents(e, W, FX, new Date())).toHaveLength(0);
  });

  it("two risk tokens changing at once is UNSUPPORTED, never priced", () => {
    const e = walletTxEffect(tx("tt", t(32), { lamports: { [W]: -5000 }, tokens: [{ owner: W, mint: MEME, pre: "100", post: "0" }, { owner: W, mint: MEME2, pre: "0", post: "5" }] }), W);
    expect(e.cls).toBe("UNSUPPORTED");
    expect(effectToEvents(e, W, FX, new Date())).toHaveLength(0);
  });
});
