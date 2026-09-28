import { describe, expect, it } from "vitest";
import { D, USDC_MINT, WSOL_MINT } from "@solbot/domain";
import { formatNotification, normalizeEnhancedSwap, TelegramNotifier } from "../src/index.ts";

// Synthetic Helius enhanced transactions shaped after helius-sdk src/enhanced/types.ts (fixture_origin: synthetic-from-sdk-types)
const W = "WatchedWallet111111111111111111111111111111";
const X = "TokenX11111111111111111111111111111111111111";
const fx = { usdcUsd: new D(1), solUsd: new D(150) };
const at = new Date("2026-10-02T10:00:05Z");
const base = { signature: "sig1", timestamp: 1_790_935_200, type: "SWAP", transactionError: null };

describe("Helius swap normalization", () => {
  it("USDC -> token is a BUY valued by USDC spent", () => {
    const r = normalizeEnhancedSwap(
      { ...base, tokenTransfers: [
        { fromUserAccount: W, toUserAccount: "Pool", mint: USDC_MINT, tokenAmount: 150 },
        { fromUserAccount: "Pool", toUserAccount: W, mint: X, tokenAmount: 1234.5678 },
      ] },
      new Set([W]), at, () => 6, fx,
    );
    expect(r.events).toHaveLength(1);
    expect(r.events[0]).toMatchObject({ side: "BUY", mint: X, tokenRaw: 1_234_567_800n, confirmed: true });
    expect(r.events[0]!.usd!.toString()).toBe("150");
    expect(r.events[0]!.blockTime.toISOString()).toBe("2026-10-02T10:00:00.000Z");
  });

  it("wSOL/native SOL -> token is valued at SOL/USD known at the time; fee-sized SOL is ignored", () => {
    const r = normalizeEnhancedSwap(
      { ...base, tokenTransfers: [
        { fromUserAccount: W, toUserAccount: "Pool", mint: WSOL_MINT, tokenAmount: 1 },
        { fromUserAccount: "Pool", toUserAccount: W, mint: X, tokenAmount: 10 },
      ], nativeTransfers: [{ fromUserAccount: W, toUserAccount: "Fee", amount: 5_000 }] },
      new Set([W]), at, () => 6, fx,
    );
    expect(r.events[0]!.usd!.toFixed(6)).toBe("150.000750"); // 1.000005 SOL * 150
  });

  it("token -> token is not priced; transfers without a base leg are not trades; failed tx ignored", () => {
    const Y = "TokenY11111111111111111111111111111111111111";
    const r1 = normalizeEnhancedSwap({ ...base, tokenTransfers: [{ fromUserAccount: W, toUserAccount: "P", mint: Y, tokenAmount: 5 }, { fromUserAccount: "P", toUserAccount: W, mint: X, tokenAmount: 5 }] }, new Set([W]), at, () => 6, fx);
    expect(r1.events).toEqual([]);
    expect(r1.dropped[0]!.reason).toMatch(/token-to-token/);
    const r2 = normalizeEnhancedSwap({ ...base, tokenTransfers: [{ fromUserAccount: "Other", toUserAccount: W, mint: X, tokenAmount: 5 }] }, new Set([W]), at, () => 6, fx);
    expect(r2.events).toEqual([]);
    const r3 = normalizeEnhancedSwap({ ...base, transactionError: { InstructionError: [0, "x"] }, tokenTransfers: [{ fromUserAccount: "P", toUserAccount: W, mint: X, tokenAmount: 5 }] }, new Set([W]), at, () => 6, fx);
    expect(r3.events).toEqual([]);
  });

  it("unknown decimals => dropped as AMOUNT_UNIT_AMBIGUOUS", () => {
    const r = normalizeEnhancedSwap({ ...base, tokenTransfers: [{ fromUserAccount: W, toUserAccount: "P", mint: USDC_MINT, tokenAmount: 150 }, { fromUserAccount: "P", toUserAccount: W, mint: X, tokenAmount: 1 }] }, new Set([W]), at, () => null, fx);
    expect(r.dropped[0]!.reason).toMatch(/AMOUNT_UNIT_AMBIGUOUS/);
  });
});

describe("Telegram notifier", () => {
  it("formats plain text with PAPER label and never leaks the bot token on errors", async () => {
    expect(formatNotification({ kind: "ENTRY", mint: "M", notionalUsd: new D(25), fillId: "paper_x" })).toMatch(/^\[PAPER\]/);
    const errors: string[] = [];
    const token = "123456:SECRET-TOKEN-VALUE";
    const bad = (async (url: string) => {
      throw new Error(`connect failed ${url}`);
    }) as unknown as typeof fetch;
    await new TelegramNotifier(token, "42", bad, (m) => errors.push(m)).notify({ kind: "ALERT", severity: "WARN", message: "x" });
    expect(errors[0]).not.toContain("SECRET-TOKEN-VALUE");
    let body = "";
    const ok = (async (_u: string, init: { body: string }) => {
      body = init.body;
      return { ok: true, status: 200 };
    }) as unknown as typeof fetch;
    await new TelegramNotifier(token, "42", ok).notify({ kind: "STATE", from: "RUNNING", to: "EXIT_ONLY", reason: "DAILY_LOSS_TRIGGER" });
    expect(JSON.parse(body)).toMatchObject({ chat_id: "42" });
    expect(JSON.parse(body).parse_mode).toBeUndefined();
  });
});
