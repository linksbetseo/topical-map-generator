import { describe, expect, it } from "vitest";
import { detectSignals, type HistTrade } from "../src/confluence-replay.ts";

const P = { minWallets: 3, windowSec: 180, minBuyUsd: 100, minRetainedBps: 8000, cooldownSec: 86_400 };
const buy = (wallet: string, t: number, usd = 150, mint = "M"): HistTrade => ({ wallet, mint, side: "BUY", t, usd, qty: usd });
const sell = (wallet: string, t: number, qty: number, mint = "M"): HistTrade => ({ wallet, mint, side: "SELL", t, usd: qty, qty });

describe("historical confluence detection", () => {
  it("fires at the third distinct wallet inside 180 s (first buy may be 170 s old)", () => {
    const s = detectSignals([buy("a", 0), buy("b", 90), buy("c", 170)], P);
    expect(s).toEqual([{ mint: "M", t: 170, wallets: ["a", "b", "c"], buyUsd: [150, 150, 150] }]);
  });

  it("does not fire when the window is exceeded or a wallet buys twice", () => {
    expect(detectSignals([buy("a", 0), buy("b", 90), buy("c", 181)], P)).toEqual([]);
    expect(detectSignals([buy("a", 0), buy("a", 10), buy("b", 20)], P)).toEqual([]);
  });

  it("sums a wallet's buys for the 100 USD minimum", () => {
    expect(detectSignals([buy("a", 0, 60), buy("a", 5, 60), buy("b", 10), buy("c", 20)], P)).toHaveLength(1);
    expect(detectSignals([buy("a", 0, 60), buy("b", 10), buy("c", 20)], P)).toEqual([]);
  });

  it("a sale of 50% before the decision invalidates that wallet's vote; later sales do not", () => {
    expect(detectSignals([buy("a", 0), sell("a", 30, 75), buy("b", 60), buy("c", 90)], P)).toEqual([]);
    expect(detectSignals([buy("a", 0), buy("b", 60), buy("c", 90), sell("a", 95, 150)], P)).toHaveLength(1);
  });

  it("one signal per mint per cooldown", () => {
    const s = detectSignals([buy("a", 0), buy("b", 1), buy("c", 2), buy("d", 3), buy("a", 100_000), buy("b", 100_001), buy("c", 100_002)], P);
    expect(s.map((x) => x.t)).toEqual([2, 100_002]);
  });
});
