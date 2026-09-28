import { describe, expect, it } from "vitest";
import { replayCopy, type ReplayCandle, type ReplayParams } from "../src/copy-replay.ts";

const P: ReplayParams = { delaySec: 10, sizeUsd: 25, slippageBps: 0, feePerSideUsd: 0, stopLossBps: 1000, takeProfitBps: 2500, trailActivationBps: 1500, trailDrawdownBps: 800, timeStopSec: 4 * 3600 };
const T = 1_790_000_000; // multiple of 60 → minute-aligned
const c = (t: number, o: number, h: number, l: number, cl: number): ReplayCandle => ({ unix_time: t, o, h, l, c: cl });
const flat = (from: number, n: number, px: number) => Array.from({ length: n }, (_, i) => c(from + i * 60, px, px, px, px));

describe("copy replay (follower result, conservative)", () => {
  it("enters at the high of the traded second after the delay, not at the leader's price", () => {
    const r = replayCopy(T, [c(T, 1, 1, 1, 1), c(T + 10, 1.08, 1.1, 1.05, 1.09)], flat(T + 60, 240, 1.1), P);
    expect(r.status).toBe("FILLED");
    if (r.status !== "FILLED") return;
    expect(r.entryPrice).toBeCloseTo(1.1);
    expect(r.exitReason).toBe("TIME_STOP");
  });

  it("uses the last traded price when nothing traded in the delay second", () => {
    const r = replayCopy(T, [c(T + 3, 1, 1.02, 1, 1.01)], flat(T + 60, 240, 1.01), P);
    expect(r.status === "FILLED" && r.entryPrice).toBeCloseTo(1.01);
  });

  it("a gap through the stop fills at the candle close, not at -10%", () => {
    const r = replayCopy(T, [c(T + 10, 1, 1, 1, 1)], [c(T + 60, 1, 1, 0.99, 1), c(T + 120, 0.7, 0.72, 0.6, 0.62)], P);
    expect(r.status === "FILLED" && r.exitReason).toBe("STOP_LOSS");
    expect(r.status === "FILLED" && r.exitPrice).toBeCloseTo(0.62);
  });

  it("trailing after +15% then -8% from the peak exits near +5.8%, and stop wins a same-candle tie with TP", () => {
    const up = replayCopy(T, [c(T + 10, 1, 1, 1, 1)], [c(T + 60, 1, 1.15, 1, 1.15), c(T + 120, 1.15, 1.15, 1.05, 1.06)], P);
    expect(up.status === "FILLED" && up.exitReason).toBe("TRAILING_STOP");
    expect(up.status === "FILLED" && up.exitPrice).toBeCloseTo(1.058);
    const both = replayCopy(T, [c(T + 10, 1, 1, 1, 1)], [c(T + 60, 1, 1.3, 0.85, 1.2)], P);
    expect(both.status === "FILLED" && both.exitReason).toBe("STOP_LOSS");
  });

  it("take-profit at +25% and costs on both sides", () => {
    const r = replayCopy(T, [c(T + 10, 1, 1, 1, 1)], [c(T + 60, 1, 1.3, 1, 1.3)], { ...P, slippageBps: 100, feePerSideUsd: 0.05 });
    expect(r.status === "FILLED" && r.exitReason).toBe("TAKE_PROFIT");
    // entry 1.01, tp 1.2625, exit 1.2625*0.99
    expect(r.status === "FILLED" && r.pnlUsd).toBeCloseTo(25 * ((1.2625 * 0.99) / 1.01) - 25 - 0.1, 6);
  });

  it("no trades after entry: conservative value zero, last price only reported", () => {
    const r = replayCopy(T, [c(T + 10, 1, 1, 1, 1)], [], P);
    expect(r.status === "FILLED" && r.exitReason).toBe("EXIT_UNPRICED");
    expect(r.status === "FILLED" && r.pnlUsd).toBe(-25);
    expect(r.status === "FILLED" && r.pnlAtLastPriceUsd).toBeCloseTo(0);
  });

  it("no price at or before the entry second: no trade, not a fill", () => {
    expect(replayCopy(T, [c(T + 40, 1, 1, 1, 1)], [], P).status).toBe("NO_ENTRY_PRICE");
  });

  it("without 1 s history, enters at the HIGH of the 1 m candle containing the entry second", () => {
    const r = replayCopy(T + 5, [], [c(T, 1, 1.2, 0.9, 1), ...flat(T + 60, 240, 1)], P);
    expect(r.status === "FILLED" && r.entryGranularity).toBe("1m");
    expect(r.status === "FILLED" && r.entryPrice).toBeCloseTo(1.2);
  });

  it("leader-exit variant: exits when the leader sells (+delay) at the minute's low, own stops off", () => {
    const path = [c(T + 60, 1, 1, 0.8, 0.85), c(T + 120, 0.85, 1.5, 0.85, 1.4), c(T + 180, 1.4, 1.45, 1.3, 1.35)];
    const r = replayCopy(T, [c(T + 10, 1, 1, 1, 1)], path, { ...P, useOwnExits: false, leaderExitTime: T + 175 });
    expect(r.status === "FILLED" && r.exitReason).toBe("LEADER_EXIT");
    expect(r.status === "FILLED" && r.exitPrice).toBeCloseTo(1.3); // own SL would have fired at 0.85
  });
});
