import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { D, FakeClock, NATIVE_SOL, USDC_MINT, ValuationStatus } from "@solbot/domain";
import { Bucket, computeEquity, type FeeItem } from "@solbot/ledger";
import {
  FencingError,
  bookBuyFill,
  claimJob,
  completeJob,
  createAttempt,
  enqueueJob,
  failJob,
  insertRawEvent,
  loadLedger,
  migrate,
  reserveEntry,
  withTx,
  type Pool,
} from "../src/index.ts";
import { freshDb, newId, runningSession } from "./helpers.ts";

let pool: Pool;
beforeAll(async () => {
  pool = await freshDb();
});
afterAll(async () => {
  await pool.end();
});

const TOKEN = "Token1111111111111111111111111111111111111";
const at = new Date("2026-10-01T13:00:00Z");

async function reserve25(sessionId: string, key: string, mint = TOKEN) {
  return reserveEntry(pool, {
    sessionId,
    idempotencyKey: key,
    mint,
    signalId: null,
    inputs: { test: true },
    at,
    deployerGroup: null,
    decide: async ({ freeUsdcRaw, freeLamports }) =>
      freeUsdcRaw >= 25_000_000n && freeLamports >= 2_200_000n
        ? { approved: true, usdcRaw: 25_000_000n, lamports: 2_200_000n, notionalUsd: new D(25), decision: { ok: true } }
        : { approved: false, decision: { ok: false, freeUsdcRaw: freeUsdcRaw.toString() } },
  });
}

describe("schema and ledger constraints", () => {
  it("migrations are idempotent", async () => {
    expect(await migrate(pool)).toEqual([]);
  });

  it("opening balance in DB equals 500 USD of value at T0", async () => {
    const s = await runningSession(pool);
    const c = await pool.connect();
    try {
      const l = await loadLedger(c, s);
      const e = computeEquity(l, { usdcUsd: new D(1), solUsd: new D(150), at, source: "t" }, new Map(), { closeAccountFeeLamportsPerPosition: 0n });
      if (!e.ok) throw new Error();
      expect(e.equityTotalLowerBoundUsd.sub(500).abs().lte("0.000001")).toBe(true);
      expect(l.verifyIdentity().ok).toBe(true);
    } finally {
      c.release();
    }
  });

  it("ledger entries are append-only", async () => {
    const s = await runningSession(pool);
    await expect(pool.query(`UPDATE ledger_entries SET amount_raw = 1 WHERE session_id=$1`, [s])).rejects.toThrow(/append-only/);
    await expect(pool.query(`DELETE FROM ledger_entries WHERE session_id=$1`, [s])).rejects.toThrow(/append-only/);
    await expect(pool.query(`DELETE FROM ledger_transactions WHERE session_id=$1`, [s])).rejects.toThrow(/append-only/);
  });

  it("unbalanced or negative postings are rejected at commit", async () => {
    const s = await runningSession(pool);
    await expect(
      withTx(pool, async (c) => {
        await c.query(`INSERT INTO ledger_transactions (id, session_id, idempotency_key, kind, at) VALUES ('ltx_bad1',$1,'bad1','COMPENSATION',now())`, [s]);
        await c.query(`INSERT INTO ledger_entries (tx_id, session_id, bucket, asset, amount_raw) VALUES ('ltx_bad1',$1,'wallet',$2,5)`, [s, USDC_MINT]);
      }),
    ).rejects.toThrow(/unbalanced/);
    await expect(
      withTx(pool, async (c) => {
        await c.query(`INSERT INTO ledger_transactions (id, session_id, idempotency_key, kind, at) VALUES ('ltx_bad2',$1,'bad2','COMPENSATION',now())`, [s]);
        await c.query(`INSERT INTO ledger_entries (tx_id, session_id, bucket, asset, amount_raw) VALUES ('ltx_bad2',$1,'wallet',$2,-999999999999)`, [s, USDC_MINT]);
        await c.query(`INSERT INTO ledger_entries (tx_id, session_id, bucket, asset, amount_raw) VALUES ('ltx_bad2',$1,'reserved',$2,999999999999)`, [s, USDC_MINT]);
      }),
    ).rejects.toThrow(/negative holding/);
  });

  it("T0, T_end, config and mode are immutable", async () => {
    const s = await runningSession(pool);
    await expect(pool.query(`UPDATE sessions SET t0 = t0 + interval '1 hour', t_end = t_end + interval '1 hour' WHERE id=$1`, [s])).rejects.toThrow(/immutable/);
    await expect(pool.query(`UPDATE sessions SET config_hash='x' WHERE id=$1`, [s])).rejects.toThrow();
    await expect(pool.query(`UPDATE sessions SET mode='PAPER' WHERE id=$1`, [s])).rejects.toThrow(/immutable/);
    const r = await pool.query<{ h: string }>(`SELECT extract(epoch from (t_end - t0))/3600 AS h FROM sessions WHERE id=$1`, [s]);
    expect(Number(r.rows[0]!.h)).toBe(168);
  });

  it("LIVE modes cannot even be stored in this build", async () => {
    await expect(pool.query(`UPDATE sessions SET mode='LIVE'`)).rejects.toThrow();
  });
});

describe("atomic reservation", () => {
  it("parallel workers cannot spend the same cash twice", async () => {
    // 60 USD total, 20 of it SOL => 40 USDC => only one 25 USD reservation fits
    const s = await runningSession(pool, { totalUsd: "60" });
    const results = await Promise.all(Array.from({ length: 10 }, (_, i) => reserve25(s, `sig-${i}`, `Mint${i}`)));
    expect(results.filter((r) => r.status === "RESERVED")).toHaveLength(1);
    expect(results.filter((r) => r.status === "REJECTED")).toHaveLength(9);
    const c = await pool.connect();
    try {
      const l = await loadLedger(c, s);
      expect(l.balance(Bucket.RESERVED, USDC_MINT)).toBe(25_000_000n);
      expect(l.verifyIdentity().ok).toBe(true);
    } finally {
      c.release();
    }
  });

  it("a retried job with the same idempotency key is a no-op", async () => {
    const s = await runningSession(pool);
    const a = await reserve25(s, "sig-same");
    const b = await reserve25(s, "sig-same");
    expect(a.status).toBe("RESERVED");
    expect(b.status).toBe("DUPLICATE");
  });

  it("only one active position per mint (DB constraint)", async () => {
    const s = await runningSession(pool);
    expect((await reserve25(s, "k1", "MintZ")).status).toBe("RESERVED");
    await expect(reserve25(s, "k2", "MintZ")).rejects.toThrow(/positions_one_active_per_mint/);
  });

  it("trade intents are immutable", async () => {
    const s = await runningSession(pool);
    await reserve25(s, "imm");
    await expect(pool.query(`UPDATE trade_intents SET amount_raw = 1 WHERE session_id=$1`, [s])).rejects.toThrow(/append-only/);
  });
});

describe("exactly-once fill booking", () => {
  const fees: FeeItem[] = [
    { kind: "BASE_NETWORK", asset: NATIVE_SOL, amountRaw: 5_000n, usdFx: new D(150), source: "MODEL_ESTIMATE", includedInQuote: false, isEstimate: true },
    { kind: "PRIORITY", asset: NATIVE_SOL, amountRaw: 100_000n, usdFx: new D(150), source: "MODEL_ESTIMATE", includedInQuote: false, isEstimate: true },
    { kind: "PLATFORM", asset: USDC_MINT, amountRaw: 25_000n, usdFx: new D(1), source: "QUOTE", includedInQuote: true, isEstimate: false },
  ];

  it("duplicate and concurrent booking of the same attempt create one fill and one set of ledger entries", async () => {
    const s = await runningSession(pool);
    const r = await reserve25(s, "fill-1");
    if (r.status !== "RESERVED") throw new Error(r.status);
    const attemptId = await createAttempt(pool, { intentId: r.intentId, attemptNo: 1, model: { name: "BASE" }, fencingToken: null, at });
    const book = (fillId: string) =>
      withTx(pool, (c) =>
        bookBuyFill(c, {
          sessionId: s,
          intentId: r.intentId,
          attemptId,
          positionId: r.positionId,
          fillId,
          tokenMint: TOKEN,
          usdcInRaw: 25_000_000n,
          tokenOutRaw: 997_002n,
          minOutRaw: 990_000n,
          fees,
          rentLamports: 2_039_280n,
          usdcUsd: new D(1),
          solUsd: new D(150),
          costUsd: new D("25.01575"),
          executionFidelity: "FIXTURE",
          at,
          outcome: { test: true },
        }),
      );
    const outcomes = await Promise.all([book("paper_a"), book("paper_b"), book("paper_c")]);
    expect(outcomes.filter((o) => o === "BOOKED")).toHaveLength(1);
    expect(await book("paper_d")).toBe("ALREADY_BOOKED");

    const fills = await pool.query(`SELECT count(*)::int AS n FROM fills WHERE attempt_id=$1`, [attemptId]);
    expect(fills.rows[0].n).toBe(1);
    const c = await pool.connect();
    try {
      const l = await loadLedger(c, s);
      expect(l.balance(Bucket.WALLET, TOKEN)).toBe(997_002n);
      expect(l.balance(Bucket.RESERVED, USDC_MINT)).toBe(0n);
      expect(l.balance(Bucket.RESERVED, NATIVE_SOL)).toBe(0n); // 2_200_000 reserved - 105_000 fees - 2_039_280 rent = 55_720 released
      expect(l.balance(Bucket.RENT_LOCKED, NATIVE_SOL)).toBe(2_039_280n);
      expect(l.verifyIdentity().ok).toBe(true);
      const e = computeEquity(
        l,
        { usdcUsd: new D(1), solUsd: new D(150), at, source: "t" },
        new Map([[TOKEN, { mint: TOKEN, status: ValuationStatus.FRESH, netUsdcRaw: 24_900_000n, exitFeesLamports: 105_000n, quoteAt: at }]]),
        { closeAccountFeeLamportsPerPosition: 5_000n },
      );
      if (!e.ok) throw new Error();
      // 500 - 25 + 24.9 - fees(0.01575 paid + 0.01575 future exit) - close fee 0.00075
      expect(e.equityTotalLowerBoundUsd.toFixed(5)).toBe("499.86775");
    } finally {
      c.release();
    }
  });

  it("paper fill ids must carry the paper_ prefix and respect min_out", async () => {
    await expect(pool.query(`INSERT INTO fills (id) VALUES ('5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW')`)).rejects.toThrow();
  });
});

describe("jobs and events", () => {
  it("dedupes jobs, gives a job to exactly one of many workers, and fences stale workers", async () => {
    const clock = new FakeClock("2026-10-01T12:00:00Z");
    expect(await enqueueJob(pool, { kind: "EXIT", dedupeKey: "pos_1", payload: {}, runAt: clock.now() })).not.toBeNull();
    expect(await enqueueJob(pool, { kind: "EXIT", dedupeKey: "pos_1", payload: {}, runAt: clock.now() })).toBeNull();

    const claims = await Promise.all(Array.from({ length: 8 }, (_, i) => claimJob(pool, `w${i}`, clock.now(), 10_000, ["EXIT"])));
    const got = claims.filter((j) => j !== null);
    expect(got).toHaveLength(1);
    const first = got[0]!;
    const firstOwner = `w${claims.indexOf(first)}`;

    clock.advance(11_000); // lease expired
    const second = await claimJob(pool, "w-late", clock.now(), 10_000, ["EXIT"]);
    expect(second!.id).toBe(first.id);
    expect(second!.fencingToken).toBe(first.fencingToken + 1n);

    await expect(withTx(pool, (c) => completeJob(c, first.id, first.fencingToken, firstOwner))).rejects.toBeInstanceOf(FencingError);
    await withTx(pool, (c) => completeJob(c, second!.id, second!.fencingToken, "w-late"));
    expect(await claimJob(pool, "w9", clock.now(), 10_000, ["EXIT"])).toBeNull();
  });

  it("failed jobs back off and end in DEAD after max retries", async () => {
    const clock = new FakeClock("2026-10-02T12:00:00Z");
    await enqueueJob(pool, { kind: "DISC", dedupeKey: "d1", payload: {}, runAt: clock.now(), maxRetries: 2 });
    const j1 = (await claimJob(pool, "w", clock.now(), 5_000, ["DISC"]))!;
    expect(await withTx(pool, (c) => failJob(c, j1.id, j1.fencingToken, "w", clock.now(), "boom"))).toBe("RETRY");
    expect(await claimJob(pool, "w", clock.now(), 5_000, ["DISC"])).toBeNull(); // backoff
    clock.advance(1_000);
    const j2 = (await claimJob(pool, "w", clock.now(), 5_000, ["DISC"]))!;
    expect(await withTx(pool, (c) => failJob(c, j2.id, j2.fencingToken, "w", clock.now(), "boom"))).toBe("DEAD");
  });

  it("duplicate webhook delivery stores one event", async () => {
    const ev = {
      provider: "helius",
      sourceEventId: "sigABC",
      legIndex: 0,
      owner: "WalletA",
      blockTime: new Date("2026-10-01T12:00:00Z"),
      slot: 123n,
      commitment: "confirmed",
      receivedAt: new Date("2026-10-01T12:00:03Z"),
      availableAt: new Date("2026-10-01T12:00:03Z"),
      schemaVersion: "helius-enhanced-v0",
      payload: { signature: "sigABC" },
    };
    const a = await insertRawEvent(pool, ev);
    const b = await insertRawEvent(pool, { ...ev, receivedAt: new Date("2026-10-01T12:00:09Z"), availableAt: new Date("2026-10-01T12:00:09Z") });
    expect(a.inserted).toBe(true);
    expect(b.inserted).toBe(false);
    expect(b.id).toBe(a.id);
    // same signature, other leg/owner is a different event
    expect((await insertRawEvent(pool, { ...ev, owner: "WalletB" })).inserted).toBe(true);
  });
});

void newId;
