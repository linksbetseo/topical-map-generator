import { NATIVE_SOL, USDC_MINT, newId, type Dec } from "@solbot/domain";
import { Bucket, buyFillTx, failedAttemptFeeTx, releaseTx, rentRecoveryTx, reserveTx, sellFillTx, type FeeItem } from "@solbot/ledger";
import { insertLedgerTx, sqlBalance } from "./ledger-repo.ts";
import { lockSession, type SessionRow } from "./sessions-repo.ts";
import { json, type Client, type Pool } from "./pool.ts";

export interface EntryReservationRequest {
  sessionId: string;
  idempotencyKey: string;
  mint: string;
  signalId: string | null;
  inputs: unknown;
  at: Date;
  deployerGroup: string | null;
  /**
   * Called under the session row lock with the locked session and current free balances.
   * Must return the risk decision computed from *this* state (risk check #1).
   */
  decide: (ctx: { c: Client; session: SessionRow; freeUsdcRaw: bigint; freeLamports: bigint }) => Promise<
    | { approved: true; usdcRaw: bigint; lamports: bigint; notionalUsd: Dec; decision: unknown }
    | { approved: false; decision: unknown }
  >;
}

export type ReservationResult =
  | { status: "RESERVED"; intentId: string; positionId: string; reservationId: string }
  | { status: "DUPLICATE"; intentId: string }
  | { status: "REJECTED"; decision: unknown };

/**
 * Atomic: lock session -> risk decision on current balances -> intent + reservation + ledger + RESERVED position.
 * Two workers (or a retried job) cannot spend the same cash twice: the session lock serializes decisions and the
 * intent idempotency key makes a retry a no-op.
 */
export async function reserveEntry(pool: Pool, req: EntryReservationRequest): Promise<ReservationResult> {
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    const session = await lockSession(c, req.sessionId);
    const dup = await c.query<{ id: string }>(`SELECT id FROM trade_intents WHERE session_id=$1 AND idempotency_key=$2`, [req.sessionId, req.idempotencyKey]);
    if (dup.rows[0]) {
      await c.query("ROLLBACK");
      return { status: "DUPLICATE", intentId: dup.rows[0].id };
    }
    const freeUsdcRaw = await sqlBalance(c, req.sessionId, Bucket.WALLET, USDC_MINT);
    const freeLamports = await sqlBalance(c, req.sessionId, Bucket.WALLET, NATIVE_SOL);
    const d = await req.decide({ c, session, freeUsdcRaw, freeLamports });
    if (!d.approved) {
      await c.query("ROLLBACK");
      return { status: "REJECTED", decision: d.decision };
    }
    const intentId = newId("int");
    const positionId = newId("pos");
    const reservationId = newId("res");
    const ltxId = newId("ltx");
    await c.query(
      `INSERT INTO trade_intents (id, session_id, strategy_version, config_hash, idempotency_key, side, kind, mint, input_mint, output_mint, amount_raw, notional_usd, signal_id, position_id, risk_decision, inputs, created_at)
       VALUES ($1,$2,$3,$4,$5,'BUY','ENTRY',$6,$7,$6,$8,$9,$10,$11,$12,$13,$14)`,
      [intentId, req.sessionId, session.strategy_version, session.config_hash, req.idempotencyKey, req.mint, USDC_MINT, d.usdcRaw.toString(), d.notionalUsd.toString(), req.signalId, positionId, json(d.decision), json(req.inputs), req.at],
    );
    await insertLedgerTx(c, reserveTx({ id: ltxId, sessionId: req.sessionId, idempotencyKey: `reserve:${intentId}`, at: req.at, refs: { intent_id: intentId } }, { usdcRaw: d.usdcRaw, lamports: d.lamports }));
    await c.query(
      `INSERT INTO balance_reservations (id, session_id, intent_id, usdc_raw, lamports, notional_usd, status, reserve_ledger_tx, created_at) VALUES ($1,$2,$3,$4,$5,$6,'ACTIVE',$7,$8)`,
      [reservationId, req.sessionId, intentId, d.usdcRaw.toString(), d.lamports.toString(), d.notionalUsd.toString(), ltxId, req.at],
    );
    await c.query(`INSERT INTO positions (id, session_id, mint, status, entry_intent_id, deployer_group) VALUES ($1,$2,$3,'RESERVED',$4,$5)`, [
      positionId,
      req.sessionId,
      req.mint,
      intentId,
      req.deployerGroup,
    ]);
    await c.query("COMMIT");
    return { status: "RESERVED", intentId, positionId, reservationId };
  } catch (e) {
    await c.query("ROLLBACK").catch(() => undefined);
    throw e;
  } finally {
    c.release();
  }
}

export async function createAttempt(c: Client | Pool, a: { intentId: string; attemptNo: number; model: unknown; fencingToken: bigint | null; at: Date }): Promise<string> {
  const id = newId("att");
  await c.query(
    `INSERT INTO order_attempts (id, intent_id, attempt_no, state, broker, model, fencing_token, started_at) VALUES ($1,$2,$3,'QUOTED','PAPER',$4,$5,$6)`,
    [id, a.intentId, a.attemptNo, json(a.model), a.fencingToken?.toString() ?? null, a.at],
  );
  return id;
}

export interface BookBuyFill {
  sessionId: string;
  intentId: string;
  attemptId: string;
  positionId: string;
  fillId: string;
  tokenMint: string;
  usdcInRaw: bigint;
  tokenOutRaw: bigint;
  minOutRaw: bigint;
  fees: FeeItem[];
  rentLamports: bigint;
  usdcUsd: Dec;
  solUsd: Dec;
  costUsd: Dec;
  executionFidelity: string;
  at: Date;
  outcome: unknown;
}

/**
 * Books a paper buy fill exactly once: fill row (UNIQUE attempt_id) + fee items + ledger + reservation + position,
 * all in one DB transaction. A duplicate call returns "ALREADY_BOOKED" without touching balances.
 */
export async function bookBuyFill(c: Client, f: BookBuyFill): Promise<"BOOKED" | "ALREADY_BOOKED"> {
  await lockSession(c, f.sessionId);
  const ins = await c.query(
    `INSERT INTO fills (id, attempt_id, position_id, side, in_mint, in_amount_raw, out_mint, out_amount_raw, min_out_raw, usdc_usd, sol_usd, execution_fidelity, filled_at)
     VALUES ($1,$2,$3,'BUY',$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (attempt_id) DO NOTHING`,
    [f.fillId, f.attemptId, f.positionId, USDC_MINT, f.usdcInRaw.toString(), f.tokenMint, f.tokenOutRaw.toString(), f.minOutRaw.toString(), f.usdcUsd.toString(), f.solUsd.toString(), f.executionFidelity, f.at],
  );
  if (ins.rowCount === 0) return "ALREADY_BOOKED";

  for (const fee of f.fees) {
    await c.query(
      `INSERT INTO fee_items (attempt_id, fill_id, kind, asset, amount_raw, usd_fx, source, included_in_quote, is_estimate) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [f.attemptId, f.fillId, fee.kind, fee.asset, fee.amountRaw.toString(), fee.usdFx?.toString() ?? null, fee.source, fee.includedInQuote, fee.isEstimate],
    );
  }
  await insertLedgerTx(
    c,
    buyFillTx(
      { id: newId("ltx"), sessionId: f.sessionId, idempotencyKey: `fill:${f.attemptId}`, at: f.at, refs: { fill_id: f.fillId, intent_id: f.intentId } },
      { usdcInRaw: f.usdcInRaw, tokenMint: f.tokenMint, tokenOutRaw: f.tokenOutRaw, fees: f.fees, rentLamports: f.rentLamports },
    ),
  );
  // release whatever of the reservation was not consumed
  const res = await c.query<{ id: string; usdc_raw: string; lamports: string }>(
    `SELECT id, usdc_raw, lamports FROM balance_reservations WHERE intent_id=$1 AND status='ACTIVE' FOR UPDATE`,
    [f.intentId],
  );
  const r = res.rows[0];
  if (!r) throw new Error(`no active reservation for ${f.intentId}`);
  const usedLamports = f.fees.filter((x) => !x.includedInQuote && x.asset === NATIVE_SOL).reduce((a, x) => a + x.amountRaw, 0n) + f.rentLamports;
  const leftUsdc = BigInt(r.usdc_raw) - f.usdcInRaw;
  const leftLamports = BigInt(r.lamports) - usedLamports;
  if (leftUsdc < 0n || leftLamports < 0n) throw new Error("fill exceeds reservation");
  if (leftUsdc > 0n || leftLamports > 0n) {
    await insertLedgerTx(c, releaseTx({ id: newId("ltx"), sessionId: f.sessionId, idempotencyKey: `release:${f.intentId}`, at: f.at }, { usdcRaw: leftUsdc, lamports: leftLamports }));
  }
  await c.query(`UPDATE balance_reservations SET status='CONSUMED', resolved_at=$2 WHERE id=$1`, [r.id, f.at]);
  await c.query(`UPDATE order_attempts SET state='PAPER_FILLED', outcome=$2, finished_at=$3 WHERE id=$1`, [f.attemptId, json(f.outcome), f.at]);
  await c.query(
    `UPDATE positions SET status='OPEN', qty_raw=$2, cost_usd=$3, rent_lamports=$4, entry_filled_at=$5, peak_net_value_usd=NULL WHERE id=$1 AND status='RESERVED'`,
    [f.positionId, f.tokenOutRaw.toString(), f.costUsd.toString(), f.rentLamports.toString(), f.at],
  );
  return "BOOKED";
}

/** Failed/not-sent entry attempt: charge estimated costs (if any) from the reservation, release the rest. */
export async function resolveFailedEntry(
  c: Client,
  a: { sessionId: string; intentId: string; attemptId: string; positionId: string; state: "FAILED_PAPER" | "CANCELLED_BEFORE_SEND"; chargedFees: FeeItem[]; reasonCode: string; outcome: unknown; at: Date; releaseReservation: boolean },
): Promise<void> {
  await lockSession(c, a.sessionId);
  for (const fee of a.chargedFees) {
    await c.query(
      `INSERT INTO fee_items (attempt_id, kind, asset, amount_raw, usd_fx, source, included_in_quote, is_estimate) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [a.attemptId, fee.kind, fee.asset, fee.amountRaw.toString(), fee.usdFx?.toString() ?? null, fee.source, fee.includedInQuote, fee.isEstimate],
    );
  }
  if (a.chargedFees.length > 0) {
    await insertLedgerTx(c, failedAttemptFeeTx({ id: newId("ltx"), sessionId: a.sessionId, idempotencyKey: `failfee:${a.attemptId}`, at: a.at }, a.chargedFees, Bucket.RESERVED));
  }
  const counts = a.state === "FAILED_PAPER";
  await c.query(`UPDATE order_attempts SET state=$2, outcome=$3, reason_code=$4, finished_at=$5, counts_toward_daily_attempts=$6 WHERE id=$1`, [
    a.attemptId,
    a.state,
    json(a.outcome),
    a.reasonCode,
    a.at,
    counts,
  ]);
  if (a.releaseReservation) {
    const res = await c.query<{ id: string; usdc_raw: string; lamports: string }>(
      `SELECT id, usdc_raw, lamports FROM balance_reservations WHERE intent_id=$1 AND status='ACTIVE' FOR UPDATE`,
      [a.intentId],
    );
    const r = res.rows[0];
    if (r) {
      const charged = a.chargedFees.filter((f) => f.asset === NATIVE_SOL && !f.includedInQuote).reduce((x, f) => x + f.amountRaw, 0n);
      const leftLamports = BigInt(r.lamports) - charged;
      if (leftLamports < 0n) throw new Error("charged fees exceed reservation");
      await insertLedgerTx(c, releaseTx({ id: newId("ltx"), sessionId: a.sessionId, idempotencyKey: `release:${a.intentId}`, at: a.at }, { usdcRaw: BigInt(r.usdc_raw), lamports: leftLamports }));
      await c.query(`UPDATE balance_reservations SET status='RELEASED', resolved_at=$2 WHERE id=$1`, [r.id, a.at]);
      await c.query(`UPDATE positions SET status='CLOSED', closed_at=$2, exit_reason='ENTRY_NOT_FILLED' WHERE id=$1 AND status='RESERVED'`, [a.positionId, a.at]);
    }
  }
}

export async function createExitIntent(
  c: Client,
  x: { sessionId: string; positionId: string; mint: string; qtyRaw: bigint; kind: "EXIT_NORMAL" | "EXIT_EMERGENCY"; exitReason: string; idempotencyKey: string; decision: unknown; inputs: unknown; at: Date },
): Promise<string> {
  const session = await lockSession(c, x.sessionId);
  const existing = await c.query<{ id: string }>(`SELECT id FROM trade_intents WHERE session_id=$1 AND idempotency_key=$2`, [x.sessionId, x.idempotencyKey]);
  if (existing.rows[0]) return existing.rows[0].id;
  const id = newId("int");
  await c.query(
    `INSERT INTO trade_intents (id, session_id, strategy_version, config_hash, idempotency_key, side, kind, mint, input_mint, output_mint, amount_raw, position_id, exit_reason, risk_decision, inputs, created_at)
     VALUES ($1,$2,$3,$4,$5,'SELL',$6,$7,$7,$8,$9,$10,$11,$12,$13,$14)`,
    [id, x.sessionId, session.strategy_version, session.config_hash, x.idempotencyKey, x.kind, x.mint, USDC_MINT, x.qtyRaw.toString(), x.positionId, x.exitReason, json(x.decision), json(x.inputs), x.at],
  );
  await c.query(`UPDATE positions SET status='EXITING', exit_reason=$2 WHERE id=$1 AND status IN ('OPEN','EXITING')`, [x.positionId, x.exitReason]);
  return id;
}

export interface BookSellFill {
  sessionId: string;
  intentId: string;
  attemptId: string;
  positionId: string;
  fillId: string;
  tokenMint: string;
  tokenInRaw: bigint;
  usdcOutRaw: bigint;
  minOutRaw: bigint;
  fees: FeeItem[];
  usdcUsd: Dec;
  solUsd: Dec;
  executionFidelity: string;
  closeAccount: { rentLamports: bigint; closeFees: FeeItem[] } | null;
  exitReason: string;
  realizedPnlUsd: Dec;
  at: Date;
  outcome: unknown;
}

/** Books a paper sell exactly once and closes the position (v1: whole position exits together). */
export async function bookSellFill(c: Client, f: BookSellFill): Promise<"BOOKED" | "ALREADY_BOOKED"> {
  await lockSession(c, f.sessionId);
  const ins = await c.query(
    `INSERT INTO fills (id, attempt_id, position_id, side, in_mint, in_amount_raw, out_mint, out_amount_raw, min_out_raw, usdc_usd, sol_usd, execution_fidelity, filled_at)
     VALUES ($1,$2,$3,'SELL',$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (attempt_id) DO NOTHING`,
    [f.fillId, f.attemptId, f.positionId, f.tokenMint, f.tokenInRaw.toString(), USDC_MINT, f.usdcOutRaw.toString(), f.minOutRaw.toString(), f.usdcUsd.toString(), f.solUsd.toString(), f.executionFidelity, f.at],
  );
  if (ins.rowCount === 0) return "ALREADY_BOOKED";
  const allFees = [...f.fees, ...(f.closeAccount?.closeFees ?? [])];
  for (const fee of allFees) {
    await c.query(
      `INSERT INTO fee_items (attempt_id, fill_id, kind, asset, amount_raw, usd_fx, source, included_in_quote, is_estimate) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [f.attemptId, f.fillId, fee.kind, fee.asset, fee.amountRaw.toString(), fee.usdFx?.toString() ?? null, fee.source, fee.includedInQuote, fee.isEstimate],
    );
  }
  await insertLedgerTx(
    c,
    sellFillTx(
      { id: newId("ltx"), sessionId: f.sessionId, idempotencyKey: `fill:${f.attemptId}`, at: f.at, refs: { fill_id: f.fillId, intent_id: f.intentId } },
      { tokenMint: f.tokenMint, tokenInRaw: f.tokenInRaw, usdcOutRaw: f.usdcOutRaw, fees: f.fees },
    ),
  );
  if (f.closeAccount && f.closeAccount.rentLamports > 0n) {
    await insertLedgerTx(
      c,
      rentRecoveryTx({ id: newId("ltx"), sessionId: f.sessionId, idempotencyKey: `rent:${f.positionId}`, at: f.at, refs: { position_id: f.positionId } }, f.closeAccount.rentLamports, f.closeAccount.closeFees),
    );
  }
  await c.query(`UPDATE order_attempts SET state='PAPER_FILLED', outcome=$2, finished_at=$3 WHERE id=$1`, [f.attemptId, json(f.outcome), f.at]);
  await c.query(
    `UPDATE positions SET status='CLOSED', qty_raw=0, closed_at=$2, exit_reason=$3, realized_pnl_usd=$4, rent_lamports=0 WHERE id=$1`,
    [f.positionId, f.at, f.exitReason, f.realizedPnlUsd.toString()],
  );
  return "BOOKED";
}

export async function resolveFailedExit(
  c: Client,
  a: { sessionId: string; attemptId: string; positionId: string; state: "FAILED_PAPER" | "CANCELLED_BEFORE_SEND"; chargedFees: FeeItem[]; reasonCode: string; outcome: unknown; at: Date; nextExitAt: Date; valuationStatus: string | null },
): Promise<void> {
  await lockSession(c, a.sessionId);
  for (const fee of a.chargedFees) {
    await c.query(
      `INSERT INTO fee_items (attempt_id, kind, asset, amount_raw, usd_fx, source, included_in_quote, is_estimate) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [a.attemptId, fee.kind, fee.asset, fee.amountRaw.toString(), fee.usdFx?.toString() ?? null, fee.source, fee.includedInQuote, fee.isEstimate],
    );
  }
  if (a.chargedFees.length > 0) {
    await insertLedgerTx(c, failedAttemptFeeTx({ id: newId("ltx"), sessionId: a.sessionId, idempotencyKey: `failfee:${a.attemptId}`, at: a.at }, a.chargedFees, Bucket.WALLET));
  }
  await c.query(`UPDATE order_attempts SET state=$2, outcome=$3, reason_code=$4, finished_at=$5 WHERE id=$1`, [a.attemptId, a.state, json(a.outcome), a.reasonCode, a.at]);
  await c.query(
    `UPDATE positions SET exit_attempts = exit_attempts + 1, next_exit_at=$2, valuation_status=COALESCE($3, valuation_status) WHERE id=$1`,
    [a.positionId, a.nextExitAt, a.valuationStatus],
  );
}
