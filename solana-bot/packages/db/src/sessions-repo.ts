import {
  HOUR_MS,
  SessionState,
  assertSessionTransition,
  newId,
  sha256Hex,
  type Mode,
  type SessionKind,
} from "@solbot/domain";
import { openingTx, type OpeningAllocation } from "@solbot/ledger";
import { insertLedgerTx } from "./ledger-repo.ts";
import { json, type Client, type Pool } from "./pool.ts";

export interface SessionRow {
  id: string;
  kind: SessionKind;
  mode: Mode;
  state: SessionState;
  strategy_name: string;
  strategy_version: string;
  config_hash: string;
  t0: Date | null;
  t_end: Date | null;
  entries_paused_by_owner: boolean;
  intervention: boolean;
  flatten_requested: boolean;
  day_start_equity: Record<string, string>;
  peak_equity_usd: string | null;
  t_end_snapshot: unknown;
  last_tick_at: Date | null;
}

export async function createSession(
  pool: Pool,
  s: { kind: SessionKind; mode: Mode; strategyName: string; strategyVersion: string; strategyCodeHash: string; config: unknown; configHash: string },
): Promise<string> {
  const id = newId("ses");
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    await c.query(`INSERT INTO strategy_versions (name, version, code_hash) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, [
      s.strategyName,
      s.strategyVersion,
      s.strategyCodeHash,
    ]);
    await c.query(`INSERT INTO config_snapshots (config_hash, config) VALUES ($1,$2) ON CONFLICT DO NOTHING`, [s.configHash, json(s.config)]);
    await c.query(
      `INSERT INTO sessions (id, kind, mode, state, strategy_name, strategy_version, config_hash) VALUES ($1,$2,$3,'DRAFT',$4,$5,$6)`,
      [id, s.kind, s.mode, s.strategyName, s.strategyVersion, s.configHash],
    );
    await c.query("COMMIT");
  } catch (e) {
    await c.query("ROLLBACK");
    throw e;
  } finally {
    c.release();
  }
  return id;
}

export async function lockSession(c: Client, id: string): Promise<SessionRow> {
  const r = await c.query<SessionRow>(`SELECT * FROM sessions WHERE id=$1 FOR UPDATE`, [id]);
  const row = r.rows[0];
  if (!row) throw new Error(`session ${id} not found`);
  return row;
}

export async function getSession(pool: Pool | Client, id: string): Promise<SessionRow | null> {
  const r = await pool.query<SessionRow>(`SELECT * FROM sessions WHERE id=$1`, [id]);
  return r.rows[0] ?? null;
}

export async function transitionSession(c: Client, id: string, to: SessionState, at: Date, reasonCode: string | null, detail?: string): Promise<SessionRow> {
  const s = await lockSession(c, id);
  if (s.state === to) return s;
  assertSessionTransition(s.state, to);
  await c.query(`UPDATE sessions SET state=$2 WHERE id=$1`, [id, to]);
  await c.query(`INSERT INTO session_transitions (session_id, from_state, to_state, reason_code, detail, at) VALUES ($1,$2,$3,$4,$5,$6)`, [
    id,
    s.state,
    to,
    reasonCode,
    detail ?? null,
    at,
  ]);
  return { ...s, state: to };
}

/**
 * READY -> RUNNING: records T0 and T_end = T0 + 168 h exactly once and books the opening allocation.
 * Only the owner action calls this; the end of 7 days never enables anything by itself.
 */
export async function startSession(c: Client, id: string, t0: Date, alloc: OpeningAllocation, durationHours: number): Promise<void> {
  const s = await lockSession(c, id);
  if (s.state !== SessionState.READY) throw new Error(`session ${id} is ${s.state}, expected READY`);
  if (s.t0 !== null) throw new Error("session already has T0");
  const tEnd = new Date(t0.getTime() + durationHours * HOUR_MS);
  await c.query(`UPDATE sessions SET t0=$2, t_end=$3 WHERE id=$1`, [id, t0, tEnd]);
  await insertLedgerTx(
    c,
    openingTx({ id: newId("ltx"), sessionId: id, idempotencyKey: "opening", at: t0, refs: { usdc_usd: alloc.usdcUsd.toString(), sol_usd: alloc.solUsd.toString() } }, alloc.usdcRaw, alloc.lamports),
  );
  await transitionSession(c, id, SessionState.RUNNING, t0, null, `T0 ${t0.toISOString()}`);
}

export async function insertRawEvent(
  c: Pool | Client,
  e: {
    provider: string;
    sourceEventId: string;
    legIndex: number;
    owner: string;
    blockTime: Date | null;
    slot: bigint | null;
    commitment: string | null;
    receivedAt: Date;
    availableAt: Date;
    schemaVersion: string;
    payload: unknown;
  },
): Promise<{ inserted: boolean; id: string }> {
  const payloadJson = json(e.payload);
  const id = newId("evt");
  const r = await c.query<{ id: string }>(
    `INSERT INTO raw_events (id, provider, source_event_id, leg_index, owner, block_time, slot, commitment, received_at, available_at, schema_version, raw_payload, raw_payload_hash)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
     ON CONFLICT (network, source_event_id, leg_index, owner) DO NOTHING RETURNING id`,
    [id, e.provider, e.sourceEventId, e.legIndex, e.owner, e.blockTime, e.slot?.toString() ?? null, e.commitment, e.receivedAt, e.availableAt, e.schemaVersion, payloadJson, sha256Hex(payloadJson)],
  );
  if (r.rowCount === 1) return { inserted: true, id };
  const existing = await c.query<{ id: string }>(
    `SELECT id FROM raw_events WHERE network='solana-mainnet' AND source_event_id=$1 AND leg_index=$2 AND owner=$3`,
    [e.sourceEventId, e.legIndex, e.owner],
  );
  return { inserted: false, id: existing.rows[0]!.id };
}
