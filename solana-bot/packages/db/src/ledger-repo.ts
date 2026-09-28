import { assertBalanced, Ledger, type LedgerEntry, type LedgerTransaction } from "@solbot/ledger";
import type { Client } from "./pool.ts";

/** Inserts a ledger transaction. Returns false if the idempotency key already exists (no-op). */
export async function insertLedgerTx(c: Client, tx: LedgerTransaction): Promise<boolean> {
  assertBalanced(tx.entries);
  const r = await c.query(
    `INSERT INTO ledger_transactions (id, session_id, idempotency_key, kind, at, refs, memo)
     VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (session_id, idempotency_key) DO NOTHING`,
    [tx.id, tx.sessionId, tx.idempotencyKey, tx.kind, tx.at, tx.refs ?? {}, tx.memo ?? null],
  );
  if (r.rowCount === 0) return false;
  for (const e of tx.entries) {
    await c.query(`INSERT INTO ledger_entries (tx_id, session_id, bucket, asset, amount_raw) VALUES ($1,$2,$3,$4,$5)`, [
      tx.id,
      tx.sessionId,
      e.bucket,
      e.asset,
      e.amountRaw.toString(),
    ]);
  }
  return true;
}

export async function loadLedger(c: Client | { query: Client["query"] }, sessionId: string): Promise<Ledger> {
  const txs = await c.query<{ id: string; idempotency_key: string; kind: LedgerTransaction["kind"]; at: Date; refs: Record<string, string>; memo: string | null }>(
    `SELECT id, idempotency_key, kind, at, refs, memo FROM ledger_transactions WHERE session_id = $1 ORDER BY at, id`,
    [sessionId],
  );
  const entries = await c.query<{ tx_id: string; bucket: string; asset: string; amount_raw: string }>(
    `SELECT tx_id, bucket, asset, amount_raw FROM ledger_entries WHERE session_id = $1 ORDER BY id`,
    [sessionId],
  );
  const byTx = new Map<string, LedgerEntry[]>();
  for (const e of entries.rows) {
    const list = byTx.get(e.tx_id) ?? [];
    list.push({ bucket: e.bucket as LedgerEntry["bucket"], asset: e.asset, amountRaw: BigInt(e.amount_raw) });
    byTx.set(e.tx_id, list);
  }
  return Ledger.replay(
    sessionId,
    txs.rows.map((t) => ({
      id: t.id,
      sessionId,
      idempotencyKey: t.idempotency_key,
      kind: t.kind,
      at: t.at,
      entries: byTx.get(t.id) ?? [],
      refs: t.refs,
      ...(t.memo ? { memo: t.memo } : {}),
    })),
  );
}

/** Balance of a bucket/asset computed in SQL (used inside locked transactions). */
export async function sqlBalance(c: Client, sessionId: string, bucket: string, asset: string): Promise<bigint> {
  const r = await c.query<{ s: string | null }>(
    `SELECT sum(amount_raw)::text AS s FROM ledger_entries WHERE session_id=$1 AND bucket=$2 AND asset=$3`,
    [sessionId, bucket, asset],
  );
  return BigInt(r.rows[0]?.s ?? "0");
}
