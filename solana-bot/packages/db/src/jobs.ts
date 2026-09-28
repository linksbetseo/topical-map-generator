import { newId } from "@solbot/domain";
import type { Client, Pool } from "./pool.ts";

/**
 * Postgres job queue with leases and fencing tokens.
 * A worker whose lease expired cannot complete (or write results for) the job:
 * every write must present the fencing token it was given at claim time.
 */

export interface Job {
  id: string;
  kind: string;
  dedupeKey: string;
  payload: unknown;
  fencingToken: bigint;
  retryCount: number;
}

export const JobPriority = { EXIT: 10, RECONCILE: 20, ENTRY: 30, ANALYTICS: 60, DISCOVERY: 80 } as const;

export async function enqueueJob(
  c: Client | Pool,
  job: { kind: string; dedupeKey: string; payload: unknown; priority?: number; runAt: Date; maxRetries?: number },
): Promise<string | null> {
  const id = newId("job");
  const r = await c.query(
    `INSERT INTO jobs (id, kind, dedupe_key, payload, priority, status, next_run_at, max_retries)
     VALUES ($1,$2,$3,$4,$5,'READY',$6,$7) ON CONFLICT (kind, dedupe_key) DO NOTHING`,
    [id, job.kind, job.dedupeKey, JSON.stringify(job.payload), job.priority ?? 100, job.runAt, job.maxRetries ?? 5],
  );
  return r.rowCount === 1 ? id : null;
}

export async function claimJob(pool: Pool, owner: string, now: Date, leaseMs: number, kinds?: string[]): Promise<Job | null> {
  const r = await pool.query<{ id: string; kind: string; dedupe_key: string; payload: unknown; fencing_token: string; retry_count: number }>(
    `UPDATE jobs SET status='LEASED', lease_owner=$1, lease_until=$2::timestamptz + ($3 || ' milliseconds')::interval,
            fencing_token = fencing_token + 1
     WHERE id = (
       SELECT id FROM jobs
       WHERE ((status='READY' AND next_run_at <= $2) OR (status='LEASED' AND lease_until < $2))
         AND ($4::text[] IS NULL OR kind = ANY($4))
       ORDER BY priority, next_run_at
       FOR UPDATE SKIP LOCKED LIMIT 1)
     RETURNING id, kind, dedupe_key, payload, fencing_token, retry_count`,
    [owner, now, String(leaseMs), kinds ?? null],
  );
  const row = r.rows[0];
  if (!row) return null;
  return { id: row.id, kind: row.kind, dedupeKey: row.dedupe_key, payload: row.payload, fencingToken: BigInt(row.fencing_token), retryCount: row.retry_count };
}

/** Verifies (under row lock) that the caller still holds the lease. Use inside the result-writing transaction. */
export async function assertFence(c: Client, jobId: string, fencingToken: bigint, owner: string): Promise<void> {
  const r = await c.query<{ fencing_token: string; lease_owner: string; status: string }>(
    `SELECT fencing_token, lease_owner, status FROM jobs WHERE id=$1 FOR UPDATE`,
    [jobId],
  );
  const row = r.rows[0];
  if (!row || row.status !== "LEASED" || BigInt(row.fencing_token) !== fencingToken || row.lease_owner !== owner) {
    throw new FencingError(jobId);
  }
}

export class FencingError extends Error {
  constructor(jobId: string) {
    super(`stale fencing token for job ${jobId}`);
  }
}

export async function completeJob(c: Client, jobId: string, fencingToken: bigint, owner: string): Promise<void> {
  await assertFence(c, jobId, fencingToken, owner);
  await c.query(`UPDATE jobs SET status='DONE', lease_owner=NULL, lease_until=NULL WHERE id=$1`, [jobId]);
}

export async function failJob(c: Client, jobId: string, fencingToken: bigint, owner: string, now: Date, error: string): Promise<"RETRY" | "DEAD"> {
  await assertFence(c, jobId, fencingToken, owner);
  const r = await c.query<{ status: string }>(
    `UPDATE jobs SET retry_count = retry_count + 1, last_error = $2, lease_owner=NULL, lease_until=NULL,
            status = CASE WHEN retry_count + 1 >= max_retries THEN 'DEAD' ELSE 'READY' END,
            next_run_at = $3::timestamptz + (least(300000, 1000 * power(2, retry_count)) || ' milliseconds')::interval
     WHERE id=$1 RETURNING status`,
    [jobId, error.slice(0, 2000), now],
  );
  return r.rows[0]!.status === "DEAD" ? "DEAD" : "RETRY";
}
