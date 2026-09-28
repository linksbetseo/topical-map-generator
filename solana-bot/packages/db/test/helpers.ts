import { D, SessionState, newId } from "@solbot/domain";
import { configHash, parseConfig } from "@solbot/config";
import { openingAllocation } from "@solbot/ledger";
import { createPool, createSession, migrate, startSession, transitionSession, withTx, type Pool } from "../src/index.ts";

export const TEST_DB_URL = process.env.TEST_DATABASE_URL ?? "postgres://solbot:solbot@localhost:5432/solbot_test";

export async function freshDb(): Promise<Pool> {
  const pool = createPool(TEST_DB_URL, 20);
  await pool.query("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;");
  await migrate(pool);
  return pool;
}

export async function runningSession(pool: Pool, opts: { totalUsd?: string; solUsd?: string; t0?: Date } = {}): Promise<string> {
  const cfg = parseConfig();
  const id = await createSession(pool, {
    kind: "DEMO",
    mode: "DEMO",
    strategyName: "confluence_v1",
    strategyVersion: "1.0.0",
    strategyCodeHash: "test",
    config: cfg,
    configHash: configHash(cfg),
  });
  const t0 = opts.t0 ?? new Date("2026-10-01T12:00:00Z");
  await withTx(pool, async (c) => {
    await transitionSession(c, id, SessionState.VALIDATING, t0, null);
    await transitionSession(c, id, SessionState.BOOTSTRAP, t0, null);
    await transitionSession(c, id, SessionState.READY, t0, null);
    const alloc = openingAllocation(new D(opts.totalUsd ?? "500"), new D("20"), new D(1), new D(opts.solUsd ?? "150"));
    await startSession(c, id, t0, alloc, 168);
  });
  return id;
}

export { newId };
