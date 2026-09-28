import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { parseConfig, configHash } from "@solbot/config";
import { createSession, type Pool } from "@solbot/db";
import { readinessChecks } from "../src/index.ts";
import { freshDb } from "../../../packages/db/test/helpers.ts";

let pool: Pool;
beforeAll(async () => {
  pool = await freshDb();
});
afterAll(async () => {
  await pool.end();
});

const cfg = parseConfig();
const now = new Date("2026-10-01T12:00:00Z");
const probes = { endpoints: async () => ({ ok: true, detail: "ok" }), canonicalUsdc: async () => ({ ok: true, detail: "ok" }), marketSource: "LIVE" as const };

async function session(): Promise<string> {
  return createSession(pool, { kind: "INFRA_TEST", mode: "PAPER", strategyName: "confluence_v1", strategyVersion: "1.0.0", strategyCodeHash: "t", config: cfg, configHash: configHash(cfg) });
}

async function fxEvery(seconds: number, skip?: [number, number]) {
  await pool.query(`DELETE FROM fx_snapshots`);
  for (let t = now.getTime() - 30 * 60_000; t <= now.getTime(); t += seconds * 1000) {
    if (skip && t >= skip[0] && t < skip[1]) continue;
    await pool.query(`INSERT INTO fx_snapshots (at, usdc_usd, sol_usd, source) VALUES ($1, 1, 150, 'test')`, [new Date(t)]);
  }
}

describe("readiness gates before 'Rozpocznij 7 dni'", () => {
  it("passes with 30 minutes of healthy data and no secrets", async () => {
    const id = await session();
    await fxEvery(30);
    const checks = await readinessChecks(pool, id, cfg, now, probes, {});
    expect(checks.filter((c) => !c.ok)).toEqual([]);
  });

  it("fails on a data gap, a signer secret, failing endpoints or a DEMO/LIVE mismatch", async () => {
    const id = await session();
    await fxEvery(30, [now.getTime() - 10 * 60_000, now.getTime() - 6 * 60_000]);
    const checks = await readinessChecks(
      pool,
      id,
      cfg,
      now,
      { ...probes, endpoints: async () => ({ ok: false, detail: "HTTP 403" }), marketSource: "FIXTURE" },
      { SIGNER_PRIVATE_KEY: "x" },
    );
    const failed = checks.filter((c) => !c.ok).map((c) => c.name);
    expect(failed).toEqual(
      expect.arrayContaining(["30 min zdrowych danych", "brak sekretów signera / LIVE wyłączony", "wymagane endpointy (read-only)", "źródło danych zgodne z typem sesji"]),
    );
  });
});
