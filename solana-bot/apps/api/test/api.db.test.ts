import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { D, FakeClock } from "@solbot/domain";
import { parseConfig } from "@solbot/config";
import type { Pool } from "@solbot/db";
import { buildApp, type ReadinessCheck } from "../src/index.ts";
import { freshDb } from "../../../packages/db/test/helpers.ts";

let pool: Pool;
beforeAll(async () => {
  pool = await freshDb();
});
afterAll(async () => {
  await pool.end();
});

const TOKEN = "owner-token-for-tests-only";
const AUTH = { authorization: `Bearer ${TOKEN}` };

function app(opts: { readiness?: ReadinessCheck[]; bootstrapReady?: boolean } = {}) {
  const clock = new FakeClock("2026-10-01T12:00:00Z");
  return {
    clock,
    app: buildApp({
      pool,
      clock,
      cfg: parseConfig(),
      ownerToken: TOKEN,
      heliusWebhookAuth: "hook-secret",
      allowedOrigins: ["http://localhost:3000"],
      codeVersion: "test",
      strategy: { name: "confluence_v1", version: "1.0.0", codeHash: "test" },
      readiness: async () => opts.readiness ?? [{ name: "all", ok: true, detail: "test" }],
      bootstrap: async () => (opts.bootstrapReady ?? false ? { ready: true, missing: [] } : { ready: false, missing: ["qualified wallets 0/20 (historical USD prices unavailable)"] }),
      startFx: async () => ({ usdcUsd: new D(1), solUsd: new D(150) }),
    }),
  };
}

describe("owner API", () => {
  it("requires the owner token for every /api route and exposes no sign/withdraw", async () => {
    const { app: a } = app();
    expect((await a.inject({ method: "GET", url: "/api/dashboard" })).statusCode).toBe(401);
    expect((await a.inject({ method: "GET", url: "/api/dashboard", headers: { authorization: "Bearer wrong" } })).statusCode).toBe(401);
    expect((await a.inject({ method: "GET", url: "/api/dashboard", headers: AUTH })).statusCode).toBe(200);
    for (const url of ["/api/sign", "/api/withdraw"]) {
      expect((await a.inject({ method: "POST", url, headers: AUTH, payload: {} })).statusCode).toBe(404);
    }
    expect((await a.inject({ method: "GET", url: "/health/live" })).statusCode).toBe(200);
  });

  it("refuses LIVE modes and foreign origins", async () => {
    const { app: a } = app();
    expect((await a.inject({ method: "POST", url: "/api/sessions", headers: AUTH, payload: { kind: "CONFLUENCE", mode: "LIVE" } })).statusCode).toBe(400);
    expect((await a.inject({ method: "POST", url: "/api/sessions", headers: { ...AUTH, origin: "https://evil.example" }, payload: {} })).statusCode).toBe(403);
  });

  it("CONFLUENCE without qualified wallets stays INSUFFICIENT_DATA and cannot start", async () => {
    const { app: a } = app();
    const created = (await a.inject({ method: "POST", url: "/api/sessions", headers: AUTH, payload: { kind: "CONFLUENCE", mode: "PAPER" } })).json();
    const v = (await a.inject({ method: "POST", url: `/api/sessions/${created.id}/validate`, headers: AUTH })).json();
    expect(v.state).toBe("INSUFFICIENT_DATA");
    expect(v.missing[0]).toMatch(/qualified wallets/);
    const st = await a.inject({ method: "POST", url: `/api/sessions/${created.id}/start`, headers: AUTH });
    expect(st.statusCode).toBe(409);
  });

  it("start requires passing readiness checks; then records T0 and T_end = T0 + 168 h", async () => {
    const failing = app({ readiness: [{ name: "30 min healthy data", ok: false, detail: "12 min" }] });
    const id1 = (await failing.app.inject({ method: "POST", url: "/api/sessions", headers: AUTH, payload: { kind: "INFRA_TEST", mode: "PAPER" } })).json().id;
    await failing.app.inject({ method: "POST", url: `/api/sessions/${id1}/validate`, headers: AUTH });
    expect((await failing.app.inject({ method: "POST", url: `/api/sessions/${id1}/start`, headers: AUTH })).statusCode).toBe(412);

    const ok = app();
    const id = (await ok.app.inject({ method: "POST", url: "/api/sessions", headers: AUTH, payload: { kind: "INFRA_TEST", mode: "PAPER" } })).json().id;
    await ok.app.inject({ method: "POST", url: `/api/sessions/${id}/validate`, headers: AUTH });
    const r = (await ok.app.inject({ method: "POST", url: `/api/sessions/${id}/start`, headers: AUTH })).json();
    expect(r.state).toBe("RUNNING");
    expect(new Date(r.tEnd).getTime() - new Date(r.t0).getTime()).toBe(168 * 3_600_000);
    expect(r.tEndWarsaw).toBeTruthy();
    const audit = (await pool.query(`SELECT action FROM audit_events WHERE session_id=$1 ORDER BY at`, [id])).rows.map((x) => x.action);
    expect(audit).toEqual(expect.arrayContaining(["CREATE_SESSION", "VALIDATE", "START_7_DAYS"]));

    // pause and flatten are audited and distinct
    await ok.app.inject({ method: "POST", url: `/api/sessions/${id}/pause-entries`, headers: AUTH });
    let s = (await ok.app.inject({ method: "GET", url: `/api/sessions/${id}`, headers: AUTH })).json().session;
    expect(s.entries_paused_by_owner).toBe(true);
    expect(s.state).toBe("RUNNING");
    await ok.app.inject({ method: "POST", url: `/api/sessions/${id}/request-flatten`, headers: AUTH });
    s = (await ok.app.inject({ method: "GET", url: `/api/sessions/${id}`, headers: AUTH })).json().session;
    expect(s.state).toBe("EXIT_ONLY");
    expect(s.intervention).toBe(true);

    const md = await ok.app.inject({ method: "GET", url: `/api/sessions/${id}/report?format=md`, headers: AUTH });
    expect(md.body).toContain("INFRA_TEST — raport nie ocenia skuteczności confluence");
    const html = await ok.app.inject({ method: "GET", url: `/api/sessions/${id}/report?format=html`, headers: AUTH });
    expect(html.headers["content-security-policy"]).toContain("default-src 'none'");
  });

  it("webhook: authenticated, durable, deduplicated", async () => {
    const { app: a } = app();
    const tx = { signature: "5sigWebhook", slot: 1, timestamp: 1_790_000_000, type: "SWAP" };
    expect((await a.inject({ method: "POST", url: "/api/webhooks/helius", payload: [tx] })).statusCode).toBe(401);
    const r1 = (await a.inject({ method: "POST", url: "/api/webhooks/helius", headers: { authorization: "hook-secret" }, payload: [tx] })).json();
    const r2 = (await a.inject({ method: "POST", url: "/api/webhooks/helius", headers: { authorization: "hook-secret" }, payload: [tx] })).json();
    expect(r1.inserted).toBe(1);
    expect(r2.inserted).toBe(0);
    const n = (await pool.query(`SELECT count(*)::int AS n FROM raw_events WHERE source_event_id='5sigWebhook'`)).rows[0].n;
    expect(n).toBe(1);
  });

  it("rate limits control requests", async () => {
    const { app: a } = app();
    let last = 0;
    for (let i = 0; i < 22; i++) last = (await a.inject({ method: "POST", url: "/api/sessions/none/pause-entries", headers: AUTH })).statusCode;
    expect(last).toBe(429);
  });
});
