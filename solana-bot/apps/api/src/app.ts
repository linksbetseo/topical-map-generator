import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from "fastify";
import { createHash, timingSafeEqual } from "node:crypto";
import { D, SessionState, formatWarsaw, type Clock, type Dec } from "@solbot/domain";
import { configHash, type Config } from "@solbot/config";
import { openingAllocation } from "@solbot/ledger";
import { createSession, getSession, insertRawEvent, json, startSession, transitionSession, withTx, type Pool } from "@solbot/db";
import { buildReport, toCsv, toHtml, toMarkdown } from "@solbot/reporting";

/**
 * Owner API. Every /api route except the provider webhook requires the owner bearer token.
 * Auth is header-only (no cookies), so there is no ambient credential for CSRF; POSTs from a
 * foreign Origin are refused anyway. There is intentionally no /api/sign or /api/withdraw.
 */

export interface ReadinessCheck {
  name: string;
  ok: boolean;
  detail: string;
}

export interface AppDeps {
  pool: Pool;
  clock: Clock;
  cfg: Config;
  ownerToken: string | null;
  heliusWebhookAuth: string | null;
  allowedOrigins: readonly string[];
  codeVersion: string;
  strategy: { name: string; version: string; codeHash: string };
  /** Brief §12: healthy data, endpoints tested, FX verified, ledger balanced, no live creds. */
  readiness: (sessionId: string) => Promise<ReadinessCheck[]>;
  /** Wallet bootstrap result for CONFLUENCE sessions (qualified count and missing data). */
  bootstrap: (sessionId: string) => Promise<{ ready: boolean; missing: string[] }>;
  startFx: () => Promise<{ usdcUsd: Dec | null; solUsd: Dec | null }>;
}

const sha = (s: string) => createHash("sha256").update(s).digest();
function safeEqual(a: string, b: string): boolean {
  return timingSafeEqual(sha(a), sha(b));
}

export function buildApp(d: AppDeps): FastifyInstance {
  const app = Fastify({ logger: false, bodyLimit: 2 * 1024 * 1024 });
  const hits = new Map<string, number[]>();

  app.addHook("onRequest", async (req: FastifyRequest, reply: FastifyReply) => {
    const url = req.url.split("?")[0]!;
    if (!url.startsWith("/api/") || url === "/api/webhooks/helius") return;
    if (!d.ownerToken) return reply.code(503).send({ error: "owner auth not configured" });
    const h = req.headers.authorization ?? "";
    if (!h.startsWith("Bearer ") || !safeEqual(h.slice(7), d.ownerToken)) return reply.code(401).send({ error: "unauthorized" });
    if (req.method !== "GET") {
      const origin = req.headers.origin;
      if (origin && !d.allowedOrigins.includes(origin)) return reply.code(403).send({ error: "origin not allowed" });
      const now = d.clock.now().getTime();
      const list = (hits.get(req.ip) ?? []).filter((t) => t > now - 60_000);
      if (list.length >= 20) return reply.code(429).send({ error: "rate limited" });
      list.push(now);
      hits.set(req.ip, list);
    }
  });

  const audit = async (sessionId: string | null, action: string, data: unknown) =>
    d.pool.query(`INSERT INTO audit_events (session_id, actor, action, data, at) VALUES ($1,'owner',$2,$3,$4)`, [sessionId, action, json(data), d.clock.now()]);

  // ------------------------------------------------------------------ health
  app.get("/health/live", async () => ({ ok: true }));
  app.get("/health/ready", async (_req, reply) => {
    try {
      await d.pool.query("SELECT 1");
      const hb = await d.pool.query<{ last_beat_at: Date }>(`SELECT max(last_beat_at) AS last_beat_at FROM heartbeats`);
      const last = hb.rows[0]?.last_beat_at ?? null;
      const workerOk = last !== null && d.clock.now().getTime() - new Date(last).getTime() < 30_000;
      return reply.code(workerOk ? 200 : 503).send({ db: true, worker: workerOk, lastHeartbeat: last });
    } catch {
      return reply.code(503).send({ db: false });
    }
  });

  // ------------------------------------------------------------------ read endpoints
  app.get("/api/dashboard", async () => {
    const s = (await d.pool.query(`SELECT * FROM sessions ORDER BY created_at DESC LIMIT 1`)).rows[0] ?? null;
    if (!s) return { session: null, message: "brak danych" };
    const eq = (await d.pool.query(`SELECT * FROM equity_snapshots WHERE session_id=$1 ORDER BY at DESC LIMIT 1`, [s.id])).rows[0] ?? null;
    const bench = (await d.pool.query(`SELECT * FROM benchmark_snapshots WHERE session_id=$1 ORDER BY at DESC LIMIT 1`, [s.id])).rows[0] ?? null;
    const open = (await d.pool.query(`SELECT id, mint, status, qty_raw, cost_usd, valuation_status, last_mark_usd, last_mark_at FROM positions WHERE session_id=$1 AND status <> 'CLOSED'`, [s.id])).rows;
    const now = d.clock.now();
    return {
      banner: s.mode, // always visible: PAPER / DEMO
      session: { id: s.id, kind: s.kind, state: s.state, t0: s.t0, tEnd: s.t_end, tEndWarsaw: s.t_end ? formatWarsaw(new Date(s.t_end)) : null, remainingMs: s.t_end ? Math.max(0, new Date(s.t_end).getTime() - now.getTime()) : null, entriesPaused: s.entries_paused_by_owner },
      startCapitalUsd: d.cfg.capital.initial_total_usd,
      equity: eq ? { totalLowerBoundUsd: eq.equity_total_lower_bound_usd, liquidLowerBoundUsd: eq.equity_liquid_lower_bound_usd, totalFreshUsd: eq.equity_total_fresh_usd, at: eq.at, uncertain: eq.uncertain } : "brak danych",
      benchmarks: bench ?? "brak danych",
      openPositions: open,
    };
  });
  app.get<{ Params: { id: string } }>("/api/sessions/:id", async (req, reply) => {
    const s = await getSession(d.pool, req.params.id);
    if (!s) return reply.code(404).send({ error: "not found" });
    const tr = (await d.pool.query(`SELECT * FROM session_transitions WHERE session_id=$1 ORDER BY at`, [s.id])).rows;
    return { session: s, transitions: tr };
  });
  app.get<{ Querystring: { session?: string } }>("/api/signals", async (req) => (await d.pool.query(`SELECT * FROM signals WHERE ($1::text IS NULL OR session_id=$1) ORDER BY first_detected_at DESC LIMIT 200`, [req.query.session ?? null])).rows);
  app.get<{ Querystring: { session?: string } }>("/api/positions", async (req) => (await d.pool.query(`SELECT * FROM positions WHERE ($1::text IS NULL OR session_id=$1) ORDER BY id DESC LIMIT 200`, [req.query.session ?? null])).rows);
  app.get<{ Params: { id: string } }>("/api/orders/:id", async (req, reply) => {
    const intent = (await d.pool.query(`SELECT * FROM trade_intents WHERE id=$1`, [req.params.id])).rows[0];
    if (!intent) return reply.code(404).send({ error: "not found" });
    const attempts = (await d.pool.query(`SELECT * FROM order_attempts WHERE intent_id=$1 ORDER BY attempt_no`, [intent.id])).rows;
    const quotes = (await d.pool.query(`SELECT id, attempt_id, role, ok, failure_code, out_amount_net_raw, price_impact_bps, router, requested_at, received_at FROM quotes WHERE attempt_id = ANY($1)`, [attempts.map((a) => a.id)])).rows;
    const fills = (await d.pool.query(`SELECT * FROM fills WHERE attempt_id = ANY($1)`, [attempts.map((a) => a.id)])).rows;
    const fees = (await d.pool.query(`SELECT * FROM fee_items WHERE attempt_id = ANY($1)`, [attempts.map((a) => a.id)])).rows;
    return { intent, attempts, quotes, fills, fees };
  });
  app.get("/api/wallets", async () => (await d.pool.query(`SELECT * FROM wallet_qualification ORDER BY computed_at DESC LIMIT 500`)).rows);
  app.get<{ Params: { mint: string } }>("/api/tokens/:mint", async (req) => ({
    token: (await d.pool.query(`SELECT * FROM tokens WHERE mint=$1`, [req.params.mint])).rows[0] ?? null,
    riskChecks: (await d.pool.query(`SELECT * FROM token_risk_checks WHERE mint=$1 ORDER BY checked_at DESC LIMIT 20`, [req.params.mint])).rows,
    rejections: (await d.pool.query(`SELECT * FROM rejection_reasons WHERE mint=$1 ORDER BY at DESC LIMIT 100`, [req.params.mint])).rows,
  }));
  app.get("/api/provider-usage", async () => (await d.pool.query(`SELECT * FROM provider_usage ORDER BY minute DESC LIMIT 500`)).rows);
  app.get("/api/audit", async () => (await d.pool.query(`SELECT * FROM audit_events ORDER BY at DESC LIMIT 500`)).rows);
  app.get<{ Params: { id: string }; Querystring: { format?: string } }>("/api/sessions/:id/report", async (req, reply) => {
    const r = await buildReport(d.pool, req.params.id, d.cfg, { codeVersion: d.codeVersion, now: d.clock.now() });
    const f = req.query.format ?? "json";
    if (f === "md") return reply.type("text/markdown; charset=utf-8").send(toMarkdown(r));
    if (f === "csv") return reply.type("text/csv; charset=utf-8").send(toCsv(r));
    if (f === "html") return reply.type("text/html; charset=utf-8").header("content-security-policy", "default-src 'none'; style-src 'unsafe-inline'").send(toHtml(r));
    return r;
  });

  // SSE: session state, alerts and equity; polling DB so no extra infrastructure.
  app.get<{ Querystring: { session: string } }>("/api/events", async (req, reply) => {
    reply.raw.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
    let last = new Date(0);
    const push = async () => {
      const alerts = (await d.pool.query(`SELECT * FROM alerts WHERE session_id=$1 AND at > $2 ORDER BY at`, [req.query.session, last])).rows;
      const s = await getSession(d.pool, req.query.session);
      reply.raw.write(`event: state\ndata: ${json({ state: s?.state ?? null })}\n\n`);
      for (const a of alerts) reply.raw.write(`event: alert\ndata: ${json(a)}\n\n`);
      if (alerts.length) last = alerts[alerts.length - 1].at;
    };
    await push();
    const timer = setInterval(() => void push().catch(() => undefined), 2_000);
    req.raw.on("close", () => clearInterval(timer));
  });

  // ------------------------------------------------------------------ control
  app.post<{ Body: { kind?: string; mode?: string } }>("/api/sessions", async (req, reply) => {
    const kind = req.body?.kind ?? "CONFLUENCE";
    const mode = req.body?.mode ?? "PAPER";
    if (!["CONFLUENCE", "INFRA_TEST", "DEMO"].includes(kind) || !["PAPER", "DEMO"].includes(mode) || (kind === "DEMO") !== (mode === "DEMO")) {
      return reply.code(400).send({ error: "kind/mode not allowed (LIVE not available)" });
    }
    const id = await createSession(d.pool, {
      kind: kind as "CONFLUENCE",
      mode: mode as "PAPER",
      strategyName: d.strategy.name,
      strategyVersion: d.strategy.version,
      strategyCodeHash: d.strategy.codeHash,
      config: d.cfg,
      configHash: configHash(d.cfg),
    });
    await audit(id, "CREATE_SESSION", { kind, mode });
    return reply.code(201).send({ id, state: "DRAFT" });
  });

  app.post<{ Params: { id: string } }>("/api/sessions/:id/validate", async (req, reply) => {
    const s = await getSession(d.pool, req.params.id);
    if (!s) return reply.code(404).send({ error: "not found" });
    const now = d.clock.now();
    if (s.state === SessionState.DRAFT) await withTx(d.pool, (c) => transitionSession(c, s.id, SessionState.VALIDATING, now, null));
    const cur = await getSession(d.pool, s.id);
    if (cur!.state === SessionState.VALIDATING) await withTx(d.pool, (c) => transitionSession(c, s.id, SessionState.BOOTSTRAP, now, null));
    const boot = s.kind === "CONFLUENCE" ? await d.bootstrap(s.id) : { ready: true, missing: [] };
    const to = boot.ready ? SessionState.READY : SessionState.INSUFFICIENT_DATA;
    const after = await getSession(d.pool, s.id);
    if (after!.state === SessionState.BOOTSTRAP) await withTx(d.pool, (c) => transitionSession(c, s.id, to, now, boot.ready ? null : "DATA_REQUIREMENT_NOT_MET", boot.missing.join("; ")));
    await audit(s.id, "VALIDATE", boot);
    return { state: (await getSession(d.pool, s.id))!.state, missing: boot.missing };
  });

  app.post<{ Params: { id: string } }>("/api/sessions/:id/start", async (req, reply) => {
    const s = await getSession(d.pool, req.params.id);
    if (!s) return reply.code(404).send({ error: "not found" });
    if (s.state !== SessionState.READY) return reply.code(409).send({ error: `session is ${s.state}, expected READY` });
    const checks = await d.readiness(s.id);
    if (checks.some((c) => !c.ok)) return reply.code(412).send({ error: "readiness checks failed", checks });
    const fx = await d.startFx();
    if (!fx.usdcUsd || !fx.solUsd) return reply.code(412).send({ error: "T0 FX not verified" });
    const t0 = d.clock.now();
    const alloc = openingAllocation(new D(d.cfg.capital.initial_total_usd), new D(d.cfg.capital.initial_sol_value_usd), fx.usdcUsd, fx.solUsd);
    await withTx(d.pool, (c) => startSession(c, s.id, t0, alloc, d.cfg.experiment.duration_hours));
    await audit(s.id, "START_7_DAYS", { t0, usdcUsd: fx.usdcUsd.toString(), solUsd: fx.solUsd.toString(), checks });
    const started = await getSession(d.pool, s.id);
    return { state: started!.state, t0: started!.t0, tEnd: started!.t_end, tEndWarsaw: formatWarsaw(new Date(started!.t_end!)) };
  });

  app.post<{ Params: { id: string } }>("/api/sessions/:id/pause-entries", async (req, reply) => {
    const r = await d.pool.query(`UPDATE sessions SET entries_paused_by_owner=true WHERE id=$1 RETURNING id`, [req.params.id]);
    if (!r.rowCount) return reply.code(404).send({ error: "not found" });
    await audit(req.params.id, "PAUSE_ENTRIES", {});
    return { entriesPaused: true };
  });

  app.post<{ Params: { id: string } }>("/api/sessions/:id/resume-entries", async (req, reply) => {
    const r = await d.pool.query(`UPDATE sessions SET entries_paused_by_owner=false WHERE id=$1 AND NOT flatten_requested RETURNING id`, [req.params.id]);
    if (!r.rowCount) return reply.code(409).send({ error: "not found or flatten requested" });
    await audit(req.params.id, "RESUME_ENTRIES", {});
    return { entriesPaused: false };
  });

  app.post<{ Params: { id: string } }>("/api/sessions/:id/request-flatten", async (req, reply) => {
    const s = await getSession(d.pool, req.params.id);
    if (!s) return reply.code(404).send({ error: "not found" });
    await d.pool.query(`UPDATE sessions SET flatten_requested=true, entries_paused_by_owner=true, intervention=true WHERE id=$1`, [s.id]);
    if (s.state === SessionState.RUNNING || s.state === SessionState.PAUSED_DATA) {
      await withTx(d.pool, (c) => transitionSession(c, s.id, SessionState.EXIT_ONLY, d.clock.now(), "FLATTEN_REQUESTED"));
    }
    await audit(s.id, "FLATTEN_REQUESTED", { note: "liquidation attempts requested; success not guaranteed" });
    return { flattenRequested: true };
  });

  // ------------------------------------------------------------------ provider webhook (durable ingest, fast 200)
  app.post("/api/webhooks/helius", async (req, reply) => {
    if (!d.heliusWebhookAuth) return reply.code(503).send({ error: "webhook auth not configured" });
    const h = req.headers.authorization ?? "";
    if (!safeEqual(h, d.heliusWebhookAuth)) return reply.code(401).send({ error: "unauthorized" });
    const body = req.body;
    const items = Array.isArray(body) ? body : [body];
    const receivedAt = d.clock.now();
    let inserted = 0;
    for (const tx of items) {
      const sig = tx && typeof tx === "object" && typeof (tx as { signature?: unknown }).signature === "string" ? (tx as { signature: string }).signature : null;
      if (!sig) continue;
      const ts = (tx as { timestamp?: unknown }).timestamp;
      const slot = (tx as { slot?: unknown }).slot;
      const r = await insertRawEvent(d.pool, {
        provider: "helius-webhook",
        sourceEventId: sig,
        legIndex: 0,
        owner: "",
        blockTime: typeof ts === "number" ? new Date(ts * 1000) : null,
        slot: typeof slot === "number" ? BigInt(slot) : null,
        commitment: "confirmed",
        receivedAt,
        availableAt: d.clock.now(),
        schemaVersion: "helius-enhanced-v0 (UNVERIFIED)",
        payload: tx,
      });
      if (r.inserted) inserted++;
    }
    return reply.code(200).send({ received: items.length, inserted });
  });

  return app;
}
