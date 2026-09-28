import { findSignerSecrets, type Config } from "@solbot/config";
import { getSession, loadLedger, type Pool } from "@solbot/db";
import { STRATEGY_VERSION } from "@solbot/strategy";

export interface ReadinessCheck {
  name: string;
  ok: boolean;
  detail: string;
}

/**
 * Pre-start gates (brief §12). The owner still has to press "Rozpocznij 7 dni"; passing these
 * never starts anything by itself and never enables LIVE.
 */
export async function readinessChecks(
  pool: Pool,
  sessionId: string,
  cfg: Config,
  now: Date,
  probes: { endpoints: () => Promise<{ ok: boolean; detail: string }>; canonicalUsdc: () => Promise<{ ok: boolean; detail: string }>; marketSource: "LIVE" | "FIXTURE" },
  env: Record<string, string | undefined> = process.env,
): Promise<ReadinessCheck[]> {
  const checks: ReadinessCheck[] = [];
  const s = await getSession(pool, sessionId);
  if (!s) return [{ name: "session", ok: false, detail: "not found" }];

  const secrets = findSignerSecrets(env);
  checks.push({ name: "brak sekretów signera / LIVE wyłączony", ok: secrets.length === 0, detail: secrets.length ? secrets.join(",") : "ok" });
  checks.push({ name: "wersja strategii", ok: s.strategy_version === STRATEGY_VERSION, detail: `${s.strategy_version} vs kod ${STRATEGY_VERSION}` });
  const dataOk = (s.kind === "DEMO") === (probes.marketSource === "FIXTURE");
  checks.push({ name: "źródło danych zgodne z typem sesji", ok: dataOk, detail: `${s.kind} / ${probes.marketSource}` });

  const window = await pool.query<{ at: Date }>(
    `SELECT at FROM fx_snapshots WHERE at BETWEEN $1 AND $2 AND usdc_usd IS NOT NULL AND sol_usd IS NOT NULL ORDER BY at`,
    [new Date(now.getTime() - 30 * 60_000), now],
  );
  const times = window.rows.map((r) => r.at.getTime());
  let maxGap = times.length ? times[0]! - (now.getTime() - 30 * 60_000) : Infinity;
  for (let i = 1; i < times.length; i++) maxGap = Math.max(maxGap, times[i]! - times[i - 1]!);
  if (times.length) maxGap = Math.max(maxGap, now.getTime() - times[times.length - 1]!);
  checks.push({ name: "30 min zdrowych danych", ok: times.length > 0 && maxGap <= 120_000, detail: `${times.length} snapshotów FX, max luka ${Number.isFinite(maxGap) ? Math.round(maxGap / 1000) : "∞"} s` });
  const last = times.length ? now.getTime() - times[times.length - 1]! : Infinity;
  checks.push({ name: "zweryfikowane kursy startowe", ok: last <= cfg.freshness.fx_ms, detail: Number.isFinite(last) ? `wiek ${Math.round(last / 1000)} s` : "brak" });

  const ep = await probes.endpoints();
  checks.push({ name: "wymagane endpointy (read-only)", ok: ep.ok, detail: ep.detail });
  const usdc = await probes.canonicalUsdc();
  checks.push({ name: "kanoniczny mint USDC", ok: usdc.ok, detail: usdc.detail });

  const c = await pool.connect();
  try {
    const id = (await loadLedger(c, sessionId)).verifyIdentity();
    checks.push({ name: "księga zbilansowana", ok: id.ok, detail: id.ok ? "ok" : id.mismatches.join(",") });
  } finally {
    c.release();
  }
  return checks;
}
