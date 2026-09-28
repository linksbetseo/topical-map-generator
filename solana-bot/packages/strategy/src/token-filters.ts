import { D, HOUR_MS, MINUTE_MS, ReasonCode, reason, type Dec, type Reason } from "@solbot/domain";
import type { Config } from "@solbot/config";

/** Provider-independent token view used by the filters (brief §5). */
export interface TokenView {
  mint: string;
  firstPoolId: string | null;
  firstPoolCreatedAt: Date | null;
  liquidityUsd: Dec | null;
  volume5mUsd: Dec | null;
  sells5m: number | null;
  priceChange5mPct: Dec | null;
  launchpad: string | null;
  graduatedAt: Date | null;
  /** When this view became available in our DB. */
  availableAt: Date;
}

export interface MintRiskView {
  passed: boolean;
  reasons: Reason[];
  availableAt: Date;
}

export interface HolderView {
  ok: boolean;
  reasons: Reason[];
  holderCount: number;
  top10Bps: number | null;
  largestBps: number | null;
  availableAt: Date;
}

export interface FilterResult {
  passed: boolean;
  reasons: Reason[];
  /** Every filter with observed and required values (shown to the owner). */
  checks: Array<{ filter: string; passed: boolean; observed: string; required: string }>;
}

export function evaluateTokenFilters(now: Date, t: TokenView | null, risk: MintRiskView | null, holders: HolderView | null, cfg: Config): FilterResult {
  const reasons: Reason[] = [];
  const checks: FilterResult["checks"] = [];
  const u = cfg.universe;
  const f = cfg.freshness;
  const add = (filter: string, passed: boolean, observed: string, required: string, code: Reason["code"]) => {
    checks.push({ filter, passed, observed, required });
    if (!passed) reasons.push(reason(code, filter, { observed, required }));
  };

  if (!t) {
    reasons.push(reason(ReasonCode.DATA_REQUIREMENT_NOT_MET, "no market metadata"));
    return { passed: false, reasons, checks };
  }
  const age = (x: Date) => now.getTime() - x.getTime();
  if (t.availableAt > now) throw new Error("look-ahead: token view from the future");
  add("market_metadata_fresh", age(t.availableAt) <= f.market_metadata_ms, `${age(t.availableAt)} ms`, `<= ${f.market_metadata_ms} ms`, ReasonCode.DATA_STALE);

  if (t.launchpad !== null && t.graduatedAt === null) {
    add("post_migration", false, `launchpad ${t.launchpad}, not graduated`, "graduated pool", ReasonCode.PRE_MIGRATION_BONDING_CURVE);
  }
  if (!t.firstPoolId || !t.firstPoolCreatedAt) {
    add("recognized_pool", false, "none", "first pool known", ReasonCode.DATA_REQUIREMENT_NOT_MET);
  } else {
    const ageMs = age(t.firstPoolCreatedAt);
    add(
      "pool_age",
      ageMs >= u.min_pool_age_minutes * MINUTE_MS && ageMs <= u.max_pool_age_hours * HOUR_MS,
      `${(ageMs / MINUTE_MS).toFixed(1)} min`,
      `${u.min_pool_age_minutes} min .. ${u.max_pool_age_hours} h`,
      ReasonCode.TOKEN_AGE_OUT_OF_RANGE,
    );
  }
  const need = (name: string, v: Dec | number | null, min: Dec | number, code: Reason["code"]) => {
    if (v === null) add(name, false, "unknown", `>= ${min}`, ReasonCode.DATA_REQUIREMENT_NOT_MET);
    else add(name, new D(v).gte(min), String(v), `>= ${min}`, code);
  };
  need("liquidity_usd", t.liquidityUsd, new D(u.min_liquidity_usd), ReasonCode.LIQUIDITY_TOO_LOW);
  need("volume_5m_usd", t.volume5mUsd, new D(u.min_volume_5m_usd), ReasonCode.VOLUME_5M_TOO_LOW);
  need("sells_5m (provider numSells)", t.sells5m, u.min_sells_5m, ReasonCode.SELLS_5M_TOO_FEW);
  if (t.priceChange5mPct === null) add("price_change_5m", false, "unknown", `<= ${u.max_price_change_5m_bps} bps`, ReasonCode.DATA_REQUIREMENT_NOT_MET);
  else {
    const bps = t.priceChange5mPct.mul(100);
    add("price_change_5m", bps.lte(u.max_price_change_5m_bps), `${bps.toFixed(0)} bps`, `<= ${u.max_price_change_5m_bps} bps`, ReasonCode.PRICE_PUMP_5M_TOO_HIGH);
  }

  if (!risk) add("mint_checks", false, "not checked", "checked", ReasonCode.DATA_REQUIREMENT_NOT_MET);
  else {
    if (risk.availableAt > now) throw new Error("look-ahead: mint check from the future");
    add("mint_checks_fresh", age(risk.availableAt) <= f.mint_risk_ms, `${age(risk.availableAt)} ms`, `<= ${f.mint_risk_ms} ms`, ReasonCode.DATA_STALE);
    if (!risk.passed) {
      for (const r of risk.reasons) reasons.push(r);
      checks.push({ filter: "mint_checks", passed: false, observed: risk.reasons.map((r) => r.code).join(","), required: "no listed restrictions" });
    } else checks.push({ filter: "mint_checks", passed: true, observed: "no listed restrictions detected", required: "no listed restrictions" });
  }

  if (!holders || !holders.ok) {
    add("holders", false, holders ? holders.reasons.map((r) => r.detail ?? r.code).join(",") : "not checked", "owner-consolidated distribution", ReasonCode.HOLDER_DATA_UNAVAILABLE);
  } else {
    if (holders.availableAt > now) throw new Error("look-ahead: holder snapshot from the future");
    add("holders_fresh", age(holders.availableAt) <= f.holder_snapshot_ms, `${age(holders.availableAt)} ms`, `<= ${f.holder_snapshot_ms} ms`, ReasonCode.DATA_STALE);
    add("holder_count", holders.holderCount >= u.min_holders, String(holders.holderCount), `>= ${u.min_holders}`, ReasonCode.HOLDERS_TOO_FEW);
    add("top10_bps", holders.top10Bps !== null && holders.top10Bps <= u.max_top10_holders_bps, String(holders.top10Bps), `<= ${u.max_top10_holders_bps}`, ReasonCode.TOP10_CONCENTRATION_TOO_HIGH);
    add("largest_bps", holders.largestBps !== null && holders.largestBps <= u.max_largest_holder_bps, String(holders.largestBps), `<= ${u.max_largest_holder_bps}`, ReasonCode.LARGEST_HOLDER_TOO_HIGH);
  }
  return { passed: reasons.length === 0, reasons, checks };
}
