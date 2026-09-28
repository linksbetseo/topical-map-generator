import { z } from "zod";
import { USDC_MINT, WSOL_MINT, canonicalJson, sha256Hex } from "@solbot/domain";

/**
 * All thresholds of the experiment. Defaults = brief v1.0. Units are in the key names.
 * Any change that affects decisions produces a new config_hash and requires a new session.
 */
const usdString = z.string().regex(/^[0-9]+(\.[0-9]+)?$/, "USD amount as decimal string");
const bpsInt = z.number().int().min(0).max(10_000);

export const ConfigSchema = z.object({
  experiment: z.object({
    strategy: z.literal("confluence_v1").default("confluence_v1"),
    strategy_version: z.string().default("1.0.0"),
    execution_profile: z.literal("jupiter_order_manual_v1").default("jupiter_order_manual_v1"),
    duration_hours: z.number().int().positive().default(168),
    entry_cutoff_before_end_hours: z.number().int().nonnegative().default(4),
    settlement_max_minutes: z.number().int().positive().default(30),
  }).prefault({}),

  capital: z.object({
    initial_total_usd: usdString.default("500"),
    initial_sol_value_usd: usdString.default("20"),
    usdc_mint: z.string().default(USDC_MINT),
    wsol_mint: z.string().default(WSOL_MINT),
  }).prefault({}),

  universe: z.object({
    min_pool_age_minutes: z.number().int().nonnegative().default(10),
    max_pool_age_hours: z.number().int().positive().default(72),
    min_liquidity_usd: usdString.default("100000"),
    min_volume_5m_usd: usdString.default("10000"),
    min_sells_5m: z.number().int().nonnegative().default(10),
    min_holders: z.number().int().nonnegative().default(200),
    max_top10_holders_bps: bpsInt.default(3_000),
    max_largest_holder_bps: bpsInt.default(1_000),
    max_price_impact_bps_per_side: bpsInt.default(100),
    max_round_trip_cost_bps: bpsInt.default(200),
    max_price_change_5m_bps: z.number().int().nonnegative().default(3_000),
    /** Token-2022 extensions allowed in v1 (neutral metadata only). */
    allowed_token2022_extensions: z.array(z.number().int()).default([18, 19]),
  }).prefault({}),

  wallets: z.object({
    qualification_window_days: z.number().int().positive().default(30),
    max_candidates: z.number().int().positive().default(200),
    max_qualified: z.number().int().positive().default(100),
    min_qualified_for_session: z.number().int().positive().default(20),
    min_episodes: z.number().int().positive().default(30),
    min_distinct_tokens: z.number().int().positive().default(20),
    min_active_days: z.number().int().positive().default(7),
    min_volume_coverage_bps: bpsInt.default(9_000),
    min_profit_factor: z.string().default("1.2"),
    min_losing_episodes: z.number().int().nonnegative().default(5),
    max_single_token_positive_pnl_bps: bpsInt.default(4_000),
    edge_min_transfer_usd: usdString.default("20"),
    edge_min_direct_transfers: z.number().int().positive().default(2),
    edge_common_funder_min_shared_mints: z.number().int().positive().default(3),
    edge_common_funder_max_gap_seconds: z.number().int().positive().default(60),
    edge_common_funder_lookback_days: z.number().int().positive().default(7),
    edge_validity_days: z.number().int().positive().default(30),
  }).prefault({}),

  signal: z.object({
    min_wallets: z.number().int().positive().default(3),
    min_clusters: z.number().int().positive().default(3),
    window_seconds: z.number().int().positive().default(180),
    min_buy_usd: usdString.default("100"),
    min_retained_bps: bpsInt.default(8_000),
    max_delivery_lag_seconds: z.number().int().positive().default(15),
    ttl_seconds: z.number().int().positive().default(30),
    mint_cooldown_hours: z.number().int().nonnegative().default(24),
  }).prefault({}),

  sizing: z.object({
    max_position_usd: usdString.default("25"),
    max_position_equity_bps: bpsInt.default(500),
    min_position_usd: usdString.default("10"),
    max_open_positions: z.number().int().positive().default(4),
    max_exposure_equity_bps: bpsInt.default(2_000),
    max_entry_attempts_per_utc_day: z.number().int().positive().default(8),
    max_entry_notional_per_utc_day_usd: usdString.default("200"),
  }).prefault({}),

  exits: z.object({
    stop_loss_bps: bpsInt.default(1_000),
    distribution_min_wallets: z.number().int().positive().default(2),
    distribution_min_reduction_bps: bpsInt.default(5_000),
    trailing_activation_bps: bpsInt.default(1_500),
    trailing_drawdown_bps: bpsInt.default(800),
    take_profit_bps: bpsInt.default(2_500),
    time_stop_hours: z.number().positive().default(4),
  }).prefault({}),

  risk: z.object({
    daily_loss_max_usd: usdString.default("20"),
    daily_loss_equity_bps: bpsInt.default(400),
    session_loss_usd: usdString.default("50"),
    drawdown_max_usd: usdString.default("50"),
    drawdown_peak_bps: bpsInt.default(1_000),
    entry_slippage_bps: bpsInt.default(100),
    exit_slippage_bps: bpsInt.default(150),
    emergency_exit_slippage_bps: bpsInt.default(300),
    entry_fee_cap_usd: usdString.default("0.20"),
    exit_fee_cap_usd: usdString.default("0.20"),
    emergency_exit_fee_cap_usd: usdString.default("0.50"),
    max_rent_per_entry_usd: usdString.default("1"),
    sol_reserve_margin_usd: usdString.default("0.50"),
  }).prefault({}),

  execution: z.object({
    profile: z.enum(["BASE", "STRESS", "SEVERE"]).default("BASE"),
    /** Lamports; model estimate until calibrated on canary (ASSUMPTION A14). */
    base_fee_lamports_per_signature: z.number().int().nonnegative().default(5_000),
    signatures_per_tx: z.number().int().positive().default(1),
    priority_fee_lamports_estimate: z.number().int().nonnegative().default(100_000),
    spl_token_account_rent_lamports: z.number().int().nonnegative().default(2_039_280),
    model_close_token_account: z.boolean().default(true),
    max_execution_attempts_per_intent: z.number().int().positive().default(3),
  }).prefault({}),

  freshness: z.object({
    quote_ms: z.number().int().positive().default(2_000),
    flow_ms: z.number().int().positive().default(15_000),
    market_metadata_ms: z.number().int().positive().default(30_000),
    mint_risk_ms: z.number().int().positive().default(60_000),
    holder_snapshot_ms: z.number().int().positive().default(120_000),
    fx_ms: z.number().int().positive().default(60_000),
  }).prefault({}),

  polling: z.object({
    position_quote_ms: z.number().int().positive().default(5_000),
    discovery_ms: z.number().int().positive().default(60_000),
    metadata_ms: z.number().int().positive().default(30_000),
    reconciliation_ms: z.number().int().positive().default(60_000),
  }).prefault({}),

  budget: z.object({
    paid_plan_purchase_allowed: z.literal(false).default(false),
    auto_upgrade: z.literal(false).default(false),
    monthly_alert_usd: usdString.default("100"),
    jupiter_rps: z.number().positive().default(1),
  }).prefault({}),
});

export type Config = z.infer<typeof ConfigSchema>;

/** Stress profiles (brief §13). Scenarios, not forecasts. */
export const EXECUTION_PROFILES = {
  BASE: { extra_delay_ms: 2_000, haircut_bps: 20, modeled_failure_bps: 0, seed: 1 },
  STRESS: { extra_delay_ms: 5_000, haircut_bps: 50, modeled_failure_bps: 1_000, seed: 2 },
  SEVERE: { extra_delay_ms: 15_000, haircut_bps: 100, modeled_failure_bps: 2_000, seed: 3 },
} as const;
export type ExecutionProfileName = keyof typeof EXECUTION_PROFILES;
export type ExecutionProfile = (typeof EXECUTION_PROFILES)[ExecutionProfileName];

export function parseConfig(input: unknown = {}): Config {
  return ConfigSchema.parse(input);
}

export function configHash(config: Config): string {
  return sha256Hex(canonicalJson(config));
}
