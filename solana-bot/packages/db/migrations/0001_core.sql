-- solbot core schema. Source of truth for the paper engine.
-- Raw on-chain amounts: numeric(78,0). USD and FX: numeric(38,18). Times: timestamptz (UTC).

CREATE TABLE IF NOT EXISTS schema_migrations (
  version text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- sessions & config
CREATE TABLE strategy_versions (
  name text NOT NULL,
  version text NOT NULL,
  code_hash text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (name, version)
);

CREATE TABLE config_snapshots (
  config_hash text PRIMARY KEY,
  config jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sessions (
  id text PRIMARY KEY,
  kind text NOT NULL CHECK (kind IN ('CONFLUENCE','INFRA_TEST','DEMO')),
  mode text NOT NULL CHECK (mode IN ('DEMO','PAPER','SHADOW')),  -- LIVE modes intentionally absent in this build
  state text NOT NULL,
  strategy_name text NOT NULL,
  strategy_version text NOT NULL,
  config_hash text NOT NULL REFERENCES config_snapshots(config_hash),
  t0 timestamptz,
  t_end timestamptz,
  entries_paused_by_owner boolean NOT NULL DEFAULT false,
  intervention boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((t0 IS NULL AND t_end IS NULL) OR (t0 IS NOT NULL AND t_end = t0 + interval '168 hours')),
  FOREIGN KEY (strategy_name, strategy_version) REFERENCES strategy_versions(name, version)
);

-- T0/T_end are set once and never changed; config_hash is frozen.
CREATE FUNCTION sessions_freeze() RETURNS trigger AS $$
BEGIN
  IF OLD.t0 IS NOT NULL AND (NEW.t0 IS DISTINCT FROM OLD.t0 OR NEW.t_end IS DISTINCT FROM OLD.t_end) THEN
    RAISE EXCEPTION 'session T0/T_end are immutable';
  END IF;
  IF NEW.config_hash IS DISTINCT FROM OLD.config_hash OR NEW.strategy_version IS DISTINCT FROM OLD.strategy_version THEN
    RAISE EXCEPTION 'session config and strategy version are immutable';
  END IF;
  IF NEW.mode IS DISTINCT FROM OLD.mode OR NEW.kind IS DISTINCT FROM OLD.kind THEN
    RAISE EXCEPTION 'session mode/kind are immutable';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER sessions_freeze BEFORE UPDATE ON sessions FOR EACH ROW EXECUTE FUNCTION sessions_freeze();

CREATE TABLE session_transitions (
  id bigserial PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  from_state text NOT NULL,
  to_state text NOT NULL,
  reason_code text,
  detail text,
  at timestamptz NOT NULL
);
CREATE INDEX ON session_transitions (session_id, at);

-- ---------------------------------------------------------------- providers & raw events
CREATE TABLE providers (
  name text PRIMARY KEY,
  plan text,
  status text NOT NULL DEFAULT 'UNVERIFIED',
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE provider_usage (
  provider text NOT NULL,
  endpoint text NOT NULL,
  minute timestamptz NOT NULL,
  calls integer NOT NULL DEFAULT 0,
  credits integer NOT NULL DEFAULT 0,
  errors integer NOT NULL DEFAULT 0,
  rate_limited integer NOT NULL DEFAULT 0,
  latency_ms_p50 integer,
  latency_ms_p95 integer,
  PRIMARY KEY (provider, endpoint, minute)
);

CREATE TABLE provider_incidents (
  id bigserial PRIMARY KEY,
  provider text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('PROVIDER_UNAVAILABLE','RATE_LIMITED','SCHEMA_CHANGED','NO_ROUTE_BURST')),
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  detail text
);

CREATE TABLE raw_events (
  id text PRIMARY KEY,
  network text NOT NULL DEFAULT 'solana-mainnet',
  provider text NOT NULL,
  source_event_id text NOT NULL,       -- e.g. transaction signature
  leg_index integer NOT NULL DEFAULT 0,
  owner text NOT NULL DEFAULT '',
  block_time timestamptz,
  slot bigint,
  commitment text,
  received_at timestamptz NOT NULL,
  available_at timestamptz NOT NULL,
  schema_version text NOT NULL,
  raw_payload jsonb NOT NULL,
  raw_payload_hash text NOT NULL,
  UNIQUE (network, source_event_id, leg_index, owner),
  CHECK (available_at >= received_at)
);
CREATE INDEX ON raw_events (available_at);

-- ---------------------------------------------------------------- wallets
CREATE TABLE wallets (
  address text PRIMARY KEY,
  candidate_source text NOT NULL,
  first_seen_at timestamptz NOT NULL
);

CREATE TABLE wallet_snapshots (
  id bigserial PRIMARY KEY,
  address text NOT NULL REFERENCES wallets(address),
  at timestamptz NOT NULL,
  data jsonb NOT NULL
);

CREATE TABLE wallet_qualification (
  session_id text NOT NULL REFERENCES sessions(id),
  address text NOT NULL REFERENCES wallets(address),
  status text NOT NULL CHECK (status IN ('QUALIFIED','REJECTED','UNKNOWN')),
  metrics jsonb NOT NULL,
  coverage_bps integer,
  reasons jsonb NOT NULL DEFAULT '[]',
  computed_at timestamptz NOT NULL,
  PRIMARY KEY (session_id, address)
);

CREATE TABLE wallet_edges (
  id bigserial PRIMARY KEY,
  a text NOT NULL,
  b text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('DIRECT_TRANSFERS','COMMON_FUNDER','DEPLOYER_RELATION')),
  source text NOT NULL,
  evidence jsonb NOT NULL,
  confidence text NOT NULL CHECK (confidence IN ('low','medium','high')),
  valid_from timestamptz NOT NULL,
  valid_to timestamptz NOT NULL,
  CHECK (a < b)
);

CREATE TABLE wallet_clusters (
  session_id text NOT NULL REFERENCES sessions(id),
  address text NOT NULL,
  cluster_id text NOT NULL,
  link_check text NOT NULL CHECK (link_check IN ('CHECKED','UNKNOWN')),
  PRIMARY KEY (session_id, address)
);

-- ---------------------------------------------------------------- tokens
CREATE TABLE tokens (
  mint text PRIMARY KEY,
  decimals integer NOT NULL CHECK (decimals BETWEEN 0 AND 18),
  token_program text NOT NULL,
  first_seen_at timestamptz NOT NULL
);

CREATE TABLE token_snapshots (
  id bigserial PRIMARY KEY,
  mint text NOT NULL REFERENCES tokens(mint),
  provider text NOT NULL,
  received_at timestamptz NOT NULL,
  available_at timestamptz NOT NULL,
  data jsonb NOT NULL,
  raw_payload_hash text NOT NULL
);
CREATE INDEX ON token_snapshots (mint, available_at);

CREATE TABLE token_risk_checks (
  id bigserial PRIMARY KEY,
  session_id text REFERENCES sessions(id),
  mint text NOT NULL REFERENCES tokens(mint),
  checked_at timestamptz NOT NULL,
  passed boolean NOT NULL,
  results jsonb NOT NULL,
  inputs jsonb NOT NULL
);

CREATE TABLE pools (
  address text PRIMARY KEY,
  mint text NOT NULL,
  source text NOT NULL,
  created_at timestamptz
);

CREATE TABLE infra_registry (
  address text PRIMARY KEY,
  kind text NOT NULL CHECK (kind IN ('POOL','PROGRAM','BURN','CEX','BRIDGE','ROUTER','LAUNCHPAD','DISTRIBUTOR')),
  source text NOT NULL,
  verified_at timestamptz NOT NULL
);

-- ---------------------------------------------------------------- signals
CREATE TABLE signals (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  strategy_version text NOT NULL,
  config_hash text NOT NULL,
  mint text NOT NULL,
  episode_key text NOT NULL,
  first_detected_at timestamptz NOT NULL,
  ttl_until timestamptz NOT NULL,
  status text NOT NULL,
  UNIQUE (session_id, mint, episode_key)
);

CREATE TABLE signal_evidence (
  id bigserial PRIMARY KEY,
  signal_id text NOT NULL REFERENCES signals(id),
  kind text NOT NULL,
  data jsonb NOT NULL
);

CREATE TABLE rejection_reasons (
  id bigserial PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  stage text NOT NULL,
  reason_code text NOT NULL,
  mint text,
  ref_id text,
  detail jsonb,
  at timestamptz NOT NULL
);
CREATE INDEX ON rejection_reasons (session_id, stage, reason_code);

-- ---------------------------------------------------------------- intents, attempts, quotes, fills, positions
CREATE TABLE trade_intents (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  strategy_version text NOT NULL,
  config_hash text NOT NULL,
  idempotency_key text NOT NULL,
  side text NOT NULL CHECK (side IN ('BUY','SELL')),
  kind text NOT NULL CHECK (kind IN ('ENTRY','EXIT_NORMAL','EXIT_EMERGENCY')),
  mint text NOT NULL,
  input_mint text NOT NULL,
  output_mint text NOT NULL,
  amount_raw numeric(78,0) NOT NULL CHECK (amount_raw > 0),
  notional_usd numeric(38,18),
  signal_id text REFERENCES signals(id),
  position_id text,
  exit_reason text,
  risk_decision jsonb NOT NULL,
  inputs jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  UNIQUE (session_id, idempotency_key)
);

CREATE FUNCTION forbid_update_delete() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER trade_intents_immutable BEFORE UPDATE OR DELETE ON trade_intents FOR EACH ROW EXECUTE FUNCTION forbid_update_delete();

CREATE TABLE order_attempts (
  id text PRIMARY KEY,
  intent_id text NOT NULL REFERENCES trade_intents(id),
  attempt_no integer NOT NULL CHECK (attempt_no BETWEEN 1 AND 3),
  state text NOT NULL,
  broker text NOT NULL CHECK (broker IN ('PAPER','SHADOW')),
  model jsonb NOT NULL,
  outcome jsonb,
  reason_code text,
  fencing_token bigint,
  counts_toward_daily_attempts boolean NOT NULL DEFAULT true,
  started_at timestamptz NOT NULL,
  finished_at timestamptz,
  UNIQUE (intent_id, attempt_no)
);

CREATE TABLE quotes (
  id text PRIMARY KEY,
  attempt_id text REFERENCES order_attempts(id),
  position_id text,
  role text NOT NULL CHECK (role IN ('Q0','Q0_REVERSE','Q1','MARK','ANALYTIC_5S','ANALYTIC_15S')),
  provider text NOT NULL,
  profile text NOT NULL,
  ok boolean NOT NULL,
  failure_code text,
  input_mint text NOT NULL,
  output_mint text NOT NULL,
  in_amount_raw numeric(78,0) NOT NULL,
  out_amount_net_raw numeric(78,0),
  price_impact_bps integer,
  router text,
  requested_at timestamptz NOT NULL,
  received_at timestamptz NOT NULL,
  raw_payload jsonb,
  raw_payload_hash text
);
CREATE INDEX ON quotes (position_id, received_at);

CREATE TABLE positions (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  mint text NOT NULL,
  status text NOT NULL CHECK (status IN ('RESERVED','OPEN','EXITING','CLOSED')),
  entry_intent_id text REFERENCES trade_intents(id),
  qty_raw numeric(78,0) NOT NULL DEFAULT 0 CHECK (qty_raw >= 0),
  cost_usd numeric(38,18),
  rent_lamports numeric(78,0) NOT NULL DEFAULT 0,
  entry_filled_at timestamptz,
  peak_net_value_usd numeric(38,18),
  trailing_active boolean NOT NULL DEFAULT false,
  valuation_status text,
  last_mark_usd numeric(38,18),
  last_mark_at timestamptz,
  closed_at timestamptz,
  exit_reason text,
  realized_pnl_usd numeric(38,18),
  deployer_group text,
  signal_wallets jsonb
);
CREATE UNIQUE INDEX positions_one_active_per_mint ON positions (session_id, mint) WHERE status IN ('RESERVED','OPEN','EXITING');

CREATE TABLE fills (
  id text PRIMARY KEY CHECK (id LIKE 'paper\_%'),   -- paper ids never look like chain signatures
  attempt_id text NOT NULL UNIQUE REFERENCES order_attempts(id),
  position_id text NOT NULL REFERENCES positions(id),
  side text NOT NULL CHECK (side IN ('BUY','SELL')),
  in_mint text NOT NULL,
  in_amount_raw numeric(78,0) NOT NULL,
  out_mint text NOT NULL,
  out_amount_raw numeric(78,0) NOT NULL,
  min_out_raw numeric(78,0) NOT NULL,
  usdc_usd numeric(38,18) NOT NULL,
  sol_usd numeric(38,18) NOT NULL,
  execution_fidelity text NOT NULL,
  filled_at timestamptz NOT NULL,
  CHECK (out_amount_raw >= min_out_raw)
);
CREATE TRIGGER fills_immutable BEFORE UPDATE OR DELETE ON fills FOR EACH ROW EXECUTE FUNCTION forbid_update_delete();

CREATE TABLE fee_items (
  id bigserial PRIMARY KEY,
  attempt_id text NOT NULL REFERENCES order_attempts(id),
  fill_id text REFERENCES fills(id),
  kind text NOT NULL,
  asset text NOT NULL,
  amount_raw numeric(78,0) NOT NULL CHECK (amount_raw >= 0),
  usd_fx numeric(38,18),
  source text NOT NULL CHECK (source IN ('QUOTE','MODEL_ESTIMATE','CHAIN','CONFIG')),
  included_in_quote boolean NOT NULL,
  is_estimate boolean NOT NULL
);

-- ---------------------------------------------------------------- ledger
CREATE TABLE ledger_transactions (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  idempotency_key text NOT NULL,
  kind text NOT NULL,
  at timestamptz NOT NULL,
  refs jsonb NOT NULL DEFAULT '{}',
  memo text,
  UNIQUE (session_id, idempotency_key)
);
CREATE TRIGGER ledger_transactions_immutable BEFORE UPDATE OR DELETE ON ledger_transactions FOR EACH ROW EXECUTE FUNCTION forbid_update_delete();

CREATE TABLE ledger_entries (
  id bigserial PRIMARY KEY,
  tx_id text NOT NULL REFERENCES ledger_transactions(id),
  session_id text NOT NULL,
  bucket text NOT NULL,
  asset text NOT NULL,
  amount_raw numeric(78,0) NOT NULL CHECK (amount_raw <> 0)
);
CREATE INDEX ON ledger_entries (session_id, bucket, asset);
CREATE TRIGGER ledger_entries_immutable BEFORE UPDATE OR DELETE ON ledger_entries FOR EACH ROW EXECUTE FUNCTION forbid_update_delete();

-- Every ledger transaction balances per asset (checked at commit time).
CREATE FUNCTION ledger_tx_balanced() RETURNS trigger AS $$
DECLARE bad record;
BEGIN
  SELECT asset, sum(amount_raw) AS s INTO bad
    FROM ledger_entries WHERE tx_id = NEW.tx_id GROUP BY asset HAVING sum(amount_raw) <> 0 LIMIT 1;
  IF FOUND THEN
    RAISE EXCEPTION 'ledger transaction % unbalanced for asset % (%)', NEW.tx_id, bad.asset, bad.s;
  END IF;
  RETURN NULL;
END $$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER ledger_entries_balanced AFTER INSERT ON ledger_entries
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_tx_balanced();

-- Holding buckets never go negative (checked at commit time).
CREATE FUNCTION ledger_no_negative_holdings() RETURNS trigger AS $$
DECLARE total numeric;
BEGIN
  IF NEW.bucket IN ('wallet','reserved','rent_locked') THEN
    SELECT sum(amount_raw) INTO total FROM ledger_entries
      WHERE session_id = NEW.session_id AND bucket = NEW.bucket AND asset = NEW.asset;
    IF total < 0 THEN
      RAISE EXCEPTION 'negative holding %/% in session %: %', NEW.bucket, NEW.asset, NEW.session_id, total;
    END IF;
  END IF;
  RETURN NULL;
END $$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER ledger_entries_non_negative AFTER INSERT ON ledger_entries
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_no_negative_holdings();

CREATE TABLE balance_reservations (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  intent_id text NOT NULL UNIQUE REFERENCES trade_intents(id),
  usdc_raw numeric(78,0) NOT NULL CHECK (usdc_raw >= 0),
  lamports numeric(78,0) NOT NULL CHECK (lamports >= 0),
  notional_usd numeric(38,18) NOT NULL,
  status text NOT NULL CHECK (status IN ('ACTIVE','CONSUMED','RELEASED')),
  reserve_ledger_tx text NOT NULL REFERENCES ledger_transactions(id),
  created_at timestamptz NOT NULL,
  resolved_at timestamptz
);

CREATE TABLE equity_snapshots (
  id bigserial PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  at timestamptz NOT NULL,
  equity_total_lower_bound_usd numeric(38,18) NOT NULL,
  equity_liquid_lower_bound_usd numeric(38,18) NOT NULL,
  equity_total_fresh_usd numeric(38,18),
  usdc_usd numeric(38,18) NOT NULL,
  sol_usd numeric(38,18) NOT NULL,
  uncertain jsonb NOT NULL DEFAULT '[]',
  breakdown jsonb NOT NULL
);
CREATE INDEX ON equity_snapshots (session_id, at);

CREATE TABLE benchmark_snapshots (
  id bigserial PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  at timestamptz NOT NULL,
  hold_start_alloc_usd numeric(38,18) NOT NULL,
  all_usdc_usd numeric(38,18) NOT NULL
);

CREATE TABLE reconciliation_runs (
  id bigserial PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  at timestamptz NOT NULL,
  ok boolean NOT NULL,
  mismatches jsonb NOT NULL DEFAULT '[]'
);

-- ---------------------------------------------------------------- jobs, outbox, audit
CREATE TABLE jobs (
  id text PRIMARY KEY,
  kind text NOT NULL,
  dedupe_key text NOT NULL,
  payload jsonb NOT NULL,
  priority integer NOT NULL DEFAULT 100,        -- lower runs first: exits before discovery
  status text NOT NULL CHECK (status IN ('READY','LEASED','DONE','DEAD')),
  lease_owner text,
  lease_until timestamptz,
  fencing_token bigint NOT NULL DEFAULT 0,
  retry_count integer NOT NULL DEFAULT 0,
  max_retries integer NOT NULL DEFAULT 5,
  next_run_at timestamptz NOT NULL,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (kind, dedupe_key)
);
CREATE INDEX ON jobs (status, next_run_at, priority);

CREATE TABLE outbox (
  id bigserial PRIMARY KEY,
  topic text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  published_at timestamptz
);

CREATE TABLE alerts (
  id bigserial PRIMARY KEY,
  session_id text,
  severity text NOT NULL CHECK (severity IN ('INFO','WARN','CRITICAL')),
  code text NOT NULL,
  message text NOT NULL,
  at timestamptz NOT NULL
);

CREATE TABLE audit_events (
  id bigserial PRIMARY KEY,
  session_id text,
  actor text NOT NULL,
  action text NOT NULL,
  data jsonb NOT NULL,
  at timestamptz NOT NULL
);
CREATE TRIGGER audit_events_immutable BEFORE UPDATE OR DELETE ON audit_events FOR EACH ROW EXECUTE FUNCTION forbid_update_delete();

CREATE TABLE reports (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  kind text NOT NULL,
  generated_at timestamptz NOT NULL,
  content jsonb NOT NULL
);
