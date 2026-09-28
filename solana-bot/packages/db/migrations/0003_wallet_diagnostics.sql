-- P0 (spec v2 §3): durable provider cache and per-wallet diagnostics of the bootstrap.

-- One row per transaction, wallet-agnostic (all balances), so any wallet can be re-derived without a
-- new request. `compact` keeps pre/post balances, token balances with owner/program, fee, error and
-- program ids; instruction data and logs are dropped (re-fetchable by signature).
CREATE TABLE rpc_tx_cache (
  signature text PRIMARY KEY,
  slot bigint NOT NULL,
  block_time timestamptz,
  extractor_version integer NOT NULL,
  compact jsonb NOT NULL,
  source text NOT NULL,                 -- provider + method, e.g. helius-rpc:getTransaction
  fetched_at timestamptz NOT NULL
);

-- Signature scans per address and window (checkpoint: a complete scan is reused, never re-paid).
CREATE TABLE rpc_signature_scans (
  id bigserial PRIMARY KEY,
  address text NOT NULL,
  gte_time timestamptz NOT NULL,
  lte_time timestamptz NOT NULL,
  complete boolean NOT NULL,
  calls integer NOT NULL,
  oldest_seen timestamptz,
  newest_seen timestamptz,
  signatures jsonb NOT NULL,            -- [[signature, slot, blockTime|null, failed]]
  scanned_at timestamptz NOT NULL
);
CREATE INDEX ON rpc_signature_scans (address, scanned_at DESC);

CREATE TABLE wallet_diagnostic_runs (
  run_id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  params jsonb NOT NULL,
  summary jsonb,
  started_at timestamptz NOT NULL,
  finished_at timestamptz
);

CREATE TABLE wallet_diagnostics (
  run_id text NOT NULL REFERENCES wallet_diagnostic_runs(run_id),
  address text NOT NULL,
  primary_reason text NOT NULL CHECK (primary_reason IN ('API_ERROR','HISTORY_INCOMPLETE','PARSER_UNSUPPORTED','MISSING_PRICE','UNKNOWN_COST_BASIS','INSUFFICIENT_SAMPLE','NEGATIVE_PNL','LOW_PF','RISK_OR_COPYABILITY_FAIL','QUALIFIED_PROVISIONAL')),
  record jsonb NOT NULL,
  computed_at timestamptz NOT NULL,
  PRIMARY KEY (run_id, address)
);
