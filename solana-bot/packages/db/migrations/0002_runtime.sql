-- Runtime observability: FX history, worker heartbeats and data gaps.

CREATE TABLE fx_snapshots (
  id bigserial PRIMARY KEY,
  at timestamptz NOT NULL,              -- when we received it (available_at)
  usdc_usd numeric(38,18),
  sol_usd numeric(38,18),
  source text NOT NULL,
  raw_payload_hash text
);
CREATE INDEX ON fx_snapshots (at);

CREATE TABLE heartbeats (
  worker_id text PRIMARY KEY,
  session_id text,
  last_beat_at timestamptz NOT NULL,
  started_at timestamptz NOT NULL
);

-- Explained or unexplained gaps (worker down, provider down, missing observations).
CREATE TABLE data_gaps (
  id bigserial PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  kind text NOT NULL CHECK (kind IN ('WORKER_DOWN','PROVIDER_DOWN','MARK_MISSING','FLOW_LATE')),
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  positions_open boolean NOT NULL DEFAULT false,
  explained boolean NOT NULL DEFAULT false,
  detail text
);
CREATE INDEX ON data_gaps (session_id, started_at);

ALTER TABLE positions ADD COLUMN exit_attempts integer NOT NULL DEFAULT 0;
ALTER TABLE positions ADD COLUMN next_exit_at timestamptz;
ALTER TABLE positions ADD COLUMN signal_id text;
ALTER TABLE sessions ADD COLUMN flatten_requested boolean NOT NULL DEFAULT false;
ALTER TABLE sessions ADD COLUMN day_start_equity jsonb NOT NULL DEFAULT '{}';
ALTER TABLE sessions ADD COLUMN peak_equity_usd numeric(38,18);
ALTER TABLE sessions ADD COLUMN t_end_snapshot jsonb;
ALTER TABLE sessions ADD COLUMN last_tick_at timestamptz;

-- Ledger replay order is insertion order, not event time.
ALTER TABLE ledger_transactions ADD COLUMN seq bigserial;
CREATE UNIQUE INDEX ledger_transactions_seq ON ledger_transactions (seq);
