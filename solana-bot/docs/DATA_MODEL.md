# Model danych (PostgreSQL)

Źródło prawdy: `packages/db/src/schema.ts` + migracje SQL w `packages/db/migrations/`.
Konwencje: `id` = `text` (ULID-podobny, z prefiksem typu), czasy `timestamptz` UTC,
kwoty raw `numeric(78,0)`, USD/kursy `numeric(38,18)`, bps `integer`.
Każdy artefakt decyzji ma `session_id`, `strategy_version`, `config_hash`.

## Sesja i konfiguracja

| Tabela | Klucz / ograniczenia | Uwagi |
|---|---|---|
| `strategy_versions` | PK `(name, version)`, `code_hash` | np. `confluence_v1` |
| `config_snapshots` | PK `config_hash` | pełny kanoniczny JSON; niezmienny |
| `sessions` | PK `id`; `kind ∈ {CONFLUENCE, INFRA_TEST, DEMO}`; `mode`; `state`; `t0`, `t_end`, `config_hash` FK; `intervention bool` | T0/T_end ustawiane raz (CHECK + trigger blokujący zmianę) |
| `session_transitions` | PK `id`; `(session_id, seq)` UNIQUE | audyt przejść stanów z powodem |

## Dostawcy i surowe zdarzenia

| Tabela | Klucz / ograniczenia |
|---|---|
| `providers` | PK `name`; plan, status |
| `provider_usage` | `(provider, endpoint, minute)` UNIQUE; calls, credits, errors, p50/p95 ms |
| `provider_incidents` | przedziały `PROVIDER_UNAVAILABLE` / `RATE_LIMITED` |
| `raw_events` | UNIQUE `(network, source_event_id, leg_index, owner)`; `block_time`, `slot`, `received_at`, `available_at`, `provider`, `schema_version`, `raw_payload jsonb`, `raw_payload_hash`, `commitment` |

## Portfele

`wallets` (PK address, źródło kandydata), `wallet_snapshots`, `wallet_qualification`
(PK `(session_id, wallet)`, metryki, pokrycie, status `QUALIFIED|REJECTED|UNKNOWN`,
powody, `computed_at < t0`), `wallet_edges` (typ, źródło, dowody jsonb, confidence,
`valid_from`, `valid_to`), `wallet_clusters` (PK `(session_id, wallet)`, `cluster_id`).

## Tokeny

`tokens` (PK mint, decimals, token_program), `token_snapshots` (metryki dostawcy +
`available_at`), `token_risk_checks` (wynik każdego filtra + dane wejściowe +
`checked_at`), `pools` (PK address, mint, source).

## Sygnały i decyzje

| Tabela | Klucz / ograniczenia |
|---|---|
| `signals` | PK `id`; UNIQUE `(session_id, mint, episode_key)` — deduplikacja epizodu; `first_detected_at`, `ttl_until`, `status` |
| `signal_evidence` | FK signal; zakupy portfeli (raw_event ids), klastry |
| `rejection_reasons` | `(session_id, stage, reason_code, mint, at)` — lejek odrzuceń |

## Zlecenia, wykonanie, pozycje

| Tabela | Klucz / ograniczenia |
|---|---|
| `trade_intents` | PK `id`; UNIQUE `(session_id, idempotency_key)`; niezmienne (trigger) |
| `order_attempts` | PK `id`; UNIQUE `(intent_id, attempt_no)`; `state` (maszyna orderu), `fencing_token` |
| `quotes` | PK `id`; FK attempt; `role ∈ {Q0, Q0_REVERSE, Q1, Q1_REVERSE, MARK, ANALYTIC_5S, ANALYTIC_15S}`; `requested_at`, `received_at`, pełny payload + hash |
| `fills` | PK `id` (`paper_…`); **UNIQUE `(attempt_id)`** — każdy fill dokładnie raz |
| `fee_items` | FK fill/attempt; `kind`, `amount_raw`, `asset`, `usd_fx`, `source`, `included_in_quote`, `is_estimate` |
| `positions` | PK `id`; UNIQUE częściowy `(session_id, mint) WHERE status IN ('OPEN','RESERVED','EXITING')` |

## Księga

| Tabela | Klucz / ograniczenia |
|---|---|
| `ledger_transactions` | PK `id`; UNIQUE `(session_id, idempotency_key)`; `kind`; `created_at` |
| `ledger_entries` | FK tx; `account`, `asset`, `amount_raw` (signed); **trigger**: brak UPDATE/DELETE; **constraint trigger (deferred)**: suma per `(tx, asset)` = 0 |
| `balance_reservations` | PK `id`; `status ∈ {ACTIVE, CONSUMED, RELEASED}`; FK intent |
| `equity_snapshots` | `(session_id, at)`; fresh vs lower-bound; flagi niepewności |
| `benchmark_snapshots` | HOLD_START_ALLOC, ALL_USDC |
| `reconciliation_runs` | wynik porównania sald z sumą księgi |

## Infrastruktura

| Tabela | Klucz / ograniczenia |
|---|---|
| `jobs` | PK `id`; `kind`, `payload`, `status ∈ {READY, LEASED, DONE, DEAD}`, `lease_owner`, `lease_until`, `fencing_token bigint`, `retry_count`, `next_run_at`, UNIQUE `(kind, dedupe_key)` |
| `outbox` | PK `id`; `topic`, `payload`, `published_at` |
| `alerts`, `audit_events`, `reports` | append-only |

## Atomowość

* Rezerwacja kapitału i rejestracja intencji: jedna transakcja DB z
  `SELECT … FOR UPDATE` na wierszu `sessions` (serializacja decyzji wejścia per sesja).
* Fill + wpisy księgi + zużycie rezerwacji + aktualizacja pozycji: jedna transakcja.
* Job wykonujący próbę sprawdza `fencing_token` przy zapisie wyniku — przeterminowany
  lease nie może zapisać fillu.
