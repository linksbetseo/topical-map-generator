# Plan implementacji

## Diagram przepływu

```
                ┌──────────────── apps/worker (jeden proces, lease na jobach) ────────────────┐
 Helius webhook │                                                                              │
  → apps/api ───┼─► raw_events (dedupe, available_at) ─► normalizacja swapów portfeli         │
                │                                              │                               │
 Jupiter Tokens ┼─► tokens / token_snapshots ─────────┐        ▼                               │
 Helius RPC ────┼─► token_risk_checks (mint, T-2022,  │   strategy/confluence_v1 (czysta)      │
                │    holderzy wg właściciela)         └──► signals + signal_evidence           │
                │                                              │ TradeIntent (immutable)        │
                │                                              ▼                               │
                │                              risk.evaluate #1 ─► balance_reservations (atom.) │
                │                                              ▼                               │
                │                         brokers/PaperBroker: Q0 → reverse → 2 s → Q1 →        │
                │                         fill model → risk.evaluate #2 → fill + ledger (1 tx) │
                │                                              ▼                               │
                │                positions ◄── monitor (sell quote co 5 s) ── exit rules        │
                │                                              ▼                               │
                │                  equity_snapshots / benchmark_snapshots / reports             │
                └──────────────────────────────────────────────────────────────────────────────┘
 apps/signer: NIE ISTNIEJE w PAPER (Etap F, osobny proces, osobne sekrety).
```

## Pakiety

| Pakiet | Zawartość | Zależności |
|---|---|---|
| `packages/domain` | jednostki (raw/bps/decimal), stałe mintów, reason codes, tryby, maszyny stanów sesji i orderu, `Clock` | decimal.js |
| `packages/config` | schemat zod, domyślne wartości briefu, hash konfiguracji, walidacja env (sekrety w PAPER) | domain, zod |
| `packages/ledger` | księga wieloaktywowa, salda, rezerwacje, equity lower bound, reconciliation | domain |
| `packages/risk` | sizing, limity, circuit breakers, loss triggery | domain, config, ledger |
| `packages/providers` | Jupiter (order/tokens/price), Helius RPC (mint, holderzy), `ReadOnlyTransport`, rate limiter priorytetowy, parser Token-2022 | domain |
| `packages/brokers` | `ExecutionBroker`, `PaperBroker` (model fillu, profile BASE/STRESS/SEVERE) | domain, ledger, providers(interfejsy) |
| `packages/strategy` | kwalifikacja portfeli, klastry, filtry tokena, `confluence_v1`, reguły wyjścia | domain, config |
| `packages/db` | schema Drizzle, migracje SQL, repozytoria, jobs z lease/fencing, outbox | drizzle-orm, pg |
| `packages/reporting` | metryki, Markdown/HTML/CSV/JSON | domain, ledger |
| `apps/worker` | pętle: discovery, monitor pozycji, sygnały, controller sesji 168 h | wszystkie |
| `apps/api` | Fastify: health, dashboard JSON, sterowanie sesją, webhook | db |
| `apps/web` | panel (po Etapie D; bez fikcyjnych danych) | api |

## Etapy

| Etap | Zakres | Kryterium zakończenia |
|---|---|---|
| A | audyt repo, kontrakty, założenia, model danych, braki | dokumenty w `docs/` |
| B | domain, config, ledger, risk, paper fill model, maszyny stanów, schema DB + migracje, testy krytyczne | testy zielone, w tym integracyjne z Postgres |
| C | adaptery read-only (Jupiter, Helius RPC), parser Token-2022, rate limiter, transport read-only | testy kontraktowe na fixtures; **weryfikacja mainnet zablokowana (G1–G3)** |
| D | confluence_v1, pozycje i wyjścia, controller 168 h, restart, benchmarki, API, raporty, Telegram | e2e na fake clock + fixtures DEMO |
| E | audyt wyniku po realnej sesji | wymaga G2+G3+G4 i 168 h pracy |
| F | shadow/signer/live | osobna zgoda właściciela |

## Kolejność testów krytycznych (Etap B)

1. Saldo startowe = dokładnie 500 USD (480 USDC + SOL o wartości 20 USD wg kursu T0).
2. Księga: każda transakcja zbilansowana per aktywo (property-based), append-only.
3. Fill model: gorszy z Q0/Q1, haircut, min_out, brak przycinania, skok −80%.
4. Opłaty zawarte w quote nie są odejmowane ponownie; slippage cap ≠ opłata.
5. Rent: zmniejsza płynne środki, nie jest stratą; odzysk nie jest zyskiem.
6. Risk: sizing, 4 pozycje, 20% ekspozycji, limity dzienne, rezerwa SOL, triggery.
7. Idempotencja: duplikat joba/webhooka/dwa workery → jeden fill (Postgres).
8. PAPER z sekretem w env → start przerwany; transport blokuje wysyłanie.
