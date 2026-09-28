# solbot — Solana spot, forward PAPER trading (500 USD wirtualnie, 168 h)

Prywatny, jednoosobowy bot badawczy. **Tryb domyślny: PAPER. LIVE jest wyłączony i w tym
buildzie nie istnieje signer.** To specyfikacja eksperymentu, nie strategia o dowiedzionej
rentowności.

Stan gotowości:

| Status | Wartość |
|---|---|
| IMPLEMENTED | tak (etapy A–D, bez panelu web) |
| TESTED_WITH_FIXTURES | tak — `pnpm test` (unit + Postgres) |
| VERIFIED_READ_ONLY_MAINNET | **nie** — brak kluczy i blokada sieci w środowisku deweloperskim (`docs/ACCESS_GAPS.md`) |
| FORWARD_TEST_COMPLETED | **nie** |
| LIVE_DISABLED | tak |

Dokumenty: `docs/IMPLEMENTATION_PLAN.md`, `docs/ASSUMPTIONS.md`, `docs/provider-contracts.md`,
`docs/DATA_MODEL.md`, `docs/ACCESS_GAPS.md`, `docs/OPERATIONS.md`, `docs/SECURITY.md`,
`docs/LIMITATIONS.md`.

## Wymagania

Node 22 LTS, pnpm 10 (`corepack enable`), PostgreSQL 16 (lokalnie albo Docker).

## Uruchomienie lokalne

Terminal (bash/zsh) — PowerShell w nawiasach, gdy się różni:

```bash
cd solana-bot
corepack enable
pnpm install --frozen-lockfile
cp .env.example .env            # PowerShell: Copy-Item .env.example .env
```

Uzupełnij `.env` (nigdy nie wpisuj klucza prywatnego — PAPER odmówi startu):
`DATABASE_URL`, `JUPITER_API_KEY`, `HELIUS_API_KEY`, `HELIUS_WEBHOOK_AUTH`, `OWNER_API_TOKEN`,
opcjonalnie `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.

Baza przez Docker:

```bash
docker compose up -d db
export DATABASE_URL=postgres://solbot:solbot@localhost:5432/solbot
# PowerShell: $env:DATABASE_URL="postgres://solbot:solbot@localhost:5432/solbot"
pnpm db:migrate
```

Testy (potrzebna baza testowa; testy DB kasują jej schemat):

```bash
export TEST_DATABASE_URL=postgres://solbot:solbot@localhost:5432/solbot_test
# PowerShell: $env:TEST_DATABASE_URL="postgres://solbot:solbot@localhost:5432/solbot_test"
pnpm typecheck
pnpm test            # wszystko
pnpm test:unit       # bez bazy
pnpm test:db         # integracyjne z Postgres
```

Weryfikacja read-only mainnet (nie wysyła transakcji):

```bash
pnpm --filter @solbot/worker check-providers
```

Oczekiwany wynik przy działającej sieci i kluczach: każda linia `VERIFIED_READ_ONLY_MAINNET`.
W tej sesji implementacyjnej wszystkie wywołania zwróciły 403 z proxy środowiska.

Procesy:

```bash
pnpm --filter @solbot/api start       # API właściciela na :8080 (OWNER_API_TOKEN wymagany)
pnpm --filter @solbot/worker start    # worker: zbieranie danych, sygnały, pozycje, raporty
```

albo całość: `docker compose up --build`.

## Przebieg eksperymentu

Wszystkie wywołania z nagłówkiem `Authorization: Bearer $OWNER_API_TOKEN`.

1. `POST /api/sessions` `{"kind":"CONFLUENCE","mode":"PAPER"}` → `DRAFT`.
   (`INFRA_TEST` = test infrastruktury, raport nie ocenia confluence; `DEMO` = fixtures.)
2. Worker w trybie zbierania zapisuje kursy FX (potrzebne 30 min zdrowych danych).
3. `POST /api/sessions/:id/validate` → `READY` albo `INSUFFICIENT_DATA` z listą braków.
   Dla `CONFLUENCE` bez ≥ 20 zakwalifikowanych portfeli sesja **nie wystartuje**
   (obecnie brak dostawcy historii portfeli i historycznych cen — G4).
4. `POST /api/sessions/:id/start` („Rozpocznij 7 dni”) — tylko gdy wszystkie bramki readiness
   przechodzą; zapisuje T0, T_end = T0 + 168 h (UTC; w odpowiedzi także Europe/Warsaw) i otwarcie
   500 USD = 480 USD w USDC + 20 USD w SOL wg kursów z T0.
5. Sterowanie: `pause-entries`, `resume-entries`, `request-flatten` (audytowane).
6. Raport: `GET /api/sessions/:id/report?format=json|md|csv|html`.

Zakończenie 168 h nigdy nie włącza LIVE. Awaria nie zatrzymuje kalendarza i nie resetuje kapitału.

## Struktura

```
apps/api        Fastify: health, owner API, SSE, webhook Helius (trwały zapis, dedupe)
apps/worker     PaperEngine (sygnały, risk, paper broker, pozycje, T_end, recovery), adaptery live
apps/signer     brak implementacji (Etap F, osobna zgoda)
apps/web        odłożony (dane dostępne przez API)
packages/domain     jednostki (bigint/decimal/bps), reason codes, maszyny stanów, zegar
packages/config     schemat parametrów briefu, hash konfiguracji, blokada sekretów w PAPER
packages/ledger     księga wieloaktywowa, rezerwacje, rent, equity lower bound
packages/risk       sizing, limity, rezerwa SOL, loss triggers
packages/providers  Jupiter (order/tokens/price), Helius RPC/DAS, parser Token-2022, transport read-only
packages/brokers    PaperBroker (Q0 → reverse → opóźnienie → Q1 → gorszy − haircut → min_out)
packages/strategy   filtry tokena, kwalifikacja portfeli, klastry, confluence_v1, wyjścia
packages/db         migracje SQL (triggery append-only i bilansu), repozytoria, jobs z fencing
packages/reporting  metryki, raport JSON/Markdown/CSV/HTML, werdykty
```
