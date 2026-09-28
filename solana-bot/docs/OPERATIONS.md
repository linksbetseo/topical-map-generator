# Operacje: Railway, backup/restore, monitoring

## Railway — stan wdrożenia (2026-09-28)

Wdrożono na polecenie właściciela. Projekt Railway `solana-bot` (workspace „linksbetseo's Projects”),
środowisko `production`, gałąź `claude/solana-bot-deploy-continue-n4nptf`, Root Directory `solana-bot`:

| Serwis | Stan | Uwagi |
|---|---|---|
| Postgres | działa | szablon Railway `postgres` |
| api | działa | `https://api-production-9b38.up.railway.app` (port 8080), healthcheck `/health/live` |
| worker | działa | bez aktywnej sesji kończy proces i jest restartowany (restart `ALWAYS`) |

Zmienne ustawione w obu serwisach: `DATABASE_URL=${{Postgres.DATABASE_URL}}`, `OWNER_API_TOKEN`,
`HELIUS_WEBHOOK_AUTH` (losowe, wartości tylko w panelu Railway → Variables), `JUPITER_API_KEY`,
`HELIUS_API_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `MODE=PAPER`, `LIVE_ENABLED=false`,
`GIT_SHA=${{RAILWAY_GIT_COMMIT_SHA}}`; w api dodatkowo `PORT=8080`.

Utworzona sesja: `ses_0mul742nr6861f92f77548816` (`CONFLUENCE`, `PAPER`, stan `DRAFT`).
Pozostało: bootstrap portfeli → rejestracja webhooka → validate → start (krok 5 poniżej).

**Bootstrap na produkcji (2026-09-28)** uruchamiano jako tymczasowy serwis Railway `bootstrap`
(ten sam obraz, sieć prywatna, bez wystawiania bazy; start command
`sh -c 'pnpm --filter @solbot/worker bootstrap-wallets <id> && pnpm --filter @solbot/worker register-webhook <id>'`,
restart `NEVER`; po przebiegu serwis usunięty, bo każdy push na gałąź uruchamiałby go ponownie).
Wynik po poprawkach paginacji i tempa Helius: 129 kandydatów, 995 wywołań Helius, 1 błąd sieci,
**0 zakwalifikowanych** (71 historii > 10 stron = boty HFT, 82 ujemny PnL, 87 profit factor < 1.2,
100 < 20 tokenów). To wynik kryteriów, nie błąd — decyzja właściciela: szersza pula kandydatów
(`BOOTSTRAP_SEED_MINTS`, `BOOTSTRAP_SEED_HOURS`) i/lub inne progi `wallets.*`. Webhooka nie
zarejestrowano (brak portfeli).

**Config-as-code (`railway.json`) jest na Railway wycofane** — API odrzuca ustawienie
`railwayConfigFile`. Pliki `deploy/railway.*.json` służą teraz tylko jako opis; te same wartości
(start command, healthcheck, restart policy) ustawiono bezpośrednio w ustawieniach serwisów.
Serwis tworzony z repo bez triggera gałęzi buduje `main` — trzeba ustawić gałąź w Settings → Source.

Koszt Railway zależy od zużycia i nie jest tu obiecywany. Proponowany układ w jednym projekcie Railway:

| Serwis | Root | Start command | Zmienne |
|---|---|---|---|
| Postgres | plugin Railway | — | — |
| api | `solana-bot` (Dockerfile) | `pnpm --filter @solbot/api start` | `DATABASE_URL`, `OWNER_API_TOKEN`, `HELIUS_WEBHOOK_AUTH`, `JUPITER_API_KEY`, `HELIUS_API_KEY`, `ALLOWED_ORIGINS`, `GIT_SHA`, `MODE=PAPER`, `LIVE_ENABLED=false` |
| worker | `solana-bot` (Dockerfile) | `pnpm --filter @solbot/worker start` | jw. + `TELEGRAM_*` (opcjonalnie) |

* Istniejąca aplikacja Python w katalogu głównym repo ma własny `railway.json`; bot musi być
  osobnym serwisem z Root Directory = `solana-bot`, aby nie zmienić jej wdrożenia.
* Deploy tylko z zatwierdzonego commitu. Worker nie pobiera ani nie wykonuje zdalnego kodu.
* Webhook Helius wskazuje na `https://<api>/api/webhooks/helius` z nagłówkiem `authHeader`
  równym `HELIUS_WEBHOOK_AUTH`.
* **Nigdy** nie ustawiaj zmiennych z kluczem prywatnym — proces odmówi startu.

### Kolejność uruchomienia na Railway (gotowe pliki: `deploy/railway.api.json`, `deploy/railway.worker.json`)

1. Railway → New Project → Deploy from GitHub → repo `linksbetseo/topical-map-generator`, gałąź
   `claude/solana-bot-deploy-continue-n4nptf` (Settings → Source → Branch).
2. Serwis **api**: Settings → Root Directory `solana-bot`, start command / healthcheck / restart jak w
   `deploy/railway.api.json` (ustawione ręcznie, bo config-as-code jest wycofane), Networking →
   Generate Domain (port 8080). Serwis **worker**: ten sam root, wartości z `deploy/railway.worker.json`.
3. Dodaj **PostgreSQL** (plugin) i w obu serwisach `DATABASE_URL=${{Postgres.DATABASE_URL}}`.
4. Zmienne (obie usługi): `JUPITER_API_KEY`, `HELIUS_API_KEY`, `HELIUS_WEBHOOK_AUTH`, `OWNER_API_TOKEN`,
   `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `MODE=PAPER`, `LIVE_ENABLED=false`. Nigdy klucza portfela.
5. Po starcie: `POST /api/sessions {"kind":"CONFLUENCE","mode":"PAPER"}` → w serwisie worker (Railway
   shell / one-off) `pnpm --filter @solbot/worker bootstrap-wallets <sessionId>` →
   `API_PUBLIC_URL=https://<api-domain> pnpm --filter @solbot/worker register-webhook <sessionId>` →
   `POST /api/sessions/:id/validate` → worker zbiera 30 min danych → `POST /api/sessions/:id/start`.
   Bootstrap uruchamiaj tuż przed startem — lista portfeli jest zamrażana na 7 dni.

## Heartbeat zewnętrzny

Martwy worker nie wyśle sam alarmu. `GET /health/ready` zwraca 503, gdy ostatni heartbeat
workera jest starszy niż 30 s. Skonfiguruj zewnętrzny monitor (np. uptime checker) na ten URL.
Luki pracy workera zapisuje też `data_gaps` (widoczne w raporcie).

## Backup / restore

```bash
# backup (logiczny, spójny)
pg_dump --format=custom --no-owner "$DATABASE_URL" > solbot-$(date -u +%Y%m%dT%H%M%SZ).dump
# restore do nowej bazy
createdb solbot_restore
pg_restore --no-owner --dbname=postgres://.../solbot_restore solbot-XXXX.dump
# test przywrócenia: uruchom reconciliation na odtworzonej bazie
DATABASE_URL=postgres://.../solbot_restore pnpm db:migrate   # powinno zwrócić "schema up to date"
```

Po przywróceniu sprawdź raport sesji (`/api/sessions/:id/report`) — tożsamość księgi musi się
zamykać (`technicalReasons` bez „rozjazd księgi”). Nie edytuj historii ręcznie: korekty tylko
wpisem kompensującym.

## Role bazy

`ops/roles.sql` tworzy rolę `solbot_paper` bez DDL i bez DELETE. Tabele append-only mają
dodatkowo triggery blokujące UPDATE/DELETE.

## Budżet API (bez zakupów)

`paid_plan_purchase_allowed=false`, `auto_upgrade=false` są wymuszone w schemacie konfiguracji
(nie da się ich włączyć). Rate limiter priorytetowy chroni monitoring pozycji przed discovery.
Zużycie zapisuje `provider_usage` (do rozbudowy o kredyty); bez danych billingowych koszt
infrastruktury w raporcie = `UNKNOWN`, nie 0.
