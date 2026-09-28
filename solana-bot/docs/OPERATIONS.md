# Operacje: Railway, backup/restore, monitoring

## Railway (instrukcja — deploy NIE został wykonany)

Nie wykonuj płatnego deployu bez zgody właściciela. Koszt Railway zależy od zużycia i nie jest
tu obiecywany. Proponowany układ w jednym projekcie Railway:

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
