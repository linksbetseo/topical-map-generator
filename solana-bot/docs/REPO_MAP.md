# Mapa repozytorium i luki względem specyfikacji naprawczej v2 (P0, 2026-09-28)

Stan: gałąź `claude/solana-bot-deploy-continue-n4nptf`. Tryb wyłącznie PAPER; w repo nie ma signera
ani wywołań wysyłających transakcje (`ReadOnlyTransport.assertReadOnlyRequest` blokuje metody wysyłające).

## Pakiety i procesy

| Miejsce | Rola | Kluczowe elementy |
|---|---|---|
| `packages/domain` | jednostki (raw bigint, `Dec`), mints, reason codes, maszyny stanów, zegar | `units.ts`, `states.ts`, `mints.ts` |
| `packages/config` | schemat zod (progi, limity), hash konfiguracji, walidacja env (odmowa startu przy sekrecie portfela) | `schema.ts`, `env.ts` |
| `packages/ledger` | księga podwójnego zapisu PAPER, rezerwacje, equity | `ledger.ts`, `postings.ts`, `equity.ts` |
| `packages/risk` | sizing, limity wejść, loss triggers | `risk.ts` |
| `packages/providers` | adaptery read-only: Jupiter (order/price/tokens), Helius RPC/DAS, **legacy** Helius Enhanced (`history.ts`), **nowe** RPC history (`solana-tx.ts`), Binance 1m | limiter `rate-limit.ts` |
| `packages/strategy` | kwalifikacja portfeli (`wallets.ts`), **nowy** normalizator sald (`balance-events.ts`), klastry, confluence, filtry tokena, wyjścia | |
| `packages/brokers` | `PaperBroker` (Q0 → 2 s → Q1, model fillu) | `paper.ts` |
| `packages/db` | migracje SQL (0001–0003), repozytoria, joby z lease/fencing | `migrations/` |
| `packages/reporting` | raport sesji (json/md/csv/html) | `report.ts` |
| `apps/api` | Fastify: owner API (Bearer), webhook Helius, health, **`/api/sessions/:id/wallet-diagnostics`** | `app.ts` |
| `apps/worker` | pętla PAPER, bootstrap portfeli (legacy), **diagnostyka P0** (`diagnostics.ts`, `diagnose-cli.ts`) | `engine.ts`, `bootstrap.ts` |
| `apps/web` | panel — nie istnieje (odłożony) | |

## Ścieżka danych portfela: legacy vs P0

| Krok | Legacy (bootstrap) | P0 (diagnostyka) |
|---|---|---|
| Kandydaci | fee payerzy kupujący 20 tokenów z Jupiter toptraded, ostatnie 6 h | bez zmian (te same 129 adresów) |
| Historia | Helius Enhanced `type=SWAP`, 100 kredytów/zapytanie, max 10 stron; bez transferów | `getSignaturesForAddress` (1 kredyt / 1000 sygnatur) + `getTransaction` (1 kredyt/tx), wszystkie typy transakcji, trwały cache |
| Kompletność | do 28.09 krótka strona = koniec (błąd, naprawiony) | skan sygnatur do granicy okna; niepełny skan lub przekroczony budżet ⇒ `HISTORY_INCOMPLETE` bez oceny |
| Normalizacja | sumy list transferów (`tokenTransfers` + `nativeTransfers`), kwoty UI float | zmiany sald pre/post (raw bigint + decimals, owner), fee i rent osobno |
| Błąd potwierdzony | **podwójne liczenie SOL** przy WSOL + unwrap (mainnet `3RjgA7Vr…`: 53,88 vs 26,83 USD) | test regresyjny `ledger-p0.test.ts` |
| Transfery | niewidoczne (tylko SWAP) → sprzedaże tokenów z transferu pomijane, zakupy wytransferowane liczone jako otwarte | `TRANSFER_IN/OUT` bez ceny, epizod `UNKNOWN_COST_BASIS` |
| Wycena | Binance 1m open SOLUSDT/USDCUSDT | to samo + USDT jako baza (parytet jawny) |
| Wynik | suma epizodów (zamknięte + otwarte po cenie Jupiter) | osobno: realized, open po cenie Jupiter, open z podłogą 0, łącznie; bez tokenów discovery; bez najlepszego tokena; epizody „dust” |

## Luki względem specyfikacji v2 (do P1–P3)

| § | Wymaganie | Stan w kodzie | Etap |
|---|---|---|---|
| 3 | raport 129 portfeli, kategorie, rozkłady | **zrobione** (P0) | P0 |
| 4 | kontrakty `HistoryProvider` / `LiveEventProvider` / … z wersją | częściowo: brak formalnych interfejsów; wersje ekstraktora/normalizatora/pricingu zapisywane | P1 |
| 4 | Helius Parsed Events (10 kredytów) obok legacy | brak adaptera | P1 |
| 4 | Jupiter: przypięty profil, Swap V2 read-only | legacy `/swap/v1` + profil bez RFQ | P1 |
| 4 | kandydaci z indeksu transakcji pul, nie z historii mintu | brak (seed z `toptraded` + historia mintu) | P1 |
| 5 | FIFO wewnątrz epizodu, próg 98% | średni koszt epizodu, domknięcie przy ≤0,1% szczytu | P1 |
| 5 | koszt pozycji sprzed okna 30 dni | sprzedaż bez widzianego zakupu jest pomijana i liczona jako `unknown_opening_inventory` | P1 |
| 5 | trader ≠ fee payer | P0 liczy po właścicielu kont (poprawnie); discovery legacy nadal bierze `feePayer` | P1 |
| 6 | trzy źródła kandydatów, prefiltr, checkpointy | checkpointy: **są** (cache); źródła: jedno | P1 |
| 6 | status `QUALIFIED_PROVISIONAL`, `SMALL_COHORT` | tylko w diagnostyce; sesja wymaga 20 | P1/P3 |
| 7 | `event_time / observed_at / available_at` dla cech | `availableAt` jest w zdarzeniach przepływu; brak dla cech portfela | P1 |
| 8 | klastry: wielokrotne znaczące transfery, bez tranzytywnego sklejania przez usługi | `clusters.ts` do przeglądu | P1 |
| 9 | próg 100 USD = **suma** zakupów portfela w oknie | liczony **per zakup** (`confluence.ts:60`) — rozbieżność | P2 |
| 9 | retencja obniżana także przez transfery wychodzące | tylko SELL (`confluence.ts:73`) — rozbieżność | P2 |
| 9 | 15 s = opóźnienie każdego zdarzenia | **zgodne** (`availableAt − blockTime` per zdarzenie) | — |
| 9 | diagnostyczny zapis 2/180 s | brak | P2 |
| 10 | wiek puli ≥ 1 h, płynność ≥ 100 tys. USD, round trip ≤ 2% | do weryfikacji w `token-filters.ts` | P2 |
| 11–12 | Q0/Q1, min output, zmiana trasy, brak trasy sprzedaży | `PaperBroker` do przeglądu pod testy #16–#21 | P2 |
| 13 | limit 100 USD w ruchomych 24 h, 8 prób, kotwice stopów | częściowo w `risk.ts` (UTC-dzień vs ruchome 24 h do sprawdzenia) | P2 |
| 14 | jeden limiter współdzielony między procesami | limitery per proces | P2 |
| 14 | webhook: szybki zapis → ACK, dedup po sygnaturze bez gubienia drugiego portfela | do testów #6–#7 na ścieżce webhooka | P2 |
| 15 | raport trzech ocen, werdykty | raport sesji istnieje; werdykty do dodania | P3 |
| 16 | 30 testów akceptacyjnych | P0: #1, #2 (Enhanced), #3, #4, #5, #7 (normalizator), #9–#12 | P0–P3 |
