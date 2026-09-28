# Założenia i decyzje techniczne

Każda decyzja ma identyfikator; zmiana decyzji wpływającej na wynik = nowa wersja
strategii/profilu i nowy `session_id`.

## Repozytorium i stack

* **A1** Repo `topical-map-generator` zawiera działającą aplikację Python (FastAPI).
  Nie jest nadpisywana. Bot żyje w osobnym katalogu `solana-bot/` jako niezależny
  monorepo pnpm. Railway dla bota będzie osobnym serwisem z `rootDirectory=solana-bot`.
* **A2** Node 22 (zainstalowany v22.22.2, gałąź LTS), pnpm 10.33, TypeScript 5.9.
  Wersje zależności przypięte w `pnpm-lock.yaml` (pobrane z rejestru npm w dniu
  implementacji, nie z pamięci).
* **A3** Pieniądze: kwoty on-chain jako `bigint` (raw), USD i kursy jako `decimal.js`
  (precyzja 40, zaokrąglenia jawne: `ROUND_DOWN` dla ilości otrzymywanych, `ROUND_UP`
  dla kosztów). W Postgres `numeric(78,0)` dla raw i `numeric(38,18)` dla USD.
  Żadnego `number` na pieniądzach (wyjątek: pola informacyjne dostawcy zapisywane w raw_payload).
* **A4** Jednostki procentowe mają sufiks w nazwie: `*_bps` (int), `*_frac` (0–1),
  `*_pct` (0–100). `priceImpact` Jupiter jest w punktach procentowych →
  `price_impact_bps = |priceImpact| × 100`, zaokrąglone w górę.

## Tryby i bezpieczeństwo

* **A5** Domyślnie `MODE=PAPER`, `LIVE_ENABLED=false`. W repo **nie ma** implementacji
  signera (Etap F). `apps/signer` zawiera tylko README.
* **A6** Start PAPER/DEMO/SHADOW przerywa się błędem konfiguracji, jeśli w środowisku
  istnieje zmienna wyglądająca na sekret podpisu (`*PRIVATE_KEY*`, `*SECRET_KEY*`,
  `*SEED*PHRASE*`, `*MNEMONIC*`, `*KEYPAIR*`, `SIGNER_*`).
* **A7** Transport HTTP w PAPER to `ReadOnlyTransport`: blokuje ścieżki `/execute`
  i metody JSON-RPC wysyłające transakcje — niezależnie od flag w UI. Test
  statyczny sprawdza, że pakiety używane przez PAPER nie importują modułów kluczy.
* **A8** Role bazy: `solbot_paper` (bez dostępu do tabel signera — tabel tych w PAPER
  nie ma w ogóle) — przygotowane w migracji, użycie opisane w README.

## Czas i dane

* **A9** Wszystkie znaczniki czasu w UTC (`timestamptz`), doba = doba UTC. UI dodaje
  Europe/Warsaw tylko do wyświetlania.
* **A10** `available_at` = chwila zapisania rekordu w naszej bazie (po walidacji).
  Decyzja w T używa wyłącznie `available_at <= T`.
* **A11** Clock jest wstrzykiwany (`Clock`), testy używają `FakeClock`. Opóźnienie
  modelowe 2 s jest „odczekiwane” przez `clock.sleep` — realne w workerze, natychmiastowe w testach.

## Model wykonania PAPER

* **A12** Profil `jupiter_order_manual_v1` (patrz provider-contracts): `/order` bez
  `taker`, z `slippageBps` = limit strony. Fidelity: `QUOTE_ONLY_NO_TAKER`.
* **A13** Fill = gorszy z Q0/Q1 (netto po opłatach zawartych w quote) × (1 − haircut).
  `min_out` z Q0 i limitu slippage. Candidate < min_out → próba nieudana (nie przycinamy).
* **A14** Opłaty sieciowe w PAPER: quote bez takera nie daje opłat dla naszego walleta,
  więc używamy modelu z konfiguracji: base fee 5000 lamportów/podpis (1 podpis),
  priority fee z konfiguracji (domyślnie 100 000 lamportów/próbę — **założenie
  do kalibracji na canary**). Każdy wpis kosztu ma `source=MODEL_ESTIMATE`.
* **A15** Nieudana próba po fazie „wysłania” kosztuje base + priority fee (model).
  Błąd quote przed wysłaniem nie kosztuje nic on-chain.
* **A16** Rent rachunku tokenowego: z `rentFeeLamports` quote, jeśli obecne i >0;
  inaczej model 2 039 280 lamportów (rachunek SPL 165 B) — dla Token-2022 z rozszerzeniami
  rent jest większy; bez odczytu rozmiaru z RPC → token odrzucony (`RENT_UNKNOWN`).
  Rent → `rent_locked_sol`, nie koszt. Odzysk po pełnym wyjściu: zamknięcie rachunku
  kosztuje base fee; odzysk nie jest zyskiem handlowym.
* **A17** Opłaty zawarte w quote zapisujemy w `fee_items` z `included_in_quote=true`
  i **bez** wpisów w księdze (są już w ilości).
* **A18** Paper nie „kupuje” SOL na gaz. Opłaty gazowe zmniejszają wirtualne SOL.

## Księga

* **A19** Księga wieloaktywowa z kontami rozliczeniowymi (trading accounts): w każdej
  transakcji suma wpisów **per aktywo** = 0. Swap: `wallet:USDC −x`, `clearing:swap:USDC +x`,
  `clearing:swap:TOKEN −y`, `wallet:TOKEN +y`. Wartość USD liczona osobno z kursów.
* **A20** Metoda kosztu: pozycja v1 bez dokupień (jedno wejście, jedno pełne wyjście),
  więc koszt = pełny koszt wejścia (USDC + opłaty modelowe w USD po kursie SOL z chwili fillu).
  Rent nie jest w koszcie nabycia (jest aktywem zablokowanym).
* **A21** Tolerancja tożsamości księgowej: 0 jednostek raw dla sald; 0,000001 USD
  dla sum USD (zaokrąglenia decimal).

## Ryzyko

* **A22** Loss triggery liczone na `equity_total_lower_bound` z wycen świeżych;
  pozycja z `NO_ROUTE` (API sprawne) wchodzi z wyceną 0 (ekonomicznie niesprzedawalna).
  Pozycja z `PROVIDER_UNAVAILABLE` nie jest liczona jako strata — sesja przechodzi
  w `PAUSED_DATA` (wejścia wstrzymane) i trigger jest oceniany po powrocie danych.
* **A23** Dzienny trigger: próg = min(20 USD, 4% equity na początku doby UTC),
  strata = equity(start doby) − equity(teraz).
* **A24** Rezerwa SOL: nowe wejście wymaga, aby po nim zostało SOL na: wyjście każdej
  otwartej pozycji (emergency fee cap 0,50 USD lub model fee, większe) + zamknięcie
  rachunków + margines z konfiguracji.

## Strategia i portfele

* **A25** Bez implementacji `WalletHistoryProvider`/`HistoricalPriceProvider` (G4)
  sesja `confluence_v1` nie może przejść z `BOOTSTRAP` do `READY`: stan
  `INSUFFICIENT_DATA` z listą braków.
* **A26** Klastry: union-find na krawędziach z `confidence >= medium` i ważnych w oknie
  30 dni; krawędzie „wspólny funder” wymagają warunku (b) z briefu; funder o nieznanym
  charakterze → brak krawędzi, ale **też brak deklaracji niezależności**: portfel bez
  wykonanego sprawdzenia powiązań ma status `UNKNOWN` i nie liczy się do sygnału.
