# Rzeczywiste braki dostępu i danych

> **G5 rozwiązane (2026-09-28):** deploy na Railway wykonany na polecenie właściciela — publiczny
> HTTPS `https://api-production-9b38.up.railway.app` (webhook: `/api/webhooks/helius`). Szczegóły
> w `OPERATIONS.md`. Webhook Helius zostanie zarejestrowany po bootstrapie portfeli.
>
> **Poprawka paginacji Helius (2026-09-28):** pierwszy bootstrap na produkcji dał 0/50 kwalifikacji,
> bo strona krótsza niż 100 była brana za koniec historii. Helius z filtrem `type` zwraca krótkie
> strony (25–53) mimo starszych wyników — koniec historii to dopiero pusta strona. Historie były więc
> po cichu ucinane (czasem do ostatnich ~20 min). Naprawione w `HeliusEnhanced.history`.

> **G4 rozwiązane (2026-09-28):** historia portfeli = Helius Enhanced Transactions (`type=SWAP`,
> 30 dni), wycena nóg SOL/USDC = Binance public 1-min klines (`SOLUSDT`, `USDCUSDT`, bez klucza;
> założenie USDT≈USD). Kandydaci = kupujący tokenów z Jupiter toptraded (bez selekcji po zyskach).
> Próbny przebieg na żywo: 6 kandydatów, 0 zakwalifikowanych (m.in. bot arbitrażowy) — pełny
> bootstrap uruchamiać tuż przed T0. Koszt kredytów Helius Enhanced API — do potwierdzenia w panelu.
>
> **Stan 2026-09-28 (później):** G1–G3 rozwiązane — sieć odblokowana, klucze Jupiter (Free) i Helius
> (Free) zweryfikowane read-only na mainnet, Telegram działa. Nadal otwarte: **G4** (historia portfeli
> z historycznymi cenami USD), **G5** (publiczny HTTPS dla webhooka), G6–G8.

> Aktualizacja: klucze Jupiter i Helius są teraz **opcjonalne** dla PAPER w wariancie
> `config/paper.keyless.json` (patrz ASSUMPTIONS A31–A32). Nadal potrzebne: dostęp do sieci (G1),
> dane portfeli (G4) i odbiór zdarzeń portfeli (G5). Obserwowanie do 100 portfeli bez Helius
> (polling publicznego RPC) przekracza limity publicznego endpointu — dla `confluence_v1`
> webhook Helius (plan Free, bez opłat, ale z kluczem) pozostaje praktycznie niezbędny.

Stan na 2026-09-28 (sesja implementacyjna). Każdy punkt to realny bloker albo
ograniczenie. Nic z tej listy nie zostało „obejście” danymi fikcyjnymi.

| # | Brak | Skutek | Kto może usunąć | Status w kodzie |
|---|------|--------|-----------------|-----------------|
| G1 | **Egress sesji deweloperskiej blokuje** `api.jup.ag`, `developers.jup.ag`, `*.helius-rpc.com`, `helius.dev`, `docs.birdeye.so`, `api.mainnet-beta.solana.com`, `solana.com` (HTTP 403 z proxy). | Nie wykonano ani jednego wywołania read-only mainnet. Żaden adapter nie ma statusu `VERIFIED_READ_ONLY_MAINNET`. | Właściciel: ustawienia sieci środowiska Claude Code albo uruchomienie lokalnie/na Railway. | Adaptery mają testy kontraktowe na fixturach zbudowanych z OpenAPI (oznaczone `fixture_origin: "openapi-example"`), nie z nagranych odpowiedzi. |
| G2 | **Brak klucza Jupiter** (`JUPITER_API_KEY`). | Keyless = 0,5 RPS, Free = 1 RPS (limit per organizacja). Budżet PAPER (sekcja „Budżet zapytań” w `provider-contracts.md`) wymaga szczytowo ok. 1,5–3 RPS → **Free nie wystarcza** do pełnej sesji z 4 pozycjami + discovery. | Właściciel: utworzenie klucza; decyzja o planie Developer (25 USD/mies.) — **nie kupiono**. | Rate limiter z priorytetami (wyjścia > reconciliation > wejścia > discovery). Przy niedoborze budżetu discovery jest wstrzymywane, nie wyjścia. |
| G3 | **Brak klucza Helius** (`HELIUS_API_KEY`). | Brak RPC (mint/freeze authority, rozszerzenia Token-2022, rozkład holderów), brak historii portfeli, brak strumienia transakcji obserwowanych adresów. | Właściciel. | Adapter RPC + parser mintów gotowy i testowany na bajtach wygenerowanych oficjalnym enkoderem `@solana-program/token-2022`. |
| G4 | **Historia cen USD dla bootstrapu portfeli** (SOL/USD i token/USD w chwili transakcji z ostatnich 30 dni). Jupiter Price v3 daje tylko cenę bieżącą. | Bez tego nie da się policzyć PnL obcych walletów w USD dla swapów SOL↔token → kwalifikacja portfeli niemożliwa → sesja `confluence_v1` pozostaje `COLLECTING / INSUFFICIENT_DATA`. | Właściciel: decyzja o Birdeye (plan z historią cen) albo akceptacja wyprowadzania kursu z własnych zapisanych snapshotów (działa tylko do przodu, od chwili startu zbierania). | `WalletHistoryProvider` i `HistoricalPriceProvider` są interfejsami; brak implementacji → `DATA_REQUIREMENT_NOT_MET`. |
| G5 | **Publiczny HTTPS endpoint dla webhooków Helius** (wymaga deployu). | Bez niego zdarzenia obserwowanych walletów tylko przez WebSocket/polling. Na Free: Enhanced API 2 req/s, Enhanced WebSockets niedostępne. Wymóg „dostarczone ≤ 15 s” może nie być spełniony dla 100 portfeli. | Właściciel: zgoda na deploy (Railway) — **nie wykonano**. | Endpoint `POST /api/webhooks/helius` z autoryzacją i trwałym zapisem (Etap D). Opóźnienie mierzone (`received_at − block_time`), sygnały po 15 s odrzucane. |
| G6 | **Helius „Parsed Streams”** (następca Enhanced Webhooks wg briefu) — nie zweryfikowano: dokumentacja zablokowana (G1), a SDK `helius-sdk` (commit 2026-09-08) nie zawiera tego API. | Nie implementuję adaptera na zgadywanym kontrakcie. | Weryfikacja po odblokowaniu dokumentacji. | Brak adaptera — jawnie. |
| G7 | **Birdeye** (opcjonalny) — dokumentacja zablokowana, klucz brak. Znany z wyszukiwarki endpoint `GET /wallet/v2/pnl/multiple` (max 50 walletów) — **niezweryfikowane** pola/okno/jednostki. | Nie używany. Metryki kwalifikacji liczymy sami (sekcja 6 briefu). | Właściciel: decyzja o planie. | Brak adaptera — jawnie. |
| G8 | **Rejestr adresów infrastrukturalnych** (autorytety pul AMM, vaulty, burn, programy) do wyłączenia z koncentracji holderów. | Bez udokumentowanego rejestru nie wyłączamy nikogo → koncentracja top10 zawyżona → więcej tokenów odrzuconych (kierunek konserwatywny). | Do zbudowania z weryfikowanych źródeł (program owner kont on-chain). | `infra_registry` pusty, każdy wpis wymaga źródła i daty. |
| G9 | **Kanoniczny mint USDC** — `EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v` pochodzi z przykładów w oficjalnym OpenAPI Jupiter. Wymóg briefu: potwierdzić przed startem. | Stan `VALIDATING` sprawdza przez RPC: właściciel = SPL Token program, `decimals=6`, zgodność z konfiguracją. Bez RPC (G3) → start zablokowany. | — | Zaimplementowane jako bramka readiness. |
| G10 | **Kurs USDC/USD** — źródło niezależne od samego USDC. Jupiter Price v3 zwraca `usdPrice` dla USDC (np. 0.9999 w przykładzie), ale to cena z rynku DEX, nie wycena emitenta. | Używamy Jupiter Price v3 z oznaczeniem źródła; brak ceny → `UNKNOWN_VALUATION`, nie 1.0. | — | Zaimplementowane. |

## Co działa bez powyższych

* Cała logika domenowa, księga, risk engine, paper broker, maszyny stanów,
  controller sesji i raporty — testowane deterministycznie (fake clock, fixtures).
* Tryb `DEMO` na fixtures (wyraźnie oznaczony w każdym rekordzie i raporcie).
* Sesja typu `INFRA_TEST` (test infrastruktury bez oceny confluence) — możliwa
  po uzyskaniu G2+G3, nawet bez G4.

## Czego NIE zrobiono (celowo)

* Nie zakupiono żadnego planu, nie utworzono kont, nie podniesiono limitów.
* Deploy (Railway, PAPER) wykonano dopiero 2026-09-28 na polecenie właściciela.
* Nie podpisano ani nie wysłano żadnej transakcji; w repo nie ma signera.
