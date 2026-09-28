# Kontrakty dostawców

Źródło weryfikacji dla Jupiter: repozytorium `github.com/jup-ag/docs`, commit
`956fe0536bc43d40f96757deb01edbe7381a2f4c` (2026-09-28), pliki
`openapi-spec/swap/v2/swap.yaml`, `openapi-spec/tokens/v2/tokens.yaml`,
`openapi-spec/price/v3/price.yaml`, `swap/order-and-execute.mdx`,
`portal/rate-limits.mdx`, `portal/plans.mdx`, `tokens/token-information.mdx`.

Źródło dla Helius: `github.com/helius-labs/helius-sdk`, commit
`b76a792979dc4581c8fd1e98121137a6530673d6` (2026-09-08), `src/enhanced/types.ts`,
`src/types/webhooks.ts`, `llms.txt`. Strona `helius.dev/docs` była niedostępna
(egress, patrz `ACCESS_GAPS.md` G1) — pola oznaczone *(SDK)* pochodzą z typów SDK,
nie z opisu API.

Token-2022: pakiet npm `@solana-program/token-2022@0.19.0` (enum `ExtensionType`
0–28, adres programu `TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb`).

**Żaden kontrakt nie został potwierdzony wywołaniem na żywo** (brak klucza + egress).
Fixtures w `packages/providers/test/fixtures/` mają pole `fixture_origin`:
`openapi-example` (skopiowane/złożone z przykładów OpenAPI) albo `synthetic-test`
(skonstruowane do testu przypadku brzegowego). Nie ma fixtures `recorded-live`.

---

## Jupiter — `GET https://api.jup.ag/swap/v2/order`

| Pole | Wartość |
|---|---|
| Auth | nagłówek `x-api-key` (bez klucza: tryb Keyless 0,5 RPS) |
| Plan | Keyless 0,5 RPS / Free 1 RPS / Developer 10 RPS (25 USD/mies.); limit **per organizacja**, okno przesuwne 60 s |
| Koszt | 1 kredyt (Free: kredyty bez limitu, tylko RPS) |
| Parametry używane | `inputMint`, `outputMint`, `amount` (raw, string), `slippageBps` = limit strony (100 entry / 150 exit / 300 emergency); **bez** `taker` w PAPER; `swapMode=ExactIn` jest jedyną wartością |
| Profil | `jupiter_order_manual_v1`: podanie `slippageBps` przełącza `mode` na `manual` (może zawęzić routing). Ten sam profil ma być użyty w LIVE, bo LIVE musi egzekwować limit slippage — dlatego PAPER nie mierzy trybu `ultra`. Odpowiedź z `mode != "manual"` → `QUOTE_NOT_COMPARABLE` |
| Parametry NIE używane | `referral*` (zakaz z briefu), `payer`, `priorityFeeLamports`, `jitoTipLamports`, `excludeRouters`, `excludeDexes` |
| Odpowiedź bez `taker` | quote **bez transakcji** (`transaction: null`). Nie potwierdza wykonania dla żadnego walleta → `execution_fidelity = QUOTE_ONLY_NO_TAKER` |
| Jednostki | `inAmount`, `outAmount`, `otherAmountThreshold`, `platformFee.amount` — string, jednostki raw mintu; `priceImpact` — **punkty procentowe** (−0.1 = −0.1%), `feeBps` / `platformFee.feeBps` — bps; `*Lamports` — lamporty; `inUsdValue`/`outUsdValue`/`swapUsdValue` — number USD (tylko informacyjnie, nie do księgi) |
| Czas | brak czasu serwera w odpowiedzi; `totalTime` = czas odpowiedzi ms. Zapisujemy własne `requested_at`, `received_at`. Brak slotu (poza `lastValidBlockHeight`, tylko z takerem) |
| Pola krytyczne (brak → `QUOTE_FIELD_MISSING`, blokada) | `inputMint`, `outputMint`, `inAmount`, `outAmount`, `router`, `swapMode`, `priceImpact`, `feeBps`, `feeMint`, `routePlan[]` (ze `swapInfo.inAmount/outAmount`) |
| Pola niekrytyczne | `platformFee` (brak → traktowane jako „opłata nieznana”, patrz niżej), `signatureFeeLamports`, `prioritizationFeeLamports`, `rentFeeLamports` (bez takera nie są specyficzne dla naszego walleta → model opłat z konfiguracji, `source=MODEL_ESTIMATE`) |
| Semantyka opłat | Opłata platformy „jest zawarta w quote i pobierana automatycznie”. Pobierana w jednym mincie (`feeMint`). Stawki zależą od pary i wieku tokena (np. 10 bps „everything else”, 50 bps „new tokens within 24 h”) — **czytamy z odpowiedzi, nie wpisujemy na stałe**. `feeBps` (łączna) może być > `platformFee.feeBps` (np. gasless recoup). |
| Niejednoznaczność | Dokumentacja nie mówi wprost, czy `outAmount` jest przed czy po opłacie pobieranej w mincie wyjściowym. **Rozstrzygamy per odpowiedź**: suma `routePlan[].swapInfo.outAmount` dla `outputMint` (wynik trasy) porównana z `outAmount`. Jeśli `outAmount == route_out − platformFee.amount` → netto; jeśli `outAmount == route_out` i `feeMint == outputMint` z niezerową opłatą → brutto, odejmujemy raz; inny przypadek → `QUOTE_SEMANTICS_UNRESOLVED`, blokada. Symetrycznie dla `feeMint == inputMint` (porównanie z `inAmount`). Test kontraktowy na żywo ma to potwierdzić. |
| Błąd biznesowy | 400 `{error}` (np. brak trasy) → `NO_ROUTE` tylko gdy treść błędu to rozpoznany brak trasy; 429 → `RATE_LIMITED`; 5xx/timeout/sieć → `PROVIDER_UNAVAILABLE`. Nierozpoznany 400 → `PROVIDER_ERROR` (nie mylimy z NO_ROUTE) |
| Nagłówki limitu | `x-ratelimit-remaining` (może być ujemne), `x-ratelimit-current`, `x-ratelimit-reset` (unix s) — tylko na 200 i 429 |
| Przechowywanie | pełny JSON odpowiedzi + `raw_payload_hash` (sha256) w `quotes.raw_payload`. Licencja/komercjalizacja: **niezweryfikowane** — tylko użytek prywatny |
| Fallback | brak. Awaria → `PROVIDER_UNAVAILABLE` → `PAUSED_DATA` dla wejść; pozycje oznaczone `UNKNOWN_VALUATION` |
| Test kontraktu | `packages/providers/test/jupiter-order.contract.test.ts` |

## Jupiter — `POST https://api.jup.ag/swap/v2/execute`

Tylko LIVE (Etap F). W PAPER/SHADOW/DEMO **zablokowane w warstwie transportu**
(`ReadOnlyTransport` odrzuca ścieżkę `/execute` i metody RPC `sendTransaction`,
`sendRawTransaction`, `sendBundle`, `simulateBundle`). Kody błędów z dokumentacji
(`-1…-3`, `-1000…-1004`, `-2000…-2004`) i pola `totalInputAmount` /
`totalOutputAmount` / `inputAmountResult` / `outputAmountResult` zapisane na potrzeby
Etapu F; księgowanie LIVE i tak z pre/post balances transakcji, nie z tej odpowiedzi.

## Jupiter — Tokens v2

| Endpoint | Koszt | Użycie | Uwagi |
|---|---|---|---|
| `GET /tokens/v2/recent` | 1 kredyt | discovery co 60 s | **„recent” = czas pierwszej puli, nie utworzenia mintu**. Domyślnie 30 mintów — lista ograniczona, nie pokrycie rynku |
| `GET /tokens/v2/search?query=<m1,m2,...>` | 10 kredytów | metadane kandydatów co 30 s, do 100 mintów w zapytaniu | |
| `GET /tokens/v2/toptraded/5m?limit=` | 5 kredytów | opcjonalne discovery | wyklucza „generic top tokens” |

Pola `MintInformation` używane: `id`, `decimals`, `tokenProgram`, `firstPool.{id,createdAt}`,
`liquidity` (USD, suma pul wg Jupiter), `stats5m.{buyVolume,sellVolume,numSells,priceChange}`,
`holderCount`, `audit.{mintAuthorityDisabled,freezeAuthorityDisabled,topHoldersPercentage (0–100)}`,
`dev`, `mcap`, `fdv`, `updatedAt`.

Ograniczenia definicji (dlaczego nie są podstawą twardych filtrów):

* `holderCount`, `audit.topHoldersPercentage` — definicja dostawcy **nie mówi**, czy liczy
  rachunki tokenowe czy właścicieli, ile to „top holders” ani co wyłącza. Brief wymaga
  top 10 **beneficjentów** z konsolidacją właściciela → liczymy sami z RPC (Helius DAS
  `getTokenAccounts` po mincie), wartości Jupiter zapisujemy informacyjnie.
* `stats5m.numSells` — liczba transakcji sprzedaży wg Jupiter; brak informacji o
  „udanych” — przyjmujemy jawnie jako proxy z oznaczeniem `definition=jupiter.stats5m.numSells`.
* `mintAuthority`/`freezeAuthority` z Jupiter — tylko informacyjnie; decyzja z odczytu mintu RPC.

## Jupiter — Price v3 `GET https://api.jup.ag/price/v3?ids=`

1 kredyt, do 50 mintów. Pola `usdPrice`, `blockId`, `liquidity`, `decimals`. Tokeny bez
wiarygodnej ceny są **pomijane** w odpowiedzi → brak klucza = `UNKNOWN_VALUATION`.
Używane do SOL/USD i USDC/USD (wycena rezerwy gazu i benchmarków), **nie** do fillów.

## Helius — RPC `https://mainnet.helius-rpc.com/?api-key=…`

Klucz w URL → **redakcja w logach** (`redactUrl`). Metody używane read-only:
`getAccountInfo` (base64; mint, token program, rozszerzenia), `getMultipleAccounts`,
`getMinimumBalanceForRentExemption`, `getSlot`, `getBalance`, DAS `getTokenAccounts`
(paginacja po `mint`, do liczenia holderów wg właściciela). Limity Free: RPC 10 req/s,
DAS/Enhanced 2 req/s, 1 M kredytów/mies. *(SDK llms.txt)*.

Metody **zabronione** w PAPER (blokada transportu): `sendTransaction`,
`sendRawTransaction`, `requestAirdrop`, `sendBundle`, `simulateBundle`.
`simulateTransaction` dozwolone wyłącznie w SHADOW (Etap F).

## Helius — Enhanced Transactions *(SDK)*

`GET https://api-mainnet.helius-rpc.com/v0/addresses/{address}/transactions`
z `type`, `gteTime`/`lteTime`, `beforeSignature`, `commitment` (`confirmed`|`finalized`),
`limit`, `sortOrder`. `POST /v0/transactions` (parsowanie po sygnaturach).
Pola: `signature`, `slot`, `timestamp` (unix s), `type`, `source`, `fee`, `feePayer`,
`tokenTransfers[].{fromUserAccount,toUserAccount,mint,tokenAmount,decimals?}`,
`nativeTransfers[]`, `transactionError`, `events` (**kształt otwarty** — SDK:
„shape differs per program”). `tokenAmount` bywa UI albo raw („UI amount or raw”)
→ normalizacja wymaga `decimals` z mintu; brak pewności → `AMOUNT_UNIT_AMBIGUOUS`.

## Helius — Webhooks *(SDK)*

`POST https://api-mainnet.helius-rpc.com/v0/webhooks?api-key=` z `webhookURL`,
`transactionTypes`, `accountAddresses`, `webhookType` (`enhanced`|`raw`|…), `authHeader`,
`txnStatus`. Autentyczność: porównanie nagłówka `Authorization` z `authHeader` (stały
sekret, porównanie w czasie stałym). Dostarczenie może się powtórzyć → deduplikacja
(`network + signature + leg_index + owner`). Webhook może zostać automatycznie
wyłączony przy wysokim failure rate → monitorowane przez heartbeat.
Parsed Streams: **niezweryfikowane** (G6).

## Birdeye

Nieużywany (G7). Brak adaptera.

---

## Budżet zapytań Jupiter (PAPER, 1 organizacja)

| Źródło | Częstotliwość | RPS |
|---|---|---|
| Sell quote otwartych pozycji (4 × co 5 s) | stałe | 0,80 |
| Discovery `recent` (co 60 s) | stałe | 0,017 |
| Metadane `search` (co 30 s) | stałe | 0,033 |
| Price v3 SOL/USDC (co 30 s) | stałe | 0,033 |
| **Suma stała** | | **≈ 0,88** |
| Próba wejścia: Q0 buy, Q0 reverse, Q1 buy, Q1 reverse | impuls ~2 s | +2 |
| Quote analityczne +5 s / +15 s | impuls | +2 na sygnał |

Free (1 RPS / 60 na minutę, okno przesuwne) daje ≈ 7 zapytań/min zapasu przy 4 pozycjach
— **niewystarczające** na wejścia i retry. Rate limiter ma kolejki priorytetowe:
`EXIT > RECONCILE > ENTRY > ANALYTICS > DISCOVERY`; brak budżetu wstrzymuje najniższe
priorytety, nigdy monitoringu pozycji.

---

## Wynik weryfikacji read-only (2026-09-28, sesja implementacyjna)

`pnpm --filter @solbot/worker check-providers` (bez kluczy):

```
FAILED  jupiter.price.v3 SOL,USDC                                   :: PROVIDER_ERROR: HTTP 403
FAILED  jupiter.tokens.v2.recent                                    :: PROVIDER_ERROR: HTTP 403
FAILED  jupiter.swap.v2.order quote-only 1 USDC->SOL (manual)       :: PROVIDER_ERROR: HTTP 403
SKIPPED_NO_KEY helius.*
```

403 pochodzi z proxy egress środowiska deweloperskiego (G1), nie od Jupiter. Status adapterów:
`IMPLEMENTED`, `TESTED_WITH_FIXTURES`; **nie** `VERIFIED_READ_ONLY_MAINNET`.
Pierwsze uruchomienie w środowisku z dostępem do sieci ma rozstrzygnąć semantykę `outAmount`
vs opłata w mincie wyjściowym (normalizer i tak weryfikuje ją per odpowiedź).
