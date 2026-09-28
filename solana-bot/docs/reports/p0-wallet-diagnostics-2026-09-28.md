# Raport P0 — diagnostyka kandydatów bootstrapu (2026-09-28)

Sesja `ses_0mul742nr6861f92f77548816`. Przebiegi: `diag_20260928134911114` (wszystkie adresy) i `diag_20260928153212752` (ponowienie 33 adresów po poprawce transakcji v1). Okno historii: 30 dni do 2026-09-28T13:49:11.000Z.

**Adresów: 230.** To suma kandydatów z trzech przebiegów starego bootstrapu (50, 122, 129 adresów, dopisywanych do tej samej sesji), a nie tylko ostatnie 129.

Progi kwalifikacji bez zmian (30 zamkniętych epizodów, 20 tokenów, 7 dni, ≥5 przegranych, PF ≥ 1,2, dodatni wynik, pokrycie wyceny ≥ 90%, największy token ≤ 40% zysku). Kategorie diagnostyczne (UNSUPPORTED ≥ 10% zdarzeń, nieznany koszt ≥ 20% epizodów) służą wyłącznie do przypisania przyczyny, nie do kwalifikacji.

## Pierwszy powód odrzucenia (suma = liczba adresów)

| Kategoria | Pierwszy powód | Wszystkie naruszenia (nakładają się) |
|---|---:|---:|
| `API_ERROR` | 0 | 0 |
| `HISTORY_INCOMPLETE` | 110 | 110 |
| `PARSER_UNSUPPORTED` | 46 | 46 |
| `MISSING_PRICE` | 2 | 3 |
| `UNKNOWN_COST_BASIS` | 14 | 26 |
| `INSUFFICIENT_SAMPLE` | 43 | 102 |
| `NEGATIVE_PNL` | 7 | 67 |
| `LOW_PF` | 2 | 78 |
| `RISK_OR_COPYABILITY_FAIL` | 3 | 96 |
| `QUALIFIED_PROVISIONAL` | 3 | 3 |
| **Razem** | **230** | – |

Kolumna „wszystkie naruszenia” nie jest lejkiem: jeden adres może naruszać kilka kryteriów.

### Skąd `HISTORY_INCOMPLETE` / `API_ERROR`

| Przyczyna | Adresy |
|---|---:|
| skan sygnatur zatrzymany po 10 stronach (>10 000 sygnatur w 30 dni) | 57 |
| ponad 2000 transakcji w oknie – odroczone (limit na portfel) | 34 |
| odroczone – wyczerpany globalny budżet 40 000 wywołań | 19 |

Portfel z niekompletną historią **nie jest portfelem przegrywającym** — jest nieoceniony. Adresy z >10 000 sygnatur w 30 dni to w praktyce boty wysokiej częstotliwości (w próbie jeden miał 9 553 z 10 000 transakcji nieudanych).

## Rozkłady wśród 120 ocenionych adresów

p10 / p25 / p50 / p75 / p90

| Metryka | Rozkład |
|---|---|
| udane transakcje w 30 dni | 8 / 27 / 134 / 314 / 836 (max 1 050) |
| rozpoznane swapy (BUY+SELL) | 2 / 8 / 45 / 152 / 391 (max 878) |
| zamknięte epizody | 0 / 0 / 3 / 21 / 70 (max 248) |
| w tym epizody „dust” (< 1 USD) | 0 / 0 / 0 / 0 / 0 (max 43) |
| mediana kosztu zamkniętego epizodu, USD | 2.22 / 9.06 / 36.22 / 241.78 / 1 080.49 (max 152 015.20) |
| różne ryzykowne tokeny (zamknięte) | 0 / 0 / 2 / 14 / 39 (max 220) |
| aktywne dni UTC | 1 / 2 / 6 / 15 / 24 (max 31) |
| przegrane epizody | 0 / 1 / 4 / 22 / 56 (max 187) |
| udział zdarzeń UNSUPPORTED | 0 / 0 / 0.04 / 0.27 / 0.47 (max 0.60) |
| PnL zrealizowany, USD | -128.43 / -9.57 / 0 / 35.83 / 443.76 (max 220 853.46) |
| PnL łączny (otwarte po cenie Jupiter), USD | -323.44 / -66.03 / 0 / 108.56 / 2 593.40 (max 194 753.62) |
| profit factor | 0 / 0.02 / 0.67 / 2.32 / 12.40 (max 4 602.27) |

## Stara vs nowa księga (120 adresów ocenionych obiema)

- wynik dodatni w starej księdze: **18**, w nowej: **52**
- zmiana znaku wyniku: **44** adresów
- przyczyny potwierdzone testami: podwójne liczenie SOL przy WSOL + unwrap (mainnet `3RjgA7Vr…`), brak transferów w historii `type=SWAP`, ucinanie historii na krótkiej stronie, seryjne 429 oznaczane jako „historia niedostępna”.

## Kandydaci `QUALIFIED_PROVISIONAL`

Status oznacza tylko spełnienie historycznych progów przy poprawnej księdze. **Możliwość zarobienia przez naśladowcę nie została zmierzona** (`COPYABILITY_UNKNOWN`).

| Adres | Zamkn. epizody (dust) | Tokeny | Dni | PnL zreal. | Otwarte (cena Jupiter) | Otwarte (podłoga 0) | PF | Bez najlepszego tokena | Bez tokenów discovery | Mediana kosztu epizodu |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `53iMSndPda7tqFjYHb33FWWer9r8YaPJudWCSLHGoKHe` | 65 (0) | 30 | 17 | 366.79 | 2524.52 | -1533.46 | 1.636 | 773.88 | 3297.66 | 1080.49 |
| `AAN5n1Csdsin69XhpfAYzWGD283ZetuX2zf61uyGSw5s` | 192 (1) | 64 | 31 | 9653.97 | 0.00 | 0.00 | 2.832 | 5256.16 | 5647.87 | 1105.79 |
| `wDgV42GrPfMKtc8fR6rEx8tTNruQuQDnHtgDdUTDxY1` | 235 (0) | 220 | 7 | 248.19 | -1.00 | -3.73 | 1.358 | 1.60 | 247.19 | 13.21 |

Ocena wstępna:
- `AAN5n1…` — najmocniejszy: wynik w całości zrealizowany, 192 epizody, 64 tokeny, 31 dni, największy zwycięzca 8% zysku, bez najlepszego tokena nadal +5 256 USD. Duże pozycje (mediana ~1 100 USD) — kopiowalność przy naszych 25 USD i opóźnieniu nieznana.
- `53iMSn…` — słaby: zrealizowane tylko +367 USD, wynik łączny dodatni wyłącznie dzięki wycenie otwartych pozycji (przy podłodze zero −1 533 USD).
- `wDgV42…` — słaby: 220 tokenów w 7 dni (styl bota), mediana 13 USD, bez najlepszego tokena +1,60 USD — wynik zależy od jednego tokena.

Uwaga do interpretacji: portfel, którego dodatni wynik łączny zależy od wyceny otwartych pozycji po bieżącej cenie (a przy podłodze zero jest ujemny), jest kandydatem słabym — wycena Jupiter to nie wartość likwidacyjna.

## Blisko progów (bez problemów z danymi, tylko liczebność)

Adresów, których **jedynym** naruszeniem jest liczebność próby: **3**.

## Zużycie API

- Helius RPC: **41 292** wywołań ≈ 41 292 kredytów (1 kredyt / wywołanie; skan sygnatur: 1 000 sygnatur na wywołanie, `getTransaction`: 1 na transakcję). Trwały cache: kolejne przebiegi nie płacą ponownie za pobrane transakcje.
- Binance (klines 1m): 256 wywołań.
- Wcześniej tego dnia (stary bootstrap, 3 przebiegi Helius Enhanced po 100 kredytów): ok. 135 000 kredytów.

## Co wynika dla P1

- **Największa oceniona kategoria to `PARSER_UNSUPPORTED` (46)**: transakcje, w których zmienia się kilka ryzykownych tokenów naraz albo token i baza idą w tę samą stronę (swapy token–token, operacje na płynności, programy botów). Wymagają rozpoznania nóg ekonomicznych i wyceny z niezależnej kotwicy (np. świece Birdeye), zamiast odrzucania.
- **110 adresów nieocenionych**: 57 to boty HFT (nie są celem), 53 odroczone przez budżet — do dokończenia z cache po zwiększeniu limitu Helius.
- Losowi kupujący popularnych tokenów dają ~1% kandydatów spełniających progi; potrzebne źródła kandydatów z selekcją (ranking traderów, wcześni kupujący, grupa kontrolna) — z tą samą weryfikacją własną księgą.

## Czego ten raport nie mówi

- Czy naśladowca (wejście po sygnale, z opóźnieniem, własnymi wyjściami i kosztami) zarobiłby — wymaga kwotowań/świec z przeszłości albo danych zbieranych od teraz.
- Nic o adresach odroczonych z powodu budżetu (53) — do oceny po zwiększeniu budżetu Helius.
- Wybór kandydatów był przypadkowy (kupujący popularnych tokenów w ostatnich godzinach); to nie jest próbka „smart money”.

Pliki: `p0-wallet-diagnostics-2026-09-28.csv` (rekord na adres, wszystkie pola), kod: `apps/worker/src/diagnostics.ts`.
