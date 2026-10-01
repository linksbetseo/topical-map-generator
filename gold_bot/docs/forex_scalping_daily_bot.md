# Bot "daily" na forex, dźwignia 1:50, scalping — co da się zrobić i o co się to rozbija

Stan na 1 października 2026. Liczby z danych, nie z folderów brokerów.

## 1. Co zmierzyłem na EURUSD (Dukascopy, ticki i M1, 15 września 2026)

| godzina UTC | ticków / h | spread (mediana / p95 / max) | zakres godziny | mediana zakresu świecy M1 |
|---|---|---|---|---|
| 13:00 (Londyn + NY) | 4 447 | 0,20 / 0,40 / 0,70 pipsa | 12,5 pipsa | 1,1–1,3 pipsa |
| 02:00 (Azja) | 1 019 | 0,30 / 0,50 / 0,70 pipsa | 5,7 pipsa | 0,4–0,6 pipsa |

Odstęp między tickami w sesji: mediana 0,3 s, maks. 17,7 s. To oznacza, że „scalp” na kilka pipsów trwa realnie
kilka–kilkanaście minut, bo świeca M1 ma ~1 pips zakresu. Nie ma mowy o setkach transakcji dziennie z Pythona.

## 2. Arytmetyka kosztów — to jest sedno

Konto ECN/raw: spread 0,2 pipsa + prowizja 3,5 USD/lot/stronę = 7 USD/lot w obie strony = **0,7 pipsa**.
Poślizg 0,1–0,2 pipsa na wykonanie. Razem **koszt okrągłej transakcji ≈ 1,1–1,3 pipsa**.

Próg rentowności (jaki procent trafień jest potrzebny, żeby wyjść na zero), koszt c = 1,2 pipsa:

| SL / TP (pipsy) | bez kosztów | **z kosztami** |
|---|---|---|
| 4 / 3 | 57 % | **74 %** |
| 5 / 5 | 50 % | **62 %** |
| 5 / 8 | 38 % | **48 %** |
| 8 / 12 | 40 % | **46 %** |

Wzór: p = (SL + c) / (SL + TP). Scalp 4/3 wymaga 74 % trafień **tylko po to, żeby nie tracić**. To jest powód,
dla którego „rozpisane czytanie świec” w ATAS-ie i inne podejścia scalpowe wyglądają dobrze na wykresie,
a słabo na rachunku: wykres nie pokazuje 1,2 pipsa, które znika przy każdym wejściu. Lepszy feed (dxFeed, footprint)
nie zmienia tej arytmetyki — zmienia tylko to, co widać. Dlatego w tym projekcie **koszty są liczone na każdej
transakcji w symulatorze**, a decyzję, czy strategia ma przewagę, podejmuje się po kosztach, nie przed.

Konsekwencja projektowa: cel zysku musi być **co najmniej 4–5× kosztów** (5–8 pipsów), a nie 2–3. To już nie jest
scalping w klasycznym sensie, tylko krótki intraday — i to jest to, co ma szansę działać z 500 USD i botem w Pythonie.

## 3. Dźwignia 1:50 — regulacje i co naprawdę daje

* **UE (ESMA, od 2018)**: klient detaliczny ma na głównych parach maks. **1:30**, na złocie 1:20, na krypto 1:2.
  1:50 w UE oznacza albo status klienta profesjonalnego (trzeba spełnić 2 z 3 kryteriów: portfel ≥ 500 tys. EUR,
  ≥ 10 istotnych transakcji na kwartał przez ostatnie 4 kwartały, ≥ 1 rok pracy w finansach na stanowisku wymagającym
  tej wiedzy — i traci się część ochrony, m.in. ochronę przed ujemnym saldem), albo brokera spoza UE (inny nadzór,
  brak ESMA, często brak gwarancji dla klienta). **Sprawdź to u konkretnego brokera przed założeniem konta.**
* Dźwignia nie zmienia ryzyka na transakcję — to ustala stop i wielkość pozycji. Dźwignia zmienia **depozyt**:

| | 1:30 | 1:50 |
|---|---|---|
| pozycja z 1 % ryzyka przy SL 4 pipsy (500 USD → 5 USD / 0,4 USD na pips) | 0,12 lota | 0,12 lota |
| wymagany depozyt na tę pozycję (EURUSD ≈ 1,15) | ~460 USD (92 % konta) | ~276 USD (55 % konta) |
| ile takich pozycji naraz | 1 (ledwo) | 1, max 2 przy mniejszym stopie |

Czyli przy 500 USD 1:50 jest wygodniejsze (nie blokuje całego konta na jednej pozycji), ale **nie pozwala ryzykować
więcej niż 1 % na transakcję, jeśli chcemy mieć więcej niż jedną pozycję**. 1:30 w praktyce też wystarczy przy 1 pozycji.

## 4. Co mikro-lot zmienia w porównaniu ze złotem

Na złocie minimum 1 oz przy SL 10 USD robiło stratę 10 USD = 2 % z 500 USD, więc bot odrzucał prawie wszystko.
Na EURUSD mikro-lot (0,01 lota = 1000 EUR) to 0,10 USD na pips: SL 4 pipsy = 0,40 USD. Przy 1 % ryzyka bot może
otworzyć 0,12 lota — **sizing wreszcie działa z 500 USD**. To największa realna zaleta forexu w tym projekcie.

## 5. Dlaczego backtest scalpu musi być na tickach

Przy świecy M1 o zakresie 1 pips i stopie 4 / celu 3 pipsy obie granice często mieszczą się w jednej świecy —
symulator na świecach nie wie, co było pierwsze (dotąd zakładaliśmy gorszy wariant). Dla scalpingu to już nie
„konserwatywne założenie”, tylko systematyczne zaniżanie wyniku o nieznaną wartość. Dukascopy udostępnia ticki
(godzinowe pliki, 20 B / tick) — silnik dostał tryb tickowy: SL/TP i wejścia sprawdzane na każdym ticku, decyzje
na zamknięciu M1. Plan: ten sam okres przeliczyć na M1 i na tickach i **zmierzyć różnicę** — jeśli jest duża,
wszystkie wcześniejsze wyniki scalpowe na M1 są do kosza.

Ograniczenie: Dukascopy dławi pobieranie do ~1 pliku / 1–2 min, więc ticki na miesiąc (24 pliki/dzień) to doba
pobierania w tle. Alternatywy darmowe do sprawdzenia: HistData.com (M1 i ticki w CSV, przez formularz),
TrueFX (ticki miesięczne, wymaga rejestracji).

## 6. Opóźnienie i wykonanie

Bot w Pythonie przez cTrader Open API z VPS w Londynie/Frankfurcie: 50–300 ms od ticku do zlecenia. Dla transakcji
trwających minuty to nieistotne; dla „scalpu” na 1–2 pipsy jest już istotne (poślizg ≈ cel). Kolejny argument za
celem 5–8 pipsów. Nie budujemy HFT i nie udajemy, że to możliwe z Pythona.

## 7. Jak wygląda „bot daily” w tym kodzie

* Okno wejść 07:00–16:00 UTC (Londyn + nakładanie z NY), **wszystko zamknięte o 16:30** (`session.flat_at`) — brak
  swapów, brak ryzyka nocnego, codzienny raport z dziennika.
* Stop czasowy (`max_hold_minutes`): scalp, który nie zadziałał w 30 min, jest zamykany — nie staje się „inwestycją”.
* Dzienny limit straty 3 % — po trzech stratach z rzędu bot kończy dzień.
* Brak wejść 20 min przed i po publikacjach (BLS/FOMC z kalendarza) i zamykanie pozycji przed nimi.
* Limit spreadu 0,4 pipsa — gdy spread się rozszerza (publikacje, noc), bot nie wchodzi.
* Konfiguracja: `config.eurusd.toml`, strategia `scalp_meanrev_v1` (odchylenie od EMA(M1) o k × ATR, wejście w stronę
  EMA, stałe SL/TP w pipsach) — pierwsza hipoteza do obalenia, nie rekomendacja.

## 8. Plan badania (w toku)

1. M1 EURUSD 3 sie – 25 wrz + ticki 15–17 wrz (pobieranie w tle).
2. `scalp_meanrev_v1` na M1 vs na tickach dla 15–17 wrz → pomiar błędu symulacji świecowej.
3. Siatka IS/OOS na M1 (SL 3/5, TP 2/3/5, stop czasowy 15/45, odchylenie 1,5/2/3) — z pełnym kosztem 1,2 pipsa.
4. Jeśli nic nie przechodzi OOS po kosztach — zmiana celu na 5–8 pipsów (krótki intraday), nie dalsze strojenie scalpu.
5. Dopiero potem: 7 dni paper na demo z brokerem, u którego faktycznie będzie konto (spread i prowizja tego brokera).

## Werdykt wstępny

Da się zbudować i uczciwie przetestować. Ale „scalping 2–3 pipsy” na koncie 500 USD z botem w Pythonie ma matematykę
przeciwko sobie (74 % trafień na zero). Rozsądna wersja tego pomysłu to bot daily z celem 5–8 pipsów, stopem czasowym,
flat o 16:30 i 1 % ryzyka — i to będziemy badać.
