# Forex — nowe hipotezy z informacją spoza samej ceny M1 (EURUSD, XAUUSD)

Zasada badania (ustalona przed uruchomieniem): parametry wybierane **wyłącznie na styczniu–marcu 2026** (IS),
sprawdzane bez zmian na **sierpniu–wrześniu 2026** (OOS). Kwiecień–lipiec jest pobierany jako trzeci niezależny okres.
Koszty jak wcześniej: spread z danych, prowizja 0,7 pipsa, poślizg 0,5 pipsa przy publikacjach.

Kalendarz BLS z bls.gov jest niedostępny z tego środowiska (403), więc zdarzenie rozpoznajemy z danych **po fakcie**:
świeca M1 o 08:30 Nowy Jork (publikacje USA: NFP, CPI, PPI, sprzedaż, PKB, zasiłki) z ruchem ≥ k × ATR(30, M1)
sprzed publikacji. Decyzja zapada dopiero po zamknięciu tej świecy — bez zaglądania w przyszłość. Strefa i czas letni
są liczone przez `zoneinfo` (08:30 NY = 12:30 UTC latem, 13:30 UTC zimą; test w `tests/test_scalp.py`).

## Hipoteza 1: reakcja na publikację o 08:30 NY (`news_reaction_v1`, `goldbot/event_study.py`)

### Ile jest zdarzeń

| okres | dni z 08:30 | szoki ≥ 3 ATR | szoki ≥ 4 ATR (i ≥ 5 pipsów) |
|---|---|---|---|
| EURUSD sty–mar | 60 | 7 | 5 |
| EURUSD sie–wrz | 38 | 7 | 7 |
| XAUUSD mar–wrz | ~140 | 17 | 14 |

**Wyraźnych szoków jest 1–2 miesięcznie na parę.** Strojenie progów na 5–7 zdarzeniach to wróżenie, więc zamiast siatki:
badanie zdarzeń (wynik ruchu po 5/15/30/60 min od wejścia, po spreadzie i prowizji), momentum vs fade.

### Wynik badania zdarzeń, szok ≥ 3 ATR, wejście 2 min po świecy zdarzenia (pipsy po kosztach, % trafień)

| | +5 min | +15 min | +30 min | +60 min |
|---|---|---|---|---|
| EURUSD sty–mar, **fade** | +0,6 (29 %) | +4,2 (57 %) | +3,5 (43 %) | +2,5 (57 %) |
| EURUSD sie–wrz, **fade** | +2,3 (86 %) | +1,0 (71 %) | +4,8 (57 %) | +6,1 (71 %) |
| EURUSD sty–mar, momentum | −2,6 | −6,2 | −5,5 | −4,5 |
| EURUSD sie–wrz, momentum | −4,2 | −2,9 | −6,7 | −8,0 |
| XAUUSD mar–wrz, momentum | +3,5 | −8,8 | +5,1 | +4,9 |
| XAUUSD mar–wrz, fade | −22,5 | −10,2 | −24,1 | −23,9 |

* **EURUSD: po szoku z danych USA cena w obu okresach częściej wraca, niż kontynuuje** — fade dodatni, momentum ujemne,
  ten sam znak w styczniu–marcu i sierpniu–wrześniu. To pierwszy wynik w tym projekcie, który ma ten sam kierunek w dwóch
  rozłącznych okresach bez dopasowywania.
* **Złoto: brak spójnego kierunku** — znak zmienia się z horyzontem, rozrzut setek pipsów. Odrzucone dla tej hipotezy.
* **Istotność statystyczna: brak.** Łącznie 14 zdarzeń EURUSD; fade +30 min: średnio +4,2 pipsa, odch. std 14,1, **t = 1,11**;
  +60 min: +4,3 pipsa, t = 1,20. Przy |t| < 2 wynik jest zgodny z przypadkiem.

### Symulacja w silniku (parametry z góry: fade, ≥ 3 ATR, wejście +2 min, SL 15 / TP 15 pipsów, wyjście po 60 min)

| okres | ryzyko | trans. | zwrot | max DD | win % | PF | wyjścia |
|---|---|---|---|---|---|---|---|
| IS sty–mar | 1 % | 7 | +0,1 % | 2,0 % | 57 | 1,04 | 3 TP / 2 SL / 2 czas |
| IS sty–mar | 2 % | 7 | +0,7 % | 4,4 % | 57 | 1,18 | |
| **OOS sie–wrz** | 1 % | 7 | **+1,2 %** | 0,9 % | 57 | 2,14 | 3 TP / 1 SL / 3 czas |
| **OOS sie–wrz** | 2 % | 7 | **+2,3 %** | 2,5 % | 57 | 1,84 | |

Dodatnie w obu okresach po kosztach, niski drawdown — ale **14 transakcji w 5 miesięcy** to ~1,4 miesięcznie.
To nie jest bot dzienny, a statystycznie wciąż może to być przypadek.

## Hipoteza 2: wybicie londyńskie tylko po skompresowanej nocy (`lb_compress_*`)

Zakres azjatycki < r × mediana z 20 poprzednich dni (parametry z góry: r = 0,8 i 0,6; reszta jak bazowe wybicie, 2 % ryzyka).

| wariant | sty–mar trans. | sty–mar zwrot | sty–mar PF | sie–wrz trans. | sie–wrz zwrot | sie–wrz PF |
|---|---|---|---|---|---|---|
| bez filtra | 47 | −29,2 % | 0,42 | 34 | +7,3 % | 1,25 |
| kompresja < 0,8× | 15 | **−0,9 %** | 0,94 | 4 | +5,3 % | 3,69 |
| kompresja < 0,6× | 5 | −2,9 % | 0,46 | 0 | — | — |

Filtr usuwa prawie całą stratę stycznia–marca (−29 % → −1 %), ale zostawia 4–15 transakcji. **To filtr strat, nie przewaga.**

## Wniosek i plan

1. Jedyna hipoteza z tym samym kierunkiem w dwóch rozłącznych okresach: **fade po szoku z danych USA na EURUSD**.
   Jest za rzadka i za mało zdarzeń, żeby była istotna (t ≈ 1,1).
2. Żeby rozstrzygnąć, potrzeba **więcej zdarzeń, nie więcej parametrów**: kwiecień–lipiec EURUSD (trzeci okres, w toku),
   inne pary reagujące na dane USA (GBPUSD, USDJPY, USDCHF) i inne pory publikacji (10:00 NY: ISM, JOLTS, zaufanie konsumentów;
   14:00 NY w dni FOMC). Przy 4 parach × 3 porach × 9 miesięcy można dojść do 100+ zdarzeń — wtedy t > 2 albo hipoteza upada.
3. Bot dzienny w sensie „codziennie handluje” nie wyłania się z żadnej z hipotez. Realistyczny kształt to bot, który
   **codziennie czuwa** i handluje tylko w dni, w których zachodzi warunek (publikacja + szok), 1–5 razy w miesiącu na parę.
