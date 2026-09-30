# Złoto — dopracowanie parametrów z podziałem in-sample / out-of-sample

Dane: Dukascopy XAU/USD M1, 3 sie – 25 wrz 2026. IS = 3 sie – 3 wrz (60 %), OOS = 3 – 25 wrz (40 %).
Ryzyko 5 %, dzienny limit 12 %, `max_spread = 1.0`, kalendarz FOMC. Ranking po IS wg netto / max obsunięcie
(zestawy < 10 transakcji odrzucone), potem sprawdzenie 5 najlepszych na OOS, którego nie widziały.

## trend_pullback_v1 (54 kombinacje)

| zestaw | IS trans | IS zwrot | IS DD | IS PF | OOS trans | OOS zwrot | OOS DD | OOS PF |
|---|---|---|---|---|---|---|---|---|
| bazowy (SL 1,5 / TP 2,0 / RSI 40) | 29 | +23,3 % | 8,4 % | 1,71 | 18 | +4,8 % | 11,5 % | 1,20 |
| #1 SL 1,5 / TP 2,0 / RSI 35 | 16 | +20,6 % | 4,7 % | 2,45 | 9 | +13,2 % | 6,1 % | 2,75 |
| #3 SL 1,5 / TP 3,0 / RSI 35 | 16 | +28,5 % | 8,6 % | 2,63 | 8 | +19,3 % | 6,5 % | 3,56 |
| #5 SL 1,5 / TP 3,0 / RSI 40 | 28 | +36,3 % | 10,2 % | 2,00 | 16 | +11,9 % | 11,4 % | 1,49 |

* Wszystkie zestawy z czołówki **utrzymały dodatni wynik na OOS** — to najważniejsze.
* Głębsza korekta (RSI 35 zamiast 40) i dalszy cel (TP 3 ATR) poprawiają jakość kosztem liczby transakcji
  (8–9 na OOS to bardzo mała próba; nie należy z tego wyciągać liczb, tylko kierunek).
* Filtr H4 `use_h4` True/False dał identyczne wyniki: w tym oknie H4 ani razu nie zmienił decyzji.

## breakout_v1 (36 kombinacji)

| zestaw | IS trans | IS zwrot | IS DD | IS PF | OOS trans | OOS zwrot | OOS DD | OOS PF |
|---|---|---|---|---|---|---|---|---|
| bazowy (SL 1,0 / TP 1,5 / N 8) | 133 | +48,0 % | 31,6 % | 1,18 | 75 | **−4,2 %** | 16,1 % | 0,95 |
| #1 SL 1,0 / TP 1,5 / N 16 | 107 | +59,0 % | 17,1 % | 1,27 | 52 | **−7,1 %** | 12,1 % | 0,87 |
| #2 SL 1,5 / TP 1,5 / N 16 | 101 | +62,2 % | 19,8 % | 1,29 | 49 | **−7,4 %** | 17,4 % | 0,89 |
| #3 SL 1,0 / TP 2,0 / N 8 | 116 | +84,0 % | 29,2 % | 1,31 | 62 | **−22,7 %** | 28,2 % | 0,71 |
| #4 SL 1,0 / TP 2,0 / N 16 | 90 | +20,6 % | 16,1 % | 1,16 | 46 | **−12,7 %** | 20,3 % | 0,77 |

* **Każdy** zestaw z czołówki IS traci na OOS. Im lepszy wynik IS, tym gorszy OOS (#3: +84 % → −23 %).
  To podręcznikowy obraz dopasowania do jednego rajdu: w sierpniu wybicia działały, we wrześniu (spadek + boczny) nie.
* Częste wejścia = 100–130 transakcji na miesiąc = 15–30 USD prowizji i spreadu z 500 USD. Przy PF ~1,2
  koszty zjadają przewagę, a przy PF < 1 nie ma czego zjadać.

## Decyzja

1. **`breakout_v1` w obecnej postaci odpada** jako strategia „częstych wejść” na złocie. Nie warto dalej stroić
   parametrów — to szukanie szumu. Sensowny kierunek dla częstych wejść to inna logika (np. wejście po korekcie
   wewnątrz dnia w kierunku trendu na M5), a nie inne liczby w tej samej.
2. `trend_pullback_v1` zostaje kandydatem. Nowy punkt wyjścia: **RSI 35, SL 1,5 ATR, TP 3,0 ATR** — ale to trzeba
   potwierdzić na dłuższej historii (pobieranie marzec–lipiec 2026 trwa) zanim zmienię domyślną konfigurację.
3. Filtr H4 do wycięcia albo wymiany, jeśli na dłuższych danych nadal nic nie zmienia.

Powtórzenie: `python -m goldbot optimize --data data/xauusd_m1.csv --config config.example.toml --risk-pct 5 --daily-loss-pct 12 --events examples/fomc_2026.csv` (z `max_spread = 1.0`).
