# Testy hipotez (2026-09-28) — wyniki

Dane: Birdeye (pakiet Standard), cache lokalny; kod `apps/worker/src/experiments-cli.ts`.
Kryterium ustalone PRZED wynikami: wariant przechodzi tylko, jeśli przy poślizgu 1,5% na stronę i opóźnieniu 15 s
suma jest > 0 w obu połowach okresu ORAZ > 0 po usunięciu najlepszej transakcji (min. 10 transakcji).

| # | Test | Próba | Najlepszy wariant (1,5%) | Wynik |
|---|---|---|---|---|
| 1 | Siatka wyjść: SL −10/−20/−30%/brak × TP +25%/brak × 30/60/240 min + wyjście za liderem (30 wariantów) | AAN5n1: 91 zakupów; sygnały 2 portfele/180 s: 25 | AAN5n1 „bez SL/TP, 60 min”: −0,37 USD/transakcję (przy 0,5%: +0,13) | 0/60 PASS |
| 2 | Luźniejsza zbieżność (3 portfele w 1 h / 6 h) | 10 / 39 sygnałów | −1,20 USD/transakcję | 0/8 PASS |
| 3 | Filtr „nie goń” (cena ≤ +5% vs 30 s wcześniej) | 48 / 25 z danymi 1 s | 2 portfele, spokojne, wyjście za liderem: ≈ 0 (pierwsza połowa +, druga −) | 0/6 PASS |
| 4 | Pierwszy zakup tokena vs dokładanie (AAN5n1) | 32 / 59 | dokładanie, za liderem: −0,90 USD | 0/4 PASS |
| 5 | Walk-forward: liderzy wybrani tylko z tygodnia 1, kopiowani w tygodniu 2 | 1 lider spełnił regułę (+79 tys. USD zrealizowane w tyg. 1), 27 zakupów | własne wyjścia: −1,56 USD | 0/6 PASS |

Obserwacje:
- Po sygnałach „2 portfele w 180 s” bez stopu cena spadała średnio o ~30–35% w ciągu godziny — kopia była płynnością wyjścia.
- Jedyne warianty bliskie zera zależą od optymistycznego poślizgu 0,5% albo od jednej połowy okresu.
- W teście 5 tylko jeden portfel spełnił regułę wyboru — test słaby, ale spójny z pozostałymi.

Łącznie z pilotem (20 scenariuszy) i testem confluence (12): **ok. 110 wariantów, żaden nie przeszedł.**
Zużycie Birdeye w testach: ok. 11 tys. CU z 30 tys. (pakiet Standard).
