# Historyczny test sygnału confluence (Birdeye, 2026-09-28)

Okno testu: 2026-09-14T16:12:13.000Z – 2026-09-28T12:07:13.000Z (14 dni; świece 1 s dostępne ~15 dni wstecz). Kandydaci: 60 (ranking Birdeye 30 dni po zysku zrealizowanym, min. 100 transakcji, zysk zrealizowany > 0 **i** łączny > 0, ≤ 3000 transakcji; + 3 portfele z diagnostyki P0). Pełne historie: 57 portfeli, 15708 transakcji ryzykownymi tokenami. Zużycie: ≈ 3890 CU, 350 zapytań.

## Liczba sygnałów (≥ 100 USD na portfel, 180 s, retencja ≥ 80%, 1 na token / 24 h)

- **3 portfele: 0** w 14 dni
- 2 portfele (diagnostycznie): 25

## Wynik naśladowcy dla sygnałów 2-portfelowych (25 USD, opłaty 0,03 USD/stronę)

| Opóźnienie | Poślizg | Wyjścia | Transakcje | Trafność | Suma USD | Średnio | Mediana | PF |
|---:|---:|---|---:|---:|---:|---:|---:|---:|
| 15 s | 150 bps | własne SL/TP/4 h | 25 | 28% | -72.59 | -2.904 | -1.138 | 0.267 |
| 15 s | 150 bps | za portfelami sygnału | 25 | 32% | -119.11 | -4.765 | -1.138 | 0.332 |
| 15 s | 50 bps | własne SL/TP/4 h | 25 | 52% | -56.92 | -2.277 | 0.035 | 0.366 |
| 15 s | 50 bps | za portfelami sygnału | 25 | 40% | -108.86 | -4.355 | -0.655 | 0.374 |
| 30 s | 150 bps | własne SL/TP/4 h | 25 | 28% | -115.17 | -4.607 | -2.897 | 0.158 |
| 30 s | 150 bps | za portfelami sygnału | 25 | 28% | -143.83 | -5.753 | -1.253 | 0.288 |
| 30 s | 50 bps | własne SL/TP/4 h | 25 | 48% | -99.29 | -3.971 | -0.261 | 0.219 |
| 30 s | 50 bps | za portfelami sygnału | 25 | 40% | -134.08 | -5.363 | -0.772 | 0.323 |
| 5 s | 150 bps | własne SL/TP/4 h | 25 | 36% | -72.87 | -2.915 | -1.219 | 0.269 |
| 5 s | 150 bps | za portfelami sygnału | 25 | 36% | -104.73 | -4.189 | -1.007 | 0.35 |
| 5 s | 50 bps | własne SL/TP/4 h | 25 | 56% | -53.62 | -2.145 | 0.383 | 0.388 |
| 5 s | 50 bps | za portfelami sygnału | 25 | 40% | -94.19 | -3.767 | -0.521 | 0.398 |

## Wnioski

- **Sygnał 3-portfelowy w tej populacji praktycznie nie występuje** (0 przez 14 dni wśród 57 najlepszych portfeli rankingu). Strategia w pierwotnej formie nie wygenerowałaby żadnej transakcji.
- Sygnał 2-portfelowy (25 zdarzeń) jest **ujemny we wszystkich 12 wariantach** — średnio od −2,1 do −5,8 USD na transakcję 25 USD; dominują stop-lossy. Część par to prawdopodobnie powiązane portfele (ta sama para powtarza się wielokrotnie; klastrów nie rozwiązywano).
- Razem z pilotem pojedynczego portfela (AAN5n1, 20/20 scenariuszy ujemnych): **brak śladu przewagi** dla kopiowania zakupów „dobrych” portfeli z opóźnieniem sekund, na poziomie 25 USD, z wyjściami SL/TP/4 h lub za liderem.

## Ograniczenia (większość działa NA KORZYŚĆ wyniku, który mimo to jest ujemny)

- Kandydaci wybrani z rankingu obejmującego okno testu (przeciek informacji).
- Brak filtrów tokena (wiek puli, płynność, bezpieczeństwo) — mogłyby zmienić skład transakcji.
- Małe próby (25 sygnałów, 91 transakcji w pilocie); świece zamiast kwotowań Q0/Q1.
- Ranking Birdeye to jedno źródło; nie testowano wczesnych kupujących (wymaga pakietu Starter).
