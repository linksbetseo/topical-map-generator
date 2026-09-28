# Pilot: odtworzenie wyniku naśladowcy — `AAN5n1Csdsin69XhpfAYzWGD283ZetuX2zf61uyGSw5s` (2026-09-28)

Dane: Birdeye (pakiet Standard) — historia swapów tradera, świece 1 s (dostępne ~15 dni wstecz) i 1 min. Kod: `packages/strategy/src/copy-replay.ts`, `apps/worker/src/replay-cli.ts`.

Okno: 30 dni. Swapy lidera: 1235; zakupy ryzykownych tokenów za SOL/USDC/USDT: 562; po filtrach sygnału (≥ 100 USD, jeden wpis na token na 24 h): **91**. Pozycja 25 USD, opłaty 0,03 USD na stronę, SL −10%, TP +25%, trailing +15%/−8%, limit 4 h.

## Wyniki (naśladowca, po kosztach)

| Scenariusz | Wyjścia | Transakcje | Trafność | Suma USD | Średnio / transakcję | PF | Suma bez najlepszej |
|---|---|---:|---:|---:|---:|---:|---:|
| opóźnienie 0 s, poślizg 50 bps | własne SL/TP/4 h | 89 | 29% | -154.3 | -1.734 | 0.227 | -160.34 |
| opóźnienie 0 s, poślizg 50 bps | za liderem | 89 | 30% | -50.9 | -0.572 | 0.472 | -68.25 |
| opóźnienie 0 s, poślizg 150 bps | własne SL/TP/4 h | 89 | 19% | -192.48 | -2.163 | 0.156 | -198.2 |
| opóźnienie 0 s, poślizg 150 bps | za liderem | 89 | 20% | -94.06 | -1.057 | 0.264 | -110.57 |
| opóźnienie 5 s, poślizg 50 bps | własne SL/TP/4 h | 91 | 30% | -149.49 | -1.643 | 0.25 | -155.53 |
| opóźnienie 5 s, poślizg 50 bps | za liderem | 91 | 32% | -44.8 | -0.492 | 0.543 | -62.71 |
| opóźnienie 5 s, poślizg 150 bps | własne SL/TP/4 h | 91 | 21% | -189.91 | -2.087 | 0.166 | -195.63 |
| opóźnienie 5 s, poślizg 150 bps | za liderem | 91 | 22% | -89.07 | -0.979 | 0.314 | -106.13 |
| opóźnienie 15 s, poślizg 50 bps | własne SL/TP/4 h | 91 | 29% | -141.61 | -1.556 | 0.257 | -147.64 |
| opóźnienie 15 s, poślizg 50 bps | za liderem | 91 | 32% | -41.92 | -0.461 | 0.554 | -59.83 |
| opóźnienie 15 s, poślizg 150 bps | własne SL/TP/4 h | 91 | 21% | -181.08 | -1.99 | 0.178 | -186.8 |
| opóźnienie 15 s, poślizg 150 bps | za liderem | 91 | 22% | -86.25 | -0.948 | 0.315 | -103.31 |
| opóźnienie 30 s, poślizg 50 bps | własne SL/TP/4 h | 91 | 30% | -139.51 | -1.533 | 0.253 | -145.55 |
| opóźnienie 30 s, poślizg 50 bps | za liderem | 91 | 30% | -39.43 | -0.433 | 0.569 | -57.34 |
| opóźnienie 30 s, poślizg 150 bps | własne SL/TP/4 h | 91 | 20% | -178.71 | -1.964 | 0.173 | -184.43 |
| opóźnienie 30 s, poślizg 150 bps | za liderem | 91 | 20% | -83.81 | -0.921 | 0.323 | -100.87 |
| opóźnienie 60 s, poślizg 50 bps | własne SL/TP/4 h | 91 | 31% | -135.34 | -1.487 | 0.282 | -141.38 |
| opóźnienie 60 s, poślizg 50 bps | za liderem | 91 | 30% | -71.72 | -0.788 | 0.431 | -89.63 |
| opóźnienie 60 s, poślizg 150 bps | własne SL/TP/4 h | 91 | 21% | -178.26 | -1.959 | 0.188 | -183.98 |
| opóźnienie 60 s, poślizg 150 bps | za liderem | 91 | 19% | -115.46 | -1.269 | 0.268 | -132.52 |

## Wnioski

- **Wszystkie 20 scenariuszy są ujemne**, także bez opóźnienia i przy optymistycznym poślizgu.
- Opóźnienie nie jest przyczyną: przy danych 1 s cena wejścia kopii jest praktycznie równa cenie lidera (mediana różnicy 0%).
- Własne wyjścia (SL/TP/4 h) są wyraźnie gorsze niż wyjście za liderem (≈ −1,6 vs ≈ −0,5 USD na transakcję), ale i wyjście za liderem daje wynik ujemny (mediana zwrotu −1,5%).
- Zysk lidera w tym okresie (+9,7 tys. USD zrealizowane wg naszej księgi) nie przenosi się na kopiowanie jego pojedynczych zakupów w tej formie — prawdopodobnie pochodzi z pozycji dużych (mediana zakupu ~640 USD), dokładania, dłuższego trzymania lub par nieobjętych filtrem.

## Ograniczenia

- Jeden portfel, ~90 transakcji — mała próba; to nie jest test sygnału confluence (3 portfele w 180 s).
- Dla ~46% zakupów (starszych niż ~15 dni) wejście z najwyższej ceny świecy 1 min (konserwatywnie); oba podzbiory są ujemne.
- Świece nie odtwarzają kwotowań Q0/Q1 ani zmian trasy; poślizg to scenariusz, nie pomiar.
- Lider wybrany na danych z tego samego okresu (przeciek informacji działa na korzyść wyniku — mimo to wynik ujemny).
