# Znane ograniczenia

1. **Brak weryfikacji na mainnet.** Żaden adapter nie został wywołany na żywo (G1–G3). Fixtures
   Jupiter są złożone ze schematu OpenAPI, Helius — z typów SDK. Pierwsze uruchomienie z siecią
   musi przejść `check-providers` i porównać semantykę opłat (`feeSemantics`).
2. **Brak bootstrapu portfeli.** Logika kwalifikacji/klastrów istnieje i jest testowana, ale nie ma
   dostawcy historii portfeli z historycznymi cenami USD (G4). Sesja `CONFLUENCE` kończy się w
   `INSUFFICIENT_DATA`. Możliwa jest sesja `INFRA_TEST` (bez oceny strategii).
3. **Strumień zdarzeń.** Tylko webhook Helius (Enhanced) — wymaga publicznego HTTPS (deploy).
   Parsed Streams niezweryfikowane. Normalizacja swapów jest konserwatywna: token↔token, brak
   decimals, brak nogi bazowej → odrzucone. `walletQty` (reguła dystrybucji) opiera się tylko na
   zaobserwowanych swapach od startu, nie na pełnym saldzie walleta.
4. **Opłaty sieciowe w PAPER są modelem** (5 000 lamportów base + 100 000 priority/próbę) — do
   kalibracji na canary. Quote bez `taker` nie jest specyficzny dla walleta.
5. **Rent Token-2022** szacowany dla konta 170 B (metadata-only mint); inne rozszerzenia i tak
   są odrzucane.
6. **Holderzy**: pełna paginacja DAS po mincie; tokeny z bardzo wieloma kontami (> 20 stron)
   → `HOLDER_DATA_UNAVAILABLE` (kierunek konserwatywny). Rejestr infrastruktury pusty → pule
   liczone jako holderzy (zawyżona koncentracja).
7. **Stress replay** liczy tylko pokrycie i efekt na wyjściu dla ilości BASE; PnL dla innej ilości
   = `REPLAY_UNSUPPORTED` (brak liniowego skalowania). Komponent losowych awarii STRESS/SEVERE
   nie jest odtwarzany w raporcie.
8. **Restart w trakcie próby** rozstrzygany konserwatywnie jako nieudana próba z szacowanym
   kosztem (nigdy ponowne wykonanie).
9. **Panel web** odłożony; dane przez API i raport HTML/Markdown.
10. **provider_usage** — tabela jest, liczniki kredytów nie są jeszcze zasilane per wywołanie.
11. **Telegram**: tylko powiadomienia wychodzące; dzienny raport wymaga harmonogramu (TODO:
    job po zakończeniu doby UTC).
12. **Stan dokładnie w T_end**: zapisywany przy pierwszym ticku ≥ T_end (z `observed_at`); przy
    ticku co 5 s różnica ≤ kilka sekund, ale jest jawnie pokazana.
