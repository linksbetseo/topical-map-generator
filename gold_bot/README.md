# Gold bot — prototyp XAU/USD (tylko symulacja)

Architektura zgodna z założeniami projektu:

```
Dane brokera (bid/ask M1) + kalendarz BLS/Fed
        ↓
Kontrola kompletności i aktualności         goldbot/data/quality.py, engine (luki, duplikaty, stale entry)
        ↓
Wskaźniki i sygnał liczone w Pythonie       goldbot/indicators.py, goldbot/strategy.py
        ↓
Opcjonalny filtr Jev (tylko blokuje)        goldbot/filters/jev.py
        ↓
Twarde reguły ryzyka w kodzie               goldbot/risk.py, goldbot/engine.py
        ↓
Symulator transakcji                        goldbot/simulator.py
        ↓
Dziennik decyzji, kosztów i wyników         goldbot/journal.py, goldbot/metrics.py
```

**Program nie wysyła prawdziwych zleceń.** Adapter cTrader ma listę dozwolonych komunikatów
(wyłącznie autoryzacja, symbole, notowania, świece), a test `tests/test_readonly.py`
pilnuje, żeby w kodzie nie pojawiło się żadne zlecenie handlowe.

Wymagania: Python 3.11+. Rdzeń nie ma zależności zewnętrznych.

## Szybki start

```bash
cd gold_bot
python -m unittest discover -s tests -t .           # 34 testy

# dane syntetyczne - WYŁĄCZNIE do sprawdzenia działania programu
python -m goldbot synth --days 45 --out data/synthetic_m1.csv
python -m goldbot backtest --data data/synthetic_m1.csv --config config.example.toml \
    --events examples/fomc_2026.csv --out runs/synth
```

### Dane historyczne z Dukascopy

Najprościej: pobierz M1 bid/ask z publicznego feedu Dukascopy (bez konta, format `.bi5`
sprawdzony empirycznie; pobieranie z odstępem 0,6 s i lokalnym cache, żeby nie dostać 429):

```bash
python -m goldbot fetch --start 2026-05-04 --end 2026-09-25 --out data/xauusd_m1.csv
python -m goldbot backtest --data data/xauusd_m1.csv --config config.example.toml \
    --events examples/fomc_2026.csv --splits 4 --out runs/real
```

Alternatywnie wyeksportuj XAU/USD M1 osobno dla BID i ASK (CSV) z narzędzia Dukascopy, potem:

```bash
python -m goldbot quality  --bid XAUUSD_BID.csv --ask XAUUSD_ASK.csv
python -m goldbot backtest --bid XAUUSD_BID.csv --ask XAUUSD_ASK.csv --config config.example.toml \
    --events examples/fomc_2026.csv --ics bls.ics --splits 4 --out runs/duka
```

Historia z Dukascopy to etap badawczy. Nie dowodzi wyniku u innego brokera. Ostateczny test
powinien używać notowań i kosztów docelowego instrumentu.

### Kalendarz

* `--ics` - plik ICS z harmonogramem BLS. Domyślnie brane są m.in. Employment Situation, CPI, PPI i JOLTS.
  Strefa `US-Eastern` jest przeliczana na UTC z uwzględnieniem czasu letniego.
* `--events` - CSV `time_utc,name,source`, np. `examples/fomc_2026.csv`.
  **Daty sprawdź na stronie Fed przed użyciem.**
* W oknie `blackout_before/after_minutes` bot nie otwiera nowych pozycji.

### Tryb paper

```bash
# test ścieżki "na żywo" bez sieci: świece -> ticki -> świece M1 -> silnik
python -m goldbot paper --source replay --data data/synthetic_m1.csv --out runs/paper

# cTrader Open API, konto demo, scope "accounts" (tylko odczyt)
pip install ctrader-open-api
export CTRADER_CLIENT_ID=... CTRADER_CLIENT_SECRET=... CTRADER_ACCESS_TOKEN=... CTRADER_ACCOUNT_ID=...
python -m goldbot paper --source ctrader --warmup historia_m1.csv --config config.example.toml --out runs/paper
```

Stan konta (saldo, pozycje, podjęte decyzje) jest zapisywany po każdej świecy do
`paper_state.json`. Po restarcie bot nie wchodzi drugi raz na tej samej decyzji.
**Adapter cTrader nie był jeszcze uruchomiony na prawdziwym koncie demo.** Przy pierwszym
starcie trzeba zweryfikować pola i jednostki specyfikacji (patrz docstring w `goldbot/data/ctrader.py`).

## Co symulator modeluje

* Long: wejście po **ask**, wyjście po **bid**; short odwrotnie. Spread jest zawarty w cenach, nie jest odejmowany drugi raz.
* Prowizja od strony, poślizg (niekorzystny) na wejściu, SL i zamknięciu rynkowym.
* Finansowanie overnight o godzinie rolloveru, potrójne w środę (wartości z konfiguracji).
* Decyzja na zamknięciu M15, wejście na **otwarciu następnej świecy M1**, bez zaglądania w przyszłość.
  H1/H4 są liczone wyłącznie z zakończonych świec.
* SL i TP w tej samej świecy M1: zawsze wariant niekorzystny (SL). Takie przypadki są liczone w raporcie (`ambiguous_sl_tp`).
* Luka cenowa przez SL: wykonanie po cenie otwarcia.
* Rozmiar pozycji: minimalny wolumen i krok pobierane ze specyfikacji. Jeżeli nawet minimum
  przekracza limit ryzyka, transakcja jest **odrzucana** (`min_volume_exceeds_risk`), a nie zmniejszana do nieistniejącego ułamka.
* Brak wejść: przerwa dzienna, weekend, piątek po 19:00 UTC, blackout wydarzeń, dzienny limit straty,
  zbyt szeroki spread, luka w danych i opóźnione wejście. Pozycje są zamykane przed weekendem.

Przy 500 USD, 1% ryzyka i minimum 1 oz większość sygnałów zostanie odrzucona. To zamierzony,
uczciwy wynik: `engine.rejected:*` w raporcie pokazuje, ile i dlaczego.

## Dwie strategie i wariant agresywny

| | `trend_pullback_v1` (`config.example.toml`) | `breakout_v1` (`config.aggressive.toml`) |
|---|---|---|
| wejście | korekta RSI na M15 w trendzie H1 **i** H4 | zamknięcie M15 poza zakresem ostatnich 8 świec, w kierunku trendu H1 |
| SL / TP | 1,5 / 2,0 ATR | 1,0 / 1,5 ATR |
| ryzyko na transakcję | 1% | 5% |
| dzienny limit straty | 3% | 12% |
| charakter | rzadkie wejścia, większość sygnałów odrzucana przy 500 USD | częste wejścia, więcej kosztów (spread + prowizja) |

```bash
python -m goldbot backtest --data data/xauusd_m1.csv --config config.aggressive.toml --events examples/fomc_2026.csv
# ta sama strategia, inne ryzyko:
python -m goldbot backtest --data data/xauusd_m1.csv --config config.aggressive.toml --risk-pct 2 --daily-loss-pct 6
```

Ryzyko 5% skaluje wynik, nie zmienia przewagi: seria 5 strat z rzędu to ok. −23% konta.
Na danych syntetycznych (błądzenie losowe) `breakout_v1` daje wynik bliski zeru po kosztach
z obsunięciem ~30% — dokładnie tak powinna wyglądać strategia bez przewagi. Wcześniejsza wersja
generatora miała wielogodzinne „reżimy dryfu”, które ta strategia trywialnie wykorzystywała
(+900 000%); to przypomnienie, że wynik na syntetyku nic nie mówi o rynku.

## Filtr Jev (wariant B)

Model **klasyfikuje** komunikaty (`relevant_to_gold`, `category`), a **kod decyduje**: blokuje wejście,
gdy w oknie jest istotny komunikat z kategorii `block_categories`. Model nie liczy cen, a jego confidence nie jest używane.

* Komunikaty w JSONL z polem `available_at` - w backteście widoczne są tylko te, które już dotarły (`examples/news_sample.jsonl`).
* Odpowiedzi są cache'owane, więc powtórny backtest jest powtarzalny i nie kosztuje drugi raz.
* Aliasy `*-latest` i `*-router` są odrzucane. Wymagana jest przypięta wersja.
* Klient używa API zgodnego z OpenAI (`/chat/completions`), więc działa z OpenRouter.
  **Stan na 30.09.2026:** publiczna lista OpenRouter zawiera tylko `typesafe/jev-router`. Ten router
  sam dobiera model i nie nadaje się do A/B, więc przypiętą wersję trzeba uzyskać bezpośrednio od TypeSafe.

```bash
export OPENROUTER_API_KEY=...
python -m goldbot backtest --data ... --config moj_config.toml --news komunikaty.jsonl --variant AB
```

Wynik: tabela A vs B (wynik netto, obsunięcie, średnia transakcja, PF, koszty) oraz statystyki per okres.
Jeżeli B nie poprawia wyniku po kosztach, filtr usuwamy.

## Ocena

Po 7 dniach paper: kompletność danych (`data_gaps`), brak podwójnych decyzji, zgodność kosztów, reakcja na rozłączenie.
Ocena strategii: chronologiczne okresy (`--splits`), wynik netto, maksymalne obsunięcie, średnia transakcja
i stabilność między okresami - nie tylko procent trafień.

## Czego jeszcze nie ma

* Kontekstu FRED (DFII10, DGS2/10, DTWEXBGS). Wymaga to modelowania momentu publikacji obserwacji, nie tylko jej daty.
* Pobierania historii przez cTrader (`ProtoOAGetTrendbarsReq`). Na razie rozgrzewka z pliku CSV.
* Testu na prawdziwym koncie demo i z prawdziwym API Jev.
