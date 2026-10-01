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
sprawdzony empirycznie; pobieranie z odstępem 4 s i lokalnym cache — przy szybszym tempie serwer
odpowiada 429/503 i wydłuża odpowiedzi do 20–40 s, więc kilka miesięcy historii to godziny w tle):

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
* `--events` - CSV `time_utc,name,source`, np. `examples/fomc_2026.csv`
  (daty 2026 zweryfikowane 30.09.2026 z kalendarzem na federalreserve.gov).
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

## Wyniki na danych realnych

Pierwszy przebieg obu strategii na 8 tygodniach (3 sie – 25 wrz 2026) realnych notowań Dukascopy,
ryzyko 1/2/3/5 %, kierunek both/long: **`results/2026-08-03_2026-09-25_dukascopy.md`**.
W skrócie: przy 500 USD ryzyko 1 % nie pozwala otworzyć żadnej pozycji (minimum 1 oz);
`trend_pullback_v1` przy 3–5 % dał +17…+35 % z obsunięciem < 10 %, `breakout_v1` podobny zwrot,
ale z obsunięciem 20–38 % i zyskiem skupionym w pierwszych 12 dniach rajdu. Za mała próba i jeden reżim
rynku — to nie jest dowód przewagi.

### Dopracowywanie parametrów (IS/OOS)

```bash
python -m goldbot optimize --data data/xauusd_m1.csv --config config.example.toml --risk-pct 5 --daily-loss-pct 12 \
    --events examples/fomc_2026.csv            # siatka domyślna dla strategii z configu
python -m goldbot optimize ... --grid '{"sl_atr_mult":[1,1.5],"tp_atr_mult":[2,3]}'
```

Siatka jest oceniana na pierwszych 60 % danych (in-sample), a 5 najlepszych zestawów sprawdzanych na ostatnich
40 % (out-of-sample), których nie widziały. Wyniki: `results/2026-09-30_gold_optimize_is_oos.md` — na złocie
`trend_pullback_v1` utrzymał dodatni wynik OOS, `breakout_v1` w każdym zestawie z czołówki IS traci na OOS.

### BTC

```bash
python -m goldbot fetch-btc --start 2026-05-01 --end 2026-09-28 --spread-pct 0.02   # archiwum Binance, ~3 min
python -m goldbot backtest --data data/btcusdt_m1.csv --config config.btc.toml --strategy trend_pullback_v1 --risk-pct 5
```

`config.btc.toml` opisuje rynek 24/7 (bez przerw i zamykania na weekend), prowizję procentową i minimalną ilość BTC.
Wynik: `results/2026-05-01_2026-09-28_btc_binance.md` — **obie strategie ze złota nie mają przewagi na BTC nawet bez
kosztów** (90 kombinacji siatki, żadna dodatnia in-sample); na spocie 1× z 500 USD depozyt, nie ryzyko, ogranicza
wielkość pozycji, a prowizje zjadają 150–350 USD.

### Forex: bot daily, 1:50, scalping — badanie

`docs/forex_scalping_daily_bot.md`: zmierzony spread EURUSD (0,2 pipsa w sesji), koszt okrągłej transakcji ≈ 1,2 pipsa,
próg rentowności scalpu 4/3 = **74 % trafień**, ESMA 1:30 vs 1:50, mikro-lot vs 500 USD, dlaczego scalping trzeba
testować na tickach. Silnik ma tryb tickowy (`backtest --ticks DIR --data warmup.csv --ticks-start ...`),
stop czasowy, okno sesji i `flat_at`; `config.eurusd.toml` + `scalp_meanrev_v1` to pierwsza hipoteza.

Wynik na 8 tygodniach EURUSD: `results/2026-08-03_2026-09-25_eurusd_scalp.md` — **scalp mean-reversion traci
w każdym z 36 zestawów już in-sample** (−10…−62 %), a bez prowizji i poślizgu jest na zerze (PF 0,9–1,0): logika nie ma
przewagi, koszty ją dobijają. Infrastruktura bota daily (mikro-loty, okno sesji, flat, stop czasowy) działa.
Za to `london_breakout_v1` (wybicie zakresu azjatyckiego po otwarciu Londynu, 1–2 wejścia dziennie, SL 8 / TP 12–16 pipsów)
był dodatni IS i OOS na lecie (34 transakcje, PF 1,25) — ale na **styczniu–marcu 2026 traci 26–34 %** (PF 0,36–0,42).
Lato było jednym reżimem, nie przewagą. Na EURUSD po kosztach nie mamy żadnej strategii z przewagą; dalsze strojenie
parametrów tych logik nie ma sensu — potrzebna jest informacja spoza samej ceny M1.

Nowe hipotezy (informacja spoza ceny M1): `results/2026-10-01_fx_new_hypotheses.md` — **fade po szoku z danych USA (08:30 NY)
na EURUSD** jest dodatni po kosztach w styczniu–marcu i sierpniu–wrześniu (parametry ustalone na IS, PF OOS 1,8–2,1),
ale to 14 transakcji w 5 miesięcy i t ≈ 1,1 — za mało, by odróżnić od przypadku. Badanie zdarzeń: `goldbot/event_study.py`.

### 7 miesięcy złota — werdykt

`results/2026-03-02_2026-09-25_gold_7m.md`: na marzec–wrzesień 2026 (krach −16 %, zjazd, rajd) tylko
`trend_pullback_v1` z **bazowymi** parametrami jest dodatni (+14 % przy 3 %, +38 % przy 5 %), ale z PF 1,1–1,2,
obsunięciem 21–30 % i 3 z 7 okresów na minusie. „Dopracowany” na lecie zestaw RSI 35 / TP 3 traci 20 % —
klasyczne dopasowanie do 8 tygodni, dlatego domyślna konfiguracja pozostaje bez zmian. `swing_v1` na złocie odpada.

### swing_v1 — wolna strategia z trailing stopem

Kanał max/min z ostatnich N świec H4, wejście z zamknięcia M15 w kierunku trendu H4, stop i trailing = k × ATR(H4).
Silnik zacieśnia SL po każdym zamknięciu M15 (`trail`). Wyniki: `results/2026-09-30_swing_v1_btc_gold.md` —
na BTC jedyna strategia z dodatnim wynikiem brutto; zestaw `N 30, trail 1,5` dodatni w obu połowach (IS +6 %, OOS +20 %),
ale to 42 transakcje i silna zależność od reżimu. Na złocie z 500 USD stop 2 × ATR(H4) nie mieści się w 1 oz.

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
