# swing_v1 (kanał H4 + trailing stop) — BTC i złoto

Odpowiedź na wynik BTC: częste wejścia M15 nie miały przewagi, BTC robił kilkutygodniowe trendy.
`swing_v1`: wejście, gdy M15 zamyka się poza kanałem max/min ostatnich N świec H4 w kierunku trendu H4 (cena vs EMA20 H4);
stop początkowy i trailing = k × ATR(H4); cel daleko (6–10 ATR). Trailing stop jest realizowany w silniku
po każdym zamknięciu M15 i może tylko zacieśniać poziom.

## BTC (Binance, 1 maja – 28 wrz 2026; perp: dźwignia 5×, prowizja 0,05 %/stronę)

Pełny okres, parametry bazowe (N 20, trail 2,0 ATR, TP 8 ATR):

| koszty | ryzyko | kier. | trans. | netto | zwrot | max DD | PF | okresy |
|---|---|---|---|---|---|---|---|---|
| perp | 5 % | both | 71 | −18 | −4 % | 36 % | 0,97 | + − + − |
| perp | 5 % | long | 37 | +68 | +14 % | 26 % | 1,23 | − − + + |
| brutto (0 kosztów) | 5 % | both | 71 | +78 | +16 % | 34 % | 1,12 | + − + − |
| brutto (0 kosztów) | 5 % | long | 37 | +130 | +26 % | 25 % | 1,44 | − − + + |

Siatka IS/OOS (18 kombinacji, perp, 5 %). IS = 1 maja – 30 lip (spadek 76 → 60 tys. i konsolidacja),
OOS = 30 lip – 28 wrz (rajd do 83 tys.):

| kierunek | zestaw | IS trans | IS zwrot | IS PF | OOS trans | OOS zwrot | OOS PF |
|---|---|---|---|---|---|---|---|
| both | bazowy | 39 | −10,7 % | 0,86 | 30 | +17,1 % | 1,32 |
| both | **#1 N 30, trail 1,5, TP 10** | 21 | **+6,3 %** | 1,14 | 21 | **+20,0 %** | 1,37 |
| both | #2 N 30, trail 1,5, TP 6 | 22 | +5,3 % | 1,12 | 22 | +6,8 % | 1,12 |
| both | #3 N 20, trail 1,5, TP 10 | 30 | −0,4 % | 0,99 | 22 | +41,9 % | 1,82 |
| long | bazowy | 15 | −17,5 % | 0,49 | 21 | +45,0 % | 2,70 |
| long | #1 N 20, trail 1,5, TP 6 | 11 | −6,2 % | 0,75 | 14 | +61,5 % | 3,12 |
| long | #2 N 20, trail 1,5, TP 10 | 11 | −6,2 % | 0,75 | 13 | +81,3 % | 4,64 |

### Interpretacja

* To **pierwsza strategia, która na BTC ma dodatni wynik brutto** (PF 1,12–1,44) i przy realnych kosztach perp
  nie jest od razu ujemna. Ale:
* Wyniki „tylko long” +45…+81 % na OOS to **rajd sierpień–wrzesień**, nie przewaga: te same zestawy na IS
  (rynek spadkowy) traciły 6–18 %. Long-only swing zarabia, gdy BTC rośnie — to tautologia, nie strategia.
* Jedyny zestaw dodatni **w obu połowach** to `both, N 30, trail 1,5 ATR, TP 10 ATR`: +6 % IS / +20 % OOS,
  PF 1,14 / 1,37, 21 + 21 transakcji, obsunięcie ~23 %. Dłuższy kanał (30 świec H4 = 5 dni) rzadziej wchodzi
  i mniej piłuje. To kandydat do dalszej weryfikacji — z 42 transakcjami nadal za mała próba.
* Obsunięcia 22–36 % przy 5 % ryzyka. Przy 3 % byłyby ~2/3 tego.

## Złoto (Dukascopy, 3 sie – 25 wrz 2026)

* Z kapitałem 500 USD: **0 transakcji** przy 3 % i 5 % — stop 2 × ATR(H4) ≈ 40–60 USD/oz przy minimum 1 oz przekracza budżet.
* Diagnostyka z 2000 USD: 3 % nadal 0 transakcji; 5 % → 18 transakcji, −6,7 %, PF 0,69 (long-only: 9 transakcji, PF 0,53).
* W tym oknie swing na złocie nie ma przewagi; okno jest zbyt krótkie (8 tygodni) dla strategii, która robi
  kilkanaście transakcji na kwartał. Do powtórzenia po dociągnięciu historii marzec–lipiec.

## Stan po tej rundzie

| rynek | strategia | werdykt |
|---|---|---|
| złoto | trend_pullback_v1 (RSI 35, SL 1,5, TP 3) | kandydat — dodatni IS i OOS, potwierdzić na dłuższej historii |
| złoto | breakout_v1 | odrzucona — dopasowanie do rajdu, OOS ujemny w każdym zestawie |
| złoto | swing_v1 | nie mieści się w 500 USD; z 2000 USD ujemna w tym oknie |
| BTC | trend_pullback_v1 / breakout_v1 | odrzucone — brak przewagi nawet bez kosztów |
| BTC | swing_v1 (both, N 30, trail 1,5) | jedyny kandydat — dodatni w obu połowach, mała próba, zależny od reżimu |
