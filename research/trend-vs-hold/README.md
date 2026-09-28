# Kup i trzymaj vs proste reguły trendu (dane dzienne, 2018-04 – 2026-09)

Dane: Binance (BTCUSDT, ETHUSDT), Yahoo Finance (GC=F złoto, EURUSD=X, GBPUSD=X, JPY=X odwrócone). Reguły ustalone z góry, bez optymalizacji; sygnał na zamknięciu, wykonanie następnego dnia; koszty za zmianę pozycji: krypto 0,15%, złoto 0,05%, FX 0,02%; gotówka 0% (konserwatywnie); bez podatków i dźwigni. FX w regułach trendu long/short. Portfele: równe wagi, rebalans miesięczny. Skrypt: `backtest.py` (pobiera dane do /tmp/bt).

### cały okres

| Aktywo | Reguła | CAGR | Łącznie | Max spadek | Najgorszy rok | Sharpe | W rynku | Zmian |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| BTC | kup i trzymaj | +34% | +1120% | -77% | -64% | 0.79 | 100% | 0 |
| BTC | filtr 200 dni | +34% | +1077% | -64% | -22% | 0.89 | 52% | 61 |
| BTC | krzyż 50/200 | +27% | +671% | -67% | -29% | 0.75 | 51% | 17 |
| BTC | wybicie 55/20 | +31% | +902% | -39% | -27% | 0.94 | 35% | 59 |
| ETH | kup i trzymaj | +25% | +583% | -90% | -67% | 0.69 | 100% | 0 |
| ETH | filtr 200 dni | +40% | +1620% | -74% | -16% | 0.87 | 51% | 53 |
| ETH | krzyż 50/200 | +28% | +721% | -79% | -31% | 0.72 | 50% | 18 |
| ETH | wybicie 55/20 | +37% | +1349% | -51% | -19% | 0.92 | 33% | 55 |
| Złoto | kup i trzymaj | +15% | +227% | -25% | -4% | 0.88 | 100% | 0 |
| Złoto | filtr 200 dni | +11% | +135% | -31% | -15% | 0.72 | 73% | 62 |
| Złoto | krzyż 50/200 | +10% | +126% | -29% | -8% | 0.68 | 73% | 14 |
| Złoto | wybicie 55/20 | +8% | +87% | -20% | -13% | 0.63 | 44% | 42 |
| EUR/USD | kup i trzymaj | -1% | -8% | -23% | -8% | -0.09 | 100% | 0 |
| EUR/USD | filtr 200 dni | +1% | +8% | -20% | -9% | 0.17 | 100% | 58 |
| EUR/USD | krzyż 50/200 | -1% | -8% | -31% | -8% | -0.11 | 100% | 14 |
| EUR/USD | wybicie 55/20 | -1% | -10% | -22% | -7% | -0.13 | 100% | 42 |
| GBP/USD | kup i trzymaj | -1% | -6% | -25% | -11% | -0.04 | 100% | 0 |
| GBP/USD | filtr 200 dni | -3% | -20% | -33% | -10% | -0.25 | 100% | 88 |
| GBP/USD | krzyż 50/200 | -4% | -30% | -31% | -12% | -0.42 | 100% | 19 |
| GBP/USD | wybicie 55/20 | -2% | -16% | -30% | -10% | -0.18 | 100% | 44 |
| JPY/USD | kup i trzymaj | -4% | -33% | -37% | -13% | -0.48 | 100% | 0 |
| JPY/USD | filtr 200 dni | -2% | -17% | -24% | -8% | -0.20 | 100% | 75 |
| JPY/USD | krzyż 50/200 | -2% | -12% | -32% | -13% | -0.13 | 100% | 12 |
| JPY/USD | wybicie 55/20 | +4% | +45% | -17% | -12% | 0.53 | 100% | 28 |
| portfel: 50/50 BTC + złoto | kup i trzymaj | +31% | +863% | -50% | -37% | 0.97 | – | – |
| portfel: 50/50 BTC + złoto | filtr 200 dni | +25% | +583% | -42% | -11% | 1.06 | – | – |
| portfel: 50/50 BTC + złoto | krzyż 50/200 | +22% | +433% | -38% | -7% | 0.90 | – | – |
| portfel: 50/50 BTC + złoto | wybicie 55/20 | +21% | +409% | -24% | -12% | 1.07 | – | – |
| portfel: BTC + ETH + złoto | kup i trzymaj | +34% | +1136% | -66% | -47% | 0.87 | – | – |
| portfel: BTC + ETH + złoto | filtr 200 dni | +33% | +1063% | -45% | -8% | 1.05 | – | – |
| portfel: BTC + ETH + złoto | krzyż 50/200 | +27% | +661% | -55% | -15% | 0.87 | – | – |
| portfel: BTC + ETH + złoto | wybicie 55/20 | +28% | +720% | -29% | -15% | 1.10 | – | – |

### 2018-04–2021-12

| Aktywo | Reguła | CAGR | Łącznie | Max spadek | Najgorszy rok | Sharpe | W rynku | Zmian |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| BTC | kup i trzymaj | +66% | +567% | -67% | -47% | 1.06 | 100% | 0 |
| BTC | filtr 200 dni | +38% | +236% | -64% | -22% | 0.88 | 53% | 28 |
| BTC | krzyż 50/200 | +46% | +316% | -67% | -0% | 0.95 | 53% | 7 |
| BTC | wybicie 55/20 | +59% | +473% | -31% | -23% | 1.28 | 39% | 22 |
| ETH | kup i trzymaj | +81% | +833% | -90% | -67% | 1.11 | 100% | 0 |
| ETH | filtr 200 dni | +74% | +702% | -74% | -11% | 1.12 | 60% | 25 |
| ETH | krzyż 50/200 | +86% | +937% | -79% | +0% | 1.20 | 58% | 8 |
| ETH | wybicie 55/20 | +80% | +801% | -42% | -18% | 1.30 | 35% | 22 |
| Złoto | kup i trzymaj | +9% | +38% | -19% | -4% | 0.63 | 100% | 0 |
| Złoto | filtr 200 dni | +3% | +13% | -25% | -15% | 0.31 | 62% | 33 |
| Złoto | krzyż 50/200 | +8% | +32% | -19% | -8% | 0.61 | 64% | 7 |
| Złoto | wybicie 55/20 | +2% | +10% | -19% | -13% | 0.29 | 36% | 20 |
| EUR/USD | kup i trzymaj | -2% | -8% | -14% | -8% | -0.31 | 100% | 0 |
| EUR/USD | filtr 200 dni | +2% | +8% | -8% | -1% | 0.35 | 100% | 18 |
| EUR/USD | krzyż 50/200 | +1% | +5% | -8% | -3% | 0.23 | 100% | 6 |
| EUR/USD | wybicie 55/20 | -2% | -6% | -22% | -7% | -0.22 | 100% | 18 |
| GBP/USD | kup i trzymaj | -1% | -4% | -20% | -10% | -0.07 | 100% | 0 |
| GBP/USD | filtr 200 dni | -2% | -6% | -13% | -3% | -0.14 | 100% | 28 |
| GBP/USD | krzyż 50/200 | -5% | -19% | -26% | -12% | -0.57 | 100% | 8 |
| GBP/USD | wybicie 55/20 | -2% | -6% | -18% | -6% | -0.13 | 100% | 16 |
| JPY/USD | kup i trzymaj | -2% | -7% | -11% | -10% | -0.27 | 100% | 0 |
| JPY/USD | filtr 200 dni | -4% | -16% | -23% | -8% | -0.66 | 100% | 50 |
| JPY/USD | krzyż 50/200 | -1% | -5% | -13% | -6% | -0.19 | 100% | 6 |
| JPY/USD | wybicie 55/20 | +2% | +7% | -11% | -6% | 0.30 | 100% | 13 |
| portfel: 50/50 BTC + złoto | kup i trzymaj | +44% | +295% | -42% | -24% | 1.14 | – | – |
| portfel: 50/50 BTC + złoto | filtr 200 dni | +25% | +131% | -40% | -11% | 0.92 | – | – |
| portfel: 50/50 BTC + złoto | krzyż 50/200 | +31% | +176% | -38% | -2% | 1.02 | – | – |
| portfel: 50/50 BTC + złoto | wybicie 55/20 | +31% | +175% | -17% | -11% | 1.25 | – | – |
| portfel: BTC + ETH + złoto | kup i trzymaj | +65% | +554% | -66% | -39% | 1.18 | – | – |
| portfel: BTC + ETH + złoto | filtr 200 dni | +46% | +319% | -45% | -4% | 1.14 | – | – |
| portfel: BTC + ETH + złoto | krzyż 50/200 | +53% | +394% | -55% | +2% | 1.20 | – | – |
| portfel: BTC + ETH + złoto | wybicie 55/20 | +49% | +352% | -24% | -13% | 1.41 | – | – |

### 2022-01–2026-09

| Aktywo | Reguła | CAGR | Łącznie | Max spadek | Najgorszy rok | Sharpe | W rynku | Zmian |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| BTC | kup i trzymaj | +14% | +83% | -67% | -64% | 0.50 | 100% | 0 |
| BTC | filtr 200 dni | +30% | +251% | -32% | -18% | 0.96 | 52% | 33 |
| BTC | krzyż 50/200 | +14% | +85% | -40% | -29% | 0.55 | 50% | 10 |
| BTC | wybicie 55/20 | +13% | +75% | -37% | -27% | 0.57 | 31% | 37 |
| ETH | kup i trzymaj | -6% | -27% | -74% | -67% | 0.25 | 100% | 0 |
| ETH | filtr 200 dni | +17% | +115% | -38% | -16% | 0.60 | 44% | 28 |
| ETH | krzyż 50/200 | -5% | -21% | -62% | -31% | 0.08 | 44% | 10 |
| ETH | wybicie 55/20 | +11% | +61% | -35% | -19% | 0.47 | 32% | 33 |
| Złoto | kup i trzymaj | +20% | +136% | -25% | -0% | 1.04 | 100% | 0 |
| Złoto | filtr 200 dni | +17% | +108% | -20% | -8% | 0.97 | 81% | 29 |
| Złoto | krzyż 50/200 | +12% | +72% | -25% | -8% | 0.73 | 80% | 7 |
| Złoto | wybicie 55/20 | +12% | +70% | -14% | +4% | 0.84 | 50% | 22 |
| EUR/USD | kup i trzymaj | +0% | +0% | -16% | -6% | 0.05 | 100% | 0 |
| EUR/USD | filtr 200 dni | +0% | +0% | -20% | -9% | 0.04 | 100% | 40 |
| EUR/USD | krzyż 50/200 | -3% | -13% | -31% | -8% | -0.33 | 100% | 8 |
| EUR/USD | wybicie 55/20 | -1% | -4% | -20% | -7% | -0.07 | 100% | 24 |
| GBP/USD | kup i trzymaj | -0% | -2% | -22% | -11% | -0.01 | 100% | 0 |
| GBP/USD | filtr 200 dni | -3% | -15% | -33% | -10% | -0.34 | 100% | 60 |
| GBP/USD | krzyż 50/200 | -3% | -13% | -31% | -9% | -0.31 | 100% | 11 |
| GBP/USD | wybicie 55/20 | -2% | -10% | -30% | -10% | -0.22 | 100% | 28 |
| JPY/USD | kup i trzymaj | -6% | -28% | -31% | -13% | -0.61 | 100% | 0 |
| JPY/USD | filtr 200 dni | -0% | -1% | -24% | -6% | 0.03 | 100% | 25 |
| JPY/USD | krzyż 50/200 | -2% | -7% | -32% | -13% | -0.11 | 100% | 6 |
| JPY/USD | wybicie 55/20 | +6% | +36% | -17% | -12% | 0.67 | 100% | 15 |
| portfel: 50/50 BTC + złoto | kup i trzymaj | +21% | +144% | -43% | -37% | 0.82 | – | – |
| portfel: 50/50 BTC + złoto | filtr 200 dni | +26% | +195% | -14% | -4% | 1.27 | – | – |
| portfel: 50/50 BTC + złoto | krzyż 50/200 | +15% | +93% | -16% | -7% | 0.80 | – | – |
| portfel: 50/50 BTC + złoto | wybicie 55/20 | +14% | +85% | -18% | -12% | 0.90 | – | – |
| portfel: BTC + ETH + złoto | kup i trzymaj | +14% | +89% | -51% | -47% | 0.54 | – | – |
| portfel: BTC + ETH + złoto | filtr 200 dni | +24% | +178% | -19% | -8% | 1.03 | – | – |
| portfel: BTC + ETH + złoto | krzyż 50/200 | +10% | +54% | -23% | -15% | 0.50 | – | – |
| portfel: BTC + ETH + złoto | wybicie 55/20 | +13% | +82% | -19% | -15% | 0.76 | – | – |
