"""Buy & hold vs simple trend rules on BTC, ETH, gold, FX (daily). Rules fixed in advance, no optimisation.
Signal at close t, position held from close t to close t+1 (next-day execution). Costs per unit of turnover.
Cash earns 0 (conservative). FX trend is long/short; crypto and gold long-only."""
import json
import numpy as np
import pandas as pd

def binance(sym):
    d = json.load(open(f"/tmp/bt/{sym}.json"))
    return pd.Series([float(x[4]) for x in d], index=pd.to_datetime([x[0] for x in d], unit="ms").normalize(), name=sym)

def yahoo(f, name, invert=False):
    r = json.load(open(f"/tmp/bt/{f}"))["chart"]["result"][0]
    s = pd.Series(r["indicators"]["quote"][0]["close"], index=pd.to_datetime(r["timestamp"], unit="s").normalize(), name=name).dropna()
    s = s[~s.index.duplicated(keep="last")]
    return 1 / s if invert else s

A = {
    "BTC": (binance("BTCUSDT"), 0.0015, "long"),
    "ETH": (binance("ETHUSDT"), 0.0015, "long"),
    "Złoto": (yahoo("y_GC%3DF.json", "GOLD"), 0.0005, "long"),
    "EUR/USD": (yahoo("y_EURUSD%3DX.json", "EURUSD"), 0.0002, "longshort"),
    "GBP/USD": (yahoo("y_GBPUSD%3DX.json", "GBPUSD"), 0.0002, "longshort"),
    "JPY/USD": (yahoo("y_JPY%3DX.json", "JPYUSD", invert=True), 0.0002, "longshort"),
}
START, END = "2018-04-01", "2026-09-27"
PERIODS = {"cały okres": (START, END), "2018-04–2021-12": (START, "2021-12-31"), "2022-01–2026-09": ("2022-01-01", END)}

def positions(p, rule, mode):
    sma = lambda n: p.rolling(n).mean()
    if rule == "kup i trzymaj":
        s = pd.Series(1.0, index=p.index)
    elif rule == "filtr 200 dni":
        s = (p > sma(200)).astype(float)
    elif rule == "krzyż 50/200":
        s = (sma(50) > sma(200)).astype(float)
    elif rule == "wybicie 55/20":
        hi, lo = p.rolling(55).max().shift(1), p.rolling(20).min().shift(1)
        s = pd.Series(np.nan, index=p.index)
        s[p > hi] = 1.0
        s[p < lo] = 0.0
        s = s.ffill().fillna(0.0)
    if mode == "longshort" and rule != "kup i trzymaj":
        s = s * 2 - 1  # +1 long / -1 short
    return s

def run(p, rule, cost, mode):
    pos = positions(p, rule, mode).shift(1).fillna(0.0)  # yesterday's signal
    ret = p.pct_change().fillna(0.0)
    turn = pos.diff().abs().fillna(pos.abs())
    return pos * ret - turn * cost, pos

def metrics(r, pos, a, b, ppy):
    r = r[a:b]; pos = pos[a:b]
    eq = (1 + r).cumprod()
    yrs = len(r) / ppy
    cagr = eq.iloc[-1] ** (1 / yrs) - 1
    dd = (eq / eq.cummax() - 1).min()
    vol = r.std() * np.sqrt(ppy)
    sharpe = r.mean() * ppy / vol if vol > 0 else np.nan
    yearly = (1 + r).groupby(r.index.year).prod() - 1
    return {"CAGR": cagr, "łącznie": eq.iloc[-1] - 1, "max spadek": dd, "zmienność": vol, "Sharpe": sharpe, "najgorszy rok": yearly.min(), "w rynku": (pos != 0).mean(), "zmian pozycji": int((pos.diff().abs() > 0).sum())}

RULES = ["kup i trzymaj", "filtr 200 dni", "krzyż 50/200", "wybicie 55/20"]
rows = []
series = {}
for name, (p, cost, mode) in A.items():
    ppy = 365 if name in ("BTC", "ETH") else 252
    p = p[:END]
    for rule in RULES:
        r, pos = run(p, rule, cost, mode)
        series[(name, rule)] = r
        for per, (a, b) in PERIODS.items():
            rows.append({"aktywo": name, "reguła": rule, "okres": per, **metrics(r, pos, a, b, ppy)})

# portfolios on a calendar-day grid (non-crypto returns are 0 on non-trading days)
cal = pd.date_range("2017-08-17", END, freq="D")
def cal_ret(name, rule):
    return series[(name, rule)].reindex(cal).fillna(0.0)
def portfolio(names, rule, rebalance="ME"):
    R = pd.concat([cal_ret(n, rule) for n in names], axis=1)
    # monthly rebalanced equal weight: weights drift within month
    out = []
    for _, blk in R.groupby(pd.Grouper(freq=rebalance)):
        w = np.repeat(1 / len(names), len(names))
        for _, row in blk.iterrows():
            gross = (w * (1 + row.values)).sum()
            out.append(gross - 1)
            w = w * (1 + row.values) / gross
    return pd.Series(out, index=R.index)
ports = {
    "50/50 BTC + złoto": ["BTC", "Złoto"],
    "BTC + ETH + złoto": ["BTC", "ETH", "Złoto"],
}
for pname, names in ports.items():
    for rule in RULES:
        r = portfolio(names, rule)
        for per, (a, b) in PERIODS.items():
            rr = r[a:b]
            eq = (1 + rr).cumprod(); yrs = len(rr) / 365
            yearly = (1 + rr).groupby(rr.index.year).prod() - 1
            vol = rr.std() * np.sqrt(365)
            rows.append({"aktywo": f"portfel: {pname}", "reguła": rule, "okres": per, "CAGR": eq.iloc[-1] ** (1 / yrs) - 1, "łącznie": eq.iloc[-1] - 1, "max spadek": (eq / eq.cummax() - 1).min(), "zmienność": vol, "Sharpe": rr.mean() * 365 / vol, "najgorszy rok": yearly.min(), "w rynku": np.nan, "zmian pozycji": np.nan})

df = pd.DataFrame(rows)
df.to_csv("/tmp/bt/results.csv", index=False)
pct = lambda x: "–" if pd.isna(x) else f"{x*100:+.0f}%"
with open("/tmp/bt/results.md", "w") as f:
    for per in PERIODS:
        f.write(f"\n### {per}\n\n| Aktywo | Reguła | CAGR | Łącznie | Max spadek | Najgorszy rok | Sharpe | W rynku | Zmian |\n|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
        for _, x in df[df.okres == per].iterrows():
            f.write(f"| {x['aktywo']} | {x['reguła']} | {pct(x['CAGR'])} | {pct(x['łącznie'])} | {pct(x['max spadek'])} | {pct(x['najgorszy rok'])} | {x['Sharpe']:.2f} | {'–' if pd.isna(x['w rynku']) else f'{x[chr(119)+chr(32)+chr(114)+chr(121)+chr(110)+chr(107)+chr(117)]*100:.0f}%'} | {'–' if pd.isna(x['zmian pozycji']) else int(x['zmian pozycji'])} |\n")
print(open("/tmp/bt/results.md").read())
