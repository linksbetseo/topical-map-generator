"""Badanie zdarzeń: co robi cena po świecy-szoku o stałej porze publikacji.

Dla każdego dnia: świeca M1 zdarzenia (np. 08:30 Nowy Jork). Szok = |close - poprzedni close| >= k * ATR(30, M1)
sprzed zdarzenia. Wejście symulowane po `wait` minutach od zamknięcia świecy zdarzenia, po cenie ask (long) / bid (short),
w kierunku ruchu (momentum). Mierzymy wynik po H minutach (wyjście po bid / ask) w jednostkach pip, PRZED i PO koszcie
prowizji (spread i tak jest w cenach). Fade = lustrzane odbicie momentum (minus koszt drugi raz).

To jest narzędzie badawcze, nie strategia: pokazuje rozkład, nie optymalizuje progów.
"""

from __future__ import annotations

from bisect import bisect_left
from datetime import timedelta
from statistics import mean, median
from zoneinfo import ZoneInfo

from goldbot.indicators import ATR
from goldbot.models import BidAskBar


def find_shocks(bars: list[BidAskBar], event_time: str, tz: str, k_atr: float, min_move: float, pip: float) -> list[dict]:
    zone = ZoneInfo(tz)
    eh, em = (int(x) for x in event_time.split(":"))
    atr, prev, out = ATR(30), None, []
    for i, b in enumerate(bars):
        m = b.mid()
        a = atr.value
        loc = b.time.astimezone(zone)
        if a and prev is not None and (loc.hour, loc.minute) == (eh, em) and loc.weekday() < 5:
            move = m.close - prev
            if abs(move) >= k_atr * a and abs(move) / pip >= min_move:
                out.append({"i": i, "time": b.time, "dir": 1 if move > 0 else -1, "move": move / pip, "atr": a / pip})
        atr.update(m.high, m.low, m.close)
        prev = m.close
    return out


def forward(bars: list[BidAskBar], times: list, shock: dict, wait: int, horizons: list[int], pip: float) -> dict | None:
    t_entry = shock["time"] + timedelta(minutes=1 + wait)  # zamknięcie świecy zdarzenia + wait
    j = bisect_left(times, t_entry)
    if j >= len(bars) or bars[j].time - t_entry > timedelta(minutes=2):
        return None
    e = bars[j]
    d = shock["dir"]
    entry = e.ask_open if d > 0 else e.bid_open
    res = {"entry_time": e.time, "spread_at_entry": (e.ask_open - e.bid_open) / pip}
    for h in horizons:
        k = bisect_left(times, t_entry + timedelta(minutes=h))
        if k >= len(bars):
            return None
        x = bars[k]
        exit_ = x.bid_open if d > 0 else x.ask_open
        res[h] = (exit_ - entry) * d / pip  # momentum, po spreadzie
    return res


def study(bars: list[BidAskBar], event_time: str = "08:30", tz: str = "America/New_York", k_atr: float = 4.0,
          min_move: float = 5.0, pip: float = 0.0001, wait: int = 2, horizons=(5, 15, 30, 60),
          commission_pips: float = 0.7) -> dict:
    times = [b.time for b in bars]
    rows = []
    for s in find_shocks(bars, event_time, tz, k_atr, min_move, pip):
        f = forward(bars, times, s, wait, list(horizons), pip)
        if f:
            rows.append({**s, **f})
    summary = {"events": len(rows)}
    for h in horizons:
        vals = [r[h] for r in rows]
        if not vals:
            continue
        # fade: wejście przeciwne - spread płacimy też w tę stronę, więc fade brutto = -(mom) - 2*spread_wejścia (przybliżenie)
        fade = [-(r[h]) - 2 * r["spread_at_entry"] for r in rows]
        summary[h] = {
            "momentum_mean": round(mean(vals) - commission_pips, 2),
            "momentum_median": round(median(vals) - commission_pips, 2),
            "momentum_hit": round(sum(1 for v in vals if v - commission_pips > 0) / len(vals) * 100, 1),
            "fade_mean": round(mean(fade) - commission_pips, 2),
            "fade_hit": round(sum(1 for v in fade if v - commission_pips > 0) / len(vals) * 100, 1),
        }
    return {"summary": summary, "rows": rows}
