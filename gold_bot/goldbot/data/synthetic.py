"""Syntetyczne dane M1 bid/ask - WYŁĄCZNIE do testów działania programu.

Wynik backtestu na tych danych nie mówi nic o rentowności strategii.
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone

from goldbot.models import BidAskBar


def generate(days: int = 30, start: datetime | None = None, price: float = 3800.0,
             annual_vol: float = 0.18, base_spread: float = 0.25, seed: int = 7) -> list[BidAskBar]:
    rng = random.Random(seed)
    t = start or datetime(2026, 6, 1, tzinfo=timezone.utc)
    end = t + timedelta(days=days)
    minute_vol = annual_vol / math.sqrt(252 * 23 * 60)
    drift_regime = 0.0
    bars = []
    while t < end:
        wd, hm = t.weekday(), t.hour * 60 + t.minute
        closed = (wd == 5) or (wd == 6 and hm < 22 * 60) or (wd == 4 and hm >= 21 * 60) or (21 * 60 <= hm < 22 * 60)
        if closed:
            t += timedelta(minutes=1)
            continue
        if rng.random() < 1 / 600:
            drift_regime = rng.gauss(0, minute_vol * 0.02)
        # wyższa zmienność w godzinach Londyn/NY
        session_mult = 1.4 if 12 * 60 <= hm < 17 * 60 else (1.0 if 7 * 60 <= hm < 12 * 60 else 0.6)
        path = [price]
        for _ in range(4):
            path.append(path[-1] * (1 + drift_regime / 4 + rng.gauss(0, minute_vol * session_mult / 2)))
        mid_o, mid_c = path[0], path[-1]
        mid_h, mid_l = max(path), min(path)
        wide = hm >= 20 * 60 + 45 or 22 * 60 <= hm < 22 * 60 + 30  # okolice rolloveru
        spread = base_spread * (2.5 if wide else 1.0)
        spread *= 1 + abs(rng.gauss(0, 0.15))
        half = spread / 2
        nd = 5 if price < 10 else 2  # pary FX vs metale
        bars.append(BidAskBar(
            time=t,
            bid_open=round(mid_o - half, nd), bid_high=round(mid_h - half, nd),
            bid_low=round(mid_l - half, nd), bid_close=round(mid_c - half, nd),
            ask_open=round(mid_o + half, nd), ask_high=round(mid_h + half, nd),
            ask_low=round(mid_l + half, nd), ask_close=round(mid_c + half, nd),
            volume=float(rng.randint(5, 200)),
        ))
        price = mid_c
        t += timedelta(minutes=1)
    return bars
