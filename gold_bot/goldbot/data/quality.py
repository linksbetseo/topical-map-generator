"""Kontrola kompletności i spójności danych przed backtestem."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from statistics import median

from goldbot.models import BidAskBar


@dataclass
class QualityReport:
    bars: int = 0
    first: str = ""
    last: str = ""
    duplicates: int = 0
    out_of_order: int = 0
    crossed_quotes: int = 0  # bid > ask
    invalid_ohlc: int = 0  # high < low itp.
    gaps_over_threshold: int = 0
    largest_gaps: list[tuple[str, float]] = field(default_factory=list)
    spread_median: float = 0.0
    spread_p95: float = 0.0
    spread_max: float = 0.0

    @property
    def ok(self) -> bool:
        return self.out_of_order == 0 and self.duplicates == 0 and self.crossed_quotes == 0 and self.invalid_ohlc == 0

    def as_dict(self) -> dict:
        d = dict(self.__dict__)
        d["ok"] = self.ok
        return d


def check(bars: list[BidAskBar], gap_minutes: int = 10, top_gaps: int = 5) -> QualityReport:
    r = QualityReport(bars=len(bars))
    if not bars:
        return r
    r.first, r.last = bars[0].time.isoformat(), bars[-1].time.isoformat()
    gaps = []
    spreads = []
    for i, b in enumerate(bars):
        spreads.append(b.spread_close)
        if b.bid_close > b.ask_close or b.bid_open > b.ask_open:
            r.crossed_quotes += 1
        for lo, hi, o, c in ((b.bid_low, b.bid_high, b.bid_open, b.bid_close), (b.ask_low, b.ask_high, b.ask_open, b.ask_close)):
            if lo > hi or not (lo <= o <= hi) or not (lo <= c <= hi):
                r.invalid_ohlc += 1
                break
        if i:
            dt = b.time - bars[i - 1].time
            if dt == timedelta(0):
                r.duplicates += 1
            elif dt < timedelta(0):
                r.out_of_order += 1
            elif dt > timedelta(minutes=gap_minutes):
                r.gaps_over_threshold += 1
                gaps.append((bars[i - 1].time.isoformat(), dt.total_seconds() / 60))
    gaps.sort(key=lambda g: -g[1])
    r.largest_gaps = gaps[:top_gaps]
    s = sorted(spreads)
    r.spread_median = round(median(s), 4)
    r.spread_p95 = round(s[int(0.95 * (len(s) - 1))], 4)
    r.spread_max = round(s[-1], 4)
    return r
