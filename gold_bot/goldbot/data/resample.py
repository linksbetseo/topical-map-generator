"""Agregacja świec M1 -> M15/H1/H4 bez zaglądania w przyszłość.

Świeca wyższego interwału jest oddawana dopiero, gdy jest kompletna: albo przetworzono
jej ostatnią minutę, albo przyszła świeca z kolejnego przedziału (luka w danych).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from goldbot.models import Bar, BidAskBar, Quote

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def bucket_start(t: datetime, minutes: int) -> datetime:
    secs = int((t - _EPOCH).total_seconds())
    size = minutes * 60
    return _EPOCH + timedelta(seconds=secs - secs % size)


class BarAggregator:
    def __init__(self, minutes: int):
        self.minutes = minutes
        self._bucket: datetime | None = None
        self._o = self._h = self._l = self._c = 0.0
        self._v = 0.0
        self._last_minute: datetime | None = None
        self.gap_inside = False

    def _emit(self) -> Bar:
        bar = Bar(self._bucket, self._o, self._h, self._l, self._c, self._v)
        self._bucket = None
        return bar

    def update(self, bar: Bar) -> list[Bar]:
        """Przyjmuje świecę M1 (mid), zwraca listę świec zakończonych po jej przetworzeniu."""
        done: list[Bar] = []
        b = bucket_start(bar.time, self.minutes)
        if self._bucket is not None and b != self._bucket:
            done.append(self._emit())
        if self._bucket is None:
            self._bucket = b
            self._o, self._h, self._l, self._c, self._v = bar.open, bar.high, bar.low, bar.close, bar.volume
        else:
            self._h = max(self._h, bar.high)
            self._l = min(self._l, bar.low)
            self._c = bar.close
            self._v += bar.volume
        self._last_minute = bar.time
        if bar.time + timedelta(minutes=1) == b + timedelta(minutes=self.minutes):
            done.append(self._emit())
        return done


class TickToM1:
    """Buduje świece M1 bid/ask z ticków (tryb na żywo)."""

    def __init__(self):
        self._minute: datetime | None = None
        self._vals: list[float] = []
        self._n = 0

    def update(self, q: Quote) -> BidAskBar | None:
        m = bucket_start(q.time, 1)
        out = None
        if self._minute is not None and m != self._minute:
            out = self.flush()
        if self._minute is None:
            self._minute = m
            self._vals = [q.bid, q.bid, q.bid, q.bid, q.ask, q.ask, q.ask, q.ask]
            self._n = 1
        else:
            v = self._vals
            v[1], v[2], v[3] = max(v[1], q.bid), min(v[2], q.bid), q.bid
            v[5], v[6], v[7] = max(v[5], q.ask), min(v[6], q.ask), q.ask
            self._n += 1
        return out

    def flush(self) -> BidAskBar | None:
        if self._minute is None:
            return None
        bar = BidAskBar(self._minute, *self._vals, volume=float(self._n))
        self._minute = None
        return bar
