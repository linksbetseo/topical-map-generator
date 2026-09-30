"""Wskaźniki liczone przyrostowo - te same klasy działają w backteście i na żywo."""

from __future__ import annotations


class EMA:
    def __init__(self, period: int):
        self.period = period
        self.alpha = 2.0 / (period + 1)
        self.value: float | None = None
        self._seed: list[float] = []

    def update(self, x: float) -> float | None:
        if self.value is None:
            self._seed.append(x)
            if len(self._seed) == self.period:
                self.value = sum(self._seed) / self.period
                self._seed = []
            return self.value
        self.value = self.alpha * x + (1 - self.alpha) * self.value
        return self.value


class RSI:
    """RSI Wildera."""

    def __init__(self, period: int = 14):
        self.period = period
        self.prev_close: float | None = None
        self.avg_gain: float | None = None
        self.avg_loss: float | None = None
        self._gains: list[float] = []
        self._losses: list[float] = []
        self.value: float | None = None

    def update(self, close: float) -> float | None:
        if self.prev_close is None:
            self.prev_close = close
            return None
        change = close - self.prev_close
        self.prev_close = close
        gain, loss = max(change, 0.0), max(-change, 0.0)
        if self.avg_gain is None:
            self._gains.append(gain)
            self._losses.append(loss)
            if len(self._gains) < self.period:
                return None
            self.avg_gain = sum(self._gains) / self.period
            self.avg_loss = sum(self._losses) / self.period
        else:
            self.avg_gain = (self.avg_gain * (self.period - 1) + gain) / self.period
            self.avg_loss = (self.avg_loss * (self.period - 1) + loss) / self.period
        if self.avg_loss == 0:
            self.value = 100.0 if self.avg_gain > 0 else 50.0
        else:
            rs = self.avg_gain / self.avg_loss
            self.value = 100.0 - 100.0 / (1.0 + rs)
        return self.value


class ATR:
    """ATR Wildera."""

    def __init__(self, period: int = 14):
        self.period = period
        self.prev_close: float | None = None
        self._trs: list[float] = []
        self.value: float | None = None

    def update(self, high: float, low: float, close: float) -> float | None:
        if self.prev_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - self.prev_close), abs(low - self.prev_close))
        self.prev_close = close
        if self.value is None:
            self._trs.append(tr)
            if len(self._trs) == self.period:
                self.value = sum(self._trs) / self.period
                self._trs = []
            return self.value
        self.value = (self.value * (self.period - 1) + tr) / self.period
        return self.value
