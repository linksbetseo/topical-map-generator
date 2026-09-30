"""Strategia: trend z H1/H4, wejście na zamknięciu świecy M15 po korekcie (RSI).

To prosty, audytowalny punkt odniesienia - nie twierdzenie, że te interwały czy parametry
są rentowne. Wszystkie obliczenia są w kodzie; żaden model językowy nie liczy cen.
"""

from __future__ import annotations

from datetime import timedelta

from goldbot.config import StrategyConfig
from goldbot.indicators import ATR, EMA, RSI
from goldbot.models import Bar, Side, Signal


class TrendPullbackStrategy:
    name = "trend_pullback_v1"

    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg
        self.h1_fast = EMA(cfg.h1_ema_fast)
        self.h1_slow = EMA(cfg.h1_ema_slow)
        self.h1_close: float | None = None
        self.h4_ema = EMA(cfg.h4_ema)
        self.h4_close: float | None = None
        self.rsi = RSI(cfg.m15_rsi_period)
        self.atr = ATR(cfg.m15_atr_period)
        self.prev_rsi: float | None = None

    def on_h1(self, bar: Bar) -> None:
        self.h1_fast.update(bar.close)
        self.h1_slow.update(bar.close)
        self.h1_close = bar.close

    def on_h4(self, bar: Bar) -> None:
        self.h4_ema.update(bar.close)
        self.h4_close = bar.close

    def trend(self) -> str | None:
        f, s = self.h1_fast.value, self.h1_slow.value
        if f is None or s is None or self.h1_close is None:
            return None
        h1 = "up" if f > s and self.h1_close > s else "down" if f < s and self.h1_close < s else "flat"
        if not self.cfg.use_h4:
            return h1
        if self.h4_ema.value is None or self.h4_close is None:
            return None
        h4 = "up" if self.h4_close > self.h4_ema.value else "down"
        return h1 if h1 == h4 else "flat"

    def on_m15(self, bar: Bar) -> Signal | None:
        """Wywoływane po zamknięciu świecy M15 (mid). Zwraca sygnał albo None."""
        prev_rsi = self.rsi.value
        rsi = self.rsi.update(bar.close)
        atr = self.atr.update(bar.high, bar.low, bar.close)
        trend = self.trend()
        if rsi is None or prev_rsi is None or atr is None or trend is None:
            return None
        if not (self.cfg.min_atr <= atr <= self.cfg.max_atr):
            return None
        decided_at = bar.time + timedelta(minutes=15)
        features = {"rsi": round(rsi, 2), "prev_rsi": round(prev_rsi, 2), "atr": round(atr, 3),
                    "trend": trend, "close": bar.close,
                    "h1_ema_fast": round(self.h1_fast.value, 3), "h1_ema_slow": round(self.h1_slow.value, 3)}
        side = None
        if trend == "up" and prev_rsi < self.cfg.rsi_long_trigger <= rsi:
            side = Side.LONG
        elif trend == "down" and prev_rsi > self.cfg.rsi_short_trigger >= rsi:
            side = Side.SHORT
        if side is None:
            return None
        return Signal(
            side=side,
            sl_distance=self.cfg.sl_atr_mult * atr,
            tp_distance=self.cfg.tp_atr_mult * atr,
            decided_at=decided_at,
            reason=f"{self.name}: trend={trend}, RSI {prev_rsi:.1f}->{rsi:.1f}",
            features=features,
        )
