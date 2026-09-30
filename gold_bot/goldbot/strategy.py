"""Strategia: trend z H1/H4, wejście na zamknięciu świecy M15 po korekcie (RSI).

To prosty, audytowalny punkt odniesienia - nie twierdzenie, że te interwały czy parametry
są rentowne. Wszystkie obliczenia są w kodzie; żaden model językowy nie liczy cen.
"""

from __future__ import annotations

from collections import deque
from datetime import timedelta

from goldbot.config import StrategyConfig
from goldbot.indicators import ATR, EMA, RSI
from goldbot.models import Bar, Side, Signal


class TrendPullbackStrategy:
    name = "trend_pullback_v1"

    def __init__(self, cfg: StrategyConfig):
        if cfg.direction not in ("both", "long", "short"):
            raise ValueError(f"strategy.direction: '{cfg.direction}' (dozwolone: both, long, short)")
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

    def allowed(self, side: Side) -> bool:
        return self.cfg.direction == "both" or self.cfg.direction == side.value

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
        if side is None or not self.allowed(side):
            return None
        return Signal(
            side=side,
            sl_distance=self.cfg.sl_atr_mult * atr,
            tp_distance=self.cfg.tp_atr_mult * atr,
            decided_at=decided_at,
            reason=f"{self.name}: trend={trend}, RSI {prev_rsi:.1f}->{rsi:.1f}",
            features=features,
        )


class BreakoutStrategy(TrendPullbackStrategy):
    """Częstsze wejścia: zamknięcie M15 powyżej maksimum (poniżej minimum) ostatnich N świec
    w kierunku trendu H1. Bez filtra H4, SL/TP z mnożników ATR z konfiguracji.

    Więcej transakcji = więcej kosztów (spread + prowizja na każdej). To celowo "agresywny"
    wariant do porównania, nie rekomendacja.
    """

    name = "breakout_v1"

    def __init__(self, cfg: StrategyConfig):
        super().__init__(cfg)
        self.window: deque[Bar] = deque(maxlen=cfg.breakout_lookback)

    def trend(self) -> str | None:
        f, s = self.h1_fast.value, self.h1_slow.value
        if f is None or s is None:
            return None
        return "up" if f > s else "down" if f < s else "flat"

    def on_m15(self, bar: Bar) -> Signal | None:
        rsi = self.rsi.update(bar.close)
        atr = self.atr.update(bar.high, bar.low, bar.close)
        trend = self.trend()
        window = list(self.window)
        self.window.append(bar)
        if atr is None or trend is None or len(window) < self.cfg.breakout_lookback:
            return None
        if not (self.cfg.min_atr <= atr <= self.cfg.max_atr):
            return None
        hi, lo = max(b.high for b in window), min(b.low for b in window)
        if hi - lo < self.cfg.breakout_min_range_atr * atr:
            return None
        side = None
        if trend == "up" and bar.close > hi:
            side = Side.LONG
        elif trend == "down" and bar.close < lo:
            side = Side.SHORT
        if side is None or not self.allowed(side):
            return None
        return Signal(
            side=side,
            sl_distance=self.cfg.sl_atr_mult * atr,
            tp_distance=self.cfg.tp_atr_mult * atr,
            decided_at=bar.time + timedelta(minutes=15),
            reason=f"{self.name}: trend={trend}, close {bar.close:.2f} vs range {lo:.2f}-{hi:.2f}",
            features={"rsi": round(rsi, 2) if rsi is not None else None, "atr": round(atr, 3), "trend": trend,
                      "range_hi": hi, "range_lo": lo, "close": bar.close},
        )


class SwingStrategy(TrendPullbackStrategy):
    """Wolna strategia: wejście, gdy M15 zamyka się poza kanałem max/min ostatnich N świec H4
    w kierunku trendu H4 (cena vs EMA H4). Stop początkowy i trailing = k * ATR(H4).
    Cel (tp_atr_mult * ATR H4) jest daleko - wyjście zwykle przez trailing stop.

    Kilkanaście transakcji na kwartał. Odpowiedź na wynik BTC: częste wejścia M15 nie miały tam przewagi.
    """

    name = "swing_v1"

    def __init__(self, cfg: StrategyConfig):
        super().__init__(cfg)
        self.h4_window: deque[Bar] = deque(maxlen=cfg.swing_lookback_h4)
        self.h4_atr = ATR(cfg.swing_atr_period_h4)
        self.extreme: dict[int, float] = {}  # id pozycji -> najlepsze zamknięcie M15 od wejścia

    def on_h4(self, bar: Bar) -> None:
        super().on_h4(bar)
        self.h4_atr.update(bar.high, bar.low, bar.close)
        self.h4_window.append(bar)

    def on_m15(self, bar: Bar) -> Signal | None:
        self.rsi.update(bar.close)
        self.atr.update(bar.high, bar.low, bar.close)
        atr = self.h4_atr.value
        if atr is None or self.h4_ema.value is None or len(self.h4_window) < self.cfg.swing_lookback_h4:
            return None
        if not (self.cfg.min_atr <= atr <= self.cfg.max_atr):
            return None
        hi = max(b.high for b in self.h4_window)
        lo = min(b.low for b in self.h4_window)
        trend = "up" if self.h4_close > self.h4_ema.value else "down"
        side = None
        if trend == "up" and bar.close > hi:
            side = Side.LONG
        elif trend == "down" and bar.close < lo:
            side = Side.SHORT
        if side is None or not self.allowed(side):
            return None
        return Signal(
            side=side,
            sl_distance=self.cfg.swing_trail_atr * atr,
            tp_distance=self.cfg.tp_atr_mult * atr,
            decided_at=bar.time + timedelta(minutes=15),
            reason=f"{self.name}: trend H4={trend}, close {bar.close:.2f} vs kanał {lo:.2f}-{hi:.2f}",
            features={"atr_h4": round(atr, 3), "trend": trend, "channel_hi": hi, "channel_lo": lo, "close": bar.close},
        )

    def trail(self, position, bar: Bar) -> float | None:
        """Nowy poziom SL po zamknięciu M15 albo None. Silnik przyjmie tylko poziom ciaśniejszy."""
        atr = self.h4_atr.value
        if atr is None:
            return None
        ext = self.extreme.get(position.id)
        if position.side is Side.LONG:
            ext = bar.close if ext is None else max(ext, bar.close)
            self.extreme[position.id] = ext
            return ext - self.cfg.swing_trail_atr * atr
        ext = bar.close if ext is None else min(ext, bar.close)
        self.extreme[position.id] = ext
        return ext + self.cfg.swing_trail_atr * atr


STRATEGIES = {TrendPullbackStrategy.name: TrendPullbackStrategy, BreakoutStrategy.name: BreakoutStrategy,
              SwingStrategy.name: SwingStrategy}


def build_strategy(cfg: StrategyConfig) -> TrendPullbackStrategy:
    try:
        return STRATEGIES[cfg.name](cfg)
    except KeyError:
        raise ValueError(f"Nieznana strategia '{cfg.name}'. Dostępne: {sorted(STRATEGIES)}") from None
