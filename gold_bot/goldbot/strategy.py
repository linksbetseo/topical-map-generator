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

    def on_m1(self, bar: Bar) -> Signal | None:
        """Hook dla strategii decydujących na M1 (scalping). Domyślnie nic."""
        return None

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


class ScalpMeanRevStrategy(TrendPullbackStrategy):
    """Scalping M1: cena odchyla się od EMA(M1) o >= k*ATR(M1) i świeca zamyka się z powrotem w stronę EMA
    (pierwsza oznaka wygaszenia impulsu) -> wejście w stronę EMA ze stałym SL/TP w pipsach.

    Decyzja na zamknięciu M1, wejście na następnym ticku / otwarciu M1. Stop czasowy i okno sesji
    są w konfiguracji (strategy.max_hold_minutes, session.trade_window_*). Przy koszcie okrągłej
    transakcji ~1 pips (spread + prowizja) cel 3 pipsy oddaje ~1/3 w kosztach - to jest hipoteza do obalenia.
    """

    name = "scalp_meanrev_v1"

    def __init__(self, cfg: StrategyConfig, pip_size: float = 0.0001):
        super().__init__(cfg)
        self.pip = pip_size
        self.ema_m1 = EMA(cfg.scalp_ema_m1)
        self.atr_m1 = ATR(cfg.scalp_atr_m1)
        self.prev_m1: Bar | None = None

    def on_m15(self, bar: Bar) -> Signal | None:
        self.rsi.update(bar.close)
        self.atr.update(bar.high, bar.low, bar.close)
        return None

    def on_m1(self, bar: Bar) -> Signal | None:
        ema = self.ema_m1.update(bar.close)
        atr = self.atr_m1.update(bar.high, bar.low, bar.close)
        prev, self.prev_m1 = self.prev_m1, bar
        if ema is None or atr is None or prev is None:
            return None
        atr_pips = atr / self.pip
        if not (self.cfg.scalp_min_atr_pips <= atr_pips <= self.cfg.scalp_max_atr_pips):
            return None
        dev = self.cfg.scalp_dev_atr * atr
        side = None
        # poprzednia świeca rozciągnięta poniżej EMA, bieżąca zamyka wyżej niż poprzednia -> long w stronę EMA
        if prev.close < ema - dev and bar.close > prev.close and bar.close < ema:
            side = Side.LONG
        elif prev.close > ema + dev and bar.close < prev.close and bar.close > ema:
            side = Side.SHORT
        if side is None or not self.allowed(side):
            return None
        return Signal(
            side=side,
            sl_distance=self.cfg.scalp_sl_pips * self.pip,
            tp_distance=self.cfg.scalp_tp_pips * self.pip,
            decided_at=bar.time + timedelta(minutes=1),
            reason=f"{self.name}: odchylenie {(bar.close - ema) / self.pip:.1f} pips od EMA, ATR {atr_pips:.2f} pips",
            features={"ema": round(ema, 6), "atr_pips": round(atr_pips, 2), "close": bar.close, "prev_close": prev.close},
        )


class NewsReactionStrategy(TrendPullbackStrategy):
    """Reakcja na publikację o stałej porze (domyślnie 08:30 czasu Nowego Jorku: NFP, CPI, PPI, sprzedaż detaliczna, PKB,
    wnioski o zasiłek). Nie potrzebuje pliku kalendarza: "było zdarzenie" rozpoznajemy po świecy M1 zdarzenia, która
    już się zamknęła (|ruch| >= k * ATR(M1) sprzed publikacji) - bez zaglądania w przyszłość.

    Po nr_wait_minutes (spread wraca do normy) wejście w kierunku ruchu ("momentum") albo przeciw ("fade"),
    pod warunkiem że cena nadal jest po tej samej stronie co zamknięcie świecy zdarzenia. Stały SL w pipsach, cel = rr * SL.
    Maksymalnie jedno wejście dziennie.
    """

    name = "news_reaction_v1"

    def __init__(self, cfg: StrategyConfig, pip_size: float = 0.0001):
        super().__init__(cfg)
        from zoneinfo import ZoneInfo
        if cfg.nr_mode not in ("momentum", "fade"):
            raise ValueError(f"strategy.nr_mode: '{cfg.nr_mode}' (momentum | fade)")
        self.pip = pip_size
        self.tz = ZoneInfo(cfg.nr_event_tz)
        # jedna albo kilka pór, np. "08:30,10:00"; każda pora = osobne zdarzenie, maks. jedna próba na zdarzenie
        self.events = [tuple(int(x) for x in t.strip().split(":")) for t in cfg.nr_event_time.split(",") if t.strip()]
        self.atr_m1 = ATR(30)
        self.prev_close: float | None = None
        self.day = None
        self.shock: dict | None = None  # {"dir": +1/-1, "close": float, "at": datetime}
        self.tried: set[tuple[int, int]] = set()

    def on_m15(self, bar: Bar) -> Signal | None:
        self.rsi.update(bar.close)
        self.atr.update(bar.high, bar.low, bar.close)
        return None

    def on_m1(self, bar: Bar) -> Signal | None:
        local = bar.time.astimezone(self.tz)
        if local.date() != self.day:
            self.day, self.shock, self.tried = local.date(), None, set()
        atr_before = self.atr_m1.value  # ATR sprzed tej świecy
        prev_close, self.prev_close = self.prev_close, bar.close
        self.atr_m1.update(bar.high, bar.low, bar.close)
        if atr_before is None or prev_close is None or local.weekday() >= 5:
            return None
        hm = (local.hour, local.minute)
        if hm in self.events and hm not in self.tried:
            move = bar.close - prev_close
            if abs(move) >= self.cfg.nr_shock_atr * atr_before and abs(move) / self.pip >= self.cfg.nr_min_move_pips:
                self.shock = {"dir": 1 if move > 0 else -1, "close": bar.close, "base": prev_close, "event": hm,
                              "move_pips": move / self.pip, "atr_pips": atr_before / self.pip, "at": bar.time}
            return None
        if self.shock is None:
            return None
        minutes_after = (bar.time - self.shock["at"]).total_seconds() / 60
        if minutes_after < self.cfg.nr_wait_minutes:
            return None
        self.tried.add(self.shock["event"])  # jedna próba na zdarzenie, niezależnie od wyniku warunku
        self.shock, shock = None, self.shock
        if shock is None:
            return None
        d = shock["dir"]
        if (bar.close - shock["base"]) * d <= 0:  # ruch już się w pełni cofnął - brak sygnału
            return None
        side = (Side.LONG if d > 0 else Side.SHORT) if self.cfg.nr_mode == "momentum" else (Side.SHORT if d > 0 else Side.LONG)
        if not self.allowed(side):
            return None
        sl = self.cfg.nr_sl_pips * self.pip
        return Signal(
            side=side, sl_distance=sl, tp_distance=self.cfg.nr_tp_rr * sl,
            decided_at=bar.time + timedelta(minutes=1),
            reason=f"{self.name}/{self.cfg.nr_mode} {shock['event'][0]:02d}:{shock['event'][1]:02d}: ruch {shock['move_pips']:+.1f} pips przy ATR {shock['atr_pips']:.2f}",
            features={"move_pips": round(shock["move_pips"], 1), "atr_pips": round(shock["atr_pips"], 2),
                      "minutes_after": minutes_after, "event": f"{shock['event'][0]:02d}:{shock['event'][1]:02d}"},
        )


class LondonBreakoutStrategy(TrendPullbackStrategy):
    """Bot "daily": zakres sesji azjatyckiej (domyślnie 00:00-07:00 UTC) -> po otwarciu Londynu wejście,
    gdy M1 zamyka się poza zakresem. Stop = min(lb_sl_pips, zakres), cel = lb_tp_rr * stop, maks. jedno
    wejście na kierunek dziennie, tylko do lb_entry_until. Reszta (flat_at, stop czasowy, limit straty) w silniku.

    1-2 transakcje dziennie, cel 10-15 pipsów: koszt 1,2 pipsa to <10 % celu, próg rentowności ~45 % trafień.
    To hipoteza po doświadczeniu ze scalpem (koszt ~35 % celu, próg 74 %).
    """

    name = "london_breakout_v1"

    def __init__(self, cfg: StrategyConfig, pip_size: float = 0.0001):
        super().__init__(cfg)
        self.pip = pip_size
        self.day = None
        self.range_hi = self.range_lo = None
        self.done: set[Side] = set()
        self._t = lambda s: tuple(int(x) for x in s.split(":"))
        self.past_ranges: deque[float] = deque(maxlen=max(cfg.lb_compress_lookback, 1))
        self._range_logged = False

    def on_m15(self, bar: Bar) -> Signal | None:
        self.rsi.update(bar.close)
        self.atr.update(bar.high, bar.low, bar.close)
        return None

    def on_m1(self, bar: Bar) -> Signal | None:
        if bar.time.date() != self.day:
            self.day, self.range_hi, self.range_lo, self.done = bar.time.date(), None, None, set()
            self._range_logged = False
        hm = (bar.time.hour, bar.time.minute)
        r0, r1, until = self._t(self.cfg.lb_range_start), self._t(self.cfg.lb_range_end), self._t(self.cfg.lb_entry_until)
        if r0 <= hm < r1:
            self.range_hi = bar.high if self.range_hi is None else max(self.range_hi, bar.high)
            self.range_lo = bar.low if self.range_lo is None else min(self.range_lo, bar.low)
            return None
        if self.range_hi is None or not (r1 <= hm < until):
            return None
        rng = (self.range_hi - self.range_lo) / self.pip
        history = sorted(self.past_ranges)
        if not self._range_logged:  # zakres dnia trafia do historii raz, po zamknięciu okna azjatyckiego
            self.past_ranges.append(rng)
            self._range_logged = True
        if self.cfg.lb_compress_lookback > 0:
            if len(history) < self.cfg.lb_compress_lookback:
                return None
            median = history[len(history) // 2]
            if rng > self.cfg.lb_compress_ratio * median:
                return None
        if not (self.cfg.lb_min_range_pips <= rng <= self.cfg.lb_max_range_pips):
            return None
        side = None
        if bar.close > self.range_hi:
            side = Side.LONG
        elif bar.close < self.range_lo:
            side = Side.SHORT
        if side is None or not self.allowed(side) or (self.cfg.lb_one_per_direction and side in self.done):
            return None
        self.done.add(side)
        sl = min(self.cfg.lb_sl_pips, rng) * self.pip
        return Signal(
            side=side, sl_distance=sl, tp_distance=self.cfg.lb_tp_rr * sl,
            decided_at=bar.time + timedelta(minutes=1),
            reason=f"{self.name}: wybicie zakresu {self.range_lo:.5f}-{self.range_hi:.5f} ({rng:.1f} pips)",
            features={"range_pips": round(rng, 1), "close": bar.close, "range_hi": self.range_hi, "range_lo": self.range_lo},
        )


STRATEGIES = {TrendPullbackStrategy.name: TrendPullbackStrategy, BreakoutStrategy.name: BreakoutStrategy,
              SwingStrategy.name: SwingStrategy, ScalpMeanRevStrategy.name: ScalpMeanRevStrategy,
              LondonBreakoutStrategy.name: LondonBreakoutStrategy, NewsReactionStrategy.name: NewsReactionStrategy}


def build_strategy(cfg: StrategyConfig, pip_size: float = 0.0001) -> TrendPullbackStrategy:
    try:
        cls = STRATEGIES[cfg.name]
    except KeyError:
        raise ValueError(f"Nieznana strategia '{cfg.name}'. Dostępne: {sorted(STRATEGIES)}") from None
    if cls in (ScalpMeanRevStrategy, LondonBreakoutStrategy, NewsReactionStrategy):
        return cls(cfg, pip_size)
    return cls(cfg)
