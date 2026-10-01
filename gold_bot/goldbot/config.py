"""Konfiguracja wczytywana z TOML (tomllib, bez zależności zewnętrznych)."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field, fields, replace
from pathlib import Path

from goldbot.models import InstrumentSpec


@dataclass(frozen=True)
class RiskConfig:
    initial_balance: float = 500.0
    risk_per_trade_pct: float = 1.0  # maks. planowana strata (z kosztami) na transakcję, % salda
    max_daily_loss_pct: float = 3.0  # po przekroczeniu: brak nowych pozycji do końca dnia UTC
    max_open_positions: int = 1
    max_spread: float = 0.60  # USD/oz; szerszy spread = brak wejścia
    max_margin_usage_pct: float = 50.0


@dataclass(frozen=True)
class StrategyConfig:
    name: str = "trend_pullback_v1"  # albo "breakout_v1" (częstsze wejścia)
    direction: str = "both"  # "both" | "long" (tylko kupno) | "short"
    h1_ema_fast: int = 20
    h1_ema_slow: int = 50
    use_h4: bool = True
    h4_ema: int = 20
    m15_rsi_period: int = 14
    rsi_long_trigger: float = 40.0
    rsi_short_trigger: float = 60.0
    m15_atr_period: int = 14
    sl_atr_mult: float = 1.5
    tp_atr_mult: float = 2.0
    min_atr: float = 0.5
    max_atr: float = 40.0
    max_hold_minutes: int = 0  # stop czasowy: zamknij pozycję po N minutach (0 = wyłączony)
    # scalp_meanrev_v1: odchylenie od EMA(M1) o k*ATR(M1), wejście w stronę EMA, SL/TP w pipsach
    scalp_ema_m1: int = 20
    scalp_atr_m1: int = 14
    scalp_dev_atr: float = 2.0
    scalp_sl_pips: float = 4.0
    scalp_tp_pips: float = 3.0
    scalp_min_atr_pips: float = 0.5  # martwy rynek = brak wejść
    scalp_max_atr_pips: float = 4.0  # zbyt nerwowy = brak wejść
    # london_breakout_v1: zakres sesji azjatyckiej [range_start, range_end) UTC, wejście na zamknięciu M1 poza zakresem
    lb_range_start: str = "00:00"
    lb_range_end: str = "07:00"
    lb_entry_until: str = "11:00"  # po tej godzinie wybicie już nie liczy się jako "otwarcie Londynu"
    lb_min_range_pips: float = 8.0
    lb_max_range_pips: float = 40.0
    lb_sl_pips: float = 8.0  # stop = min(lb_sl_pips, zakres) - nie szerzej niż cały zakres
    lb_tp_rr: float = 1.5  # cel = lb_tp_rr * stop
    lb_one_per_direction: bool = True
    lb_compress_lookback: int = 0  # >0: wchodź tylko gdy zakres azjatycki < lb_compress_ratio * mediana z N poprzednich dni
    lb_compress_ratio: float = 1.0
    # news_reaction_v1: reakcja na publikację o stałej godzinie (domyślnie 08:30 Nowy Jork = dane makro USA)
    nr_event_time: str = "08:30"
    nr_event_tz: str = "America/New_York"
    nr_shock_atr: float = 4.0  # świeca zdarzenia musi mieć |close - poprzedni close| >= k * ATR(M1) sprzed zdarzenia
    nr_min_move_pips: float = 5.0
    nr_wait_minutes: int = 2  # ile minut po zamknięciu świecy zdarzenia czekamy (spread wraca do normy)
    nr_mode: str = "momentum"  # "momentum" = w kierunku ruchu, "fade" = przeciw
    nr_sl_pips: float = 10.0
    nr_tp_rr: float = 1.5
    # swing_v1: wybicie kanału z ostatnich N świec H4 w kierunku trendu H4, trailing stop k*ATR(H4)
    swing_lookback_h4: int = 20
    swing_atr_period_h4: int = 14
    swing_trail_atr: float = 2.0
    # breakout_v1: wybicie ponad max / poniżej min ostatnich N świec M15
    breakout_lookback: int = 8
    breakout_min_range_atr: float = 1.0  # zakres N świec musi mieć co najmniej tyle ATR (odsiewa flatę)


@dataclass(frozen=True)
class SessionConfig:
    # Codzienna przerwa handlowa (UTC). U wielu brokerów XAU ma przerwę ok. 21:00-22:00 UTC;
    # sprawdź godziny w specyfikacji instrumentu u swojego brokera.
    weekend_trading: bool = False  # True dla rynków 24/7 (krypto)
    daily_break_start: str = "20:55"
    daily_break_end: str = "22:05"
    no_new_entries_friday_after: str = "19:00"
    close_before_weekend_at: str = "20:45"  # piątek, UTC; pusty napis = nie zamykaj
    rollover_utc: str = "21:00"  # moment naliczania finansowania overnight
    trade_window_start: str = ""  # nowe wejścia tylko w oknie [start, end) UTC; pusty = brak ograniczenia
    trade_window_end: str = ""
    flat_at: str = ""  # codziennie o tej godzinie UTC zamknij wszystko (bot "daily")
    max_entry_delay_minutes: int = 3  # jeśli następna świeca przychodzi później - anuluj wejście
    max_data_gap_minutes: int = 10  # przerwa w danych unieważnia bieżącą decyzję


@dataclass(frozen=True)
class CalendarConfig:
    blackout_before_minutes: int = 30
    blackout_after_minutes: int = 30
    close_positions_before_event: bool = False


@dataclass(frozen=True)
class JevConfig:
    enabled: bool = False
    base_url: str = ""  # zweryfikuj format endpointu w dokumentacji TypeSafe AI
    api_key_env: str = "JEV_API_KEY"
    model: str = ""  # przypięta wersja, NIE "jev-latest"
    timeout_seconds: float = 20.0
    lookback_minutes: int = 240
    max_items: int = 20
    block_categories: tuple[str, ...] = ("geopolitical_escalation", "central_bank_surprise", "market_structure_disruption")
    on_error: str = "block"  # "block" = brak wejścia przy błędzie modelu, "allow" = przepuść
    cache_path: str = "runs/jev_cache.jsonl"


@dataclass(frozen=True)
class BotConfig:
    instrument: InstrumentSpec = field(default_factory=InstrumentSpec)
    risk: RiskConfig = field(default_factory=RiskConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    session: SessionConfig = field(default_factory=SessionConfig)
    calendar: CalendarConfig = field(default_factory=CalendarConfig)
    jev: JevConfig = field(default_factory=JevConfig)


def _build(cls, data: dict | None):
    obj = cls()
    if not data:
        return obj
    known = {f.name: f for f in fields(cls)}
    unknown = set(data) - set(known)
    if unknown:
        raise ValueError(f"Nieznane klucze w sekcji {cls.__name__}: {sorted(unknown)}")
    values = {}
    for k, v in data.items():
        if isinstance(getattr(obj, k), tuple) and isinstance(v, list):
            v = tuple(v)
        values[k] = v
    return replace(obj, **values)


def load_config(path: str | Path | None) -> BotConfig:
    if path is None:
        return BotConfig()
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    allowed = {"instrument", "risk", "strategy", "session", "calendar", "jev"}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"Nieznane sekcje konfiguracji: {sorted(unknown)}")
    return BotConfig(
        instrument=_build(InstrumentSpec, raw.get("instrument")),
        risk=_build(RiskConfig, raw.get("risk")),
        strategy=_build(StrategyConfig, raw.get("strategy")),
        session=_build(SessionConfig, raw.get("session")),
        calendar=_build(CalendarConfig, raw.get("calendar")),
        jev=_build(JevConfig, raw.get("jev")),
    )
