"""Podstawowe struktury danych. Wszystkie czasy są świadome strefy (UTC)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class Side(str, Enum):
    LONG = "long"
    SHORT = "short"


@dataclass(frozen=True)
class Bar:
    """Świeca jednej strony notowań (np. mid). `time` = początek świecy."""

    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass(frozen=True)
class BidAskBar:
    """Świeca M1 z osobnymi cenami bid i ask. `time` = początek świecy."""

    time: datetime
    bid_open: float
    bid_high: float
    bid_low: float
    bid_close: float
    ask_open: float
    ask_high: float
    ask_low: float
    ask_close: float
    volume: float = 0.0

    @property
    def spread_open(self) -> float:
        return self.ask_open - self.bid_open

    @property
    def spread_close(self) -> float:
        return self.ask_close - self.bid_close

    def mid(self) -> Bar:
        return Bar(
            time=self.time,
            open=(self.bid_open + self.ask_open) / 2,
            high=(self.bid_high + self.ask_high) / 2,
            low=(self.bid_low + self.ask_low) / 2,
            close=(self.bid_close + self.ask_close) / 2,
            volume=self.volume,
        )


@dataclass(frozen=True)
class Quote:
    """Pojedyncze notowanie (tick). `received_at` = kiedy dotarło do nas."""

    time: datetime
    bid: float
    ask: float
    received_at: datetime | None = None
    source: str = ""


@dataclass(frozen=True)
class InstrumentSpec:
    """Specyfikacja instrumentu. Wartości należy pobrać od brokera (cTrader/MT5),
    przykładowe domyślne liczby w config.example.toml są tylko punktem startowym."""

    symbol: str = "XAUUSD"
    contract_size: float = 100.0  # uncji na 1 lot
    min_volume_lots: float = 0.01
    volume_step_lots: float = 0.01
    max_volume_lots: float = 50.0
    commission_per_lot_per_side: float = 3.5  # USD
    commission_pct_per_side: float = 0.0  # % wartości pozycji (giełdy krypto, np. 0.1)
    swap_long_per_lot_per_night: float = -0.0  # USD, ujemne = koszt
    swap_short_per_lot_per_night: float = -0.0
    triple_swap_weekday: int = 2  # 0 = poniedziałek, 2 = środa
    leverage: float = 20.0
    slippage_per_oz: float = 0.05  # jednostki ceny na 1 jednostkę instrumentu (XAU: USD/oz; EURUSD: 0.00002 = 0.2 pipsa)
    pip_size: float = 0.01  # XAU: 0.01; pary FX: 0.0001 (JPY: 0.01) - używane przez strategie w pipsach

    @property
    def min_volume_oz(self) -> float:
        return self.min_volume_lots * self.contract_size

    @property
    def volume_step_oz(self) -> float:
        return self.volume_step_lots * self.contract_size


@dataclass
class Signal:
    side: Side
    sl_distance: float  # USD na uncję od ceny wejścia
    tp_distance: float
    decided_at: datetime  # moment zamknięcia świecy M15, na której podjęto decyzję
    reason: str
    features: dict = field(default_factory=dict)


@dataclass
class Position:
    id: int
    side: Side
    volume_oz: float
    entry_time: datetime
    entry_price: float  # ask dla long, bid dla short, z poślizgiem
    sl: float
    tp: float
    commission_paid: float = 0.0
    swap_paid: float = 0.0
    signal_reason: str = ""


@dataclass
class Trade:
    id: int
    side: Side
    volume_oz: float
    entry_time: datetime
    entry_price: float
    exit_time: datetime
    exit_price: float
    exit_reason: str
    gross_pnl: float
    commission: float
    swap: float
    net_pnl: float
    signal_reason: str = ""
    ambiguous_bar: bool = False
