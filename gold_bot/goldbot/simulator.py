"""Symulator wykonania: long wchodzi po ask i wychodzi po bid, short odwrotnie.

Spread jest zawarty w cenach wykonania - nie odejmujemy go drugi raz.
Doliczamy prowizję, poślizg i finansowanie overnight.
Gdy w jednej świecy M1 dotknięto i SL, i TP, przyjmujemy wariant niekorzystny (SL).
"""

from __future__ import annotations

from datetime import datetime

from goldbot.models import BidAskBar, InstrumentSpec, Position, Side, Trade


class Account:
    def __init__(self, balance: float, spec: InstrumentSpec):
        self.balance = balance
        self.spec = spec
        self.positions: list[Position] = []
        self.trades: list[Trade] = []
        self._next_id = 1

    # --- wycena -------------------------------------------------------------------------
    def unrealized(self, bid: float, ask: float) -> float:
        total = 0.0
        for p in self.positions:
            if p.side is Side.LONG:
                total += (bid - p.entry_price) * p.volume_oz
            else:
                total += (p.entry_price - ask) * p.volume_oz
        return total

    def equity(self, bid: float, ask: float) -> float:
        return self.balance + self.unrealized(bid, ask)

    def used_margin(self) -> float:
        return sum(p.entry_price * p.volume_oz / self.spec.leverage for p in self.positions)

    # --- operacje -----------------------------------------------------------------------
    def _commission(self, volume_oz: float, price: float) -> float:
        fixed = self.spec.commission_per_lot_per_side * volume_oz / self.spec.contract_size
        return fixed + self.spec.commission_pct_per_side / 100.0 * volume_oz * price

    def open(self, side: Side, volume_oz: float, bar: BidAskBar, sl_distance: float, tp_distance: float,
             reason: str = "") -> Position:
        slip = self.spec.slippage_per_oz
        if side is Side.LONG:
            price = bar.ask_open + slip
            sl, tp = price - sl_distance, price + tp_distance
        else:
            price = bar.bid_open - slip
            sl, tp = price + sl_distance, price - tp_distance
        comm = self._commission(volume_oz, price)
        self.balance -= comm
        pos = Position(self._next_id, side, volume_oz, bar.time, price, sl, tp, commission_paid=comm, signal_reason=reason)
        self._next_id += 1
        self.positions.append(pos)
        return pos

    def close(self, pos: Position, price: float, time: datetime, reason: str, ambiguous: bool = False) -> Trade:
        gross = (price - pos.entry_price) * pos.volume_oz if pos.side is Side.LONG else (pos.entry_price - price) * pos.volume_oz
        comm = self._commission(pos.volume_oz, price)
        self.balance += gross - comm
        total_comm = pos.commission_paid + comm
        trade = Trade(pos.id, pos.side, pos.volume_oz, pos.entry_time, pos.entry_price, time, price, reason,
                      round(gross, 4), round(total_comm, 4), round(pos.swap_paid, 4),
                      round(gross - total_comm + pos.swap_paid, 4), pos.signal_reason, ambiguous)
        self.positions.remove(pos)
        self.trades.append(trade)
        return trade

    def close_at_market(self, pos: Position, bar: BidAskBar, reason: str, use_open: bool = True) -> Trade:
        slip = self.spec.slippage_per_oz
        if pos.side is Side.LONG:
            price = (bar.bid_open if use_open else bar.bid_close) - slip
        else:
            price = (bar.ask_open if use_open else bar.ask_close) + slip
        return self.close(pos, price, bar.time, reason)

    def apply_swap(self, nights: int) -> float:
        total = 0.0
        for p in self.positions:
            per_lot = self.spec.swap_long_per_lot_per_night if p.side is Side.LONG else self.spec.swap_short_per_lot_per_night
            amount = per_lot * p.volume_oz / self.spec.contract_size * nights
            p.swap_paid += amount
            total += amount
        self.balance += total
        return total

    def check_exits(self, bar: BidAskBar) -> list[Trade]:
        """Sprawdza SL/TP na świecy M1. Luka cenowa na otwarciu = wykonanie po cenie otwarcia."""
        closed = []
        slip = self.spec.slippage_per_oz
        for pos in list(self.positions):
            if pos.side is Side.LONG:
                o, h, l = bar.bid_open, bar.bid_high, bar.bid_low
                if o <= pos.sl:
                    closed.append(self.close(pos, o - slip, bar.time, "sl_gap"))
                    continue
                if o >= pos.tp:
                    closed.append(self.close(pos, o, bar.time, "tp_gap"))
                    continue
                hit_sl, hit_tp = l <= pos.sl, h >= pos.tp
                if hit_sl:
                    closed.append(self.close(pos, pos.sl - slip, bar.time, "sl", ambiguous=hit_tp))
                elif hit_tp:
                    closed.append(self.close(pos, pos.tp, bar.time, "tp"))
            else:
                o, h, l = bar.ask_open, bar.ask_high, bar.ask_low
                if o >= pos.sl:
                    closed.append(self.close(pos, o + slip, bar.time, "sl_gap"))
                    continue
                if o <= pos.tp:
                    closed.append(self.close(pos, o, bar.time, "tp_gap"))
                    continue
                hit_sl, hit_tp = h >= pos.sl, l <= pos.tp
                if hit_sl:
                    closed.append(self.close(pos, pos.sl + slip, bar.time, "sl", ambiguous=hit_tp))
                elif hit_tp:
                    closed.append(self.close(pos, pos.tp, bar.time, "tp"))
        return closed
