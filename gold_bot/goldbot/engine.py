"""Silnik zdarzeniowy: przetwarza świece M1 bid/ask - ten sam kod w backteście i w trybie paper.

Kolejność na każdej świecy M1:
 1. kontrola danych (duplikaty, luki), finansowanie overnight
 2. realizacja oczekującego wejścia po cenie OTWARCIA tej świecy (decyzja zapadła wcześniej)
 3. wymuszone zamknięcia (weekend, opcjonalnie wydarzenia z kalendarza)
 4. kontrola SL/TP
 5. agregacja do M15/H1/H4; po zamknięciu M15 - decyzja strategii i reguły ryzyka
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, time, timedelta

from goldbot.calendar import EventCalendar
from goldbot.config import BotConfig
from goldbot.data.resample import BarAggregator
from goldbot.filters.base import SignalFilter
from goldbot.journal import Journal
from goldbot.models import BidAskBar, Side, Signal
from goldbot.risk import size_position
from goldbot.simulator import Account
from goldbot.strategy import TrendPullbackStrategy


def _hm(s: str) -> time | None:
    if not s:
        return None
    h, m = s.split(":")
    return time(int(h), int(m))


@dataclass
class _Pending:
    signal: Signal


class Engine:
    def __init__(self, cfg: BotConfig, strategy: TrendPullbackStrategy | None = None,
                 signal_filter: SignalFilter | None = None, calendar: EventCalendar | None = None,
                 journal: Journal | None = None):
        self.cfg = cfg
        self.strategy = strategy or TrendPullbackStrategy(cfg.strategy)
        self.filter = signal_filter or SignalFilter()
        self.calendar = calendar or EventCalendar()
        self.journal = journal or Journal()
        self.account = Account(cfg.risk.initial_balance, cfg.instrument)
        self.m15, self.h1, self.h4 = BarAggregator(15), BarAggregator(60), BarAggregator(240)
        self.pending: _Pending | None = None
        self.last_bar: BidAskBar | None = None
        self.last_gap_at: datetime | None = None
        self.decided_keys: set[datetime] = set()
        self.day: object = None
        self.day_start_equity = cfg.risk.initial_balance
        self.peak_equity = cfg.risk.initial_balance
        self.max_drawdown = 0.0
        self.max_drawdown_pct = 0.0
        self.equity_curve: list[tuple[datetime, float]] = []
        self.stats = Counter()
        self.swap_total = 0.0
        s = cfg.session
        self._break = (_hm(s.daily_break_start), _hm(s.daily_break_end))
        self._fri_cut = _hm(s.no_new_entries_friday_after)
        self._weekend_close = _hm(s.close_before_weekend_at)
        self._rollover = _hm(s.rollover_utc)

    # --- sesja ------------------------------------------------------------------------
    def entries_allowed(self, t: datetime) -> tuple[bool, str]:
        wd, tt = t.weekday(), t.time()
        if wd == 5 or (wd == 6 and self._break[1] and tt < self._break[1]):
            return False, "weekend"
        if wd == 4 and self._fri_cut and tt >= self._fri_cut:
            return False, "friday_cutoff"
        b0, b1 = self._break
        if b0 and b1 and (b0 <= tt < b1 if b0 < b1 else tt >= b0 or tt < b1):
            return False, "daily_break"
        return True, ""

    def _rollovers_between(self, a: datetime, b: datetime) -> list[datetime]:
        if not self._rollover:
            return []
        out = []
        d = a.date()
        while d <= b.date():
            r = datetime.combine(d, self._rollover, tzinfo=a.tzinfo)
            if a < r <= b:
                out.append(r)
            d += timedelta(days=1)
        return out

    # --- główna pętla -----------------------------------------------------------------
    def on_bar(self, bar: BidAskBar) -> None:
        prev = self.last_bar
        if prev is not None and bar.time <= prev.time:
            self.stats["bars_skipped_non_monotonic"] += 1
            return
        self.stats["bars"] += 1
        if prev is not None:
            gap = bar.time - prev.time
            if gap > timedelta(minutes=self.cfg.session.max_data_gap_minutes):
                self.last_gap_at = bar.time
                self.stats["data_gaps"] += 1
            if self.account.positions:
                for r in self._rollovers_between(prev.time, bar.time):
                    nights = 3 if r.weekday() == self.cfg.instrument.triple_swap_weekday else 1
                    amt = self.account.apply_swap(nights)
                    self.swap_total += amt
                    self.journal.log(r, "swap", amount=round(amt, 4), nights=nights)
        if bar.time.date() != self.day:
            self.day = bar.time.date()
            self.day_start_equity = self.account.equity(bar.bid_open, bar.ask_open)

        self._fill_pending(bar)
        self._forced_closes(bar)
        for tr in self.account.check_exits(bar):
            self.journal.log(bar.time, "exit", trade_id=tr.id, reason=tr.exit_reason, price=tr.exit_price,
                             net_pnl=tr.net_pnl, ambiguous_bar=tr.ambiguous_bar)
            if tr.ambiguous_bar:
                self.stats["ambiguous_sl_tp_bars"] += 1

        self._mark(bar)
        self.last_bar = bar

        mid = bar.mid()
        for b in self.h4.update(mid):
            self.strategy.on_h4(b)
        for b in self.h1.update(mid):
            self.strategy.on_h1(b)
        for b in self.m15.update(mid):
            sig = self.strategy.on_m15(b)
            if sig is not None:
                self._decide(sig, bar)

    def _mark(self, bar: BidAskBar) -> None:
        eq = self.account.equity(bar.bid_close, bar.ask_close)
        self.peak_equity = max(self.peak_equity, eq)
        dd = self.peak_equity - eq
        if dd > self.max_drawdown:
            self.max_drawdown = dd
        if self.peak_equity > 0:
            self.max_drawdown_pct = max(self.max_drawdown_pct, dd / self.peak_equity * 100)
        if bar.time.minute == 0 or not self.equity_curve:
            self.equity_curve.append((bar.time, round(eq, 2)))

    def _reject(self, t: datetime, sig: Signal, reason: str, **extra) -> None:
        key = reason.split(" ")[0]
        self.stats[f"rejected:{key}"] += 1
        self.journal.log(t, "decision", action="reject", reason=reason, side=sig.side, signal=sig.reason,
                         features=sig.features, **extra)

    def _decide(self, sig: Signal, bar: BidAskBar) -> None:
        t = sig.decided_at
        self.stats["signals"] += 1
        if t in self.decided_keys:
            self._reject(t, sig, "duplicate_decision")
            return
        self.decided_keys.add(t)
        if self.last_gap_at and self.last_gap_at > t - timedelta(minutes=15):
            return self._reject(t, sig, "data_gap")
        ok, why = self.entries_allowed(t)
        if not ok:
            return self._reject(t, sig, f"session_{why}")
        if self.pending is not None or len(self.account.positions) >= self.cfg.risk.max_open_positions:
            return self._reject(t, sig, "max_positions")
        eq = self.account.equity(bar.bid_close, bar.ask_close)
        if (eq - self.day_start_equity) <= -self.day_start_equity * self.cfg.risk.max_daily_loss_pct / 100:
            return self._reject(t, sig, "daily_loss_limit")
        cal = self.cfg.calendar
        near = self.calendar.events_near(t, timedelta(minutes=cal.blackout_before_minutes), timedelta(minutes=cal.blackout_after_minutes))
        if near:
            return self._reject(t, sig, "event_blackout", events=[f"{e.time.isoformat()} {e.name}" for e in near])
        entry_est = bar.ask_close if sig.side is Side.LONG else bar.bid_close
        free_margin = eq - self.account.used_margin()
        sizing = size_position(sig.side, entry_est, sig.sl_distance, eq, self.cfg.instrument, self.cfg.risk, free_margin)
        if not sizing.ok:
            return self._reject(t, sig, sizing.reason)
        verdict = self.filter.evaluate(sig, t)
        if not verdict.allow:
            return self._reject(t, sig, f"filter:{verdict.reason}", filter_details=verdict.details)
        self.pending = _Pending(sig)
        self.stats["accepted"] += 1
        self.journal.log(t, "decision", action="accept", side=sig.side, signal=sig.reason, features=sig.features,
                         planned_volume_oz=sizing.volume_oz, planned_loss=sizing.planned_loss, filter=verdict.reason)

    def _fill_pending(self, bar: BidAskBar) -> None:
        if self.pending is None:
            return
        sig = self.pending.signal
        self.pending = None
        if bar.time - sig.decided_at > timedelta(minutes=self.cfg.session.max_entry_delay_minutes):
            return self._reject(bar.time, sig, "stale_entry")
        ok, why = self.entries_allowed(bar.time)
        if not ok:
            return self._reject(bar.time, sig, f"session_{why}")
        if bar.spread_open > self.cfg.risk.max_spread:
            return self._reject(bar.time, sig, "spread_too_wide", spread=round(bar.spread_open, 3))
        eq = self.account.equity(bar.bid_open, bar.ask_open)
        entry = bar.ask_open if sig.side is Side.LONG else bar.bid_open
        sizing = size_position(sig.side, entry, sig.sl_distance, eq, self.cfg.instrument, self.cfg.risk,
                               eq - self.account.used_margin())
        if not sizing.ok:
            return self._reject(bar.time, sig, sizing.reason)
        pos = self.account.open(sig.side, sizing.volume_oz, bar, sig.sl_distance, sig.tp_distance, sig.reason)
        self.stats["entries"] += 1
        self.journal.log(bar.time, "entry", trade_id=pos.id, side=pos.side, volume_oz=pos.volume_oz,
                         price=round(pos.entry_price, 3), sl=round(pos.sl, 3), tp=round(pos.tp, 3),
                         spread=round(bar.spread_open, 3), planned_loss=sizing.planned_loss)

    def _forced_closes(self, bar: BidAskBar) -> None:
        if not self.account.positions:
            return
        reason = None
        if self._weekend_close and bar.time.weekday() == 4 and bar.time.time() >= self._weekend_close:
            reason = "weekend_close"
        elif self.cfg.calendar.close_positions_before_event and self.calendar.events_near(
                bar.time, timedelta(minutes=self.cfg.calendar.blackout_before_minutes), timedelta(0)):
            reason = "event_close"
        if reason:
            for p in list(self.account.positions):
                tr = self.account.close_at_market(p, bar, reason)
                self.journal.log(bar.time, "exit", trade_id=tr.id, reason=reason, price=tr.exit_price, net_pnl=tr.net_pnl)

    def finish(self) -> None:
        if self.last_bar is None:
            return
        for p in list(self.account.positions):
            tr = self.account.close_at_market(p, self.last_bar, "end_of_data", use_open=False)
            self.journal.log(self.last_bar.time, "exit", trade_id=tr.id, reason="end_of_data", price=tr.exit_price, net_pnl=tr.net_pnl)
        self._mark(self.last_bar)
