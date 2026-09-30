"""Tryb paper: notowania na żywo (lub odtwarzane) -> świece M1 -> ten sam silnik co w backteście.

Stan konta jest zapisywany do pliku po każdej świecy, dzięki czemu restart programu
nie powoduje ponownego wejścia na tej samej decyzji ani utraty otwartej pozycji.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator

from goldbot.data.resample import TickToM1
from goldbot.engine import Engine
from goldbot.models import BidAskBar, Position, Quote, Side


def warmup(engine: Engine, bars: Iterable[BidAskBar]) -> int:
    """Rozgrzewa wskaźniki na historii - bez decyzji i bez transakcji."""
    n = 0
    for bar in bars:
        mid = bar.mid()
        for b in engine.h4.update(mid):
            engine.strategy.on_h4(b)
        for b in engine.h1.update(mid):
            engine.strategy.on_h1(b)
        for b in engine.m15.update(mid):
            engine.strategy.on_m15(b)
        n += 1
    return n


def save_state(engine: Engine, path: str | Path) -> None:
    acc = engine.account
    state = {
        "balance": acc.balance,
        "next_id": acc._next_id,
        "positions": [{**asdict(p), "side": p.side.value, "entry_time": p.entry_time.isoformat()} for p in acc.positions],
        "decided_keys": sorted(k.isoformat() for k in engine.decided_keys)[-200:],
        "last_bar_time": engine.last_bar.time.isoformat() if engine.last_bar else None,
        "trades_closed": len(acc.trades),
    }
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(path)


def load_state(engine: Engine, path: str | Path) -> bool:
    p = Path(path)
    if not p.exists():
        return False
    s = json.loads(p.read_text())
    acc = engine.account
    acc.balance = s["balance"]
    acc._next_id = s["next_id"]
    acc.positions = []
    for d in s["positions"]:
        d["side"] = Side(d["side"])
        d["entry_time"] = datetime.fromisoformat(d["entry_time"])
        acc.positions.append(Position(**d))
    engine.decided_keys = {datetime.fromisoformat(k) for k in s["decided_keys"]}
    return True


def replay_quotes(bars: Iterable[BidAskBar]) -> Iterator[Quote]:
    """Zamienia świece na sekwencję ticków (open, ekstrema, close) - test ścieżki na żywo bez sieci."""
    for b in bars:
        up = b.bid_close >= b.bid_open
        seq = [(b.bid_open, b.ask_open),
               (b.bid_low, b.ask_low) if up else (b.bid_high, b.ask_high),
               (b.bid_high, b.ask_high) if up else (b.bid_low, b.ask_low),
               (b.bid_close, b.ask_close)]
        for i, (bid, ask) in enumerate(seq):
            t = b.time + timedelta(seconds=15 * i)
            yield Quote(t, bid, ask, received_at=t, source="replay")


class PaperRunner:
    def __init__(self, engine: Engine, state_path: str | Path | None = None, max_quote_age_seconds: float = 5.0):
        self.engine = engine
        self.state_path = state_path
        self.m1 = TickToM1()
        self.max_quote_age = timedelta(seconds=max_quote_age_seconds)
        self.stale_quotes = 0
        if state_path and load_state(engine, state_path):
            engine.journal.log(datetime.now().astimezone(), "state_restored", positions=len(engine.account.positions))

    def on_quote(self, q: Quote) -> None:
        if q.received_at and q.received_at - q.time > self.max_quote_age:
            self.stale_quotes += 1
            return
        if q.bid <= 0 or q.ask < q.bid:
            return
        bar = self.m1.update(q)
        if bar is not None:
            self._on_bar(bar)

    def _on_bar(self, bar: BidAskBar) -> None:
        self.engine.on_bar(bar)
        if self.state_path:
            save_state(self.engine, self.state_path)

    def on_disconnect(self, reason: str) -> None:
        # Nie flushujemy niepełnej minuty jako kompletnej świecy; luka zostanie wykryta
        # przez silnik (data_gap) po wznowieniu strumienia.
        self.engine.journal.log(datetime.now().astimezone(), "disconnected", reason=reason)
