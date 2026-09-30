"""Dziennik decyzji (JSONL) i transakcji (CSV)."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict
from datetime import datetime
from enum import Enum
from pathlib import Path

from goldbot.models import Trade


def _default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, Enum):
        return o.value
    raise TypeError(type(o))


class Journal:
    def __init__(self, path: str | Path | None = None, variant: str = "A"):
        self.variant = variant
        self.records: list[dict] = []
        self._f = None
        if path:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._f = open(path, "w", encoding="utf-8")

    def log(self, time: datetime, kind: str, **data) -> None:
        rec = {"time": time, "variant": self.variant, "kind": kind, **data}
        self.records.append(rec)
        if self._f:
            self._f.write(json.dumps(rec, default=_default, ensure_ascii=False) + "\n")

    def close(self) -> None:
        if self._f:
            self._f.close()
            self._f = None


def write_trades_csv(path: str | Path, trades: list[Trade]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = None
        for t in trades:
            row = {k: _default(v) if isinstance(v, (datetime, Enum)) else v for k, v in asdict(t).items()}
            if w is None:
                w = csv.DictWriter(f, fieldnames=list(row))
                w.writeheader()
            w.writerow(row)
