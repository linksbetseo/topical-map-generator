"""Komunikaty tekstowe dostarczane filtrowi (JSONL).

Każdy wiersz: {"id": "...", "published_at": "ISO", "available_at": "ISO", "source": "...", "text": "..."}

`available_at` = kiedy komunikat realnie trafił do naszego systemu. W backteście bot widzi
tylko komunikaty z available_at <= moment decyzji (brak informacji z przyszłości).
"""

from __future__ import annotations

import json
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from goldbot.data.loaders import parse_time


@dataclass(frozen=True)
class NewsItem:
    id: str
    available_at: datetime
    text: str
    source: str = ""
    published_at: datetime | None = None


def load_news_jsonl(path: str | Path) -> list[NewsItem]:
    items = []
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            if "available_at" not in d:
                raise ValueError(f"{path}:{n}: brak available_at - bez niego backtest zaglądałby w przyszłość")
            items.append(NewsItem(
                id=str(d["id"]), available_at=parse_time(d["available_at"]), text=d["text"],
                source=d.get("source", ""), published_at=parse_time(d["published_at"]) if d.get("published_at") else None,
            ))
    return items


class NewsStore:
    def __init__(self, items: list[NewsItem]):
        self.items = sorted(items, key=lambda i: i.available_at)
        self._times = [i.available_at for i in self.items]

    def window(self, now: datetime, lookback: timedelta, limit: int) -> list[NewsItem]:
        hi = bisect_right(self._times, now)
        lo = bisect_right(self._times, now - lookback)
        return self.items[lo:hi][-limit:]
