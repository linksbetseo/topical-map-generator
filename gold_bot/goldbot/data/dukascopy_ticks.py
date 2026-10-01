"""Ticki Dukascopy: {SYMBOL}/{YYYY}/{MM0}/{DD}/{HH}h_ticks.bi5 (miesiąc od zera, godzina UTC).

Rekord 20 bajtów big-endian: uint32 ms od początku godziny, uint32 ask, uint32 bid, float32 wolumen ask, float32 wolumen bid.
Skala ceny jak dla świec (EURUSD 1e5, XAUUSD 1e3). Format sprawdzony empirycznie 01.10.2026.

Lokalny katalog cache: pliki nazwane {YYYY-MM-DD}_{HH}h_ticks.bi5 (jak zapisuje skrypt pobierania).
"""

from __future__ import annotations

import lzma
import re
import struct
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

from goldbot.data.dukascopy_feed import PRICE_SCALE
from goldbot.models import Quote

TICK = struct.Struct(">IIIff")
_NAME = re.compile(r"^(\d{4}-\d{2}-\d{2})_(\d{2})h_ticks\.bi5$")


def decode_ticks(raw: bytes, hour_start: datetime, scale: float, source: str = "dukascopy") -> list[Quote]:
    if not raw:
        return []
    data = lzma.decompress(raw)
    if len(data) % TICK.size:
        raise ValueError(f"nieprawidłowa długość danych tickowych: {len(data)}")
    out = []
    for i in range(0, len(data), TICK.size):
        ms, ask, bid, _av, _bv = TICK.unpack_from(data, i)
        out.append(Quote(hour_start + timedelta(milliseconds=ms), bid / scale, ask / scale, source=source))
    return out


def iter_ticks_dir(path: str | Path, symbol: str = "EURUSD", start: datetime | None = None,
                   end: datetime | None = None) -> Iterator[Quote]:
    """Czyta po kolei pliki godzinowe z katalogu (posortowane), pomija puste (brak handlu)."""
    scale = PRICE_SCALE.get(symbol, 100000.0)
    files = []
    for f in Path(path).iterdir():
        m = _NAME.match(f.name)
        if m:
            d = datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(hours=int(m.group(2)))
            files.append((d, f))
    for hour_start, f in sorted(files):
        if (start and hour_start + timedelta(hours=1) <= start) or (end and hour_start >= end):
            continue
        for q in decode_ticks(f.read_bytes(), hour_start, scale):
            if (start is None or q.time >= start) and (end is None or q.time < end):
                yield q
