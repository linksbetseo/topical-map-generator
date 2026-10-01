"""Wczytywanie historii M1 bid/ask.

Obsługiwane formaty:
* wspólny CSV: time,bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close,volume
  (time w ISO 8601; bez strefy = UTC)
* eksport Dukascopy - osobne pliki BID i ASK:
  "Gmt time,Open,High,Low,Close,Volume" z czasem "01.09.2026 00:00:00.000"
  (lub "Local time" z sufiksem "GMT+0200").
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from goldbot.models import BidAskBar

COMMON_HEADER = [
    "time", "bid_open", "bid_high", "bid_low", "bid_close",
    "ask_open", "ask_high", "ask_low", "ask_close", "volume",
]


def parse_time(s: str) -> datetime:
    s = s.strip()
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(?: GMT([+-])(\d{2})(\d{2}))?$", s)
    if m:
        d, mo, y, hh, mm, ss, frac, sign, oh, om = m.groups()
        t = datetime(int(y), int(mo), int(d), int(hh), int(mm), int(ss), tzinfo=timezone.utc)
        if frac:
            t += timedelta(microseconds=int(frac.ljust(6, "0")[:6]))
        if sign:
            off = timedelta(hours=int(oh), minutes=int(om))
            t = t - off if sign == "+" else t + off
        return t
    t = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.astimezone(timezone.utc)


def load_common_csv(path: str | Path) -> list[BidAskBar]:
    bars = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            bars.append(BidAskBar(
                time=parse_time(row["time"]),
                bid_open=float(row["bid_open"]), bid_high=float(row["bid_high"]),
                bid_low=float(row["bid_low"]), bid_close=float(row["bid_close"]),
                ask_open=float(row["ask_open"]), ask_high=float(row["ask_high"]),
                ask_low=float(row["ask_low"]), ask_close=float(row["ask_close"]),
                volume=float(row.get("volume") or 0),
            ))
    return bars


def save_common_csv(path: str | Path, bars: list[BidAskBar]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(COMMON_HEADER)
        for b in bars:
            nd = 6 if b.bid_close < 100 else 3  # pary FX potrzebują 5-6 miejsc, metale 3
            w.writerow([
                b.time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                f"{b.bid_open:.{nd}f}", f"{b.bid_high:.{nd}f}", f"{b.bid_low:.{nd}f}", f"{b.bid_close:.{nd}f}",
                f"{b.ask_open:.{nd}f}", f"{b.ask_high:.{nd}f}", f"{b.ask_low:.{nd}f}", f"{b.ask_close:.{nd}f}",
                f"{b.volume:g}",
            ])


def _load_dukascopy_side(path: str | Path) -> dict[datetime, tuple[float, float, float, float, float]]:
    out = {}
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        if len(header) < 5 or "time" not in header[0].lower():
            raise ValueError(f"{path}: nieoczekiwany nagłówek Dukascopy: {header}")
        for row in reader:
            if not row or not row[0].strip():
                continue
            t = parse_time(row[0])
            o, h, l, c = (float(x) for x in row[1:5])
            v = float(row[5]) if len(row) > 5 and row[5] else 0.0
            out[t] = (o, h, l, c, v)
    return out


@dataclass
class MergeReport:
    merged: int = 0
    only_bid: int = 0
    only_ask: int = 0
    dropped_flat_zero_volume: int = 0
    notes: list[str] = field(default_factory=list)


def load_dukascopy_pair(bid_path: str | Path, ask_path: str | Path,
                        drop_flat_zero_volume: bool = True) -> tuple[list[BidAskBar], MergeReport]:
    """Łączy eksport BID i ASK. Świece bez pary są pomijane i liczone w raporcie.

    Dukascopy wypełnia okresy bez handlu (np. weekend) płaskimi świecami z wolumenem 0 -
    domyślnie je usuwamy, żeby strategia nie liczyła wskaźników na sztucznych danych.
    """
    bid = _load_dukascopy_side(bid_path)
    ask = _load_dukascopy_side(ask_path)
    rep = MergeReport()
    rep.only_bid = len(bid.keys() - ask.keys())
    rep.only_ask = len(ask.keys() - bid.keys())
    bars = []
    for t in sorted(bid.keys() & ask.keys()):
        bo, bh, bl, bc, bv = bid[t]
        ao, ah, al, ac, av = ask[t]
        if drop_flat_zero_volume and bv == 0 and av == 0 and bo == bh == bl == bc and ao == ah == al == ac:
            rep.dropped_flat_zero_volume += 1
            continue
        bars.append(BidAskBar(t, bo, bh, bl, bc, ao, ah, al, ac, volume=bv))
    rep.merged = len(bars)
    return bars, rep
