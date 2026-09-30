"""Kalendarz ważnych publikacji: filtr "nie otwieraj pozycji w pobliżu wydarzenia".

Źródła:
* BLS udostępnia harmonogram publikacji jako plik ICS (np. Employment Situation, CPI, PPI).
* Terminy posiedzeń FOMC publikuje Fed (federalreserve.gov) - wpisujemy je do CSV.

Kalendarz zawiera tylko terminy. Nie zawiera wyników publikacji ani konsensusu -
bot omija te momenty zamiast grać na reakcję.
"""

from __future__ import annotations

import csv
import re
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from goldbot.data.loaders import parse_time

# BLS używa TZID w stylu "US-Eastern"; mapujemy na strefy IANA.
_TZ_ALIASES = {
    "US-Eastern": "America/New_York",
    "US/Eastern": "America/New_York",
    "Eastern Standard Time": "America/New_York",
}

DEFAULT_BLS_KEYWORDS = (
    "Employment Situation", "Consumer Price Index", "Producer Price Index",
    "Job Openings", "Employment Cost Index", "Real Earnings",
)


@dataclass(frozen=True)
class Event:
    time: datetime  # UTC
    name: str
    source: str = ""


class EventCalendar:
    def __init__(self, events: list[Event] | None = None):
        self.events = sorted(events or [], key=lambda e: e.time)
        self._times = [e.time for e in self.events]

    def extend(self, events: list[Event]) -> None:
        self.__init__(self.events + list(events))

    def __len__(self) -> int:
        return len(self.events)

    def events_near(self, t: datetime, before: timedelta, after: timedelta) -> list[Event]:
        """Wydarzenia w oknie [t - after, t + before]: przed publikacją czekamy `before`,
        po publikacji `after`."""
        lo = bisect_left(self._times, t - after)
        hi = bisect_left(self._times, t + before + timedelta(microseconds=1))
        return self.events[lo:hi]


def load_events_csv(path: str | Path) -> list[Event]:
    """CSV: time_utc,name,source  (time_utc w ISO 8601)."""
    out = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if not row.get("time_utc") or row["time_utc"].startswith("#"):
                continue
            out.append(Event(parse_time(row["time_utc"]), row["name"].strip(), (row.get("source") or "").strip()))
    return out


def _unfold(text: str) -> list[str]:
    lines: list[str] = []
    for raw in text.splitlines():
        if raw[:1] in (" ", "\t") and lines:
            lines[-1] += raw[1:]
        else:
            lines.append(raw.rstrip("\r"))
    return lines


def _parse_ics_dt(prop: str, value: str) -> datetime | None:
    params = dict(p.split("=", 1) for p in prop.split(";")[1:] if "=" in p)
    if params.get("VALUE") == "DATE" or re.fullmatch(r"\d{8}", value):
        return None  # wydarzenie całodniowe - brak godziny publikacji, pomijamy
    fmt = "%Y%m%dT%H%M%S"
    if value.endswith("Z"):
        return datetime.strptime(value[:-1], fmt).replace(tzinfo=timezone.utc)
    naive = datetime.strptime(value, fmt)
    tzid = params.get("TZID", "UTC").strip('"')
    tz = ZoneInfo(_TZ_ALIASES.get(tzid, tzid))
    return naive.replace(tzinfo=tz).astimezone(timezone.utc)


def parse_ics(text: str, source: str = "ics", keywords: tuple[str, ...] | None = None) -> list[Event]:
    events, cur = [], None
    for line in _unfold(text):
        if line == "BEGIN:VEVENT":
            cur = {}
        elif line == "END:VEVENT":
            if cur and cur.get("dt") and cur.get("summary"):
                if not keywords or any(k.lower() in cur["summary"].lower() for k in keywords):
                    events.append(Event(cur["dt"], cur["summary"], source))
            cur = None
        elif cur is not None and ":" in line:
            prop, value = line.split(":", 1)
            name = prop.split(";", 1)[0].upper()
            if name == "DTSTART":
                cur["dt"] = _parse_ics_dt(prop, value.strip())
            elif name == "SUMMARY":
                cur["summary"] = value.strip().replace("\\,", ",").replace("\;", ";")
    return events


def load_ics(path: str | Path, source: str = "BLS", keywords: tuple[str, ...] | None = DEFAULT_BLS_KEYWORDS) -> list[Event]:
    return parse_ics(Path(path).read_text(encoding="utf-8", errors="replace"), source, keywords)
