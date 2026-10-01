"""Pobieranie historii M1 bid/ask z publicznego feedu Dukascopy.

Adres: https://datafeed.dukascopy.com/datafeed/{SYMBOL}/{YYYY}/{MM}/{DD}/{BID|ASK}_candles_min_1.bi5
UWAGA: miesiąc w URL jest liczony OD ZERA (styczeń = 00). Dzień = doba UTC.

Plik .bi5 to strumień LZMA z rekordami po 24 bajty (big-endian):
  uint32 sekunda od początku doby, uint32 open, uint32 close, uint32 low, uint32 high, float32 wolumen
Ceny XAUUSD są w tysięcznych USD (4626305 -> 4626.305).

Format i skala zostały sprawdzone empirycznie 30.09.2026 na plikach z maja 2026 r.
Serwer wymaga nagłówka User-Agent i zwraca 429 przy zbyt częstych zapytaniach,
dlatego pobieramy z odstępem i zapisujemy surowe pliki w lokalnym cache.
To dane do badań; zasady użycia opisuje Dukascopy na stronie data-export.
"""

from __future__ import annotations

import lzma
import struct
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from goldbot.models import BidAskBar

BASE_URL = "https://datafeed.dukascopy.com/datafeed"
USER_AGENT = "Mozilla/5.0 (goldbot-prototype research; contact: repo owner)"
PRICE_SCALE = {"XAUUSD": 1000.0, "XAGUSD": 1000.0, "EURUSD": 100000.0, "GBPUSD": 100000.0, "USDJPY": 1000.0, "AUDUSD": 100000.0, "USDCHF": 100000.0, "USDCAD": 100000.0}
RECORD = struct.Struct(">IIIIIf")


def url_for(symbol: str, day: date, side: str) -> str:
    return f"{BASE_URL}/{symbol}/{day.year:04d}/{day.month - 1:02d}/{day.day:02d}/{side}_candles_min_1.bi5"


def decode_candles(raw: bytes, day: date, scale: float) -> list[tuple[datetime, float, float, float, float, float]]:
    """Zwraca listę (time, open, high, low, close, volume). Pusty plik = brak handlu."""
    if not raw:
        return []
    data = lzma.decompress(raw)
    if len(data) % RECORD.size:
        raise ValueError(f"nieprawidłowa długość danych: {len(data)}")
    base = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    out = []
    for i in range(0, len(data), RECORD.size):
        sec, o, c, lo, hi, vol = RECORD.unpack_from(data, i)
        out.append((base + timedelta(seconds=sec), o / scale, hi / scale, lo / scale, c / scale, float(vol)))
    return out


class DukascopyDownloader:
    def __init__(self, cache_dir: str | Path = "data/dukascopy_cache", symbol: str = "XAUUSD",
                 delay_seconds: float = 4.0, timeout: float = 30.0, max_retries: int = 5):
        self.cache = Path(cache_dir) / symbol
        self.symbol = symbol
        self.scale = PRICE_SCALE.get(symbol, 100000.0)
        self.delay = delay_seconds
        self.timeout = timeout
        self.max_retries = max_retries
        self.requests = 0
        self.cache_hits = 0
        self._last_request = 0.0

    def _fetch(self, url: str) -> bytes:
        """Pobiera plik; 404 traktujemy jako brak danych (pusty wynik)."""
        backoff = 2.0
        for attempt in range(self.max_retries):
            wait = self.delay - (time.monotonic() - self._last_request)
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()
            self.requests += 1
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    return r.read()
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return b""
                if e.code == 429 or e.code >= 500:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise
            except (urllib.error.URLError, TimeoutError):
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError(f"Nie udało się pobrać {url} po {self.max_retries} próbach")

    def _raw(self, day: date, side: str) -> bytes:
        path = self.cache / f"{day.isoformat()}_{side}.bi5"
        if path.exists():
            self.cache_hits += 1
            return path.read_bytes()
        raw = self._fetch(url_for(self.symbol, day, side))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return raw

    def day_bars(self, day: date, drop_flat_zero_volume: bool = True) -> list[BidAskBar]:
        bid = decode_candles(self._raw(day, "BID"), day, self.scale)
        ask = decode_candles(self._raw(day, "ASK"), day, self.scale)
        ask_by_time = {a[0]: a for a in ask}
        bars = []
        for t, bo, bh, bl, bc, bv in bid:
            a = ask_by_time.get(t)
            if a is None:
                continue
            _, ao, ah, al, ac, av = a
            if drop_flat_zero_volume and bv == 0 and av == 0 and bo == bh == bl == bc and ao == ah == al == ac:
                continue
            bars.append(BidAskBar(t, bo, bh, bl, bc, ao, ah, al, ac, volume=bv))
        return bars

    def range_bars(self, start: date, end: date, progress=None) -> list[BidAskBar]:
        """Świece M1 dla dni [start, end] włącznie. Soboty pomijamy (brak handlu)."""
        bars = []
        day = start
        while day <= end:
            if day.weekday() != 5:
                bars.extend(self.day_bars(day))
            if progress:
                progress(day, len(bars))
            day += timedelta(days=1)
        return bars
