"""Historia 1m BTCUSDT z publicznego archiwum Binance (data.binance.vision) - bez klucza.

Adres: https://data.binance.vision/data/spot/daily/klines/{SYMBOL}/1m/{SYMBOL}-1m-{YYYY-MM-DD}.zip
Zip zawiera CSV bez nagłówka: open_time, open, high, low, close, volume, close_time, quote_volume,
trades, taker_base, taker_quote, ignore. Czas w ms lub (pliki od 2025) w mikrosekundach.

To świece z transakcji (mid), nie bid/ask. Bid/ask odtwarzamy jako mid ± spread/2, gdzie spread
jest ZAŁOŻENIEM (spread_pct). Spot BTCUSDT ma spread rzędu 0,01 %, CFD u brokera 0,05-0,1 % -
ustaw wartość docelowego miejsca wykonania; wynik zależy od tego założenia.
"""

from __future__ import annotations

import csv
import io
import time
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from goldbot.models import BidAskBar

BASE_URL = "https://data.binance.vision/data/spot/daily/klines"


def url_for(symbol: str, day: date) -> str:
    return f"{BASE_URL}/{symbol}/1m/{symbol}-1m-{day.isoformat()}.zip"


def _ts(v: str) -> datetime:
    x = int(v)
    if x > 10**14:  # mikrosekundy
        x //= 1000
    return datetime.fromtimestamp(x / 1000, timezone.utc)


def decode_klines(raw: bytes, spread_pct: float) -> list[BidAskBar]:
    if not raw:
        return []
    z = zipfile.ZipFile(io.BytesIO(raw))
    text = z.read(z.namelist()[0]).decode()
    half = spread_pct / 100.0 / 2.0
    out = []
    for row in csv.reader(io.StringIO(text)):
        if not row or not row[0].isdigit():
            continue
        t = _ts(row[0])
        o, h, l, c = (float(x) for x in row[1:5])
        vol = float(row[5])
        out.append(BidAskBar(
            t, o * (1 - half), h * (1 - half), l * (1 - half), c * (1 - half),
            o * (1 + half), h * (1 + half), l * (1 + half), c * (1 + half), volume=vol,
        ))
    return out


class BinanceDownloader:
    def __init__(self, cache_dir: str | Path = "data/binance_cache", symbol: str = "BTCUSDT",
                 spread_pct: float = 0.02, delay_seconds: float = 0.3, timeout: float = 60.0, max_retries: int = 4):
        self.cache = Path(cache_dir) / symbol
        self.symbol = symbol
        self.spread_pct = spread_pct
        self.delay = delay_seconds
        self.timeout = timeout
        self.max_retries = max_retries
        self.requests = 0
        self.cache_hits = 0

    def _fetch(self, url: str) -> bytes:
        backoff = 3.0
        for _ in range(self.max_retries):
            time.sleep(self.delay)
            self.requests += 1
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "goldbot-prototype/0.1"}),
                                            timeout=self.timeout) as r:
                    return r.read()
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return b""
                time.sleep(backoff)
                backoff *= 2
            except (urllib.error.URLError, TimeoutError):
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError(f"Nie udało się pobrać {url}")

    def day_bars(self, day: date) -> list[BidAskBar]:
        path = self.cache / f"{day.isoformat()}.zip"
        if path.exists():
            self.cache_hits += 1
            raw = path.read_bytes()
        else:
            raw = self._fetch(url_for(self.symbol, day))
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        return decode_klines(raw, self.spread_pct)

    def range_bars(self, start: date, end: date, progress=None) -> list[BidAskBar]:
        bars = []
        day = start
        while day <= end:
            bars.extend(self.day_bars(day))
            if progress:
                progress(day, len(bars))
            day += timedelta(days=1)
        return bars
