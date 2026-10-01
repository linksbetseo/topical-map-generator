"""Gold-API: pomocnicza cena referencyjna (dashboard / porównanie z brokerem).

GET https://api.gold-api.com/price/XAU - bez klucza; dostawca zaleca cache 30 s.
To NIE jest feed tradingowy: brak bid/ask konkretnego brokera, a próbkowanie co 30 s
nie odtwarza ruchów między odczytami. Nie używamy tej ceny do wykonania transakcji.
"""

from __future__ import annotations

import json
import time
import urllib.request

URL = "https://api.gold-api.com/price/XAU"
CACHE_SECONDS = 30.0


class GoldApiReference:
    def __init__(self, url: str = URL, cache_seconds: float = CACHE_SECONDS, timeout: float = 10.0):
        self.url, self.cache_seconds, self.timeout = url, cache_seconds, timeout
        self._cached: dict | None = None
        self._at = 0.0

    def get(self) -> dict:
        now = time.monotonic()
        if self._cached is not None and now - self._at < self.cache_seconds:
            return self._cached
        req = urllib.request.Request(self.url, headers={"User-Agent": "goldbot-prototype/0.1"})
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            self._cached = json.loads(r.read().decode())
        self._at = now
        return self._cached

    def deviation_from(self, broker_mid: float) -> float | None:
        """Różnica (USD) między ceną brokera a ceną referencyjną - do alertów, nie do decyzji."""
        price = self.get().get("price")
        return None if price is None else broker_mid - float(price)
