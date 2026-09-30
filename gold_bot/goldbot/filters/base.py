"""Interfejs filtra sygnałów. Filtr może tylko ZABLOKOWAĆ wejście - nigdy go nie tworzy
ani nie zmienia wielkości pozycji, SL czy TP."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from goldbot.models import Signal


@dataclass
class FilterVerdict:
    allow: bool
    reason: str = ""
    details: dict = field(default_factory=dict)


class SignalFilter:
    name = "none"

    def evaluate(self, signal: Signal, now: datetime) -> FilterVerdict:
        return FilterVerdict(True, "no_filter")
