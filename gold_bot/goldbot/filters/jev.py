"""Eksperymentalny filtr Jev: model KLASYFIKUJE dostarczone komunikaty, kod DECYDUJE.

Zasady:
* model nie liczy cen, ryzyka ani dat - tylko odpowiada, czego dotyczy tekst;
* "confidence" modelu nie jest prawdopodobieństwem zysku i nie jest używane do decyzji;
* decyzja: blokujemy nowe wejście, jeśli w oknie jest komunikat istotny dla złota
  z kategorii z listy `block_categories` (reguła w kodzie, konfigurowalna);
* przypinamy konkretną wersję modelu - alias "jev-latest" jest odrzucany;
* odpowiedzi są cache'owane po (model, id komunikatu, wersja promptu) - backtest jest
  powtarzalny i nie płacimy dwa razy za ten sam tekst.

Format API: wywołanie zgodne z OpenAI chat-completions (POST {base_url}/chat/completions),
więc działa zarówno z bezpośrednim API dostawcy, jak i z OpenRouter
(base_url = "https://openrouter.ai/api/v1", api_key_env = "OPENROUTER_API_KEY").
Stan na 30.09.2026: publiczna lista modeli OpenRouter zawiera tylko "typesafe/jev-router"
(router wybierający model per zapytanie, cena zmienna) - nie nadaje się do eksperymentu
z przypiętą wersją. Połączenie z prawdziwym API nie zostało przetestowane.
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from goldbot.config import JevConfig
from goldbot.filters.base import FilterVerdict, SignalFilter
from goldbot.filters.news import NewsItem, NewsStore
from goldbot.models import Signal

PROMPT_VERSION = "v1"

CATEGORIES = (
    "central_bank_surprise",        # nieoczekiwana decyzja/komunikat banku centralnego
    "scheduled_macro_release",      # publikacja danych makro z kalendarza
    "geopolitical_escalation",      # eskalacja konfliktu, sankcje
    "market_structure_disruption",  # awaria giełdy/brokera, zawieszenie handlu, zmiana depozytów
    "physical_gold_flows",          # popyt banków centralnych, ETF, import/eksport
    "other",
)

SYSTEM_PROMPT = (
    "You classify short news items for a gold (XAU/USD) research system. "
    "Do not predict prices and do not give trading advice. For each item answer with JSON only: "
    '{"items": [{"id": "<id>", "relevant_to_gold": true|false, "category": "<one of: '
    + ", ".join(CATEGORIES) + '>"}]}'
)

Transport = Callable[[str, dict, dict, float], dict]


def _http_post(url: str, headers: dict, payload: dict, timeout: float) -> dict:
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


class JevError(RuntimeError):
    pass


def parse_classification(content: str, expected_ids: set[str]) -> dict[str, dict]:
    """Ściśle waliduje odpowiedź modelu. Niepoprawny JSON / brakujące id = błąd."""
    text = content.strip()
    if text.startswith("```"):
        text = text.strip("`")
        text = text[text.find("{"):]
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise JevError(f"invalid_json: {e}") from e
    out = {}
    for it in data.get("items", []):
        iid = str(it.get("id"))
        cat = it.get("category")
        rel = it.get("relevant_to_gold")
        if iid not in expected_ids or cat not in CATEGORIES or not isinstance(rel, bool):
            raise JevError(f"invalid_item: {it}")
        out[iid] = {"relevant_to_gold": rel, "category": cat}
    missing = expected_ids - out.keys()
    if missing:
        raise JevError(f"missing_ids: {sorted(missing)}")
    return out


class JevClassifier:
    def __init__(self, cfg: JevConfig, transport: Transport | None = None):
        if not cfg.model:
            raise ValueError("jev.model musi wskazywać konkretną wersję modelu")
        if cfg.model.endswith(("latest", "router")):
            # np. "jev-latest" albo "typesafe/jev-router" z OpenRouter - router sam wybiera model
            # dla każdego zapytania, więc wynik A/B nie byłby powtarzalny
            raise ValueError(f"'{cfg.model}' nie wskazuje stałej wersji modelu - przypnij konkretną wersję")
        self.cfg = cfg
        self.transport = transport or _http_post
        self.cache: dict[str, dict] = {}
        self.calls = 0
        self.input_chars = 0
        self._cache_path = Path(cfg.cache_path) if cfg.cache_path else None
        if self._cache_path and self._cache_path.exists():
            for line in self._cache_path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    d = json.loads(line)
                    self.cache[d["key"]] = d["value"]

    def _key(self, item: NewsItem) -> str:
        return f"{self.cfg.model}|{PROMPT_VERSION}|{item.id}"

    def _store(self, key: str, value: dict) -> None:
        self.cache[key] = value
        if self._cache_path:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._cache_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({"key": key, "value": value}) + "\n")

    def classify(self, items: list[NewsItem]) -> dict[str, dict]:
        result, todo = {}, []
        for it in items:
            k = self._key(it)
            if k in self.cache:
                result[it.id] = self.cache[k]
            else:
                todo.append(it)
        if todo:
            user = json.dumps([{"id": it.id, "source": it.source, "text": it.text[:1500]} for it in todo], ensure_ascii=False)
            payload = {
                "model": self.cfg.model,
                "temperature": 0,
                "messages": [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}],
            }
            key = os.environ.get(self.cfg.api_key_env, "")
            if not key or not self.cfg.base_url:
                raise JevError("brak base_url lub klucza API")
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {key}"}
            self.calls += 1
            self.input_chars += len(SYSTEM_PROMPT) + len(user)
            try:
                resp = self.transport(self.cfg.base_url.rstrip("/") + "/chat/completions", headers, payload, self.cfg.timeout_seconds)
                content = resp["choices"][0]["message"]["content"]
            except JevError:
                raise
            except Exception as e:  # sieć, timeout, zły format
                raise JevError(f"request_failed: {type(e).__name__}: {e}") from e
            parsed = parse_classification(content, {it.id for it in todo})
            for it in todo:
                self._store(self._key(it), parsed[it.id])
                result[it.id] = parsed[it.id]
        return result


class JevNewsFilter(SignalFilter):
    name = "jev_news"

    def __init__(self, cfg: JevConfig, news: NewsStore, classifier: JevClassifier | None = None):
        self.cfg = cfg
        self.news = news
        self.classifier = classifier or JevClassifier(cfg)
        self.errors = 0

    def evaluate(self, signal: Signal, now: datetime) -> FilterVerdict:
        items = self.news.window(now, timedelta(minutes=self.cfg.lookback_minutes), self.cfg.max_items)
        if not items:
            return FilterVerdict(True, "no_news_in_window")
        try:
            labels = self.classifier.classify(items)
        except JevError as e:
            self.errors += 1
            allow = self.cfg.on_error == "allow"
            return FilterVerdict(allow, f"jev_error_{'allow' if allow else 'block'}", {"error": str(e)})
        blocking = [
            {"id": it.id, "category": labels[it.id]["category"]}
            for it in items
            if labels[it.id]["relevant_to_gold"] and labels[it.id]["category"] in self.cfg.block_categories
        ]
        details = {"items": len(items), "labels": labels, "blocking": blocking}
        if blocking:
            return FilterVerdict(False, "blocked_by_news_category", details)
        return FilterVerdict(True, "no_blocking_news", details)
