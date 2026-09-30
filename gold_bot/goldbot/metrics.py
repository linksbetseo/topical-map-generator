"""Ocena wyniku: wynik netto, obsunięcie, średnia transakcja, stabilność między okresami."""

from __future__ import annotations

from datetime import datetime

from goldbot.models import Trade


def trade_stats(trades: list[Trade]) -> dict:
    n = len(trades)
    if not n:
        return {"trades": 0, "net_pnl": 0.0}
    net = [t.net_pnl for t in trades]
    wins = [x for x in net if x > 0]
    losses = [x for x in net if x <= 0]
    gross_win, gross_loss = sum(wins), -sum(losses)
    return {
        "trades": n,
        "net_pnl": round(sum(net), 2),
        "gross_pnl": round(sum(t.gross_pnl for t in trades), 2),
        "commission": round(sum(t.commission for t in trades), 2),
        "swap": round(sum(t.swap for t in trades), 2),
        "avg_trade": round(sum(net) / n, 2),
        "win_rate_pct": round(len(wins) / n * 100, 1),
        "avg_win": round(gross_win / len(wins), 2) if wins else 0.0,
        "avg_loss": round(-gross_loss / len(losses), 2) if losses else 0.0,
        "profit_factor": round(gross_win / gross_loss, 2) if gross_loss else None,
        "ambiguous_sl_tp": sum(1 for t in trades if t.ambiguous_bar),
        "exit_reasons": _count(t.exit_reason for t in trades),
        "long": sum(1 for t in trades if t.side.value == "long"),
        "short": sum(1 for t in trades if t.side.value == "short"),
    }


def _count(it) -> dict:
    out: dict = {}
    for x in it:
        out[x] = out.get(x, 0) + 1
    return out


def by_period(trades: list[Trade], start: datetime, end: datetime, n: int) -> list[dict]:
    """Dzieli okres na n chronologicznych części i liczy statystyki osobno (wg czasu wejścia)."""
    if n <= 1:
        return []
    span = (end - start) / n
    out = []
    for i in range(n):
        a, b = start + span * i, start + span * (i + 1)
        part = [t for t in trades if a <= t.entry_time < b or (i == n - 1 and t.entry_time == end)]
        s = trade_stats(part)
        s.update({"period": i + 1, "from": a.isoformat(), "to": b.isoformat()})
        out.append(s)
    return out
