"""Uruchamianie backtestu i porównania A/B (A = bez filtra, B = z filtrem Jev)."""

from __future__ import annotations

import json
from pathlib import Path

from goldbot.calendar import EventCalendar
from goldbot.config import BotConfig
from goldbot.engine import Engine
from goldbot.filters.base import SignalFilter
from goldbot.journal import Journal, write_trades_csv
from goldbot.metrics import by_period, trade_stats
from goldbot.models import BidAskBar


def run_variant(bars: list[BidAskBar], cfg: BotConfig, variant: str, signal_filter: SignalFilter | None = None,
                calendar: EventCalendar | None = None, out_dir: str | Path | None = None, splits: int = 1) -> dict:
    out = Path(out_dir) if out_dir else None
    journal = Journal(out / f"decisions_{variant}.jsonl" if out else None, variant=variant)
    engine = Engine(cfg, signal_filter=signal_filter, calendar=calendar, journal=journal)
    for b in bars:
        engine.on_bar(b)
    engine.finish()
    journal.close()
    acc = engine.account
    result = {
        "variant": variant,
        "strategy": engine.strategy.name,
        "risk_per_trade_pct": cfg.risk.risk_per_trade_pct,
        "filter": (signal_filter or SignalFilter()).name,
        "initial_balance": cfg.risk.initial_balance,
        "final_balance": round(acc.balance, 2),
        "return_pct": round((acc.balance / cfg.risk.initial_balance - 1) * 100, 2),
        "max_drawdown": round(engine.max_drawdown, 2),
        "max_drawdown_pct": round(engine.max_drawdown_pct, 2),
        **trade_stats(acc.trades),
        "engine": dict(sorted(engine.stats.items())),
    }
    if bars and splits > 1:
        result["periods"] = by_period(acc.trades, bars[0].time, bars[-1].time, splits)
    jf = getattr(signal_filter, "classifier", None)
    if jf is not None:
        result["jev_usage"] = {"calls": jf.calls, "input_chars": jf.input_chars,
                               "errors": getattr(signal_filter, "errors", 0)}
    if out:
        out.mkdir(parents=True, exist_ok=True)
        write_trades_csv(out / f"trades_{variant}.csv", acc.trades)
        with open(out / f"equity_{variant}.csv", "w") as f:
            f.write("time,equity\n")
            for t, e in engine.equity_curve:
                f.write(f"{t.isoformat()},{e}\n")
        (out / f"summary_{variant}.json").write_text(json.dumps(result, indent=2, ensure_ascii=False))
    return result


COMPARE_KEYS = ("strategy", "risk_per_trade_pct", "trades", "net_pnl", "return_pct", "max_drawdown", "max_drawdown_pct", "avg_trade",
                "win_rate_pct", "profit_factor", "commission", "swap")


def format_comparison(results: list[dict]) -> str:
    head = f"{'metryka':<18}" + "".join(f"{r['variant'] + ' (' + r['filter'] + ')':>26}" for r in results)
    lines = [head, "-" * len(head)]
    for k in COMPARE_KEYS:
        lines.append(f"{k:<18}" + "".join(f"{str(r.get(k, '-')):>26}" for r in results))
    return "\n".join(lines)
