"""Dopracowywanie parametrów bez oszukiwania samego siebie: siatka parametrów oceniana
na części in-sample (IS), a wybrane zestawy sprawdzane na późniejszej części out-of-sample (OOS).

Jeżeli najlepsze zestawy z IS nie trzymają wyniku na OOS, "poprawa" była dopasowaniem do szumu.
"""

from __future__ import annotations

import itertools
import json
from dataclasses import replace
from pathlib import Path

from goldbot.backtest import run_variant
from goldbot.calendar import EventCalendar
from goldbot.config import BotConfig
from goldbot.models import BidAskBar

# Domyślne siatki - celowo małe (kilkadziesiąt kombinacji), żeby nie przeszukiwać tysięcy wariantów.
GRIDS = {
    "trend_pullback_v1": {
        "sl_atr_mult": [1.0, 1.5, 2.0],
        "tp_atr_mult": [1.5, 2.0, 3.0],
        "rsi_long_trigger": [35.0, 40.0, 45.0],
        "use_h4": [True, False],
    },
    "swing_v1": {
        "swing_lookback_h4": [12, 20, 30],
        "swing_trail_atr": [1.5, 2.0, 3.0],
        "tp_atr_mult": [6.0, 10.0],
    },
    "scalp_meanrev_v1": {
        "scalp_dev_atr": [1.5, 2.0, 3.0],
        "scalp_sl_pips": [3.0, 5.0],
        "scalp_tp_pips": [2.0, 3.0, 5.0],
        "max_hold_minutes": [15, 45],
    },
    "london_breakout_v1": {
        "lb_sl_pips": [6.0, 8.0, 12.0],
        "lb_tp_rr": [1.0, 1.5, 2.0],
        "lb_entry_until": ["09:00", "11:00"],
        "lb_min_range_pips": [5.0, 10.0],
    },
    "breakout_v1": {
        "sl_atr_mult": [1.0, 1.5, 2.0],
        "tp_atr_mult": [1.5, 2.0, 3.0],
        "breakout_lookback": [6, 8, 12, 16],
    },
}


def _score(r: dict) -> float:
    """Jedna liczba do rankingu: wynik netto skorygowany o obsunięcie; kara za małą próbę."""
    if r["trades"] < 10:
        return -1e9
    dd = max(r["max_drawdown"], 1.0)
    return r["net_pnl"] / dd


def split_bars(bars: list[BidAskBar], is_fraction: float) -> tuple[list[BidAskBar], list[BidAskBar]]:
    cut = int(len(bars) * is_fraction)
    return bars[:cut], bars[cut:]


def run_grid(bars: list[BidAskBar], cfg: BotConfig, grid: dict[str, list], calendar: EventCalendar | None = None,
             is_fraction: float = 0.6, top: int = 5, progress=None) -> dict:
    strat = cfg.strategy
    is_bars, oos_bars = split_bars(bars, is_fraction)
    keys = list(grid)
    combos = list(itertools.product(*(grid[k] for k in keys)))
    rows = []
    for i, values in enumerate(combos):
        params = dict(zip(keys, values))
        if "rsi_long_trigger" in params and "rsi_short_trigger" not in params:
            params["rsi_short_trigger"] = 100.0 - params["rsi_long_trigger"]
        c = replace(cfg, strategy=replace(strat, **params))
        r_is = run_variant(is_bars, c, "IS", calendar=calendar)
        rows.append({"params": params, "is": _slim(r_is), "score_is": _score(r_is)})
        if progress:
            progress(i + 1, len(combos), params, r_is)
    rows.sort(key=lambda x: -x["score_is"])
    for row in rows[:top]:
        c = replace(cfg, strategy=replace(strat, **row["params"]))
        row["oos"] = _slim(run_variant(oos_bars, c, "OOS", calendar=calendar))
    baseline_is = _slim(run_variant(is_bars, cfg, "IS", calendar=calendar))
    baseline_oos = _slim(run_variant(oos_bars, cfg, "OOS", calendar=calendar))
    return {
        "strategy": strat.name,
        "bars_is": len(is_bars), "bars_oos": len(oos_bars),
        "is_range": [is_bars[0].time.isoformat(), is_bars[-1].time.isoformat()] if is_bars else None,
        "oos_range": [oos_bars[0].time.isoformat(), oos_bars[-1].time.isoformat()] if oos_bars else None,
        "combos": len(combos),
        "baseline": {"is": baseline_is, "oos": baseline_oos},
        "top": rows[:top],
        "all": rows,
    }


def _slim(r: dict) -> dict:
    return {k: r.get(k) for k in ("trades", "net_pnl", "return_pct", "max_drawdown_pct", "win_rate_pct", "profit_factor")}


def format_report(res: dict) -> str:
    def line(label, d):
        return (f"{label:<34}{d['trades']:>6}{d['net_pnl']:>10}{d['return_pct']:>8}{d['max_drawdown_pct']:>8}"
                f"{str(d['win_rate_pct']):>7}{str(d['profit_factor']):>7}")
    out = [f"Strategia {res['strategy']}: {res['combos']} kombinacji; IS {res['is_range'][0][:10]}..{res['is_range'][1][:10]}"
           f" ({res['bars_is']} świec), OOS {res['oos_range'][0][:10]}..{res['oos_range'][1][:10]} ({res['bars_oos']} świec)",
           f"{'zestaw':<34}{'trans':>6}{'netto':>10}{'zwrot%':>8}{'maxDD%':>8}{'win%':>7}{'PF':>7}",
           "-" * 80,
           line("bazowy (config)  IS", res["baseline"]["is"]),
           line("                 OOS", res["baseline"]["oos"])]
    for i, row in enumerate(res["top"], 1):
        p = ", ".join(f"{k}={v}" for k, v in row["params"].items() if k != "rsi_short_trigger")
        out.append(f"#{i} {p}")
        out.append(line("   IS", row["is"]))
        out.append(line("   OOS", row["oos"]))
    return "\n".join(out)


def save_report(res: dict, path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(res, indent=2, ensure_ascii=False))
