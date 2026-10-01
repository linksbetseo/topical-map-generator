"""Wiersz poleceń: python -m goldbot <polecenie> ..."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from goldbot.backtest import format_comparison, run_variant
from goldbot.calendar import EventCalendar, load_events_csv, load_ics
from goldbot.config import load_config
from goldbot.data import quality
from goldbot.data.loaders import load_common_csv, load_dukascopy_pair, save_common_csv


def _load_bars(args):
    if getattr(args, "ticks", None) and not args.data:
        return []
    if args.data:
        return load_common_csv(args.data)
    if args.bid and args.ask:
        bars, rep = load_dukascopy_pair(args.bid, args.ask)
        print(f"Dukascopy: połączono {rep.merged} świec, tylko BID {rep.only_bid}, tylko ASK {rep.only_ask}, "
              f"usunięte płaskie z wolumenem 0: {rep.dropped_flat_zero_volume}")
        return bars
    sys.exit("Podaj --data (wspólny CSV) albo --bid i --ask (eksport Dukascopy)")


def _load_calendar(args) -> EventCalendar:
    cal = EventCalendar()
    for p in args.events or []:
        cal.extend(load_events_csv(p))
    for p in args.ics or []:
        cal.extend(load_ics(p))
    return cal


def _add_data_args(p):
    p.add_argument("--data", help="CSV w formacie wspólnym (bid/ask M1)")
    p.add_argument("--bid", help="eksport Dukascopy BID M1")
    p.add_argument("--ask", help="eksport Dukascopy ASK M1")


def cmd_synth(args):
    from goldbot.data.synthetic import generate
    bars = generate(days=args.days, seed=args.seed, price=args.price)
    save_common_csv(args.out, bars)
    print(f"Zapisano {len(bars)} syntetycznych świec M1 do {args.out} (tylko do testów działania programu)")


def cmd_fetch(args):
    from datetime import date
    from goldbot.data.dukascopy_feed import DukascopyDownloader
    dl = DukascopyDownloader(cache_dir=args.cache, symbol=args.symbol, delay_seconds=args.delay)
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    def progress(day, n):
        if day.weekday() == 4 or day == end:
            print(f"  {day}: {n} świec, zapytań {dl.requests}, z cache {dl.cache_hits}", flush=True)
    bars = dl.range_bars(start, end, progress)
    save_common_csv(args.out, bars)
    rep = quality.check(bars)
    print(f"Zapisano {len(bars)} świec M1 do {args.out}; spread mediana {rep.spread_median}, p95 {rep.spread_p95}, "
          f"luk >10 min: {rep.gaps_over_threshold}, ok={rep.ok}")
    return 0


def cmd_fetch_btc(args):
    from datetime import date
    from goldbot.data.binance_feed import BinanceDownloader
    dl = BinanceDownloader(cache_dir=args.cache, symbol=args.symbol, spread_pct=args.spread_pct)
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    def progress(day, n):
        if day.day in (1, 15) or day == end:
            print(f"  {day}: {n} świec, zapytań {dl.requests}, z cache {dl.cache_hits}", flush=True)
    bars = dl.range_bars(start, end, progress)
    save_common_csv(args.out, bars)
    rep = quality.check(bars)
    print(f"Zapisano {len(bars)} świec M1 do {args.out} (spread założony {args.spread_pct}%); luk >10 min: {rep.gaps_over_threshold}, ok={rep.ok}")
    return 0


def cmd_quality(args):
    bars = _load_bars(args)
    rep = quality.check(bars, gap_minutes=args.gap_minutes)
    print(json.dumps(rep.as_dict(), indent=2))
    return 0 if rep.ok else 1


def _apply_overrides(cfg, args):
    from dataclasses import replace
    risk = cfg.risk
    if args.risk_pct is not None:
        risk = replace(risk, risk_per_trade_pct=args.risk_pct)
    if args.daily_loss_pct is not None:
        risk = replace(risk, max_daily_loss_pct=args.daily_loss_pct)
    strategy = cfg.strategy
    if args.strategy:
        strategy = replace(strategy, name=args.strategy)
    if args.direction:
        strategy = replace(strategy, direction=args.direction)
    return replace(cfg, risk=risk, strategy=strategy)


def cmd_backtest(args):
    cfg = _apply_overrides(load_config(args.config), args)
    bars = _load_bars(args)
    rep = quality.check(bars)
    if not rep.ok and bars:
        print("Dane nie przeszły kontroli jakości:", json.dumps(rep.as_dict(), indent=2))
        if not args.force:
            return 1
    cal = _load_calendar(args)
    out = Path(args.out)
    variants = ["A", "B"] if args.variant == "AB" else [args.variant]
    results = []
    ticks = None
    if getattr(args, "ticks", None):
        from goldbot.data.dukascopy_ticks import iter_ticks_dir
        from goldbot.data.loaders import parse_time
        t0 = parse_time(args.ticks_start) if args.ticks_start else None
        t1 = parse_time(args.ticks_end) if args.ticks_end else None
        if bars and t0 is None:
            sys.exit("Przy --ticks z --data (rozgrzewka) podaj --ticks-start, żeby rozgrzewka kończyła się przed tickami")
        if bars:
            bars = [b for b in bars if b.time < t0]
        ticks = list(iter_ticks_dir(args.ticks, cfg.instrument.symbol, t0, t1))
        print(f"Ticki: {len(ticks)} ({args.ticks}); rozgrzewka: {len(bars)} świec M1")
    for v in variants:
        flt = None
        if v == "B":
            from goldbot.filters.jev import JevNewsFilter
            from goldbot.filters.news import NewsStore, load_news_jsonl
            if not args.news:
                sys.exit("Wariant B wymaga --news (JSONL z komunikatami)")
            flt = JevNewsFilter(cfg.jev, NewsStore(load_news_jsonl(args.news)))
        results.append(run_variant(bars, cfg, v, flt, cal, out, splits=args.splits, ticks=ticks))
    print(f"Dane: {rep.first} -> {rep.last}, świec: {rep.bars}, wydarzeń w kalendarzu: {len(cal)}")
    print(format_comparison(results))
    for r in results:
        print(f"\n[{r['variant']}] silnik: {json.dumps(r['engine'], ensure_ascii=False)}")
        for p in r.get("periods", []):
            print(f"  okres {p['period']}: {p['from'][:10]}..{p['to'][:10]} transakcji={p['trades']} netto={p['net_pnl']}")
    print(f"\nWyniki i dziennik decyzji: {out}/")
    return 0


def cmd_optimize(args):
    import json as _json
    from goldbot.optimize import GRIDS, format_report, run_grid, save_report
    cfg = _apply_overrides(load_config(args.config), args)
    bars = _load_bars(args)
    cal = _load_calendar(args)
    grid = _json.loads(args.grid) if args.grid else GRIDS[cfg.strategy.name]
    def progress(i, n, params, r):
        if i % 10 == 0 or i == n:
            print(f"  {i}/{n}", flush=True)
    oos = load_common_csv(args.oos_data) if args.oos_data else None
    res = run_grid(bars, cfg, grid, cal, is_fraction=args.is_fraction, top=args.top, progress=progress, oos_bars=oos)
    print(format_report(res))
    out = Path(args.out) / f"optimize_{cfg.strategy.name}.json"
    save_report(res, out)
    print(f"\nPełny wynik: {out}")
    return 0


def cmd_paper(args):
    from goldbot.engine import Engine
    from goldbot.journal import Journal
    from goldbot.paper import PaperRunner, replay_quotes, warmup

    cfg = load_config(args.config)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    journal = Journal(out / "paper_decisions.jsonl", variant="paper")
    engine = Engine(cfg, calendar=_load_calendar(args), journal=journal)
    if args.warmup:
        print(f"Rozgrzewka wskaźników: {warmup(engine, load_common_csv(args.warmup))} świec")
    runner = PaperRunner(engine, state_path=out / "paper_state.json")
    if args.source == "replay":
        if not args.data:
            sys.exit("--source replay wymaga --data")
        for q in replay_quotes(load_common_csv(args.data)):
            runner.on_quote(q)
        engine.finish()
        journal.close()
        print(json.dumps({"balance": round(engine.account.balance, 2), "trades": len(engine.account.trades),
                          "engine": dict(engine.stats)}, indent=2))
        return 0
    from goldbot.data.ctrader import CTraderReadOnlyFeed
    feed = CTraderReadOnlyFeed(on_quote=runner.on_quote, on_disconnect=runner.on_disconnect)
    feed.start()
    return 0


def cmd_price(args):
    from goldbot.data.gold_api import GoldApiReference
    print(json.dumps(GoldApiReference().get(), indent=2))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="goldbot", description="Prototyp bota XAU/USD - wyłącznie symulacja")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("synth", help="wygeneruj syntetyczne dane do testów")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--price", type=float, default=3800.0)
    p.add_argument("--out", default="data/synthetic_m1.csv")
    p.set_defaults(fn=cmd_synth)

    p = sub.add_parser("fetch", help="pobierz historię M1 bid/ask (XAUUSD, EURUSD, ...) z publicznego feedu Dukascopy")
    p.add_argument("--symbol", default="XAUUSD")
    p.add_argument("--start", required=True, help="YYYY-MM-DD (UTC)")
    p.add_argument("--end", required=True)
    p.add_argument("--out", default="data/xauusd_m1.csv")
    p.add_argument("--cache", default="data/dukascopy_cache")
    p.add_argument("--delay", type=float, default=4.0, help="odstęp między zapytaniami (s); przy mniejszym serwer dławi do 429")
    p.set_defaults(fn=cmd_fetch)

    p = sub.add_parser("fetch-btc", help="pobierz historię 1m z archiwum Binance (mid; bid/ask = mid ± spread/2)")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--spread-pct", type=float, default=0.02, help="założony spread w %% (spot ~0.01-0.02, CFD 0.05-0.1)")
    p.add_argument("--out", default="data/btcusdt_m1.csv")
    p.add_argument("--cache", default="data/binance_cache")
    p.set_defaults(fn=cmd_fetch_btc)

    p = sub.add_parser("quality", help="kontrola kompletności danych")
    _add_data_args(p)
    p.add_argument("--gap-minutes", type=int, default=10)
    p.set_defaults(fn=cmd_quality)

    p = sub.add_parser("backtest", help="backtest; --variant AB porównuje strategię bez i z filtrem Jev")
    _add_data_args(p)
    p.add_argument("--config")
    p.add_argument("--events", action="append", help="CSV wydarzeń (time_utc,name,source)")
    p.add_argument("--ics", action="append", help="kalendarz ICS (np. BLS)")
    p.add_argument("--news", help="JSONL komunikatów dla filtra Jev")
    p.add_argument("--variant", choices=["A", "B", "AB"], default="A")
    p.add_argument("--ticks", help="katalog z tickami Dukascopy (tryb tickowy; --data = rozgrzewka M1)")
    p.add_argument("--ticks-start", help="ISO; od kiedy brać ticki (rozgrzewka M1 do tej chwili)")
    p.add_argument("--ticks-end", help="ISO; do kiedy")
    p.add_argument("--strategy", choices=["trend_pullback_v1", "breakout_v1", "swing_v1", "scalp_meanrev_v1", "london_breakout_v1", "news_reaction_v1"], help="nadpisz strategy.name")
    p.add_argument("--direction", choices=["both", "long", "short"], help="nadpisz strategy.direction")
    p.add_argument("--risk-pct", type=float, help="nadpisz risk.risk_per_trade_pct")
    p.add_argument("--daily-loss-pct", type=float, help="nadpisz risk.max_daily_loss_pct")
    p.add_argument("--splits", type=int, default=3, help="liczba chronologicznych okresów w raporcie")
    p.add_argument("--out", default="runs/latest")
    p.add_argument("--force", action="store_true", help="uruchom mimo błędów jakości danych")
    p.set_defaults(fn=cmd_backtest)

    p = sub.add_parser("optimize", help="siatka parametrów strategii: ocena in-sample, weryfikacja out-of-sample")
    _add_data_args(p)
    p.add_argument("--config")
    p.add_argument("--events", action="append")
    p.add_argument("--ics", action="append")
    p.add_argument("--strategy", choices=["trend_pullback_v1", "breakout_v1", "swing_v1", "scalp_meanrev_v1", "london_breakout_v1", "news_reaction_v1"])
    p.add_argument("--direction", choices=["both", "long", "short"])
    p.add_argument("--risk-pct", type=float)
    p.add_argument("--daily-loss-pct", type=float)
    p.add_argument("--grid", help='JSON, np. {"sl_atr_mult":[1,1.5],"tp_atr_mult":[2,3]}')
    p.add_argument("--is-fraction", type=float, default=0.6, help="udział danych in-sample (reszta = out-of-sample)")
    p.add_argument("--oos-data", help="osobny CSV out-of-sample (wtedy --data w całości = in-sample)")
    p.add_argument("--top", type=int, default=5)
    p.add_argument("--out", default="runs/optimize")
    p.set_defaults(fn=cmd_optimize)

    p = sub.add_parser("paper", help="symulacja na notowaniach na żywo (cTrader, tylko odczyt) lub odtwarzanych")
    p.add_argument("--source", choices=["ctrader", "replay"], default="replay")
    p.add_argument("--data", help="CSV do odtworzenia (dla --source replay)")
    p.add_argument("--warmup", help="CSV z historią do rozgrzania wskaźników")
    p.add_argument("--config")
    p.add_argument("--events", action="append")
    p.add_argument("--ics", action="append")
    p.add_argument("--out", default="runs/paper")
    p.set_defaults(fn=cmd_paper)

    p = sub.add_parser("price", help="cena referencyjna z Gold-API (nie do wykonania transakcji)")
    p.set_defaults(fn=cmd_price)

    args = ap.parse_args(argv)
    return args.fn(args) or 0
