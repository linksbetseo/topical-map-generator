import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from goldbot.backtest import run_variant
from goldbot.config import BotConfig, RiskConfig, SessionConfig, StrategyConfig, load_config
from goldbot.data.dukascopy_ticks import decode_ticks
from goldbot.data.synthetic import generate
from goldbot.engine import Engine
from goldbot.models import InstrumentSpec, Side
from goldbot.paper import replay_quotes
from tests.helpers import T0, flat, minutes

FX = InstrumentSpec(symbol="EURUSD", contract_size=100000.0, min_volume_lots=0.01, volume_step_lots=0.01,
                    commission_per_lot_per_side=3.5, leverage=50.0, slippage_per_oz=0.00002, pip_size=0.0001)
SESSION = SessionConfig(daily_break_start="21:55", daily_break_end="22:05", trade_window_start="07:00",
                        trade_window_end="16:00", flat_at="16:30", rollover_utc="22:00")


def fx_bars(days=20, seed=4):
    return generate(days=days, seed=seed, price=1.15, annual_vol=0.08, base_spread=0.00002)


class ScalpEngineTest(unittest.TestCase):
    def setUp(self):
        self.cfg = BotConfig(instrument=FX, risk=RiskConfig(risk_per_trade_pct=1.0, max_daily_loss_pct=3.0, max_spread=0.0004),
                             strategy=StrategyConfig(name="scalp_meanrev_v1", scalp_sl_pips=4, scalp_tp_pips=3, max_hold_minutes=30),
                             session=SESSION)

    def test_micro_lot_sizing_works_with_500_usd(self):
        from goldbot.risk import size_position
        r = size_position(Side.LONG, 1.15, 4 * 0.0001, 500, FX, self.cfg.risk, free_margin=500)
        self.assertTrue(r.ok, r.reason)
        self.assertGreaterEqual(r.volume_oz, 1000)  # co najmniej 0.01 lota
        self.assertLessEqual(r.planned_loss, 5.0)

    def test_scalp_trades_on_synthetic_fx_and_respects_window(self):
        bars = fx_bars()
        r = run_variant(bars, self.cfg, "S")
        self.assertGreater(r["trades"], 0)
        eng = Engine(self.cfg)
        for b in bars:
            eng.on_bar(b)
        for rec in eng.journal.records:
            if rec["kind"] == "entry":
                self.assertTrue(7 <= rec["time"].hour < 16, rec["time"])
            if rec["kind"] == "exit":
                self.assertLessEqual(rec["time"].time(), datetime(2000, 1, 1, 16, 31).time())

    def test_time_stop_and_flat_at(self):
        eng = Engine(self.cfg)
        start = datetime(2026, 6, 2, 10, 0, tzinfo=timezone.utc)
        eng.on_bar(flat(start, 1.15, spread=0.00002))
        eng.account.open(Side.LONG, 1000, flat(start, 1.15, spread=0.00002), 0.01, 0.01)  # SL/TP daleko
        eng.on_bar(flat(start + minutes(29), 1.15, spread=0.00002))
        self.assertEqual(len(eng.account.positions), 1)
        eng.on_bar(flat(start + minutes(31), 1.15, spread=0.00002))
        self.assertEqual(len(eng.account.positions), 0)
        self.assertEqual(eng.account.trades[-1].exit_reason, "time_stop")
        eng.account.open(Side.SHORT, 1000, flat(start + minutes(31), 1.15, spread=0.00002), 0.01, 0.01)
        eng.on_bar(flat(start.replace(hour=16, minute=30), 1.15, spread=0.00002))
        self.assertEqual(eng.account.trades[-1].exit_reason, "flat_at")
        self.assertFalse(eng.entries_allowed(start.replace(hour=16, minute=45))[0])  # okno sesji albo flat_at
        self.assertEqual(eng.entries_allowed(start.replace(hour=6)), (False, "outside_trade_window"))

    def test_tick_mode_runs_and_has_no_ambiguous_bars(self):
        bars = fx_bars(days=12)
        warm, live = bars[:6000], bars[6000:]
        r = run_variant(warm, self.cfg, "T", ticks=list(replay_quotes(live)))
        self.assertEqual(r["engine"]["warmup_bars"], 6000)
        self.assertGreater(r["engine"]["ticks"], 0)
        self.assertEqual(r.get("ambiguous_sl_tp", 0), 0)
        self.assertAlmostEqual(r["final_balance"], 500 + r["net_pnl"], places=1)

    def test_decode_ticks(self):
        import lzma
        import struct
        raw = lzma.compress(struct.pack(">IIIff", 1500, 115463, 115461, 0.1, 0.2))
        (q,) = decode_ticks(raw, datetime(2026, 9, 15, 13, tzinfo=timezone.utc), 100000.0)
        self.assertEqual(q.time, datetime(2026, 9, 15, 13, 0, 1, 500000, tzinfo=timezone.utc))
        self.assertAlmostEqual(q.ask - q.bid, 0.00002)

    def test_eurusd_config_loads(self):
        cfg = load_config("config.eurusd.toml")
        self.assertEqual(cfg.instrument.leverage, 50.0)
        self.assertEqual(cfg.strategy.name, "scalp_meanrev_v1")


if __name__ == "__main__":
    unittest.main()
