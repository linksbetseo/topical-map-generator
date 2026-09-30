import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from goldbot.backtest import run_variant
from goldbot.calendar import Event, EventCalendar
from goldbot.config import BotConfig, RiskConfig
from goldbot.data.synthetic import generate
from goldbot.engine import Engine
from goldbot.filters.base import FilterVerdict, SignalFilter
from goldbot.models import Side, Signal
from goldbot.paper import PaperRunner, replay_quotes
from tests.helpers import T0, flat, minutes


class BlockAll(SignalFilter):
    name = "block_all"

    def evaluate(self, signal, now):
        return FilterVerdict(False, "test_block")


class EngineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bars = generate(days=40, seed=3)
        cls.cfg = BotConfig(risk=RiskConfig(risk_per_trade_pct=5.0))  # więcej wejść na potrzeby testu

    def test_backtest_runs_and_balances_reconcile(self):
        r = run_variant(self.bars, self.cfg, "A")
        self.assertGreater(r["trades"], 0)
        self.assertAlmostEqual(r["final_balance"], 500 + r["net_pnl"], places=1)

    def test_filter_can_only_remove_trades(self):
        a = run_variant(self.bars, self.cfg, "A")
        b = run_variant(self.bars, self.cfg, "B", signal_filter=BlockAll())
        self.assertEqual(b["trades"], 0)
        # bez otwartych pozycji do filtra dociera co najmniej tyle sygnałów, ile A zaakceptował
        self.assertGreaterEqual(b["engine"].get("rejected:filter:test_block", 0), a["engine"]["accepted"])
        self.assertEqual(b["engine"].get("accepted", 0), 0)

    def test_entry_happens_after_decision_never_before(self):
        eng = Engine(self.cfg)
        for bar in self.bars:
            eng.on_bar(bar)
        eng.finish()
        decisions = {r["time"] for r in eng.journal.records if r["kind"] == "decision" and r["action"] == "accept"}
        entries = [r["time"] for r in eng.journal.records if r["kind"] == "entry"]
        self.assertTrue(entries)
        for e in entries:
            self.assertTrue(any(d <= e <= d + minutes(3) for d in decisions))

    def test_event_blackout_blocks(self):
        # wydarzenie co godzinę przez cały okres => każde okno M15 jest w blackoucie (±30 min)
        start, n = self.bars[0].time, int((self.bars[-1].time - self.bars[0].time).total_seconds() // 3600) + 2
        cal = EventCalendar([Event(start + minutes(60 * i), "test event") for i in range(n)])
        eng2 = Engine(self.cfg, calendar=cal)
        for bar in self.bars:
            eng2.on_bar(bar)
        self.assertEqual(eng2.stats["entries"], 0)
        self.assertGreater(eng2.stats["rejected:event_blackout"], 0)

    def test_weekend_session_rules(self):
        eng = Engine(BotConfig())
        sat = datetime(2026, 6, 6, 12, 0, tzinfo=timezone.utc)
        fri_late = datetime(2026, 6, 5, 19, 30, tzinfo=timezone.utc)
        self.assertEqual(eng.entries_allowed(sat), (False, "weekend"))
        self.assertEqual(eng.entries_allowed(fri_late), (False, "friday_cutoff"))
        self.assertTrue(eng.entries_allowed(T0)[0])
        self.assertEqual(eng.entries_allowed(T0.replace(hour=21, minute=30)), (False, "daily_break"))

    def test_stale_pending_entry_cancelled(self):
        eng = Engine(self.cfg)
        eng.on_bar(flat(T0, 3800))
        from goldbot.engine import _Pending
        eng.pending = _Pending(Signal(Side.LONG, 2, 4, T0 + minutes(1), "x"))
        eng.on_bar(flat(T0 + minutes(30), 3800))
        self.assertEqual(eng.stats["rejected:stale_entry"], 1)
        self.assertFalse(eng.account.positions)

    def test_duplicate_bars_ignored(self):
        eng = Engine(self.cfg)
        eng.on_bar(flat(T0, 3800))
        eng.on_bar(flat(T0, 3800))
        self.assertEqual(eng.stats["bars_skipped_non_monotonic"], 1)

    def test_paper_replay_matches_and_restart_keeps_state(self):
        bars = self.bars[:15000]
        with tempfile.TemporaryDirectory() as d:
            state = Path(d, "state.json")
            eng = Engine(self.cfg)
            runner = PaperRunner(eng, state_path=state)
            quotes = list(replay_quotes(bars))
            for q in quotes[: len(quotes) // 2]:
                runner.on_quote(q)
            balance, open_pos = eng.account.balance, len(eng.account.positions)
            eng2 = Engine(self.cfg)
            PaperRunner(eng2, state_path=state)
            self.assertAlmostEqual(eng2.account.balance, balance)
            self.assertEqual(len(eng2.account.positions), open_pos)
            self.assertEqual(eng2.decided_keys, eng.decided_keys)


if __name__ == "__main__":
    unittest.main()
