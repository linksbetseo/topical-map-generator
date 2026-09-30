import unittest

from goldbot.models import InstrumentSpec, Side
from goldbot.simulator import Account
from tests.helpers import T0, bar, flat, minutes

SPEC = InstrumentSpec(commission_per_lot_per_side=3.5, slippage_per_oz=0.0,
                      swap_long_per_lot_per_night=-20.0, swap_short_per_lot_per_night=5.0)


class ExecutionTest(unittest.TestCase):
    def test_long_enters_at_ask_exits_at_bid(self):
        acc = Account(500, SPEC)
        pos = acc.open(Side.LONG, 1.0, flat(T0, 3800.0, spread=0.4), sl_distance=10, tp_distance=20)
        self.assertAlmostEqual(pos.entry_price, 3800.4)
        self.assertAlmostEqual(pos.sl, 3790.4)
        # natychmiastowe zamknięcie po bid = strata równa spreadowi, bez podwójnego liczenia
        tr = acc.close_at_market(pos, flat(T0 + minutes(1), 3800.0, spread=0.4), "test")
        self.assertAlmostEqual(tr.gross_pnl, -0.4)
        self.assertAlmostEqual(tr.commission, 2 * 3.5 / 100)
        self.assertAlmostEqual(acc.balance, 500 - 0.4 - 0.07)

    def test_short_enters_at_bid_exits_at_ask(self):
        acc = Account(500, SPEC)
        pos = acc.open(Side.SHORT, 2.0, flat(T0, 3800.0, spread=0.5), 10, 20)
        self.assertAlmostEqual(pos.entry_price, 3800.0)
        tr = acc.close_at_market(pos, flat(T0 + minutes(1), 3800.0, spread=0.5), "test")
        self.assertAlmostEqual(tr.gross_pnl, -1.0)

    def test_slippage_is_adverse(self):
        spec = InstrumentSpec(commission_per_lot_per_side=0, slippage_per_oz=0.1)
        acc = Account(500, spec)
        pos = acc.open(Side.LONG, 1.0, flat(T0, 3800.0, spread=0.2), 10, 20)
        self.assertAlmostEqual(pos.entry_price, 3800.3)

    def test_sl_and_tp_in_same_bar_assumes_sl(self):
        acc = Account(500, SPEC)
        pos = acc.open(Side.LONG, 1.0, flat(T0, 3800.0, spread=0.0), 5, 5)
        trades = acc.check_exits(bar(T0 + minutes(1), 3800, 3806, 3794, 3801, spread=0.0))
        self.assertEqual(trades[0].exit_reason, "sl")
        self.assertTrue(trades[0].ambiguous_bar)
        self.assertAlmostEqual(trades[0].exit_price, pos.sl)

    def test_gap_through_stop_fills_at_open(self):
        acc = Account(500, SPEC)
        acc.open(Side.LONG, 1.0, flat(T0, 3800.0, spread=0.0), 5, 5)
        trades = acc.check_exits(bar(T0 + minutes(60), 3790, 3791, 3789, 3790, spread=0.0))
        self.assertEqual(trades[0].exit_reason, "sl_gap")
        self.assertAlmostEqual(trades[0].exit_price, 3790.0)

    def test_short_stop_uses_ask(self):
        acc = Account(500, SPEC)
        pos = acc.open(Side.SHORT, 1.0, flat(T0, 3800.0, spread=0.5), 5, 5)  # wejście po bid 3800
        # bid high 3804.6 < SL 3805, ale ask high 3805.1 >= SL -> stop
        trades = acc.check_exits(bar(T0 + minutes(1), 3800, 3804.6, 3799, 3800, spread=0.5))
        self.assertEqual(trades[0].exit_reason, "sl")
        self.assertAlmostEqual(pos.sl, 3805.0)

    def test_swap(self):
        acc = Account(500, SPEC)
        acc.open(Side.LONG, 10.0, flat(T0, 3800.0, spread=0.0), 5, 5)
        amt = acc.apply_swap(3)
        self.assertAlmostEqual(amt, -20.0 * 0.1 * 3)


if __name__ == "__main__":
    unittest.main()


class PctCommissionTest(unittest.TestCase):
    def test_pct_commission_both_sides(self):
        spec = InstrumentSpec(contract_size=1.0, commission_per_lot_per_side=0.0, commission_pct_per_side=0.1, slippage_per_oz=0.0)
        acc = Account(500, spec)
        pos = acc.open(Side.LONG, 0.01, flat(T0, 80000.0, spread=0.0), 100, 200)
        self.assertAlmostEqual(pos.commission_paid, 0.8)  # 0.1% * 800 USD
        tr = acc.close_at_market(pos, flat(T0 + minutes(1), 80000.0, spread=0.0), "t")
        self.assertAlmostEqual(tr.commission, 1.6)
        self.assertAlmostEqual(acc.balance, 498.4)
