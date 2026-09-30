import unittest

from goldbot.config import RiskConfig
from goldbot.models import InstrumentSpec, Side
from goldbot.risk import size_position

SPEC = InstrumentSpec(commission_per_lot_per_side=3.5, slippage_per_oz=0.05, leverage=20)


class SizingTest(unittest.TestCase):
    def test_rejects_when_min_volume_exceeds_risk(self):
        # przykład z założeń: 1 oz, stop 10 USD => >2% z 500 USD
        r = size_position(Side.LONG, 3800, 10.0, 500, SPEC, RiskConfig(risk_per_trade_pct=2.0), free_margin=500)
        self.assertFalse(r.ok)
        self.assertIn("min_volume_exceeds_risk", r.reason)

    def test_volume_respects_step_and_budget(self):
        r = size_position(Side.LONG, 3800, 3.0, 500, SPEC, RiskConfig(risk_per_trade_pct=2.0), free_margin=2000)
        self.assertTrue(r.ok)
        self.assertAlmostEqual(r.volume_oz % 1.0, 0.0)  # krok 0.01 lota = 1 oz
        self.assertLessEqual(r.planned_loss, 10.0)
        self.assertEqual(r.volume_oz, 3.0)

    def test_margin_cap(self):
        r = size_position(Side.LONG, 3800, 0.5, 500, SPEC, RiskConfig(risk_per_trade_pct=5.0, max_margin_usage_pct=50),
                          free_margin=100)
        self.assertFalse(r.ok)
        self.assertIn("insufficient_margin", r.reason)


if __name__ == "__main__":
    unittest.main()
