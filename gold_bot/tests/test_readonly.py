import re
import unittest
from pathlib import Path

PKG = Path(__file__).resolve().parents[1] / "goldbot"


class ReadOnlyTest(unittest.TestCase):
    def test_no_order_messages_anywhere(self):
        pattern = re.compile(r"ProtoOA(NewOrder|AmendPositionSLTP|ClosePosition|CancelOrder|AmendOrder)Req|order_send")
        for f in PKG.rglob("*.py"):
            self.assertIsNone(pattern.search(f.read_text()), f"{f} zawiera wywołanie zlecenia")

    def test_ctrader_allowlist_is_read_only(self):
        from goldbot.data.ctrader import ALLOWED_REQUESTS
        self.assertFalse(any("Order" in n or "Position" in n for n in ALLOWED_REQUESTS))


if __name__ == "__main__":
    unittest.main()
