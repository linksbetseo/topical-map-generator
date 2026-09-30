import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from goldbot.calendar import EventCalendar, Event, parse_ics
from goldbot.data import quality
from goldbot.data.loaders import load_dukascopy_pair, parse_time
from goldbot.data.resample import BarAggregator, TickToM1
from goldbot.models import Bar, Quote
from tests.helpers import T0, flat, minutes


class ResampleTest(unittest.TestCase):
    def test_m15_emitted_only_after_last_minute(self):
        agg = BarAggregator(15)
        out = []
        for i in range(15):
            done = agg.update(Bar(T0 + minutes(i), 1 + i, 2 + i, 0.5 + i, 1.5 + i))
            if i < 14:
                self.assertEqual(done, [])
            out += done
        self.assertEqual(len(out), 1)
        b = out[0]
        self.assertEqual((b.time, b.open, b.high, b.low, b.close), (T0, 1, 16, 0.5, 15.5))

    def test_gap_flushes_incomplete_bucket(self):
        agg = BarAggregator(15)
        agg.update(Bar(T0, 1, 1, 1, 1))
        done = agg.update(Bar(T0 + minutes(40), 2, 2, 2, 2))
        self.assertEqual([b.time for b in done], [T0])

    def test_tick_to_m1(self):
        agg = TickToM1()
        self.assertIsNone(agg.update(Quote(T0, 10, 10.2)))
        agg.update(Quote(T0 + minutes(0.5), 11, 11.3))
        b = agg.update(Quote(T0 + minutes(1), 9, 9.1))
        self.assertEqual((b.bid_open, b.bid_high, b.bid_close, b.ask_high), (10, 11, 11, 11.3))


class LoaderTest(unittest.TestCase):
    def test_dukascopy_time_formats(self):
        self.assertEqual(parse_time("01.09.2026 13:45:00.000"), datetime(2026, 9, 1, 13, 45, tzinfo=timezone.utc))
        self.assertEqual(parse_time("01.09.2026 15:45:00.000 GMT+0200"), datetime(2026, 9, 1, 13, 45, tzinfo=timezone.utc))

    def test_dukascopy_pair_merge(self):
        with tempfile.TemporaryDirectory() as d:
            bid, ask = Path(d, "bid.csv"), Path(d, "ask.csv")
            bid.write_text("Gmt time,Open,High,Low,Close,Volume\n"
                           "01.09.2026 00:00:00.000,3800,3801,3799,3800.5,10\n"
                           "01.09.2026 00:01:00.000,3800.5,3800.5,3800.5,3800.5,0\n"
                           "01.09.2026 00:02:00.000,3800,3801,3799,3800.5,10\n")
            ask.write_text("Gmt time,Open,High,Low,Close,Volume\n"
                           "01.09.2026 00:00:00.000,3800.3,3801.3,3799.3,3800.8,10\n"
                           "01.09.2026 00:01:00.000,3800.8,3800.8,3800.8,3800.8,0\n")
            bars, rep = load_dukascopy_pair(bid, ask)
            self.assertEqual(len(bars), 1)
            self.assertEqual(rep.only_bid, 1)
            self.assertEqual(rep.dropped_flat_zero_volume, 1)
            self.assertAlmostEqual(bars[0].ask_close, 3800.8)

    def test_quality_detects_problems(self):
        bars = [flat(T0, 3800), flat(T0, 3800), flat(T0 + minutes(30), 3800, spread=-0.1)]
        rep = quality.check(bars)
        self.assertEqual(rep.duplicates, 1)
        self.assertEqual(rep.crossed_quotes, 1)
        self.assertEqual(rep.gaps_over_threshold, 1)
        self.assertFalse(rep.ok)


class CalendarTest(unittest.TestCase):
    ICS = ("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART;TZID=US-Eastern:20261106T083000\r\n"
           "SUMMARY:Employment Situation for October 2026\r\nEND:VEVENT\r\n"
           "BEGIN:VEVENT\r\nDTSTART;TZID=US-Eastern:20260715T083000\r\nSUMMARY:Consumer Price Index\r\n  for June 2026\r\nEND:VEVENT\r\n"
           "BEGIN:VEVENT\r\nDTSTART;VALUE=DATE:20261107\r\nSUMMARY:All day\r\nEND:VEVENT\r\n"
           "BEGIN:VEVENT\r\nDTSTART;TZID=US-Eastern:20261106T100000\r\nSUMMARY:Some Other Release\r\nEND:VEVENT\r\n"
           "END:VCALENDAR\r\n")

    def test_ics_timezones_and_filter(self):
        ev = parse_ics(self.ICS, keywords=("Employment Situation", "Consumer Price Index"))
        self.assertEqual(len(ev), 2)
        # 8:30 EST (listopad) = 13:30 UTC; 8:30 EDT (lipiec) = 12:30 UTC
        self.assertEqual(ev[0].time, datetime(2026, 11, 6, 13, 30, tzinfo=timezone.utc))
        self.assertEqual(ev[1].time, datetime(2026, 7, 15, 12, 30, tzinfo=timezone.utc))
        self.assertEqual(ev[1].name, "Consumer Price Index for June 2026")

    def test_blackout_window(self):
        cal = EventCalendar([Event(T0, "NFP")])
        self.assertTrue(cal.events_near(T0 - minutes(20), minutes(30), minutes(15)))
        self.assertTrue(cal.events_near(T0 + minutes(15), minutes(30), minutes(15)))
        self.assertFalse(cal.events_near(T0 + minutes(16), minutes(30), minutes(15)))
        self.assertFalse(cal.events_near(T0 - minutes(31), minutes(30), minutes(15)))


if __name__ == "__main__":
    unittest.main()


class DukascopyFeedTest(unittest.TestCase):
    def test_url_month_is_zero_based(self):
        from datetime import date
        from goldbot.data.dukascopy_feed import url_for
        self.assertTrue(url_for("XAUUSD", date(2026, 5, 1), "BID").endswith("/XAUUSD/2026/04/01/BID_candles_min_1.bi5"))

    def test_decode_candles(self):
        import lzma
        import struct
        from datetime import date
        from goldbot.data.dukascopy_feed import RECORD, decode_candles
        raw = lzma.compress(RECORD.pack(60, 4626305, 4625275, 4623035, 4626805, 0.041))
        (t, o, h, l, c, v), = decode_candles(raw, date(2026, 5, 1), 1000.0)
        self.assertEqual(t, datetime(2026, 5, 1, 0, 1, tzinfo=timezone.utc))
        self.assertEqual((o, h, l, c), (4626.305, 4626.805, 4623.035, 4625.275))
        self.assertEqual(decode_candles(b"", date(2026, 5, 1), 1000.0), [])


class BinanceFeedTest(unittest.TestCase):
    def test_decode_klines_builds_bid_ask(self):
        import io
        import zipfile
        from goldbot.data.binance_feed import decode_klines
        row = "1788220800000000,80000,80100,79900,80050,8.6,1788220859999999,1,1,1,1,0\n"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("x.csv", row)
        (b,) = decode_klines(buf.getvalue(), spread_pct=0.02)
        self.assertEqual(b.time, datetime(2026, 9, 1, 0, 0, tzinfo=timezone.utc))
        self.assertAlmostEqual(b.ask_close - b.bid_close, 80050 * 0.0002, places=6)
        self.assertAlmostEqual((b.bid_open + b.ask_open) / 2, 80000.0)
