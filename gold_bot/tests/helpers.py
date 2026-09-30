from datetime import datetime, timedelta, timezone

from goldbot.models import BidAskBar

T0 = datetime(2026, 6, 2, 10, 0, tzinfo=timezone.utc)  # wtorek


def bar(t, bid_o, bid_h, bid_l, bid_c, spread=0.3, vol=10.0):
    return BidAskBar(t, bid_o, bid_h, bid_l, bid_c, bid_o + spread, bid_h + spread, bid_l + spread, bid_c + spread, vol)


def flat(t, price, spread=0.3):
    return bar(t, price, price, price, price, spread)


def minutes(n):
    return timedelta(minutes=n)
