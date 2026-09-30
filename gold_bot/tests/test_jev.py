import json
import os
import unittest
from datetime import timedelta

from goldbot.config import JevConfig
from goldbot.filters.jev import JevClassifier, JevError, JevNewsFilter, parse_classification
from goldbot.filters.news import NewsItem, NewsStore
from goldbot.models import Side, Signal
from tests.helpers import T0, minutes


def fake_transport(labels):
    calls = []

    def t(url, headers, payload, timeout):
        calls.append(payload)
        items = json.loads(payload["messages"][1]["content"])
        out = {"items": [{"id": it["id"], **labels[it["id"]]} for it in items]}
        return {"choices": [{"message": {"content": json.dumps(out)}}]}
    t.calls = calls
    return t


CFG = JevConfig(enabled=True, base_url="http://example.invalid/v1", model="jev-2026-09-01", cache_path="")
SIG = Signal(Side.LONG, 5, 10, T0, "test")


class JevTest(unittest.TestCase):
    def setUp(self):
        os.environ["JEV_API_KEY"] = "test"

    def test_rejects_latest_alias(self):
        with self.assertRaises(ValueError):
            JevClassifier(JevConfig(model="jev-latest"))
        with self.assertRaises(ValueError):
            JevClassifier(JevConfig(model="typesafe/jev-router"))

    def test_only_past_news_visible(self):
        store = NewsStore([NewsItem("past", T0 - minutes(10), "a"), NewsItem("future", T0 + minutes(1), "b")])
        self.assertEqual([i.id for i in store.window(T0, timedelta(hours=4), 10)], ["past"])

    def test_blocks_on_configured_category_and_caches(self):
        labels = {"n1": {"relevant_to_gold": True, "category": "geopolitical_escalation"},
                  "n2": {"relevant_to_gold": True, "category": "physical_gold_flows"}}
        tr = fake_transport(labels)
        store = NewsStore([NewsItem("n1", T0 - minutes(5), "x"), NewsItem("n2", T0 - minutes(3), "y")])
        f = JevNewsFilter(CFG, store, JevClassifier(CFG, transport=tr))
        v = f.evaluate(SIG, T0)
        self.assertFalse(v.allow)
        self.assertEqual(v.details["blocking"], [{"id": "n1", "category": "geopolitical_escalation"}])
        f.evaluate(SIG, T0)
        self.assertEqual(len(tr.calls), 1)  # druga ocena z cache

    def test_irrelevant_item_does_not_block(self):
        labels = {"n1": {"relevant_to_gold": False, "category": "geopolitical_escalation"}}
        store = NewsStore([NewsItem("n1", T0 - minutes(5), "x")])
        f = JevNewsFilter(CFG, store, JevClassifier(CFG, transport=fake_transport(labels)))
        self.assertTrue(f.evaluate(SIG, T0).allow)

    def test_error_policy(self):
        def broken(*a):
            return {"choices": [{"message": {"content": "BUY with 90% confidence"}}]}
        store = NewsStore([NewsItem("n1", T0 - minutes(5), "x")])
        f = JevNewsFilter(CFG, store, JevClassifier(CFG, transport=broken))
        v = f.evaluate(SIG, T0)
        self.assertFalse(v.allow)
        self.assertEqual(v.reason, "jev_error_block")

    def test_parse_rejects_unknown_category(self):
        with self.assertRaises(JevError):
            parse_classification('{"items":[{"id":"a","relevant_to_gold":true,"category":"buy_signal"}]}', {"a"})


if __name__ == "__main__":
    unittest.main()
