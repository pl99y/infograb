import unittest
from unittest.mock import patch

from services.f1.xfeed import get_f1_live_from_feed
from services.f1.queries import fetch_f1_live_payload
from scripts.export_status import f1_live_status


class F1IdleTests(unittest.TestCase):
    def test_flashscore_field_delimiters(self):
        from services.f1.xfeed import _record_to_dict
        self.assertEqual(_record_to_dict("AA\u00f7123\u00acBB\u00f7race"), {"AA": "123", "BB": "race"})

    def test_idle_does_not_require_removed_score_link(self):
        strategy = {
            "mode": "between_rounds",
            "previous_round": {"name": "Las Vegas", "flashscore_url": None},
            "next_round": {"name": "Qatar", "date_text": "27 - 29 NOV"},
        }
        with patch("services.f1.xfeed.resolve_round_strategy", return_value=strategy), \
             patch("services.f1.xfeed.fetch_round_page_and_feed") as feed, \
             patch("services.f1.queries._fetch_previous_result", return_value={"ok": True, "rows": [{"position": 1}]}):
            payload = fetch_f1_live_payload()
        feed.assert_not_called()
        self.assertEqual(f1_live_status(payload), "success")
        self.assertEqual(payload["round"]["name"], "Qatar")
        self.assertTrue(payload["previous_result"]["ok"])

    def test_race_weekend_missing_feed_still_fails(self):
        with patch("services.f1.xfeed.resolve_round_strategy", return_value={"mode": "live", "current_round": {"name": "Qatar"}}):
            payload = get_f1_live_from_feed()
        self.assertTrue(payload["error"])
        self.assertEqual(f1_live_status(payload), "failed")

    def test_invalid_calendar_is_not_idle_success(self):
        with patch("services.f1.xfeed.resolve_round_strategy", side_effect=ValueError("invalid calendar")):
            with self.assertRaises(ValueError):
                get_f1_live_from_feed()


if __name__ == "__main__":
    unittest.main()
