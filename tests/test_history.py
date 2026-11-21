import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from parsers.history_parser import parse_history_events
from services.history.pipeline import CACHE, factual_fallback, failure_reason, get_history_today, refresh_history_once, validate_history
from storage import get_dashboard_snapshot, initialize_database, save_dashboard_snapshot


def event(identifier="event", year=1985):
    return {"id": identifier, "year": year, "month_day": "11-20", "fact": "微软发布 Windows 1.0，基于 MS-DOS 的图形环境。",
            "language": "zh", "china": False, "period": "modern",
            "pages": [{"title": "Windows 1.0", "url": "https://zh.wikipedia.org/wiki/Windows_1.0", "extract": "基于 MS-DOS", "qid": "Q1"}]}


def generated(identifier="event", **kwargs):
    return {"id": identifier, "title": "窗户还得靠地基", "comment": "窗户是装上了，地基还得靠 DOS。", "fact_zh": "伪造的替代事实", "support_titles": ["Windows 1.0"], **kwargs}


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = str(Path(self.temp.name) / "history.db")
        initialize_database(self.db)
        self.now = datetime(2026, 11, 20, 18, 7, tzinfo=timezone.utc)  # Nov 21, 02:07 in China.

    def save(self, data):
        save_dashboard_snapshot(CACHE, data, db_path=self.db)

    def test_chinese_fact_is_immutable(self):
        result = validate_history({"items": [generated()]}, [event()])
        self.assertEqual(result[0]["fact"], event()["fact"])

    def test_unknown_ids_and_unsupported_numbers_rejected(self):
        for row in [generated("unknown"), generated(comment="这一次足足改了9999个窗口。"), generated(comment="谁能想到窗户仍需地基。")]:
            with self.assertRaises(ValueError):
                validate_history({"items": [row]}, [event()])

    def test_duplicate_topic_not_displayed_twice(self):
        result = validate_history({"items": [generated(), generated("other")]}, [event(), event("other")])
        self.assertEqual(len(result), 1)

    def test_source_links_and_calendar_filter(self):
        def raw(year, url):
            return {"year": year, "text": "中国历史事件", "pages": [{"title": "事件", "extract": "具体事件背景", "content_urls": {"desktop": {"page": url}}}]}
        payload = {"sources": [{"ok": True, "language": "zh", "events": [
            raw(1402, "https://zh.wikipedia.org/wiki/example"), raw(1985, "javascript:alert(1)"),
            raw(1986, "https://evil.example/wiki/example"), raw(1987, "https://zh.wikipedia.org/wiki/example")]}]}
        result = parse_history_events(payload, date(2026, 11, 20))
        self.assertEqual([e["year"] for e in result], [1987])
        self.assertTrue(result[0]["china"])

    def test_debian_correction(self):
        payload = {"sources": [{"ok": True, "language": "zh", "events": [{
            "year": 1993, "text": "Debian的首个版本发布", "pages": [{"title": "Debian", "extract": "背景",
            "content_urls": {"desktop": {"page": "https://zh.wikipedia.org/wiki/Debian"}}}]}]}]}
        item = parse_history_events(payload, date(2026, 8, 16))[0]
        self.assertEqual(item["fact"], "伊恩·默多克正式创立 Debian 项目。")

    @patch("services.history.pipeline.generate_history")
    @patch("services.history.pipeline.fetch_history_events")
    def test_once_per_chinese_day_and_four_slots(self, fetch, generate):
        cards = [{"id": str(i), "title": "标题", "fact": "事实"} for i in range(4)]
        self.save({"date": "2026-11-21", "cards": cards, "display_slot": int(self.now.timestamp()) // 21600, "selected_index": 0})
        for slot in range(4):
            refresh_history_once(db_path=self.db, now=self.now + timedelta(hours=6 * slot))
            self.assertEqual(get_history_today(db_path=self.db)["selected_index"], slot)
        fetch.assert_not_called()
        generate.assert_not_called()

    @patch("services.history.pipeline.generate_history", side_effect=ValueError("invalid"))
    @patch("services.history.pipeline.parse_history_events", return_value=[event()])
    @patch("services.history.pipeline.fetch_history_events", return_value={})
    def test_failure_uses_today_facts_and_backs_off(self, fetch, parse, generate):
        self.save({"date": "2026-11-20", "cards": [{"id": "old", "fact": "昨天"}]})
        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-only"}):
            first = refresh_history_once(db_path=self.db, now=self.now)
            refresh_history_once(db_path=self.db, now=self.now + timedelta(minutes=15))
        self.assertEqual(first["status"], "partial")
        self.assertEqual(get_history_today(db_path=self.db)["date"], "2026-11-21")
        self.assertEqual(get_history_today(db_path=self.db)["cards"][0]["fact"], event()["fact"])
        self.assertEqual(get_history_today(db_path=self.db)["cards"][0]["comment"], "")
        generate.assert_called_once()
        fetch.assert_called_once()

    @patch("services.history.pipeline.generate_history", return_value=[{"id": "new", "fact": "今天"}])
    def test_fallback_retries_next_slot_and_upgrades(self, generate):
        self.save({"date": "2026-11-21", "cards": [{"id": "fallback", "fact": "当天原文"}],
                   "generation_status": "fallback", "last_attempt_at": self.now.isoformat(),
                   "raw_date": "2026-11-21", "raw": [event()],
                   "display_slot": int(self.now.timestamp()) // 21600, "selected_index": 0})
        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-only"}):
            refresh_history_once(db_path=self.db, now=self.now + timedelta(minutes=15))
            generate.assert_not_called()
            result = refresh_history_once(db_path=self.db, now=self.now + timedelta(hours=6))
        self.assertEqual(result["status"], "success")
        generate.assert_called_once()
        self.assertEqual(get_dashboard_snapshot(CACHE, db_path=self.db)["generation_status"], "generated")

    @patch("services.history.pipeline.generate_history")
    def test_backoff_migrates_yesterday_without_extra_request(self, generate):
        self.save({"date": "2026-11-20", "cards": [{"id": "old", "fact": "昨天"}],
                   "last_attempt_at": self.now.isoformat(), "raw_date": "2026-11-21", "raw": [event()]})
        refresh_history_once(db_path=self.db, now=self.now + timedelta(minutes=15))
        self.assertEqual(get_history_today(db_path=self.db)["date"], "2026-11-21")
        generate.assert_not_called()

    def test_fallback_cannot_translate_or_duplicate_topics(self):
        self.assertEqual(len(factual_fallback([event(), event("duplicate"), {**event("en"), "language": "en"}])), 1)
        self.assertEqual(failure_reason(ValueError("secret provider payload")), "ValueError")

    @patch("services.history.pipeline.generate_history", side_effect=ValueError("invalid"))
    def test_no_sources_keeps_actual_date(self, generate):
        self.save({"date": "2026-11-20", "cards": [{"id": "old", "fact": "昨天"}],
                   "raw_date": "2026-11-21", "raw": [{**event(), "language": "en"}]})
        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-only"}):
            refresh_history_once(db_path=self.db, now=self.now)
        self.assertEqual(get_history_today(db_path=self.db)["date"], "2026-11-20")

    @patch("services.history.pipeline.generate_history", return_value=[{"id": "new", "fact": "今天"}])
    def test_new_day_replaces_previous_and_bounds_seen(self, generate):
        self.save({"date": "2026-11-20", "cards": [{"id": "old"}], "raw_date": "2026-11-21", "raw": [event()],
                   "seen": {"stale": "2025-01-01", **{str(i): "2026-11-20" for i in range(500)}}})
        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-only"}):
            result = refresh_history_once(db_path=self.db, now=self.now)
        self.assertEqual(result["status"], "success")
        saved = get_dashboard_snapshot(CACHE, db_path=self.db)
        self.assertEqual(saved["cards"][0]["id"], "new")
        self.assertEqual(saved["date"], "2026-11-21")
        self.assertLessEqual(len(saved["seen"]), 360)
        self.assertNotIn("stale", saved["seen"])
        self.assertNotIn("raw", get_history_today(db_path=self.db))


if __name__ == "__main__":
    unittest.main()
