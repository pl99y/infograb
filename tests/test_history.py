import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from parsers.history_parser import parse_history_events
from services.history.pipeline import CACHE, MODEL, factual_fallback, failure_reason, generate_history, get_history_today, refresh_history_once, validate_history
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

    @patch("services.history.pipeline.requests.post")
    def test_generation_is_one_packed_request_to_exact_model(self, post):
        post.return_value.json.return_value = {"modelVersion": MODEL, "candidates": [{
            "finishReason": "STOP", "content": {"parts": [{"text": json.dumps({"items": [generated()]})}]}}]}
        candidates = [event(), event("second")]
        generate_history(candidates, "test-only")
        post.assert_called_once()
        self.assertIn("gemini-3.5-flash-lite:generateContent", post.call_args.args[0])
        packed = post.call_args.kwargs["json"]["contents"][0]["parts"][0]["text"]
        self.assertEqual(json.loads(packed), candidates)

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

    def days(self):
        return (get_dashboard_snapshot(CACHE, db_path=self.db) or {}).get("days", {})

    def public(self, now=None):
        return get_history_today(db_path=self.db, now=now or self.now)

    @patch("services.history.pipeline.generate_history")
    @patch("services.history.pipeline.fetch_history_events")
    def test_legacy_migration_discards_seen_and_successful_raw(self, fetch, generate):
        self.save({"date": "2026-11-21", "generation_status": "generated", "cards": [{"fact": "今天"}],
                   "raw": [event()], "raw_date": "2026-11-21", "seen": {str(i): "2026-11-20" for i in range(500)}})
        refresh_history_once(db_path=self.db, now=self.now)
        root = get_dashboard_snapshot(CACHE, db_path=self.db)
        self.assertNotIn("seen", root)
        self.assertNotIn("raw", self.days()["2026-11-21"])
        self.assertNotIn("seen", self.days()["2026-11-21"])
        generate.assert_not_called()
        fetch.assert_not_called()

    @patch("services.history.pipeline.generate_history")
    def test_four_long_update_slots(self, generate):
        cards = [{"id": str(i), "fact": "事实"} for i in range(4)]
        self.save({"days": {"2026-11-21": {"date": "2026-11-21", "generation_status": "generated", "cards": cards}}})
        for slot in range(4):
            now = self.now + timedelta(hours=6 * slot)
            # At 20:07 tomorrow is prepared; seed it so this test only measures rotation.
            tomorrow = {"date": "2026-11-22", "generation_status": "generated", "cards": cards}
            current = self.days().get("2026-11-21")
            self.save({"days": {"2026-11-21": current, "2026-11-22": tomorrow}})
            refresh_history_once(db_path=self.db, now=now)
            self.assertEqual(self.public(now)["selected_index"], slot)
        generate.assert_not_called()

    @patch("services.history.pipeline.generate_history", return_value=[{"id": "new", "fact": "明天", "comment": "短评"}])
    @patch("services.history.pipeline.parse_history_events", return_value=[event()])
    @patch("services.history.pipeline.fetch_history_events", return_value={})
    def test_evening_prefetch_published_before_midnight_and_promoted(self, fetch, parse, generate):
        evening = datetime(2026, 11, 21, 12, 7, tzinfo=timezone.utc)
        self.save({"date": "2026-11-21", "generation_status": "generated", "cards": [{"fact": "今天"}]})
        with patch.dict("os.environ", {"GEMINI_API_KEY": "test"}):
            refresh_history_once(db_path=self.db, now=evening)
            before = self.public(evening)
            self.assertEqual(before["date"], "2026-11-21")
            self.assertEqual(before["days"]["2026-11-22"]["cards"][0]["fact"], "明天")
            self.assertNotIn("raw", self.days()["2026-11-22"])
            midnight = datetime(2026, 11, 21, 16, 0, tzinfo=timezone.utc)
            self.assertEqual(self.public(midnight)["date"], "2026-11-22")
            self.assertEqual(self.public(midnight)["cards"][0]["fact"], "明天")
            refresh_history_once(db_path=self.db, now=midnight, retry_only=True)
        generate.assert_called_once()
        fetch.assert_called_once_with(date(2026, 11, 22))
        self.assertEqual(set(self.days()), {"2026-11-22"})

    @patch("services.history.pipeline.generate_history", side_effect=ValueError("No history cards passed validation"))
    def test_shared_target_day_budget_survives_midnight(self, generate):
        # 23:00 China: tomorrow's retries continue across midnight, without resetting the budget.
        start = datetime(2026, 11, 21, 15, 0, tzinfo=timezone.utc)
        self.save({"days": {
            "2026-11-21": {"date": "2026-11-21", "generation_status": "generated", "cards": [{"fact": "今天"}]},
            "2026-11-22": {"date": "2026-11-22", "raw": [event()]}}})
        with patch.dict("os.environ", {"GEMINI_API_KEY": "test"}):
            refresh_history_once(db_path=self.db, now=start)
            for minute, expected in ((14, 1), (15, 2), (44, 2), (45, 3), (104, 3),
                                     (105, 4), (164, 4), (165, 5), (500, 5)):
                refresh_history_once(db_path=self.db, now=start + timedelta(minutes=minute), retry_only=True)
                self.assertEqual(generate.call_count, expected)
            refresh_history_once(db_path=self.db, now=start + timedelta(hours=7))
        self.assertEqual(generate.call_count, 5)
        saved = self.days()["2026-11-22"]
        self.assertEqual(saved["attempt_count"], 5)
        self.assertIsNone(saved["next_attempt_at"])
        self.assertEqual(saved["generation_status"], "fallback")

    @patch("services.history.pipeline.generate_history", return_value=[{"id": "new", "fact": "生成成功"}])
    def test_fallback_upgrade_drops_raw_without_additional_calls(self, generate):
        self.save({"days": {"2026-11-21": {"date": "2026-11-21", "generation_status": "fallback",
            "cards": [{"fact": "原文"}], "raw": [event()], "last_attempt_at": self.now.isoformat(), "attempt_count": 1}}})
        with patch.dict("os.environ", {"GEMINI_API_KEY": "test"}):
            refresh_history_once(db_path=self.db, now=self.now + timedelta(minutes=14), retry_only=True)
            generate.assert_not_called()
            refresh_history_once(db_path=self.db, now=self.now + timedelta(minutes=15), retry_only=True)
            refresh_history_once(db_path=self.db, now=self.now + timedelta(minutes=30), retry_only=True)
        generate.assert_called_once()
        self.assertNotIn("raw", self.days()["2026-11-21"])
        self.assertEqual(self.public()["cards"][0]["fact"], "生成成功")

    @patch("services.history.pipeline.generate_history")
    @patch("services.history.pipeline.fetch_history_events")
    def test_fast_job_cannot_start_unplanned_generation(self, fetch, generate):
        refresh_history_once(db_path=self.db, now=self.now, retry_only=True)
        generate.assert_not_called()
        fetch.assert_not_called()
        self.assertEqual(self.days(), {})

    @patch("services.history.pipeline.generate_history")
    def test_only_today_and_tomorrow_survive_cleanup(self, generate):
        records = {f"2026-11-{day}": {"date": f"2026-11-{day}", "generation_status": "generated", "cards": [{"fact": "史实"}]}
                   for day in (19, 20, 21, 22, 23)}
        self.save({"days": records})
        refresh_history_once(db_path=self.db, now=self.now, retry_only=True)
        self.assertEqual(set(self.days()), {"2026-11-21", "2026-11-22"})
        generate.assert_not_called()

    def test_fallback_cannot_translate_or_duplicate_topics(self):
        self.assertEqual(len(factual_fallback([event(), event("duplicate"), {**event("en"), "language": "en"}])), 1)
        self.assertEqual(failure_reason(ValueError("secret provider payload")), "ValueError")


if __name__ == "__main__":
    unittest.main()
