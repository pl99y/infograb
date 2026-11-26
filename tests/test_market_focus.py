import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from services.market.calendar import _calendars, get_market_calendar
from services.commodities.pipeline import CACHE as COMMODITIES, SPECS, fetch_commodities_once, get_commodities
from services.focus.pipeline import CACHE, assess_alert, get_focus, normalize_candidates, store_focus
from services.hotsearch import pipeline as hotsearch
from storage import get_connection, get_dashboard_snapshot, initialize_database, insert_market_snapshot, save_dashboard_snapshot

NOW = datetime(2026, 11, 23, 3, tzinfo=timezone.utc)


def candidate(title="重大事件已获确认", url="https://www.bbc.com/news/1", kind="news", **kwargs):
    return {"title": title, "url": url, "published_at": NOW.isoformat(), "source_name": "source",
            "source_kind": kind, **kwargs}


class MarketFocusTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.db = str(Path(self.temp.name) / "data.db"); initialize_database(self.db)

    def test_calendar_holidays_lunch_close_and_half_day(self):
        data = _calendars()["calendars"]
        us = {r["date"]: r for r in data["XNYS"]["sessions"]}
        self.assertNotIn("2026-11-26", us)
        self.assertTrue(us["2026-11-27"]["close"].startswith("2026-11-27T18:00"))
        self.assertTrue(us["2026-07-01"]["open"].endswith("13:30:00+00:00"))
        self.assertTrue(us["2026-11-24"]["open"].endswith("14:30:00+00:00"))
        china = {r["date"]: r for r in data["XSHG"]["sessions"]}
        self.assertNotIn("2026-10-01", china)
        self.assertTrue(china["2026-11-23"]["break_start"].endswith("03:30:00+00:00"))
        self.assertEqual(data["XSHG"]["valid_until"], "2026-12-31")
        japan = {r["date"]: r for r in data["XTKS"]["sessions"]}
        self.assertNotIn("2026-11-23", japan)
        self.assertTrue(japan["2026-11-24"]["close"].endswith("06:30:00+00:00"))
        hk = {r["date"]: r for r in data["XHKG"]["sessions"]}
        self.assertTrue(hk["2026-12-24"]["auction_start"].endswith("04:00:00+00:00"))
        self.assertTrue(hk["2026-12-24"]["close"].endswith("04:10:00+00:00"))
        self.assertLess(len(json.dumps(get_market_calendar(NOW))), 20000)

    def test_quote_time_round_trip_and_migration_idempotent(self):
        initialize_database(self.db)
        con = get_connection(self.db)
        insert_market_snapshot(con, snapshot_key="sp500", name="S&P", symbol="^GSPC", price=100,
                               change_percent=1, source_name="test", fetched_at=NOW.isoformat(),
                               quote_time=(NOW-timedelta(hours=2)).isoformat())
        con.commit()
        self.assertEqual(con.execute('SELECT quote_time FROM market_snapshots').fetchone()[0], (NOW-timedelta(hours=2)).isoformat())
        con.close()

    @patch("services.commodities.pipeline.YahooMarketSnapshotAdapter")
    def test_commodity_rollover_partial_failure_and_bounded_snapshot(self, adapter):
        save_dashboard_snapshot(COMMODITIES, {"items": [{"quote_key": "gold", "contract_name": "Old month", "price": 10}]}, db_path=self.db)
        def quote(spec):
            if spec.key == "silver": raise RuntimeError("offline")
            return {"price": 100, "change_percent": 8, "contract_name": "New month", "quote_time": NOW.isoformat()}
        adapter.return_value.fetch_one.side_effect = quote
        result = fetch_commodities_once(db_path=self.db)
        self.assertEqual(result["status"], "partial")
        items = get_commodities(db_path=self.db)
        self.assertEqual(len(items), 6)
        self.assertTrue(items[0]["contract_rolled"])
        self.assertIsNone(items[0]["change_percent"])
        self.assertEqual(items[1]["status"], "unavailable")
        self.assertEqual(items[3]["unit"], "美分/蒲式耳")
        fetch_commodities_once(db_path=self.db)
        self.assertEqual(len(get_commodities(db_path=self.db)), 6)

    def test_focus_time_source_dedup_and_safe_urls(self):
        rows = [candidate(), candidate(url="https://bbc.com/news/2"), candidate(url="https://news.un.org/en/1"),
                candidate("旧新闻", published_at=(NOW-timedelta(days=2)).isoformat()),
                candidate("恶意链接", url="javascript:alert(1)"), candidate("Rainbet casino", kind="telegram")]
        result = normalize_candidates(rows, NOW)
        self.assertEqual(len(result), 2)  # Same BBC origin deduped, UN corroboration retained.
        self.assertTrue(any(r["official"] for r in result))
        fake = normalize_candidates([candidate(url="https://who.int.evil.example/1")], NOW)
        self.assertFalse(fake[0]["official"])

    def test_each_panel_can_contribute_and_input_is_bounded(self):
        rows = [candidate(str(i), url=f"https://www.bbc.com/{i}", score=5) for i in range(100)]
        rows += [candidate("空间警报", url="https://www.swpc.noaa.gov/1", kind="space", score=1)]
        result = normalize_candidates(rows, NOW)
        self.assertLessEqual(len(result), 40)
        self.assertTrue(any(r["source_kind"] == "space" for r in result))

    def evidence(self):
        return normalize_candidates([candidate("多国基础设施中断官方通报", url="https://news.un.org/en/1"),
            candidate("多国基础设施中断进展", url="https://www.bbc.com/1"),
            candidate("多国基础设施中断详情", url="https://www.zaobao.com.sg/1")], NOW)

    def test_alert_gate_needs_official_and_two_independent_news_origins(self):
        rows = self.evidence()
        choice = {"level": "critical", "global_impact": True, "event_type": "global_financial_disruption",
                  "support_ids": [r["id"] for r in rows]}
        self.assertEqual(assess_alert(choice, rows)["shadow_level"], "critical")
        for missing in rows:
            subset = [r for r in rows if r is not missing]
            self.assertNotEqual(assess_alert(choice, subset)["shadow_level"], "critical")
        rows[0]["title"] = "演习情景：多国基础设施中断"
        self.assertEqual(assess_alert(choice, rows)["shadow_level"], "normal")

    def test_telegram_only_cannot_trigger_alert_or_fallback(self):
        rows = normalize_candidates([candidate("突发重大消息", url="https://t.me/channel/1", kind="telegram")], NOW)
        choice = {"level": "critical", "global_impact": True, "event_type": "nuclear_use", "support_ids": [rows[0]["id"]]}
        self.assertEqual(assess_alert(choice, rows)["shadow_level"], "normal")
        store_focus(rows, None, db_path=self.db, now=NOW)
        self.assertFalse(get_focus(db_path=self.db, now=NOW).get("text"))

    def test_shadow_mode_expiry_bad_model_ids_and_unsupported_numbers(self):
        rows = self.evidence()
        choice = {"id": rows[0]["id"], "text": "多国基础设施中断已获官方通报。", "level": "critical",
                  "global_impact": True, "event_type": "global_financial_disruption", "support_ids": [r["id"] for r in rows]}
        with patch.dict("os.environ", {"INFOGRAB_ALERT_COLORS": "0"}):
            store_focus(rows, choice, db_path=self.db, now=NOW)
        self.assertEqual(get_focus(db_path=self.db, now=NOW)["alert_level"], "normal")
        self.assertEqual(get_dashboard_snapshot(CACHE, db_path=self.db)["assessment"]["shadow_level"], "critical")
        self.assertEqual(get_focus(db_path=self.db, now=NOW + timedelta(hours=1)), {})
        choice.update(text="9999国发生中断。", support_ids=[{}, "invalid"])
        store_focus(rows, choice, db_path=self.db, now=NOW)
        self.assertEqual(get_focus(db_path=self.db, now=NOW)["text"], rows[0]["title"])
        store_focus(rows, {"id": []}, db_path=self.db, now=NOW)

    def test_active_alert_outage_becomes_pending_instead_of_false_all_clear(self):
        rows = self.evidence()
        choice = {"id": rows[0]["id"], "text": "多国基础设施中断官方通报", "level": "critical",
                  "global_impact": True, "event_type": "global_financial_disruption", "support_ids": [r["id"] for r in rows]}
        with patch.dict("os.environ", {"INFOGRAB_ALERT_COLORS": "1"}):
            store_focus(rows, choice, db_path=self.db, now=NOW)
        store_focus([], None, db_path=self.db, now=NOW+timedelta(hours=1))
        pending = get_focus(db_path=self.db, now=NOW+timedelta(hours=1))
        self.assertEqual(pending["alert_level"], "major")
        self.assertIn('待核实', pending["text"])

    @patch.object(hotsearch.requests, "post")
    @patch.object(hotsearch, "GEMINI_API_KEY", "test")
    def test_digest_and_focus_share_one_model_request(self, post):
        post.return_value.status_code = 200
        post.return_value.json.return_value = {"candidates": [{"content": {"parts": [{"text": json.dumps({
            "summary": "摘要", "priority_items": [], "focus": {"id": "id", "text": "关注"}})}]}}]}
        result = hotsearch.generate_hotsearch_ai_digest([{"title": "热搜"}], self.evidence())
        post.assert_called_once()
        self.assertIn('gemini-3.1-flash-lite', post.call_args.args[0])
        self.assertEqual(result["focus"]["id"], "id")
        prompt = post.call_args.kwargs["json"]["contents"][0]["parts"][0]["text"]
        self.assertIn('热搜', prompt); self.assertIn(self.evidence()[0]["id"], prompt)


if __name__ == '__main__': unittest.main()
