import tempfile
import unittest
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch
import requests

from collectors.focus_news_fetcher import FEEDS, ITEMS_PER_FEED, fetch_focus_news, news_priority
from services.focus.pipeline import normalize_candidates, store_focus, get_focus, fetch_hotsearch_bundle
from storage import initialize_database

NOW = datetime(2026, 11, 27, tzinfo=timezone.utc)


def row(i, source="bbc.com", **values):
    return {"title": str(i), "url": f"https://{source}/{i}", "published_at": NOW.isoformat(),
            "source_kind": "news", "source_name": source, "score": 4, **values}


class FocusNewsTests(unittest.TestCase):
    def test_topic_priority_does_not_promote_small_accident(self):
        self.assertEqual(news_priority("Nine injured after Bangkok oil refinery explosion"), 2)
        self.assertEqual(news_priority("Central bank cuts interest rates"), 5)
        self.assertEqual(news_priority("Shipping halted through major strait"), 5)
        self.assertEqual(news_priority("Actor to stand trial"), 1)
        self.assertEqual(news_priority("Election live", categories=["Opinion"]), 0)
        self.assertEqual(news_priority("A discussion", url="https://aljazeera.com/sports/1"), 0)
        self.assertEqual(news_priority("Local incident", "wars and oil supply mentioned in footer"), 2)

    @patch("collectors.focus_news_fetcher.requests.get")
    def test_four_serial_feeds_bounded_and_bad_item_isolated(self, get):
        valid = "<item><title>Ceasefire agreed</title><link>https://example.com/1</link><pubDate>Fri, 27 Nov 2026 00:00:00 GMT</pubDate><author>AFP</author></item>"
        invalid = "<item><pubDate>broken</pubDate></item>"
        response = Mock(); response.content = ("<rss><channel>"+invalid+valid*30+"</channel></rss>").encode()
        get.return_value = response
        rows = fetch_focus_news()
        self.assertEqual(get.call_count, 4)
        self.assertEqual(len(rows), 4*ITEMS_PER_FEED)
        self.assertEqual({r["source_name"] for r in rows}, {name for name, _ in FEEDS})
        self.assertTrue(all(r["authors"] == "AFP" for r in rows))
        get.side_effect = [requests.Timeout(), response, response, response]
        self.assertEqual(len(fetch_focus_news()), 3*ITEMS_PER_FEED)

    def test_source_quotas_preserve_domestic_news_and_other_panels(self):
        rows = [row(i, source="bbc.com", score=5) for i in range(100)]
        for source in ["aljazeera.com", "france24.com", "news.un.org", "zaobao.com.sg"]:
            rows += [row(i, source=source, score=2) for i in range(12)]
        rows.append(row("space", source="noaa.gov", source_kind="space", score=1))
        result = normalize_candidates(rows, NOW)
        counts = Counter(r["origin"] for r in result if r["source_kind"] == "news")
        self.assertLessEqual(len(result), 40)
        self.assertLessEqual(sum(counts.values()), 16)
        self.assertLessEqual(max(counts.values()), 4)
        self.assertEqual(len(counts), 5)
        self.assertTrue(any(r["source_kind"] == "space" for r in result))

    def test_ai_declining_does_not_force_a_fallback_headline(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as folder:
            db=str(Path(folder)/"test.db");initialize_database(db)
            candidates=normalize_candidates([row("Important ceasefire", score=5)], NOW)
            store_focus(candidates, None, db_path=db, now=NOW, editor_declined=True)
            state=get_focus(db_path=db, now=NOW)
            self.assertEqual(state["text"], "")
            self.assertEqual(state["status"], "quiet")
            store_focus(candidates, None, db_path=db, now=NOW)
            self.assertEqual(get_focus(db_path=db, now=NOW)["text"], "Important ceasefire")
            candidates=normalize_candidates([row("Nine injured in local accident", score=2, international_feed=True)], NOW)
            store_focus(candidates, None, db_path=db, now=NOW)
            self.assertEqual(get_focus(db_path=db, now=NOW)["status"], "quiet")

    @patch("services.focus.pipeline.store_focus")
    @patch("services.hotsearch.pipeline.fetch_hotsearch_snapshot", return_value={"ai_digest":{"ok":True,"focus":None}})
    @patch("services.focus.pipeline.build_focus_candidates", return_value=[])
    def test_shared_editor_none_is_preserved(self, build, fetch, store):
        fetch_hotsearch_bundle(db_path="unused")
        fetch.assert_called_once()
        self.assertTrue(store.call_args.kwargs["editor_declined"])
