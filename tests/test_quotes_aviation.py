import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from parsers.aviation_parser import parse_adsbfi_squawk_alerts
from services.aviation.pipeline import _record_current_adsbfi_alert, _mark_inactive_adsbfi_alerts, _alert_dedupe_key, ADSBFI_SOURCE_NAME
from services.aviation.queries import get_airport_alerts
from services.commodities.pipeline import CACHE, get_commodities, usable_fx, TROY_OUNCE_GRAMS
from storage import initialize_database, get_connection, save_dashboard_snapshot


class QuotesAviationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.db = str(Path(self.directory.name) / "test.db")
        initialize_database(self.db)
        self.now = datetime.now(timezone.utc)

    def tearDown(self):
        self.directory.cleanup()

    def test_cny_gram_conversion_preserves_underlying_quotes(self):
        fx = {"rate": 7, "quote_time": self.now.isoformat()}
        rows = [{"quote_key": "gold", "price": TROY_OUNCE_GRAMS * 100}, {"quote_key": "silver", "price": TROY_OUNCE_GRAMS}, {"quote_key": "copper", "price": 4.5}]
        save_dashboard_snapshot(CACHE, {"items": rows, "fx": fx}, db_path=self.db)
        result = get_commodities(db_path=self.db)
        self.assertEqual(result[0]["display_price"], 700)
        self.assertEqual(result[1]["display_price"], 7)
        self.assertEqual(result[0]["price"], rows[0]["price"])
        self.assertEqual(result[0]["display_unit"], "人民币/克")
        self.assertNotIn("display_price", result[2])
        self.assertEqual(get_commodities(db_path=self.db), result)

    def test_invalid_or_expired_fx_never_labels_usd_as_cny(self):
        for rate in [0, -7, float("nan"), float("inf")]:
            self.assertFalse(usable_fx({"rate": rate, "quote_time": self.now.isoformat()}))
        fx = {"rate": 7, "quote_time": (self.now-timedelta(days=8)).isoformat()}
        save_dashboard_snapshot(CACHE, {"items": [{"quote_key": "gold", "price": 3000}], "fx": fx}, db_path=self.db)
        result = get_commodities(db_path=self.db)[0]
        self.assertIsNone(result["display_price"])
        self.assertEqual(result["conversion_status"], "unavailable")
        self.assertTrue(usable_fx({"rate": 7, "quote_time": (self.now-timedelta(days=3)).isoformat()}))

    def alert(self, hex_id="abc123", seen=20, stamp=None, **kwargs):
        stamp = stamp or self.now
        return parse_adsbfi_squawk_alerts({"now": stamp.timestamp()*1000, "ac": [{"hex": hex_id, "r": "N123AB", "seen": seen, "squawk": "7700", **kwargs}]}, "https://example.com", squawk="7700", fetched_at=stamp.isoformat())

    def test_actual_observation_time_and_callsign_separation(self):
        alert = self.alert(flight=" ABC123 ")[0]
        self.assertEqual(alert.callsign, "ABC123")
        self.assertEqual(alert.extra["broadcast_callsign"], "ABC123")
        self.assertEqual(alert.extra["registration"], "N123AB")
        self.assertEqual(datetime.fromisoformat(alert.extra["last_seen_utc"]), self.now-timedelta(seconds=20))
        fallback = self.alert()[0]
        self.assertEqual(fallback.callsign, "N123AB")
        self.assertEqual(fallback.extra["broadcast_callsign"], "")
        self.assertEqual(self.alert(seen=400), [])
        self.assertEqual(self.alert(squawk="7600"), [])

    def test_sort_by_observation_and_reactivation_resets_episode(self):
        con = get_connection(self.db)
        try:
            old = self.alert(stamp=self.now-timedelta(hours=2))[0]
            key = _alert_dedupe_key(old)
            _record_current_adsbfi_alert(con, alert=old, dedupe_key=key, fetched_at=(self.now-timedelta(hours=2)).isoformat())
            _mark_inactive_adsbfi_alerts(con, active_dedupe_keys=set(), checked_at=(self.now-timedelta(hours=1)).isoformat(), successful_squawks={"7700"})
            current = self.alert()[0]
            _record_current_adsbfi_alert(con, alert=current, dedupe_key=key, fetched_at=self.now.isoformat())
            other = self.alert(hex_id="def456", seen=100)[0]
            _record_current_adsbfi_alert(con, alert=other, dedupe_key=_alert_dedupe_key(other), fetched_at=(self.now+timedelta(seconds=1)).isoformat())
            con.commit()
            # A previously present callsign must clear when the broadcast omits it.
            self.assertEqual(get_airport_alerts(db_path=self.db)[0]["extra"]["broadcast_callsign"], "")
            rows = get_airport_alerts(db_path=self.db)
            self.assertEqual(rows[0]["extra"]["hex"], "abc123")
            self.assertEqual(rows[0]["extra"]["first_seen_utc"], current.extra["last_seen_utc"])
            self.assertEqual(rows[0]["event_date_text"], current.event_date_text)
            # A delayed older snapshot cannot revive/regress the latest record.
            _record_current_adsbfi_alert(con, alert=old, dedupe_key=key, fetched_at=self.now.isoformat())
            con.commit()
            self.assertEqual(get_airport_alerts(db_path=self.db)[0]["extra"]["last_seen_utc"], current.extra["last_seen_utc"])
        finally:
            con.close()
