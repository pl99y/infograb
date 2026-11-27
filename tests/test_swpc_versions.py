import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from parsers.space_weather_parser import parse_swpc_alerts_payload, latest_swpc_versions
from services.space_weather.pipeline import _replace_swpc_alerts, _read_existing_alert_cache, _build_translation_plan, _reuse_cached_forecast_translations
from services.space_weather.queries import get_space_weather_alerts
from storage import initialize_database, get_connection


def message(serial, headline="WATCH: Geomagnetic Storm Category G1 Predicted", code="WATA20"):
    return f"Space Weather Message Code: {code}\nSerial Number: {serial}\nIssue Time: 2026 Nov 25 0833 UTC\n{headline}\nPotential Impacts: Weak power grid fluctuations."


class SwpcVersionTests(unittest.TestCase):
    def alerts(self):
        rows=[{"product_id":"A20F","issue_datetime":"2026-11-24 14:19:26.753","message":message(1139)},
              {"product_id":"A20F","issue_datetime":"2026-11-25 08:33:12.430","message":message(1140)}]
        return rows

    def test_latest_series_before_limit_keeps_other_products(self):
        rows=self.alerts()+[{"product_id":"ALTK05","issue_datetime":"2026-11-23 08:00:00","message":message(42,"ALERT: Geomagnetic K-index of 5", "ALTK05")}]
        parsed=parse_swpc_alerts_payload(rows,now=datetime(2026,11,27,tzinfo=timezone.utc),max_items=2)
        self.assertEqual([r["serial_number"] for r in parsed],["1140","42"])
        cancelled={**parsed[0],"message_type":"cancel_watch","serial_number":"1141","issue_datetime":"2026-11-26T00:00:00+00:00"}
        self.assertEqual(latest_swpc_versions(parsed+[cancelled])[0]["serial_number"],"1141")

    def test_existing_database_versions_and_translation_cache(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as folder:
            db=str(Path(folder)/"test.db");initialize_database(db);con=get_connection(db)
            try:
                # Insert pre-fix history rather than using the new parser's collapse.
                parsed=[]
                for row in self.alerts():
                    parsed+=parse_swpc_alerts_payload([row],now=datetime(2026,11,27,tzinfo=timezone.utc))
                parsed[-1]["headline"]="观看：预计将出现G1级地磁风暴"
                parsed[-1]["impacts_text"]="可能出现轻微电网波动。"
                _replace_swpc_alerts(con,parsed,fetched_at="2026-11-27T00:00:00Z");con.commit()
                items=get_space_weather_alerts(db_path=db)
                self.assertEqual(len(items),1)
                self.assertEqual(items[0]["serial_number"],"1140")
                self.assertTrue(items[0]["headline"].startswith("监视："))
                cache=_read_existing_alert_cache(con)
                fresh=parse_swpc_alerts_payload(self.alerts(),now=datetime(2026,11,27,tzinfo=timezone.utc))
                texts,slots,_=_build_translation_plan(fresh,{},existing_alerts=cache)
                self.assertEqual(texts,[])
                self.assertTrue(fresh[0]["headline"].startswith("监视："))
                # English fallback is not a successful Chinese translation.
                source=parse_swpc_alerts_payload(self.alerts()[:1],now=datetime(2026,11,27,tzinfo=timezone.utc))
                texts,slots,_=_build_translation_plan(source,{},existing_alerts=cache)
                self.assertTrue(texts)
                # A corrected body with the same identity must be translated again.
                fresh=parse_swpc_alerts_payload(self.alerts(),now=datetime(2026,11,27,tzinfo=timezone.utc))
                fresh[0]["impacts_text"]="Changed forecast impacts."
                texts,slots,_=_build_translation_plan(fresh,{},existing_alerts=cache)
                self.assertEqual(texts,["Changed forecast impacts."])
            finally:con.close()

    def test_forecast_reuses_only_translated_fields(self):
        panel={"geomagnetic":{"observed_summary":"Observed English","expected_summary":"Expected English"}}
        cache={"geomagnetic":{"observed_summary":"Observed English","expected_summary":"预计轻微扰动"}}
        self.assertTrue(_reuse_cached_forecast_translations(panel,cache))
        texts,_,_=_build_translation_plan([],panel)
        self.assertEqual(texts,["Observed English"])
