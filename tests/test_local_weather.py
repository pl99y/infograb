import copy
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import requests

from collectors.local_weather_fetcher import MODELS
from parsers.local_weather_parser import summarize_weather
from services.local_weather.pipeline import (CACHE, generate_line, get_local_weather,
                                              refresh_local_weather_once, validate_line)
from storage import get_dashboard_snapshot, initialize_database


NOW = datetime(2026, 11, 22, 15, 23, tzinfo=timezone.utc)  # 23:23 China, crossing midnight.


def sample():
    start = datetime(2026, 11, 22, 23)
    hourly = {"time": [(start + timedelta(hours=i)).isoformat(timespec="minutes") for i in range(8)]}
    metadata, units = {}, {}
    for m in MODELS:
        metadata[m] = {"last_run_initialisation_time": NOW.timestamp() - 21600,
                       "last_run_availability_time": NOW.timestamp() - 3600,
                       "update_interval_seconds": 21600}
        for v, n, unit in (("temperature_2m", 22, "°C"), ("precipitation", 0.1, "mm"),
                           ("wind_speed_10m", 4, "m/s"), ("wind_gusts_10m", 8, "m/s"),
                           ("relative_humidity_2m", 88, "%"), ("is_day", 0, "")):
            hourly[f"{v}_{m}"] = [n] * 8
            units[f"{v}_{m}"] = unit
    return {"hourly": hourly, "hourly_units": units}, metadata


class LocalWeatherTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = str(Path(self.temp.name) / "weather.db")
        initialize_database(self.db)

    def test_midnight_window_and_missing_optional_fields(self):
        raw, meta = sample()
        raw["hourly"]["wind_gusts_10m_jma_gsm"] = [None] * 8
        result = summarize_weather(raw, meta, NOW)
        self.assertEqual(result["model_count"], 8)
        self.assertEqual(result["gust_model_count"], 7)
        self.assertTrue(result["hours"][0].startswith("2026-11-23T00:00"))
        self.assertTrue(result["valid_until"].startswith("2026-11-23T05:00"))

    def test_stale_null_and_wrong_units_do_not_vote(self):
        raw, meta = sample()
        models = list(MODELS)
        meta[models[0]]["last_run_availability_time"] = NOW.timestamp() - 200000
        raw["hourly"][f"temperature_2m_{models[1]}"][3] = None
        raw["hourly_units"][f"wind_speed_10m_{models[2]}"] = "km/h"
        result = summarize_weather(raw, meta, NOW)
        self.assertEqual(result["model_count"], 5)
        self.assertEqual(result["rain_models"], 5)

    def test_gap_and_too_few_fresh_models_rejected(self):
        raw, meta = sample()
        with self.assertRaises(ValueError): summarize_weather(raw, {}, NOW)
        raw["hourly"]["time"].pop(3)
        with self.assertRaises(ValueError): summarize_weather(raw, meta, NOW)

    def test_rain_votes_not_probability_and_temperature_spread_not_trend(self):
        raw, meta = sample()
        for m in list(MODELS)[:3]:
            raw["hourly"][f"precipitation_{m}"] = [0] * 8
            raw["hourly"][f"temperature_2m_{m}"] = [18] * 8
        summary = summarize_weather(raw, meta, NOW)
        self.assertEqual((summary["rain_models"], summary["dry_models"]), (5, 3))
        self.assertIn("rain_disagreement", summary["tags"])
        self.assertNotIn("cooling", summary["tags"])
        for text in ("未来六小时暴雨将至，快准备接水。", "降雨概率为62.5%，看来要下雨了。", "柏油马路已经半熔化了，小心。"):
            with self.assertRaises(ValueError): validate_line(text, summary)
        self.assertEqual(validate_line("今日带伞指数零点三，伞本人正在犹豫。", summary), "今日带伞指数零点三，伞本人正在犹豫。")

    @patch("services.local_weather.pipeline.generate_line", return_value="天空正在纠结要不要给地面补点水。")
    @patch("services.local_weather.pipeline.fetch_local_weather", side_effect=lambda *args: sample())
    def test_one_generation_per_slot_private_point_and_expiry(self, fetch, generate):
        with patch.dict("os.environ", {"INFOGRAB_WEATHER_LATITUDE": "22.27", "INFOGRAB_WEATHER_LONGITUDE": "113.57", "GEMINI_API_KEY": "test"}):
            refresh_local_weather_once(db_path=self.db, now=NOW)
            refresh_local_weather_once(db_path=self.db, now=NOW + timedelta(minutes=15))
        fetch.assert_called_once_with(22.27, 113.57)
        generate.assert_called_once()
        public = get_local_weather(db_path=self.db, now=NOW)
        self.assertEqual(set(public), {"text", "generated_at", "valid_until"})
        self.assertNotIn("22.27", json.dumps(public))
        self.assertEqual(get_local_weather(db_path=self.db, now=NOW + timedelta(hours=6)), {})

    @patch("services.local_weather.pipeline.generate_line", side_effect=requests.HTTPError())
    @patch("services.local_weather.pipeline.fetch_local_weather", side_effect=lambda *args: sample())
    def test_editor_failure_has_fresh_rule_fallback_without_retry(self, fetch, generate):
        with patch.dict("os.environ", {"INFOGRAB_WEATHER_LATITUDE": "22.27", "INFOGRAB_WEATHER_LONGITUDE": "113.57", "GEMINI_API_KEY": "test"}):
            result = refresh_local_weather_once(db_path=self.db, now=NOW)
            refresh_local_weather_once(db_path=self.db, now=NOW)
        self.assertEqual(result["status"], "partial")
        self.assertTrue(get_local_weather(db_path=self.db, now=NOW)["text"])
        generate.assert_called_once()
        self.assertEqual(get_dashboard_snapshot(CACHE, db_path=self.db)["status"], "fallback")

    @patch("services.local_weather.pipeline.fetch_local_weather", side_effect=requests.Timeout())
    @patch("services.local_weather.pipeline.generate_line")
    def test_source_failure_does_not_call_editor(self, generate, fetch):
        with patch.dict("os.environ", {"INFOGRAB_WEATHER_LATITUDE": "22.27", "INFOGRAB_WEATHER_LONGITUDE": "113.57"}):
            result = refresh_local_weather_once(db_path=self.db, now=NOW)
            refresh_local_weather_once(db_path=self.db, now=NOW)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(get_local_weather(db_path=self.db, now=NOW), {})
        fetch.assert_called_once()
        generate.assert_not_called()

    @patch("services.local_weather.pipeline.requests.post")
    def test_single_packed_request_uses_exact_model(self, post):
        summary = summarize_weather(*sample(), NOW)
        post.return_value.json.return_value = {"modelVersion": "gemini-3.5-flash-lite", "candidates": [{
            "finishReason": "STOP", "content": {"parts": [{"text": json.dumps({"text": "空气湿度正在冲业绩，晾衣服需要一点耐心。"})}]}}]}
        generate_line(summary, "test")
        post.assert_called_once()
        self.assertIn("gemini-3.5-flash-lite:generateContent", post.call_args.args[0])
        packed = post.call_args.kwargs["json"]["contents"][0]["parts"][0]["text"]
        self.assertEqual(json.loads(packed), summary)


if __name__ == "__main__":
    unittest.main()
