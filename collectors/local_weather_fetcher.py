"""Small, serial point forecast and freshness queries; no local model files."""
import requests

MODELS = {
    "ecmwf_ifs025": "ecmwf_ifs025",
    "ncep_gfs_global": "ncep_gfs013",
    "icon_global": "dwd_icon",
    "jma_gsm": "jma_gsm",
    "cmc_gem_gdps": "cmc_gem_gdps",
    "ukmo_global_deterministic_10km": "ukmo_global_deterministic_10km",
    "meteofrance_arpege_world": "meteofrance_arpege_world025",
    "cma_grapes_global": "cma_grapes_global",
}
VARIABLES = ("temperature_2m", "apparent_temperature", "relative_humidity_2m",
             "precipitation", "wind_speed_10m", "wind_gusts_10m", "cloud_cover",
             "is_day", "shortwave_radiation")


def fetch_local_weather(latitude, longitude):
    with requests.Session() as session:
        session.headers["User-Agent"] = "InfoGrab/1.0 (personal weather summary)"
        response = session.get("https://api.open-meteo.com/v1/forecast", params={
            "latitude": latitude, "longitude": longitude, "timezone": "Asia/Shanghai",
            "hourly": ",".join(VARIABLES), "forecast_hours": 8,
            "models": ",".join(MODELS), "wind_speed_unit": "ms",
        }, timeout=(5, 30))
        response.raise_for_status()
        payload = response.json()
        metadata = {}
        for model, directory in MODELS.items():
            try:
                response = session.get(f"https://api.open-meteo.com/data/{directory}/static/meta.json",
                                       timeout=(3, 5))
                response.raise_for_status()
                metadata[model] = response.json()
            except (requests.RequestException, ValueError):
                print(f"[warn] Local weather freshness unavailable: {model}")
        return payload, metadata
