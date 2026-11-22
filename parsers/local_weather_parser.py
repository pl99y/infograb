"""Compute the facts before allowing the editor to make jokes about them."""
import math
from datetime import datetime
from statistics import median
from zoneinfo import ZoneInfo

from collectors.local_weather_fetcher import MODELS

LOCAL_TIME = ZoneInfo("Asia/Shanghai")


def summarize_weather(payload, metadata, now):
    hourly = payload.get("hourly", {})
    times = [datetime.fromisoformat(t).replace(tzinfo=LOCAL_TIME) for t in hourly.get("time", [])]
    indices = [i for i, t in enumerate(times) if t > now][:6]
    if len(indices) != 6 or any((times[b] - times[a]).total_seconds() != 3600
                                for a, b in zip(indices, indices[1:])):
        raise ValueError("Incomplete six-hour weather window")

    def values(variable, model):
        raw = hourly.get(f"{variable}_{model}", [])
        return [raw[i] if i < len(raw) and isinstance(raw[i], (int, float))
                and not isinstance(raw[i], bool) and math.isfinite(raw[i]) else None for i in indices]

    valid = []
    for model in MODELS:
        meta = metadata.get(model, {})
        initialized = meta.get("last_run_initialisation_time", 0)
        available = meta.get("last_run_availability_time", 0)
        cycle = meta.get("update_interval_seconds", 21600)
        if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (initialized, available, cycle)):
            continue
        if not (-600 <= now.timestamp() - available <= max(43200, cycle * 2)
                and -600 <= now.timestamp() - initialized <= max(64800, cycle * 3)):
            continue
        units = payload.get("hourly_units", {})
        if all(units.get(f"{v}_{model}") == unit and None not in values(v, model)
               for v, unit in (("temperature_2m", "°C"), ("precipitation", "mm"), ("wind_speed_10m", "m/s"))):
            valid.append(model)
    if len(valid) < 3:
        raise ValueError("Too few fresh complete weather models")

    def all_values(variable):
        return [v for m in valid for v in values(variable, m) if v is not None]

    temperatures = [median([values("temperature_2m", m)[i] for m in valid]) for i in range(6)]
    rain = [sum(values("precipitation", m)) for m in valid]
    gusts = [max(v) for m in valid if None not in (v := values("wind_gusts_10m", m))]
    humidity = all_values("relative_humidity_2m")
    radiation = all_values("shortwave_radiation")
    day = all_values("is_day")
    tags = []
    if max(temperatures) >= 32: tags.append("hot")
    if min(temperatures) <= 14: tags.append("cold")
    if temperatures[-1] - temperatures[0] <= -3: tags.append("cooling")
    if median(rain) >= 20: tags.append("heavy_rain")
    if gusts and median(gusts) >= 17.2: tags.append("strong_wind")
    if humidity and median(humidity) >= 85: tags.append("humid")
    if day and any(day) and radiation and median(radiation) >= 600: tags.append("sunny")
    rain_votes = sum(v >= 0.1 for v in rain)
    if 0 < rain_votes < len(valid): tags.append("rain_disagreement")
    if 0 < median(rain) < 2: tags.append("drizzle")
    return {
        "model_count": len(valid), "hours": [times[i].isoformat() for i in indices],
        "temperature_medians_c": [round(t, 1) for t in temperatures],
        "temperature_max_spread_c": round(max(max(values("temperature_2m", m)[i] for m in valid)
                                              - min(values("temperature_2m", m)[i] for m in valid) for i in range(6)), 1),
        "rain_models": rain_votes, "dry_models": len(valid) - rain_votes,
        "rain_total_range_mm": [round(min(rain), 1), round(max(rain), 1)],
        "gust_model_count": len(gusts), "gust_max_range_ms": [min(gusts), max(gusts)] if gusts else None,
        "humidity_range_pct": [min(humidity), max(humidity)] if humidity else None,
        "daylight": bool(day and any(day)), "tags": tags,
        "valid_until": times[indices[-1]].isoformat(),
    }
