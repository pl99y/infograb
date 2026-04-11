from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

from .tc_adapter import parse_typhoon_payloads as _parse_typhoon_adapter

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - optional dependency fallback
    BeautifulSoup = None  # type: ignore


WMO_LIST_URL = "https://severeweather.wmo.int/list.html"
USGS_MAP_URL = "https://earthquake.usgs.gov/earthquakes/map"
MIROVA_NRT_URL = "https://www.mirovaweb.it/NRT/"
GDACS_ALERTS_URL = "https://www.gdacs.org/Alerts/"

MIROVA_LEVEL_ORDER = ["extreme", "very_high", "high", "moderate", "low", "unknown"]
MIROVA_THRESHOLDS = [
    (10000.0, "extreme"),
    (1000.0, "very_high"),
    (100.0, "high"),
    (10.0, "moderate"),
    (-1.0, "low"),
]


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()



def _norm_text(value: Any) -> str:
    return str(value or "").strip()



def _compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", _norm_text(value))



def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None



def _to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    text = _norm_text(value).lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None



def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()



def _build_map_url(lat: Any, lon: Any) -> str | None:
    lat_f = _to_float(lat)
    lon_f = _to_float(lon)
    if lat_f is None or lon_f is None:
        return None
    return f"https://www.google.com/maps?q={lat_f},{lon_f}"



def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _norm_text(value)
    if not text:
        return None

    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d %b %Y %H:%M UTC",
        "%d %b %Y %H:%M",
        "%d-%b-%Y %H:%M:%S",
        "%d-%b-%Y %H:%M:%S UTC",
        "%d-%b-%Y %H:%M",
        "%d-%b-%Y %H:%M UTC",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S GMT",
    ):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass

    return None



def _timestamp_to_iso(value: Any, *, fallback: str) -> str:
    if value is None or value == "":
        return fallback

    if isinstance(value, (int, float)):
        number = float(value)
        if number > 1e12:
            number = number / 1000.0
        try:
            return datetime.fromtimestamp(number, tz=timezone.utc).isoformat()
        except Exception:
            return fallback

    dt = _parse_iso_datetime(value)
    if dt is not None:
        return dt.isoformat()

    try:
        number = float(str(value).strip())
        if number > 1e12:
            number = number / 1000.0
        return datetime.fromtimestamp(number, tz=timezone.utc).isoformat()
    except Exception:
        return fallback



def _safe_json(block: dict) -> Any:
    if not isinstance(block, dict):
        return None
    if not block.get("ok", False):
        return None
    return block.get("data")



def _safe_text(block: dict) -> str:
    if not isinstance(block, dict):
        return ""
    if not block.get("ok", False):
        return ""
    return _norm_text(block.get("text"))



def _collect_errors(raw: dict, keys: list[str], *, ignore_messages: set[str] | None = None) -> list[str]:
    ignore_messages = ignore_messages or set()
    errors: list[str] = []
    for key in keys:
        block = raw.get(key, {})
        if isinstance(block, dict) and block.get("ok") is False and block.get("error"):
            message = str(block["error"])
            if message in ignore_messages:
                continue
            errors.append(f"{key}: {message}")
    return errors



def _flatten_event_dict(obj: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(key, str) and prefix:
                joined = f"{prefix}.{key}"
            else:
                joined = str(key)
            if isinstance(value, dict):
                out.update(_flatten_event_dict(value, joined))
            else:
                out[joined] = value
                if isinstance(key, str):
                    out.setdefault(key, value)
    return out



def _pick_first(flat: dict[str, Any], keys: list[str]) -> Any:
    lower_map = {k.lower(): v for k, v in flat.items()}
    for key in keys:
        if key in flat:
            return flat[key]
        value = lower_map.get(key.lower())
        if value is not None:
            return value
    for flat_key, value in flat.items():
        flat_key_l = flat_key.lower()
        for key in keys:
            key_l = key.lower()
            if flat_key_l == key_l or flat_key_l.endswith("." + key_l):
                return value
    return None



def _iter_candidate_dicts(obj: Any):
    stack = [obj]
    seen_ids: set[int] = set()
    candidate_keys = {
        "eventtype",
        "eventType",
        "eventid",
        "episodeid",
        "alertlevel",
        "alertLevel",
        "eventname",
        "mag",
        "VRP MW",
        "volcano",
        "iscurrent",
    }
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            object_id = id(current)
            if object_id in seen_ids:
                continue
            seen_ids.add(object_id)
            if any(key in current for key in candidate_keys):
                yield current
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            stack.extend(current)



def _clean_html_text(value: Any) -> str:
    text = _norm_text(value)
    text = re.sub(r"<[^>]+>", " ", text)
    return _compact_text(text)



def _severity_rank(level: str | None, order: list[str]) -> int:
    text = _norm_text(level).lower()
    try:
        return order.index(text)
    except ValueError:
        return len(order)



def _humanize_compact_name(value: str) -> str:
    text = _compact_text(value)
    if not text:
        return text
    if " " in text:
        return text
    text = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", text)
    text = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", text)
    return _compact_text(text)


# -----------------------------------------------------------------------------
# unified natural hazard parsing
# -----------------------------------------------------------------------------

def parse_natural_hazard_payloads(raw: dict) -> dict:
    fetched_at = _norm_text(raw.get("fetched_at") or _iso_now())
    weather_alerts = _parse_wmo_items(raw, fetched_at, window_hours=12)

    disaster_events: list[dict] = []
    disaster_events.extend(_parse_usgs_quakes(raw, fetched_at, min_mag=5.0))
    disaster_events.extend(_parse_mirova_volcano(raw, fetched_at, allowed_levels={"extreme", "very_high", "high"}))
    disaster_events.extend(_parse_gdacs_flood(raw, fetched_at, active_within_days=14))
    disaster_events.extend(_parse_tsunami_homepage(raw.get("tsunami_home", {}), fetched_at))

    typhoon_events, typhoon_errors = _parse_typhoon_adapter(raw, fetched_at)
    disaster_events.extend(typhoon_events)

    return {
        "weather_alert_records": weather_alerts,
        "disaster_event_records": disaster_events,
        "weather_errors": _collect_errors(
            raw,
            ["wmo_all"],
        ),
        "disaster_errors": _collect_errors(
            raw,
            [
                "usgs_earthquakes",
                "mirova_nrt",
                "gdacs_events_api",
                "tsunami_home",
                "jtwc_abio",
                "jtwc_abpw",
            ],
        ) + list(typhoon_errors),
    }


# -----------------------------------------------------------------------------
# weather alerts (WMO extreme only)
# -----------------------------------------------------------------------------

def _wmo_severity_label(code: Any) -> str:
    try:
        s = int(code)
    except Exception:
        return "unknown"
    return {
        1: "minor",
        2: "moderate",
        3: "severe",
        4: "extreme",
    }.get(s, "unknown")



def _parse_wmo_items(raw: dict, fetched_at: str, *, window_hours: int = 24) -> list[dict]:
    block = raw.get("wmo_all", {})
    data = _safe_json(block)
    if not isinstance(data, dict):
        return []

    items = data.get("items", []) or []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    out: list[dict] = []

    for item in items:
        if not isinstance(item, dict):
            continue

        severity_label = _wmo_severity_label(item.get("s"))
        if severity_label != "extreme":
            continue

        sent_dt = _parse_iso_datetime(item.get("sent"))
        effective_dt = _parse_iso_datetime(item.get("effective"))
        expires_dt = _parse_iso_datetime(item.get("expires"))
        ref_dt = sent_dt or effective_dt or expires_dt
        if ref_dt is None or ref_dt < cutoff:
            continue

        title = _compact_text(item.get("headline") or item.get("event"))
        area = _compact_text(item.get("areaDesc"))
        if not title:
            continue

        item_id = _norm_text(item.get("id") or item.get("identifier"))
        sent_iso = sent_dt.isoformat() if sent_dt else fetched_at
        effective_iso = effective_dt.isoformat() if effective_dt else sent_iso
        expires_iso = expires_dt.isoformat() if expires_dt else None
        cap_path = _norm_text(item.get("capURL") or item.get("url"))

        payload = dict(item)
        payload["cap_path"] = cap_path
        payload["source_page"] = WMO_LIST_URL
        payload["last_updated"] = data.get("lastUpdated")

        out.append(
            {
                "source_primary": "WMO",
                "title": title,
                "summary": area,
                "severity_level": "extreme",
                "color_level": "extreme",
                "event_type": "weather",
                "location_text": area,
                "issued_at": sent_iso,
                "effective_at": effective_iso,
                "expires_at": expires_iso,
                "source_url": WMO_LIST_URL,
                "detail_url": WMO_LIST_URL,
                "dedupe_key": item_id or _sha1(f"WMO|{title}|{sent_iso}|{area}"),
                "payload": payload,
                # compatibility aliases
                "source_name": "WMO",
                "alert_level": "extreme",
                "published_at": sent_iso,
                "updated_at": expires_iso or effective_iso,
                "region_text": area,
                "link_url": WMO_LIST_URL,
            }
        )

    out.sort(key=lambda rec: rec.get("issued_at") or "", reverse=True)
    return out



def _parse_weather_impl(raw: dict) -> dict:
    parsed = parse_natural_hazard_payloads(raw)
    return {
        "records": list(parsed.get("weather_alert_records", [])),
        "errors": list(parsed.get("weather_errors", [])),
    }



def parse_weather_alerts_payloads(raw: dict) -> dict:
    return _parse_weather_impl(raw)



# -----------------------------------------------------------------------------
# earthquakes (USGS only)
# -----------------------------------------------------------------------------

def _quake_severity(mag: float | None) -> tuple[str | None, str | None]:
    if mag is None:
        return None, None
    if mag >= 7.0:
        return "critical", "critical"
    if mag >= 6.0:
        return "high", "high"
    if mag >= 5.0:
        return "moderate", "moderate"
    if mag >= 4.0:
        return "elevated", "elevated"
    return "low", "low"



def _parse_usgs_quakes(raw: dict, fetched_at: str, *, min_mag: float = 2.5) -> list[dict]:
    data = _safe_json(raw.get("usgs_earthquakes", {}))
    if not isinstance(data, dict):
        return []

    features = data.get("features", []) or []
    out: list[dict] = []

    for feat in features:
        if not isinstance(feat, dict):
            continue

        props = feat.get("properties", {}) or {}
        geom = feat.get("geometry", {}) or {}
        coords = geom.get("coordinates", []) or []

        mag = _to_float(props.get("mag"))
        if mag is None or mag < min_mag:
            continue

        lon = _to_float(coords[0] if len(coords) > 0 else None)
        lat = _to_float(coords[1] if len(coords) > 1 else None)
        depth = _to_float(coords[2] if len(coords) > 2 else None)
        severity, color = _quake_severity(mag)
        place = _compact_text(props.get("place") or "Unknown region")
        occurred_at = _timestamp_to_iso(props.get("time"), fallback=fetched_at)
        updated_at = _timestamp_to_iso(props.get("updated"), fallback=occurred_at)
        external_id = _norm_text(feat.get("id") or props.get("code"))
        title = _compact_text(props.get("title") or f"M {mag:.1f} - {place}")
        detail_url = _norm_text(props.get("url") or USGS_MAP_URL)

        out.append(
            {
                "event_family": "instant",
                "event_type": "earthquake",
                "severity_level": severity,
                "severity_color": color,
                "title": title,
                "summary": place,
                "occurred_at": occurred_at,
                "updated_at": updated_at,
                "location_text": place,
                "lat": lat,
                "lon": lon,
                "source_primary": "USGS",
                "source_secondary": None,
                "external_id": external_id,
                "external_id_secondary": None,
                "dedupe_key": external_id or _sha1(f"EQ|USGS|{title}|{occurred_at}"),
                "status": _norm_text(props.get("status") or "active"),
                "map_url": detail_url or _build_map_url(lat, lon) or USGS_MAP_URL,
                "payload": {
                    "mag": mag,
                    "depth_km": depth,
                    "place": place,
                    "detail": _norm_text(props.get("detail")),
                    "alert": props.get("alert"),
                    "sig": props.get("sig"),
                    "tsunami": props.get("tsunami"),
                    "source_page": USGS_MAP_URL,
                },
            }
        )

    out.sort(key=lambda rec: rec.get("occurred_at") or "", reverse=True)
    return out


# -----------------------------------------------------------------------------
# volcano (MIROVA, merged by volcano+time)
# -----------------------------------------------------------------------------

def _mirova_infer_level(vrp_mw: float | None) -> str | None:
    if vrp_mw is None:
        return None
    for threshold, label in MIROVA_THRESHOLDS:
        if vrp_mw > threshold:
            return label
    return None



def _mirova_clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", _norm_text(value))



def _parse_mirova_datetime(value: Any, fetched_at: str) -> str:
    dt = _parse_iso_datetime(value)
    return dt.isoformat() if dt is not None else fetched_at



def _parse_mirova_table(soup: Any) -> Any:
    for table in soup.find_all("table"):
        headers = [_mirova_clean_text(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
        header_blob = " | ".join(headers)
        if "volcano" in header_blob and "vrp" in header_blob:
            return table
    return soup.find("table")



def _merge_mirova_records(records: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str], list[dict]] = {}
    for record in records:
        group_key = (
            _norm_text(record.get("external_id") or record.get("title")).lower(),
            _norm_text(record.get("occurred_at")),
        )
        grouped.setdefault(group_key, []).append(record)

    merged: list[dict] = []
    for group_records in grouped.values():
        primary = max(
            group_records,
            key=lambda rec: (
                _to_float((rec.get("payload") or {}).get("vrp_mw")) or -1.0,
                -_severity_rank(rec.get("severity_level"), MIROVA_LEVEL_ORDER),
            ),
        )
        payload_rows = [rec.get("payload") or {} for rec in group_records]
        sensors = []
        for rec in group_records:
            sensor = _norm_text(rec.get("source_secondary") or (rec.get("payload") or {}).get("sensor"))
            if sensor and sensor not in sensors:
                sensors.append(sensor)
        urls = []
        for rec in group_records:
            url = _norm_text(rec.get("map_url") or (rec.get("payload") or {}).get("volcano_url"))
            if url and url not in urls:
                urls.append(url)
        row_classes = []
        for rec in group_records:
            for cls in (rec.get("payload") or {}).get("row_class") or []:
                cls_text = _norm_text(cls)
                if cls_text and cls_text not in row_classes:
                    row_classes.append(cls_text)

        best_vrp = max((_to_float((rec.get("payload") or {}).get("vrp_mw")) or -1.0) for rec in group_records)
        best_distance = min(
            [d for d in ((_to_float((rec.get("payload") or {}).get("distance_km"))) for rec in group_records) if d is not None],
            default=None,
        )
        best_level = min(
            (_severity_rank(rec.get("severity_level"), MIROVA_LEVEL_ORDER) for rec in group_records),
            default=len(MIROVA_LEVEL_ORDER) - 1,
        )
        severity_level = MIROVA_LEVEL_ORDER[min(best_level, len(MIROVA_LEVEL_ORDER) - 1)]
        if severity_level == "unknown" and best_vrp >= 0:
            severity_level = _mirova_infer_level(best_vrp) or "unknown"

        summary_parts = []
        if best_vrp >= 0:
            summary_parts.append(f"VRP {best_vrp:.2f} MW")
        if sensors:
            summary_parts.append(" / ".join(sensors))
        if best_distance is not None:
            summary_parts.append(f"{best_distance:.2f} km")

        payload = dict(primary.get("payload") or {})
        payload["source_page"] = MIROVA_NRT_URL
        payload["sensors"] = sensors
        payload["row_class"] = row_classes
        payload["reports"] = payload_rows
        payload["merged_count"] = len(group_records)

        merged.append(
            {
                **primary,
                "severity_level": severity_level,
                "severity_color": severity_level,
                "summary": " · ".join(summary_parts),
                "source_secondary": " / ".join(sensors) if sensors else primary.get("source_secondary"),
                "external_id_secondary": " / ".join(sensors) if sensors else primary.get("external_id_secondary"),
                "map_url": urls[0] if urls else primary.get("map_url") or MIROVA_NRT_URL,
                "dedupe_key": _sha1(
                    f"VO|MIROVA|{primary.get('external_id')}|{primary.get('title')}|{primary.get('occurred_at')}"
                ),
                "payload": payload,
            }
        )

    merged.sort(key=lambda rec: rec.get("occurred_at") or "", reverse=True)
    return merged



def _parse_mirova_volcano(raw: dict, fetched_at: str, *, allowed_levels: set[str] | None = None) -> list[dict]:
    page = raw.get("mirova_nrt", {})
    html = _safe_text(page)
    if not html or BeautifulSoup is None:
        return []

    soup = BeautifulSoup(html, "html.parser")
    table = _parse_mirova_table(soup)
    if table is None:
        return []

    headers = [_mirova_clean_text(th.get_text(" ", strip=True)) for th in table.find_all("th")]
    base_records: list[dict] = []

    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue

        values = [_mirova_clean_text(td.get_text(" ", strip=True)) for td in cells]
        if len(values) < 6:
            continue

        row: dict[str, Any] = {}
        if headers and len(headers) == len(values):
            row.update(dict(zip(headers, values)))
        else:
            row.update(
                {
                    "Time (UTC)": values[0],
                    "ID Volc": values[1],
                    "Volcano": values[2],
                    "VRP MW": values[3],
                    "Distance km": values[4],
                    "Sensor": values[5],
                }
            )

        volcano_url = ""
        for link in tr.find_all("a"):
            href = _norm_text(link.get("href"))
            if href:
                volcano_url = urljoin(page.get("final_url") or MIROVA_NRT_URL, href)
                break

        vrp_mw = _to_float(row.get("VRP MW"))
        distance_km = _to_float(row.get("Distance km"))
        level = None
        classes = [str(x).strip().lower() for x in (tr.get("class") or []) if str(x).strip()]
        for cls in classes:
            normalized = cls.replace("-", "_")
            if normalized in MIROVA_LEVEL_ORDER:
                level = normalized
                break
        if level is None:
            level = _mirova_infer_level(vrp_mw) or "unknown"

        event_time_iso = _parse_mirova_datetime(f"{row.get('Time (UTC)', '')} UTC", fetched_at)
        volcano = _humanize_compact_name(_compact_text(row.get("Volcano")))
        sensor = _compact_text(row.get("Sensor"))
        volcano_id = _norm_text(row.get("ID Volc"))
        title = volcano or f"Volcano {volcano_id}"

        summary_parts = []
        if vrp_mw is not None:
            summary_parts.append(f"VRP {vrp_mw:.2f} MW")
        if sensor:
            summary_parts.append(sensor)
        if distance_km is not None:
            summary_parts.append(f"{distance_km:.2f} km")
        summary = " · ".join(summary_parts)

        base_records.append(
            {
                "event_family": "instant",
                "event_type": "volcano",
                "severity_level": level,
                "severity_color": level,
                "title": title,
                "summary": summary,
                "occurred_at": event_time_iso,
                "updated_at": event_time_iso,
                "location_text": volcano,
                "lat": None,
                "lon": None,
                "source_primary": "MIROVA",
                "source_secondary": sensor or None,
                "external_id": volcano_id,
                "external_id_secondary": sensor or None,
                "dedupe_key": _sha1(f"VO|MIROVA|{volcano_id}|{sensor}|{event_time_iso}|{row.get('VRP MW', '')}"),
                "status": "active",
                "map_url": volcano_url or MIROVA_NRT_URL,
                "payload": {
                    "volcano_id": volcano_id,
                    "volcano_url": volcano_url,
                    "vrp_mw": vrp_mw,
                    "distance_km": distance_km,
                    "sensor": sensor,
                    "row_class": [cls.replace("-", "_") for cls in classes],
                    "source_page": MIROVA_NRT_URL,
                },
            }
        )

    merged = _merge_mirova_records(base_records)
    if allowed_levels:
        merged = [rec for rec in merged if _norm_text(rec.get("severity_level")).lower() in allowed_levels]
    return merged


# -----------------------------------------------------------------------------
# flood (GDACS API, current/near-current only)
# -----------------------------------------------------------------------------

def _gdacs_is_flood(node: dict[str, Any]) -> bool:
    flat = _flatten_event_dict(node)
    candidates = [
        _norm_text(_pick_first(flat, ["eventtype", "eventType", "event_type"])),
        _norm_text(_pick_first(flat, ["type", "hazard", "hazardType"])),
        _norm_text(_pick_first(flat, ["category", "categoryCode"])),
        _norm_text(_pick_first(flat, ["eventname", "title", "name"])),
    ]
    for value in candidates:
        upper = value.upper()
        lower = value.lower()
        if upper == "FL" or ":FL" in upper:
            return True
        if "flood" in lower:
            return True
    return False



def _gdacs_alert_level(value: Any) -> str:
    text = _norm_text(value).strip().lower()
    if not text:
        return "unknown"
    if "red" in text:
        return "red"
    if "orange" in text:
        return "orange"
    if "green" in text:
        return "green"
    if "yellow" in text:
        return "yellow"
    return text



def _build_gdacs_report_url(event_id: str, episode_id: str) -> str:
    if event_id and episode_id:
        return f"https://www.gdacs.org/report.aspx?eventid={event_id}&episodeid={episode_id}&eventtype=FL"
    return GDACS_ALERTS_URL



def _extract_gdacs_url_map(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        return {str(k): _norm_text(v) for k, v in value.items() if _norm_text(v)}
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text.replace("'", '"'))
                if isinstance(parsed, dict):
                    return {str(k): _norm_text(v) for k, v in parsed.items() if _norm_text(v)}
            except Exception:
                pass
        if text:
            return {"report": text}
    return {}



def _extract_gdacs_point(node: Any) -> tuple[float | None, float | None]:
    if isinstance(node, dict):
        geometry = node.get("geometry")
        if isinstance(geometry, dict):
            coords = geometry.get("coordinates")
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                lon = _to_float(coords[0])
                lat = _to_float(coords[1])
                if lat is not None and lon is not None:
                    return lat, lon
        props = node.get("properties")
        if isinstance(props, dict):
            lat = _to_float(props.get("lat") or props.get("latitude") or props.get("centerlat"))
            lon = _to_float(props.get("lon") or props.get("lng") or props.get("longitude") or props.get("centerlon"))
            if lat is not None and lon is not None:
                return lat, lon
    return None, None



def _parse_gdacs_flood(raw: dict, fetched_at: str, *, active_within_days: int = 14) -> list[dict]:
    data = _safe_json(raw.get("gdacs_events_api", {}))
    if data is None:
        return []

    out: list[dict] = []
    seen_keys: set[str] = set()
    now_utc = datetime.now(timezone.utc)
    active_cutoff = now_utc - timedelta(days=active_within_days)

    for node in _iter_candidate_dicts(data):
        if not isinstance(node, dict):
            continue
        if not _gdacs_is_flood(node):
            continue

        flat = _flatten_event_dict(node)
        event_id = _norm_text(_pick_first(flat, ["eventid", "eventId"]))
        episode_id = _norm_text(_pick_first(flat, ["episodeid", "episodeId"]))
        alert_level = _gdacs_alert_level(_pick_first(flat, ["alertlevel", "alertLevel", "level", "severity"]))
        if alert_level not in {"red", "orange"}:
            continue

        occurred_at = _timestamp_to_iso(
            _pick_first(flat, ["fromdate", "fromDate", "eventdate", "date", "insertdate", "createdAt"]),
            fallback=fetched_at,
        )
        end_at = _timestamp_to_iso(
            _pick_first(flat, ["todate", "toDate", "enddate", "endDate", "expires", "until"]),
            fallback=occurred_at,
        )
        updated_at = _timestamp_to_iso(
            _pick_first(flat, ["datemodified", "lastupdate", "updatedAt", "modified", "todate", "toDate"]),
            fallback=end_at,
        )
        occurred_dt = _parse_iso_datetime(occurred_at)
        end_dt = _parse_iso_datetime(end_at)
        updated_dt = _parse_iso_datetime(updated_at)
        is_current = _to_bool(_pick_first(flat, ["iscurrent", "isCurrent"]))

        active_like = any(dt is not None and dt >= active_cutoff for dt in (updated_dt, end_dt))
        if not active_like:
            continue

        url_map = _extract_gdacs_url_map(_pick_first(flat, ["url", "reporturl", "reportURL", "detailurl", "detailURL", "link"]))
        report_url = url_map.get("report") or _build_gdacs_report_url(event_id, episode_id)
        geometry_url = url_map.get("geometry")
        details_url = url_map.get("details")

        title = _clean_html_text(
            _pick_first(flat, ["name", "eventname", "title", "description", "htmldescription"])
        )
        country = _clean_html_text(
            _pick_first(flat, ["country", "countryname", "countryName", "location"])
        )
        if not country:
            countries = []
            affected = _pick_first(flat, ["affectedcountries"])
            if isinstance(affected, list):
                for entry in affected:
                    if isinstance(entry, dict):
                        name = _clean_html_text(entry.get("countryname") or entry.get("countryName"))
                        if name and name not in countries:
                            countries.append(name)
            country = ", ".join(countries)

        if not title:
            title = f"Flood in {country}" if country else "Flood"
        location = country or title

        lat, lon = _extract_gdacs_point(node)
        if lat is None or lon is None:
            lat = _to_float(_pick_first(flat, ["lat", "latitude", "centerlat", "y"]))
            lon = _to_float(_pick_first(flat, ["lon", "lng", "longitude", "centerlon", "x"]))

        dedupe_key = _sha1(f"FL|GDACS|{event_id}|{episode_id}|{title}")
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        payload = {
            "eventid": event_id,
            "episodeid": episode_id,
            "alertlevel": alert_level,
            "report_url": report_url,
            "geometry_url": geometry_url,
            "details_url": details_url,
            "source_page": GDACS_ALERTS_URL,
            "active_within_days": active_within_days,
            "raw": node,
        }

        out.append(
            {
                "event_family": "instant",
                "event_type": "flood",
                "severity_level": alert_level,
                "severity_color": alert_level,
                "title": title,
                "summary": country,
                "occurred_at": occurred_at,
                "updated_at": updated_at,
                "location_text": location,
                "lat": lat,
                "lon": lon,
                "source_primary": "GDACS",
                "source_secondary": None,
                "external_id": event_id or title,
                "external_id_secondary": episode_id or None,
                "dedupe_key": dedupe_key,
                "status": "active",
                "map_url": report_url or GDACS_ALERTS_URL,
                "payload": payload,
            }
        )

    severity_order = ["red", "orange", "yellow", "green", "unknown"]
    out.sort(
        key=lambda rec: (
            _severity_rank(rec.get("severity_level"), severity_order),
            -int((_parse_iso_datetime(rec.get("updated_at")) or _parse_iso_datetime(rec.get("occurred_at")) or datetime(1970, 1, 1, tzinfo=timezone.utc)).timestamp()),
        )
    )
    return out


# -----------------------------------------------------------------------------
# tsunami.gov homepage warning block
# -----------------------------------------------------------------------------

def _extract_tsunami_home_block(text: str) -> str:
    normalized = text.replace("\r", "")
    m = re.search(r"Information\s*(.*?)\s*Alerts/Threats", normalized, re.S | re.I)
    if m:
        return _compact_text(m.group(1))
    return _compact_text(normalized)


def _extract_tsunami_home_lines(text: str) -> list[str]:
    normalized = text.replace("\r", "")
    m = re.search(r"Information\s*(.*?)\s*Alerts/Threats", normalized, re.S | re.I)
    block = m.group(1) if m else normalized
    lines = []
    for raw_line in block.splitlines():
        line = _compact_text(raw_line)
        if line:
            lines.append(line)
    return lines


def _pick_tsunami_level(text: str) -> tuple[str | None, str | None, str | None]:
    lowered = text.lower()
    if "no tsunami warning, advisory, watch, or threat" in lowered:
        return None, None, None
    if "tsunami warning" in lowered or re.search(r"\bwarning\b", lowered):
        return "warning", "critical", "critical"
    if "tsunami advisory" in lowered or re.search(r"\badvisory\b", lowered):
        return "advisory", "high", "high"
    if "tsunami watch" in lowered or re.search(r"\bwatch\b", lowered):
        return "watch", "moderate", "moderate"
    if "tsunami threat" in lowered or re.search(r"\bthreat\b", lowered):
        return "threat", "elevated", "elevated"
    return None, None, None


def _parse_tsunami_homepage(block: dict, fetched_at: str) -> list[dict]:
    html = _safe_text(block)
    if not html:
        return []

    bulletin_url = None
    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        page_text = soup.get_text("\n", strip=True)
        for a in soup.find_all("a"):
            label = _compact_text(a.get_text(" ", strip=True)).lower()
            href = _norm_text(a.get("href"))
            if label == "view bulletin" and href:
                bulletin_url = urljoin(block.get("final_url") or "https://www.tsunami.gov/", href)
                break
    else:
        page_text = _clean_html_text(html)

    compact_block = _extract_tsunami_home_block(page_text)
    lines = _extract_tsunami_home_lines(page_text)
    if not compact_block:
        return []

    message_level, severity_level, severity_color = _pick_tsunami_level(compact_block)
    if message_level is None:
        return []

    headline = None
    description = None
    for line in lines:
        lowered = line.lower()
        if any(token in lowered for token in ["tsunami warning", "tsunami advisory", "tsunami watch", "tsunami threat"]):
            headline = line
            continue
        if headline and description is None and line != "Earthquake:" and not line.startswith("Magnitude:") and not line.startswith("Depth:") and not line.startswith("Location:"):
            description = line
            break

    if not headline:
        headline = f"Tsunami {message_level.title()}"

    quake_mag = None
    mag_match = re.search(r"Magnitude:\s*([0-9.]+)", page_text, re.I)
    if mag_match:
        quake_mag = _to_float(mag_match.group(1))
    quake_depth = None
    depth_match = re.search(r"Depth:\s*([0-9.]+)\s*(?:mi\.|miles|kilometers|km)", page_text, re.I)
    if depth_match:
        quake_depth = _to_float(depth_match.group(1))
    origin_match = re.search(r"Origin Time:\s*([0-9T:\-]+Z)", page_text, re.I)
    origin_iso = _timestamp_to_iso(origin_match.group(1), fallback=fetched_at) if origin_match else fetched_at

    issue_iso = None
    for pat in (
        r"(?:Issued|Issue Time|Bulletin Issue Time|Message Time)\s*:\s*([0-9T:\-]+Z)",
        r"(?:Issued|Issue Time|Bulletin Issue Time|Message Time)\s*:\s*([0-9]{1,2}\s+[A-Za-z]{3}\s+[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?\s*UTC)",
        r"(?:Issued|Issue Time|Bulletin Issue Time|Message Time)\s*:\s*([A-Za-z]{3},\s*[0-9]{1,2}\s+[A-Za-z]{3}\s+[0-9]{4}\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\s*(?:GMT|UTC))",
    ):
        m = re.search(pat, page_text, re.I)
        if not m:
            continue
        issue_iso = _timestamp_to_iso(m.group(1), fallback="")
        if issue_iso:
            break
    stable_updated_at = issue_iso or origin_iso

    lat = lon = None
    latlon_match = re.search(r"Lat:\s*([0-9.]+)\s*°?\s*([NS])\s*Lon:\s*([0-9.]+)\s*°?\s*([EW])", page_text, re.I)
    if latlon_match:
        lat = _to_float(latlon_match.group(1))
        if lat is not None and latlon_match.group(2).upper() == "S":
            lat = -lat
        lon = _to_float(latlon_match.group(3))
        if lon is not None and latlon_match.group(4).upper() == "W":
            lon = -lon

    location_match = re.search(r"Location:\s*(.+?)(?:Note:|view bulletin|Alerts/Threats|$)", page_text, re.I | re.S)
    quake_location = _compact_text(location_match.group(1)) if location_match else "Tsunami.gov"

    payload = {
        "message_level": message_level,
        "headline": headline,
        "description": description or headline,
        "source_page": "https://www.tsunami.gov/",
        "bulletin_url": bulletin_url,
        "related_earthquake": {
            "mag": quake_mag,
            "origin_time": origin_iso,
            "depth": quake_depth,
            "lat": lat,
            "lon": lon,
            "location": quake_location,
        },
        "raw_block": compact_block,
    }

    return [{
        "event_family": "instant",
        "event_type": "tsunami",
        "severity_level": severity_level,
        "severity_color": severity_color,
        "title": headline,
        "summary": description or quake_location,
        "occurred_at": origin_iso,
        "updated_at": stable_updated_at,
        "location_text": quake_location,
        "lat": lat,
        "lon": lon,
        "source_primary": "Tsunami.gov",
        "source_secondary": None,
        "external_id": _sha1(f"TSU|{message_level}|{headline}|{origin_iso}"),
        "external_id_secondary": None,
        "dedupe_key": _sha1(f"TSU|HOME|{message_level}|{headline}|{origin_iso}"),
        "status": "active",
        "map_url": bulletin_url or "https://www.tsunami.gov/",
        "payload": payload,
    }]


# -----------------------------------------------------------------------------
# typhoon (left intentionally unchanged for later dedicated rewrite)
# -----------------------------------------------------------------------------

_INVEST_RE = re.compile(r"\b(INVEST\s+\d{2}[A-Z])\b", re.I)
_AREA_RE = re.compile(r"AREA OF CONVECTION\s+\(([^)]+)\)", re.I)
_JTWC_WINDS_RE = re.compile(r"MAXIMUM SUSTAINED SURFACE WINDS ARE ESTIMATED AT\s+(\d{1,3})\s+TO\s+(\d{1,3})\s+KNOTS", re.I)
_NHC_SYS_RE = re.compile(r"(Tropical\s+(?:Storm|Depression|Cyclone|Disturbance|Weather Outlook).{0,120})", re.I)



# -----------------------------------------------------------------------------
# compatibility entrypoint used by existing disaster_ingest
# -----------------------------------------------------------------------------

def parse_disaster_payloads(raw: dict) -> dict:
    parsed = parse_natural_hazard_payloads(raw)
    return {
        "records": list(parsed.get("disaster_event_records", [])),
        "errors": list(parsed.get("disaster_errors", [])),
    }
