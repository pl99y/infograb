from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from .tc_parsers import ParsedItem, parse_bulletin, to_platform_rows

GOOGLE_MAP_BASE = "https://www.google.com/maps?q={lat},{lon}"


LIVE_ALIAS_BY_URL = {
    "MIATWOAT": "nhc_at",
    "MIATWOEP": "nhc_ep",
    "HFOTWOCP": "cphc_cp",
}


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _map_url(lat: Any, lon: Any) -> str | None:
    lat_f = _to_float(lat)
    lon_f = _to_float(lon)
    if lat_f is None or lon_f is None:
        return None
    return GOOGLE_MAP_BASE.format(lat=lat_f, lon=lon_f)


MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


HEADER_TIME_RE = re.compile(r"(?P<day>\d{2})(?P<hour>\d{2})(?P<minute>\d{2})Z(?P<mon>[A-Z]{3})(?P<year>\d{4})")
NHC_TIME_RE = re.compile(r"(?P<day>\d{2})/(?P<hour>\d{4})Z")
JTWC_WMO_HEADER_RE = re.compile(r"^[A-Z]{4}\d{2}\s+[A-Z]{4}\s+(?P<day>\d{2})(?P<hour>\d{2})(?P<minute>\d{2})\b", re.M)
JTWC_RANGE_RE = re.compile(r"/(?P<start>\d{6}Z[A-Z]{3}\d{4})-(?P<end>\d{6}Z[A-Z]{3}\d{4})//")


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _norm(value)
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
        return None


def _parse_full_zulu_stamp(value: str) -> datetime | None:
    m = HEADER_TIME_RE.fullmatch(_norm(value).upper())
    if not m:
        return None
    try:
        return datetime(
            int(m.group("year")),
            MONTHS[m.group("mon")],
            int(m.group("day")),
            int(m.group("hour")),
            int(m.group("minute")),
            tzinfo=timezone.utc,
        )
    except Exception:
        return None


def _closest_nonfuture(candidates: list[datetime], fetched_dt: datetime | None) -> datetime | None:
    if not candidates:
        return None
    unique = []
    seen = set()
    for dt in candidates:
        if not isinstance(dt, datetime):
            continue
        key = dt.isoformat()
        if key in seen:
            continue
        seen.add(key)
        unique.append(dt)
    if not unique:
        return None
    if fetched_dt is None:
        return max(unique)
    allowed_future_seconds = 0
    not_too_future = [dt for dt in unique if (dt - fetched_dt).total_seconds() <= allowed_future_seconds]
    past_or_near = [dt for dt in not_too_future if dt <= fetched_dt + timedelta(seconds=allowed_future_seconds)]
    if past_or_near:
        return max(past_or_near)
    return min(unique, key=lambda dt: abs((dt - fetched_dt).total_seconds()))


def _infer_jtwc_header_datetime(text: str, fetched_at: str) -> datetime | None:
    m = JTWC_WMO_HEADER_RE.search(text or "")
    if not m:
        return None
    fetched_dt = _parse_iso_datetime(fetched_at)
    if fetched_dt is None:
        return None

    day = int(m.group("day"))
    hour = int(m.group("hour"))
    minute = int(m.group("minute"))

    candidates: list[datetime] = []
    year = fetched_dt.year
    month = fetched_dt.month
    for offset in (-1, 0, 1):
        test_month = month + offset
        test_year = year
        while test_month < 1:
            test_month += 12
            test_year -= 1
        while test_month > 12:
            test_month -= 12
            test_year += 1
        try:
            candidates.append(datetime(test_year, test_month, day, hour, minute, tzinfo=timezone.utc))
        except Exception:
            pass

    return _closest_nonfuture(candidates, fetched_dt)


def _extract_issue_time(text: str, fetched_at: str) -> str:
    fetched_dt = _parse_iso_datetime(fetched_at)
    candidates: list[datetime] = []

    jtwc_header_dt = _infer_jtwc_header_datetime(text or "", fetched_at)
    if jtwc_header_dt is not None:
        candidates.append(jtwc_header_dt)

    range_match = JTWC_RANGE_RE.search(text or "")
    if range_match:
        start_dt = _parse_full_zulu_stamp(range_match.group("start"))
        if start_dt is not None:
            candidates.append(start_dt)

    for m in HEADER_TIME_RE.finditer((text or "").upper()):
        try:
            candidates.append(
                datetime(
                    int(m.group("year")),
                    MONTHS[m.group("mon")],
                    int(m.group("day")),
                    int(m.group("hour")),
                    int(m.group("minute")),
                    tzinfo=timezone.utc,
                )
            )
        except Exception:
            pass

    for pat in (
        r"(?P<hour>\d{1,2})(?P<minute>\d{2})\s+UTC\s+[A-Z]{3}\s+(?P<mon>[A-Z]{3})\s+(?P<day>\d{1,2})\s+(?P<year>\d{4})",
        r"(?P<hour>\d{1,2})(?P<minute>\d{2})\s+UTC\s+(?P<day>\d{1,2})\s+(?P<mon>[A-Z]{3})\s+(?P<year>\d{4})",
    ):
        for m2 in re.finditer(pat, (text or "").upper()):
            try:
                candidates.append(
                    datetime(
                        int(m2.group("year")),
                        MONTHS[m2.group("mon")],
                        int(m2.group("day")),
                        int(m2.group("hour")),
                        int(m2.group("minute")),
                        tzinfo=timezone.utc,
                    )
                )
            except Exception:
                pass

    chosen = _closest_nonfuture(candidates, fetched_dt)
    if chosen is not None:
        return chosen.isoformat()
    return fetched_at


SEVERITY_ORDER = {
    "critical": 0,
    "extreme": 1,
    "high": 2,
    "moderate": 3,
    "elevated": 4,
    "low": 5,
    "info": 6,
}


def _severity_from_disturbance(meta: dict[str, Any]) -> tuple[str, str]:
    system_class = _norm(meta.get("system_class") or meta.get("storm_class")).lower()
    if "subtropical cyclone" in system_class:
        wind = meta.get("wind_kt") or meta.get("wind_kt_max")
        try:
            wind_i = int(wind) if wind is not None else None
        except Exception:
            wind_i = None
        if wind_i is not None:
            if wind_i >= 64:
                return "extreme", "extreme"
            if wind_i >= 34:
                return "high", "high"
            return "moderate", "moderate"
        return "moderate", "moderate"

    level = _norm(meta.get("development_level")).upper()
    if not level:
        level = _norm(meta.get("chance_48h_level") or meta.get("chance_7d_level")).upper()
    if level == "HIGH":
        return "high", "high"
    if level == "MEDIUM":
        return "elevated", "elevated"
    return "low", "low"


def _severity_from_warning(meta: dict[str, Any]) -> tuple[str, str]:
    storm_class = _norm(meta.get("storm_class")).lower()
    wind = meta.get("wind_kt")
    try:
        wind_i = int(wind) if wind is not None else None
    except Exception:
        wind_i = None

    if any(x in storm_class for x in ["typhoon", "hurricane", "super typhoon"]):
        return "extreme", "extreme"
    if any(x in storm_class for x in ["severe tropical storm", "tropical storm", "cyclone"]):
        return "high", "high"
    if any(x in storm_class for x in ["tropical depression"]):
        return "moderate", "moderate"
    if wind_i is not None:
        if wind_i >= 64:
            return "extreme", "extreme"
        if wind_i >= 34:
            return "high", "high"
        return "moderate", "moderate"
    return "moderate", "moderate"


def _severity_for_item(item: ParsedItem) -> tuple[str, str]:
    meta = item.metadata or {}
    if item.item_type == "disturbance":
        return _severity_from_disturbance(meta)
    if item.item_type in {"warning", "forecast_advisory"}:
        return _severity_from_warning(meta)
    if item.item_type == "active_systems":
        return "moderate", "moderate"
    return "moderate", "moderate"


def _title_for_item(item: ParsedItem, row: dict[str, Any]) -> str:
    meta = item.metadata or {}
    if item.item_type == "disturbance":
        if meta.get("invest_id"):
            return f"INVEST {meta['invest_id']}"
        if meta.get("system_id"):
            prefix = _norm(meta.get("system_prefix")) or "SS"
            return f"{prefix.upper()} {meta['system_id']}"
    if item.item_type == "warning":
        storm_class = _norm(meta.get("storm_class")).title()
        storm_id = _norm(meta.get("storm_id"))
        storm_name = _norm(meta.get("storm_name")).title()
        base = " ".join([x for x in [storm_class, storm_id] if x]).strip()
        return f"{base} ({storm_name})" if storm_name else (base or row.get("title") or item.title)
    if item.item_type == "forecast_advisory":
        return _norm(meta.get("storm_title")).title() or row.get("title") or item.title
    if item.item_type == "active_systems":
        started = meta.get("active_started") or []
        if len(started) == 1:
            return str(started[0])
        return row.get("title") or item.title
    return row.get("title") or item.title


def _summary_for_item(item: ParsedItem, row: dict[str, Any]) -> str:
    meta = item.metadata or {}
    if item.item_type == "disturbance":
        parts: list[str] = []
        risk_48 = _norm(row.get("risk_48h"))
        risk_7d = _norm(row.get("risk_7d"))
        if risk_48:
            parts.append(f"48h {risk_48}")
        if risk_7d:
            parts.append(f"7d {risk_7d}")
        wind = _norm(row.get("wind"))
        if wind:
            parts.append(f"wind {wind}")
        pressure = meta.get("pressure_mb")
        if pressure is not None:
            parts.append(f"{pressure} mb")
        dev = _norm(meta.get("development_level"))
        if dev:
            parts.append(dev.title())
        if not parts and row.get("summary"):
            parts.append(str(row["summary"]))
        return " · ".join(parts)

    if item.item_type in {"warning", "forecast_advisory"}:
        parts = []
        wind = _norm(row.get("wind"))
        if wind:
            parts.append(f"wind {wind}")
        pressure = meta.get("pressure_mb")
        if pressure is not None:
            parts.append(f"{pressure} mb")
        wave = meta.get("wave_height_ft")
        if wave is not None:
            parts.append(f"wave {wave} ft")
        nxt = _norm(row.get("next_update"))
        if nxt:
            parts.append(f"next {nxt}")
        move_deg = meta.get("movement_deg")
        move_kt = meta.get("movement_kt")
        if move_deg is not None and move_kt is not None:
            parts.append(f"move {move_deg}° at {move_kt} kt")
        return " · ".join(parts) if parts else _norm(row.get("summary"))

    if item.item_type == "active_systems":
        started = meta.get("active_started") or []
        final = meta.get("active_final") or []
        parts = []
        if started:
            parts.append("active: " + "; ".join(started))
        if final:
            parts.append("final: " + "; ".join(final))
        return " | ".join(parts)

    return _norm(row.get("summary"))


def _location_text(item: ParsedItem, row: dict[str, Any]) -> str | None:
    meta = item.metadata or {}
    location = _norm(row.get("location"))
    if location:
        return location
    basin = _norm(meta.get("basin_section"))
    if basin:
        return basin
    if item.item_type == "active_systems":
        active_started = meta.get("active_started") or []
        if active_started:
            return "; ".join(active_started)
    return None


def _source_secondary(item: ParsedItem, meta: dict[str, Any]) -> str | None:
    if item.item_type == "warning":
        return _norm(meta.get("warning_nr")) or None
    if item.item_type == "forecast_advisory":
        return _norm(meta.get("advisory_number")) or None
    return item.item_type


def _status_for_item(item: ParsedItem, meta: dict[str, Any]) -> str:
    if item.item_type == "disturbance":
        if _norm(meta.get("system_class")).lower() == "subtropical cyclone":
            return "active"
        return (_norm(meta.get("development_level")).lower() or "monitoring")
    if item.item_type in {"warning", "forecast_advisory", "active_systems"}:
        return "active"
    return "active"


def _event_external_id(item: ParsedItem, meta: dict[str, Any]) -> str:
    return (
        _norm(meta.get("storm_id"))
        or _norm(meta.get("system_id"))
        or _norm(meta.get("invest_id"))
        or _norm(meta.get("storm_title"))
        or item.key
    )


def _source_alias_from_url(url: str) -> str | None:
    text = _norm(url).upper()
    for token, alias in LIVE_ALIAS_BY_URL.items():
        if token in text:
            return alias
    return None


def _collect_source_items(raw: dict, fetched_at: str) -> tuple[list[ParsedItem], list[str]]:
    items: list[ParsedItem] = []
    errors: list[str] = []

    for key in ("jtwc_abio", "jtwc_abpw"):
        block = raw.get(key, {}) or {}
        if not block.get("ok"):
            if block.get("error"):
                errors.append(f"{key}: {block.get('error')}")
            continue
        text = _norm(block.get("text"))
        if not text:
            continue
        try:
            parsed = parse_bulletin(text, key)
            issue_time = _extract_issue_time(text, fetched_at)
            if issue_time:
                for item in parsed:
                    meta = dict(item.metadata or {})
                    meta["bulletin_issue_at"] = issue_time
                    item.metadata = meta
            items.extend(parsed)
        except Exception as exc:
            errors.append(f"{key}: {exc}")

    for idx, block in enumerate(raw.get("nhc_twos", []) or []):
        if not isinstance(block, dict):
            continue
        if not block.get("ok"):
            if block.get("error"):
                errors.append(f"nhc_twos[{idx}]: {block.get('error')}")
            continue
        text = _norm(block.get("text"))
        if not text:
            continue
        alias = _source_alias_from_url(_norm(block.get("url"))) or "nhc_at"
        try:
            parsed = parse_bulletin(text, alias)
            issue_time = _extract_issue_time(text, fetched_at)
            if issue_time:
                for item in parsed:
                    meta = dict(item.metadata or {})
                    meta["bulletin_issue_at"] = issue_time
                    item.metadata = meta
            items.extend(parsed)
        except Exception as exc:
            errors.append(f"{alias}: {exc}")

    return items, errors


def parse_typhoon_payloads(raw: dict, fetched_at: str) -> tuple[list[dict], list[str]]:
    parsed_items, errors = _collect_source_items(raw, fetched_at)
    rows = to_platform_rows(parsed_items)

    events: list[dict] = []
    seen: set[str] = set()
    for item, row in zip(parsed_items, rows):
        meta = item.metadata or {}
        severity_level, severity_color = _severity_for_item(item)
        title = _title_for_item(item, row)
        summary = _summary_for_item(item, row)
        location_text = _location_text(item, row)
        lat = meta.get("lat")
        lon = meta.get("lon")
        map_url = _map_url(lat, lon)
        source_primary = item.source.upper().replace("_", "-")
        external_id = _event_external_id(item, meta)
        dedupe_key = f"TC|{item.source}|{item.key}|{row.get('content_hash') or item.content_hash}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        raw_occurred_at = _norm(meta.get("bulletin_issue_at")) or _extract_issue_time(item.text, fetched_at)
        occurred_dt = _parse_iso_datetime(raw_occurred_at)
        fetched_dt = _parse_iso_datetime(fetched_at)
        if occurred_dt is not None and fetched_dt is not None and occurred_dt > fetched_dt:
            occurred_at = fetched_at
        else:
            occurred_at = raw_occurred_at or fetched_at
        updated_at = occurred_at

        event = {
            "event_family": "instant",
            "event_type": "typhoon",
            "severity_level": severity_level,
            "severity_color": severity_color,
            "title": title,
            "summary": summary or title,
            "occurred_at": occurred_at,
            "updated_at": updated_at,
            "location_text": location_text,
            "lat": lat,
            "lon": lon,
            "source_primary": source_primary,
            "source_secondary": _source_secondary(item, meta),
            "external_id": external_id,
            "external_id_secondary": _norm(meta.get("warning_nr") or meta.get("advisory_number") or item.item_type) or None,
            "dedupe_key": dedupe_key,
            "status": _status_for_item(item, meta),
            "map_url": map_url,
            "payload": {
                "item_type": item.item_type,
                "source": item.source,
                "source_key": item.key,
                "content_hash": row.get("content_hash") or item.content_hash,
                "platform_summary": row.get("summary"),
                "platform_location": row.get("location"),
                "wind": row.get("wind"),
                "risk_48h": row.get("risk_48h"),
                "risk_7d": row.get("risk_7d"),
                "next_update": row.get("next_update"),
                "raw_text": item.text,
                "metadata": meta,
            },
        }
        events.append(event)

    events.sort(
        key=lambda e: datetime.fromisoformat((e.get("occurred_at") or fetched_at).replace("Z", "+00:00")).timestamp() if (e.get("occurred_at") or fetched_at) else 0,
        reverse=True,
    )
    return events, errors
