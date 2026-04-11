from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any


class SpaceWeatherParseError(ValueError):
    pass


FORECAST_DAY_RE = re.compile(r"\b([A-Z][a-z]{2}\s+\d{2})(?!\d)\b")
FORECAST_DAY_TRIPLET_RE = re.compile(
    r"\b([A-Z][a-z]{2}\s+\d{2})(?!\d)\s+([A-Z][a-z]{2}\s+\d{2})(?!\d)\s+([A-Z][a-z]{2}\s+\d{2})(?!\d)\b"
)



def _parse_isoish_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None



def _parse_issue_label(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.strptime(text, "%Y %b %d %H%M UTC").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        return text



def _load_json_if_needed(payload: Any) -> Any:
    if isinstance(payload, (list, dict)):
        return payload
    if isinstance(payload, str):
        return json.loads(payload)
    raise SpaceWeatherParseError("Unsupported JSON payload type.")



def _normalize_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")



def _compact_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())



def _find_headline(lines: list[str]) -> str:
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Space Weather Message Code:"):
            continue
        if stripped.startswith("Serial Number:"):
            continue
        if stripped.startswith("Issue Time:"):
            continue
        return stripped
    return ""



def _message_type_from_headline(headline: str) -> str:
    head = (headline or "").upper()
    for prefix in (
        "CONTINUED ALERT",
        "EXTENDED WARNING",
        "CANCEL WARNING",
        "CANCEL WATCH",
        "CANCEL ALERT",
        "CANCEL SUMMARY",
        "ALERT",
        "WARNING",
        "WATCH",
        "SUMMARY",
    ):
        if head.startswith(prefix):
            return prefix.lower().replace(" ", "_")
    return "notice"



def _parse_alert_message(message: str) -> dict[str, Any]:
    lines = [line.rstrip() for line in str(message or "").splitlines()]
    headline = _find_headline(lines)
    message_type = _message_type_from_headline(headline)

    details: dict[str, str] = {}
    impacts_lines: list[str] = []
    description_lines: list[str] = []
    current_section: str | None = None

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if line == headline:
            continue
        if line.startswith("NOAA Space Weather Scale descriptions can be found at"):
            continue
        if line.startswith("www.swpc.noaa.gov/"):
            continue

        if line.startswith("Potential Impacts:"):
            current_section = "impacts"
            impacts_lines.append(line.split(":", 1)[1].strip())
            continue
        if line.startswith("Description:"):
            current_section = "description"
            description_lines.append(line.split(":", 1)[1].strip())
            continue

        if current_section == "impacts":
            impacts_lines.append(line)
            continue
        if current_section == "description":
            description_lines.append(line)
            continue

        if ":" in line:
            key, value = line.split(":", 1)
            details[_normalize_key(key)] = value.strip()

    return {
        "headline": headline,
        "message_type": message_type,
        "message_code": details.get("space_weather_message_code"),
        "serial_number": details.get("serial_number"),
        "issue_time": _parse_issue_label(details.get("issue_time")),
        "noaa_scale": details.get("noaa_scale"),
        "details": details,
        "impacts_text": "\n".join(part for part in impacts_lines if part),
        "description_text": "\n".join(part for part in description_lines if part),
    }



def parse_swpc_alerts_payload(
    payload: Any,
    *,
    max_items: int = 30,
    max_age_days: int = 5,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    rows = _load_json_if_needed(payload)
    if not isinstance(rows, list):
        raise SpaceWeatherParseError("SWPC alerts payload must be a list.")

    now_utc = now or datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=max_age_days)
    parsed_rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for item in rows:
        if not isinstance(item, dict):
            continue
        issue_dt = _parse_isoish_dt(item.get("issue_datetime"))
        if issue_dt is None or issue_dt < cutoff:
            continue

        parsed_message = _parse_alert_message(item.get("message") or "")
        product_id = str(item.get("product_id") or "").strip()
        serial = str(parsed_message.get("serial_number") or "").strip()
        dedupe_key = f"{product_id}|{item.get('issue_datetime') or ''}|{serial}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        parsed_rows.append(
            {
                "dedupe_key": dedupe_key,
                "product_id": product_id,
                "issue_datetime": issue_dt.isoformat(),
                "headline": parsed_message.get("headline") or "",
                "message_type": parsed_message.get("message_type") or "notice",
                "message_code": parsed_message.get("message_code"),
                "serial_number": parsed_message.get("serial_number"),
                "issue_time": parsed_message.get("issue_time"),
                "noaa_scale": parsed_message.get("noaa_scale"),
                "details": parsed_message.get("details") or {},
                "impacts_text": parsed_message.get("impacts_text") or "",
                "description_text": parsed_message.get("description_text") or "",
                "message_raw": str(item.get("message") or ""),
            }
        )

    parsed_rows.sort(key=lambda row: row.get("issue_datetime") or "", reverse=True)
    return parsed_rows[: max(0, int(max_items))]



def _extract_issued_at(text: str) -> str | None:
    match = re.search(r"^:Issued:\s*(.+)$", text, re.M)
    return _parse_issue_label(match.group(1).strip()) if match else None



def _section(text: str, start_heading: str, end_heading: str | None = None) -> str:
    start = text.find(start_heading)
    if start < 0:
        return ""
    start += len(start_heading)
    if end_heading:
        end = text.find(end_heading, start)
        if end < 0:
            end = len(text)
    else:
        end = len(text)
    return text[start:end].strip()



def _single_line_match(section: str, pattern: str) -> str | None:
    match = re.search(pattern, section, re.I | re.M | re.S)
    if not match:
        return None
    return _compact_spaces(match.group(1))


def _extract_day_labels(*texts: str) -> list[str]:
    for text in texts:
        text = str(text or "")
        if not text.strip():
            continue

        for match in FORECAST_DAY_TRIPLET_RE.finditer(text):
            labels = [value.strip() for value in match.groups() if str(value or "").strip()]
            if len(labels) == 3 and len(set(labels)) == 3:
                return labels

        labels = []
        seen: set[str] = set()
        for value in FORECAST_DAY_RE.findall(text):
            label = str(value or "").strip()
            if not label or label in seen:
                continue
            seen.add(label)
            labels.append(label)
            if len(labels) >= 3:
                return labels[:3]

    return []



def _parse_table_row(section: str, label: str, expected_values: int = 3) -> list[str]:
    for raw_line in section.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.lower().startswith(label.lower()):
            parts = re.split(r"\s{2,}|\t+", line)
            if parts and parts[0].strip().lower() == label.lower():
                values = [part.strip() for part in parts[1:] if part.strip()]
                if len(values) >= expected_values:
                    return values[:expected_values]
            remainder = re.sub(rf"^{re.escape(label)}\s*", "", line, flags=re.I).strip()
            values = re.split(r"\s+", remainder)
            return [value.strip() for value in values[:expected_values] if value.strip()]
    return []



def _parse_geomag_probability_rows(section: str) -> dict[str, list[int]]:
    rows: dict[str, list[int]] = {}
    labels = [
        "Active",
        "Minor storm",
        "Moderate storm",
        "Strong-Extreme storm",
    ]
    for label in labels:
        match = re.search(rf"^{re.escape(label)}\s+([0-9/]+)\s*$", section, re.M)
        if not match:
            rows[_normalize_key(label)] = []
            continue
        values = [int(part) for part in match.group(1).split("/") if part.strip().isdigit()]
        rows[_normalize_key(label)] = values
    return rows



def parse_swpc_forecast_payloads(forecast_text: str, geomag_text: str) -> dict[str, Any]:
    if not str(forecast_text or "").strip():
        raise SpaceWeatherParseError("3-day forecast text is empty.")
    if not str(geomag_text or "").strip():
        raise SpaceWeatherParseError("3-day geomagnetic forecast text is empty.")

    forecast_text = str(forecast_text)
    geomag_text = str(geomag_text)

    geomagnetic_section = _section(
        forecast_text,
        "A. NOAA Geomagnetic Activity Observation and Forecast",
        "B. NOAA Solar Radiation Activity Observation and Forecast",
    )
    solar_section = _section(
        forecast_text,
        "B. NOAA Solar Radiation Activity Observation and Forecast",
        "C. NOAA Radio Blackout Activity and Forecast",
    )
    radio_section = _section(
        forecast_text,
        "C. NOAA Radio Blackout Activity and Forecast",
        None,
    )

    day_labels = _extract_day_labels(geomag_text, forecast_text)

    forecast = {
        "forecast_issued_at": _extract_issued_at(forecast_text),
        "geomag_issued_at": _extract_issued_at(geomag_text),
        "geomagnetic": {
            "observed_summary": _single_line_match(
                geomagnetic_section,
                r"(The greatest observed 3 hr Kp over the past 24 hours was.+?\.)",
            ),
            "expected_summary": _single_line_match(
                geomagnetic_section,
                r"(The greatest expected 3 hr Kp for.+?\.)",
            ),
            "rationale": _single_line_match(geomagnetic_section, r"Rationale:\s*(.+)$"),
            "days": day_labels,
            "ap_index": {
                "observed": _single_line_match(geomag_text, r"Observed Ap\s+(.+)$"),
                "estimated": _single_line_match(geomag_text, r"Estimated Ap\s+(.+)$"),
                "predicted": _single_line_match(geomag_text, r"Predicted Ap\s+(.+)$"),
            },
            "probabilities": _parse_geomag_probability_rows(geomag_text),
        },
        "solar_radiation": {
            "observed_summary": _single_line_match(
                solar_section,
                r"(Solar radiation, as observed by NOAA GOES-18 over the past 24 hours, was.+?\.)",
            ),
            "days": day_labels,
            "s1_or_greater": _parse_table_row(solar_section, "S1 or greater"),
            "rationale": _single_line_match(solar_section, r"Rationale:\s*(.+)$"),
        },
        "radio_blackout": {
            "observed_summary": _single_line_match(
                radio_section,
                r"(No radio blackouts were observed over the past 24 hours\.|.+?radio blackouts.+?\.)",
            ),
            "days": day_labels,
            "r1_r2": _parse_table_row(radio_section, "R1-R2"),
            "r3_or_greater": _parse_table_row(radio_section, "R3 or greater"),
            "rationale": _single_line_match(radio_section, r"Rationale:\s*(.+)$"),
        },
        "raw_forecast_text": forecast_text,
        "raw_geomag_text": geomag_text,
    }

    return forecast
