from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from bs4 import BeautifulSoup


class AviationParseError(ValueError):
    pass


@dataclass
class AviationAlert:
    callsign: str
    status_text: str
    alert_type: str
    squawk_code: str
    event_date_text: str | None
    departure_time_text: str | None
    departure_airport: str | None
    arrival_time_text: str | None
    arrival_airport: str | None
    duration_text: str | None
    aircraft_text: str | None
    distance_text: str | None
    age_hours: float | None
    source_name: str
    source_url: str
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class AirportDisruption:
    rank: int
    airport_name: str
    iata: str
    country: str | None
    disruption_index: float
    canceled_flights: int
    canceled_percent: float
    delayed_flights: int
    delayed_percent: float
    average_delay_min: int
    region: str
    period: str
    direction: str
    source_name: str
    source_url: str
    extra: dict[str, Any] = field(default_factory=dict)


FULL_DATE_RE = re.compile(r"\d{4}\s+[A-Za-z]{3}\s+\d{1,2}")
ALERT_RE = re.compile(r"(7500|7600|7700)\s*-\s*[^\n]+", re.I)
DURATION_RE = re.compile(r"\b\d{2}h\d{2}m\b", re.I)
DISTANCE_RE = re.compile(r"\b\d+\s*NM\b", re.I)
TIME_TZ_RE = re.compile(r"\b\d{1,2}:\d{2}(?:\s+[A-Z]{2,5})\b")
TIME_RE = re.compile(r"^\d{1,2}:\d{2}(?:\s+[A-Z]{2,5})?$")
AIRCRAFT_RE = re.compile(r"\b([A-Z0-9]{2,5})\s*\(\s*([A-Z0-9-]{3,})\s*\)", re.I)
AIRPORT_RE = re.compile(r"([A-Za-z0-9'.,\- ]+\((?:\s*[A-Z0-9]{2,4}(?:/[A-Z0-9]{4})?\s*)\))")


def _clean_lines_from_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]

    cleaned: list[str] = []
    for line in lines:
        low = line.lower()
        if line in {"Replay", "Load More", "Image", "LIVE"}:
            continue
        if low.startswith("more flight data is available for purchase"):
            continue
        if low.startswith("on demand api"):
            break
        if low.startswith("have you considered going ad-free"):
            break
        if low.startswith("disable your ad-blocker"):
            break
        if line in {"Last Flight Alerts and Emergencies", "List of last Alerts and Emergencies"}:
            continue
        if line in {"About Us", "Blog", "FAQ", "Store", "Contact Us", "Subscribe", "Log in"}:
            continue
        if low.startswith("copyright"):
            break
        if line == ".":
            continue
        cleaned.append(line)
    return cleaned


def _clean_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def _parse_age_hours(status_text: str) -> float | None:
    text = status_text.upper().strip()
    m = re.search(r"(\d+)\s*M\s*AGO", text)
    if m:
        return int(m.group(1)) / 60.0
    m = re.search(r"(\d+)\s*H\s*(\d+)\s*M\s*AGO", text)
    if m:
        return int(m.group(1)) + int(m.group(2)) / 60.0
    m = re.search(r"(\d+)\s*H\s*AGO", text)
    if m:
        return float(int(m.group(1)))
    m = re.search(r"(\d+)\s*D\s*AGO", text)
    if m:
        return float(int(m.group(1)) * 24)
    return None


def _looks_like_full_date(value: str) -> bool:
    return bool(FULL_DATE_RE.fullmatch(value.strip()))


def _parse_full_date(value: str) -> datetime | None:
    try:
        return datetime.strptime(value.strip(), "%Y %b %d")
    except Exception:
        return None


def _looks_like_alert_type(value: str) -> bool:
    return bool(ALERT_RE.fullmatch(value.strip()))


def _looks_like_status_text(value: str) -> bool:
    text = value.strip().upper()
    return (
        "AGO" in text
        or text == "LANDED"
        or text == "DEPARTED"
        or text.startswith("LANDED ")
        or text.startswith("DEPARTED ")
        or text.startswith("STATUS N/A")
    )


def _looks_like_duration(value: str) -> bool:
    return bool(DURATION_RE.fullmatch(value.strip()))


def _looks_like_distance(value: str) -> bool:
    return bool(DISTANCE_RE.fullmatch(value.strip().upper()))


def _looks_like_callsign(value: str) -> bool:
    value = value.strip().upper()
    if not value or _looks_like_full_date(value) or _looks_like_status_text(value) or _looks_like_alert_type(value):
        return False
    if _looks_like_duration(value) or _looks_like_distance(value):
        return False
    if any(x in value for x in [" ", "(", ")", ",", "/"]):
        return False
    if len(value) > 16:
        return False
    return bool(re.fullmatch(r"[A-Z0-9][A-Z0-9-]{1,15}", value))


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _clean_airport_text(text: str | None) -> str | None:
    text = _normalize_space(text or "")
    if not text:
        return None
    text = text.replace(" - ", " ")
    text = text.strip(" -")
    return text or None


def _extract_marker(row_text: str, marker: str) -> str | None:
    pattern = rf"{marker}\s+(.+?)(?=(?:Last seen near|Diverted to|{AIRCRAFT_RE.pattern}|STATUS\s+N/A|LANDED|DEPARTED|{ALERT_RE.pattern}|$))"
    m = re.search(pattern, row_text, re.I)
    if not m:
        return None
    value = _clean_airport_text(m.group(1))
    if not value:
        return None
    return f"{marker} {value}"


def _extract_aircraft(row_text: str) -> str | None:
    m = AIRCRAFT_RE.search(row_text)
    if not m:
        return None
    return f"{m.group(1).upper()} ({m.group(2).upper()})"


def _derive_callsign(
    raw_callsign: str | None,
    aircraft_text: str | None,
    departure_airport: str | None,
    arrival_airport: str | None,
) -> tuple[str, bool, str]:
    callsign = _normalize_space(raw_callsign or "").upper()
    if callsign:
        return callsign, False, "flight_column"

    aircraft = _normalize_space(aircraft_text or "")
    if aircraft:
        m = AIRCRAFT_RE.search(aircraft)
        if m:
            aircraft_type = m.group(1).upper().strip()
            registration = m.group(2).upper().strip()
            if registration:
                return registration, True, "aircraft_registration"
            if aircraft_type:
                return aircraft_type, True, "aircraft_type"
        return aircraft.upper(), True, "aircraft_text"

    dep = _clean_airport_text(departure_airport)
    arr = _clean_airport_text(arrival_airport)
    if dep and arr:
        return f"{dep} -> {arr}", True, "route"
    if dep:
        return dep, True, "departure_airport"
    if arr:
        return arr, True, "arrival_airport"

    return "Unknown Flight", True, "synthetic_unknown"


def _extract_status(row_text: str) -> str | None:
    patterns = [
        r"STATUS\s+N/A",
        r"LANDED\s+\d{1,2}:\d{2}\s+[A-Z]{2,5}",
        r"DEPARTED\s+\d{1,2}:\d{2}\s+[A-Z]{2,5}",
        r"LANDED\s+\d+[DH](?:\s+\d+M)?\s+AGO",
        r"DEPARTED\s+\d+[DH](?:\s+\d+M)?\s+AGO",
        r"LANDED",
        r"DEPARTED",
    ]
    for pat in patterns:
        m = re.search(pat, row_text, re.I)
        if m:
            return _normalize_space(m.group(0)).title().replace("N/a", "N/A")
    return None


def _extract_route_fields(row_lines: list[str], row_text: str) -> tuple[str | None, str | None, str | None, str | None, dict[str, Any]]:
    extra: dict[str, Any] = {}

    dep_airport = _extract_marker(row_text, "First seen near")
    arr_airport = _extract_marker(row_text, "Last seen near")

    diverted_m = re.search(
        r"Diverted to\s+(.+?)(?=(?:\d{1,2}:\d{2}\s+[A-Z]{2,5}|" + AIRCRAFT_RE.pattern + r"|STATUS\s+N/A|LANDED|DEPARTED|" + ALERT_RE.pattern + r"|$))",
        row_text,
        re.I,
    )
    diverted_to = _clean_airport_text(diverted_m.group(1)) if diverted_m else None

    airport_matches = [_clean_airport_text(m.group(1)) for m in AIRPORT_RE.finditer(row_text)]
    airport_matches = [x for x in airport_matches if x]

    time_matches = TIME_TZ_RE.findall(row_text)
    dep_time = time_matches[0] if len(time_matches) >= 1 else None
    arr_time = time_matches[1] if len(time_matches) >= 2 else None

    if dep_airport is None and airport_matches:
        dep_airport = airport_matches[0]
    if arr_airport is None:
        if diverted_to:
            arr_airport = f"Diverted to {diverted_to}"
            if len(airport_matches) >= 2:
                extra["planned_arrival_airport"] = airport_matches[1]
        elif len(airport_matches) >= 2:
            arr_airport = airport_matches[1]

    if dep_airport and dep_time is None:
        for i, line in enumerate(row_lines):
            if dep_airport in line and i + 1 < len(row_lines) and TIME_RE.fullmatch(row_lines[i + 1]):
                dep_time = row_lines[i + 1]
                break
    if arr_airport and arr_time is None:
        for i, line in enumerate(row_lines):
            if arr_airport.replace("Diverted to ", "") in line and i + 1 < len(row_lines) and TIME_RE.fullmatch(row_lines[i + 1]):
                arr_time = row_lines[i + 1]
                break

    return dep_time, dep_airport, arr_time, arr_airport, extra


def _parse_airnav_current_section(lines: list[str], source_url: str) -> list[AviationAlert]:
    header_idx = None
    for i, line in enumerate(lines):
        if "DateFlightOriginSTD" in line and "Squawk" in line:
            header_idx = i
            break
    if header_idx is None:
        return []

    body = lines[header_idx + 1:]
    rows: list[list[str]] = []
    current: list[str] = []
    for line in body:
        if _looks_like_full_date(line):
            if current:
                rows.append(current)
            current = [line]
        elif current:
            current.append(line)
    if current:
        rows.append(current)

    today_utc = datetime.now(timezone.utc).date()
    oldest_keep = today_utc - timedelta(days=1)

    alerts: list[AviationAlert] = []
    for row in rows:
        event_date_text = row[0].strip()
        row_date = _parse_full_date(event_date_text)
        if row_date and row_date.date() < oldest_keep:
            continue

        body_lines = [x.strip() for x in row[1:] if x.strip() and x.strip() not in {"LIVE", "Replay"}]
        if not body_lines:
            continue

        raw_callsign = next((x for x in body_lines if _looks_like_callsign(x)), "")

        row_text = _normalize_space(" ".join(body_lines))
        alert_m = ALERT_RE.search(row_text)
        if not alert_m:
            continue
        alert_type = _normalize_space(alert_m.group(0))
        squawk_code = alert_type.split("-", 1)[0].strip()
        status_text = _extract_status(row_text) or "Status N/A"
        duration_m = DURATION_RE.search(row_text)
        distance_m = DISTANCE_RE.search(row_text)
        aircraft_text = _extract_aircraft(row_text)
        dep_time, dep_airport, arr_time, arr_airport, extra = _extract_route_fields(body_lines, row_text)
        callsign, callsign_missing, callsign_source = _derive_callsign(raw_callsign, aircraft_text, dep_airport, arr_airport)
        age_hours = _parse_age_hours(status_text)

        alerts.append(
            AviationAlert(
                callsign=callsign,
                status_text=status_text,
                alert_type=alert_type,
                squawk_code=squawk_code,
                event_date_text=event_date_text,
                departure_time_text=dep_time,
                departure_airport=dep_airport,
                arrival_time_text=arr_time,
                arrival_airport=arr_airport,
                duration_text=duration_m.group(0) if duration_m else None,
                aircraft_text=aircraft_text,
                distance_text=distance_m.group(0) if distance_m else None,
                age_hours=age_hours,
                source_name="AirNav Radar",
                source_url=source_url,
                extra={
                    "parser_version": "airnav_current_v6",
                    "raw_row": row,
                    "raw_text": row_text,
                    "raw_callsign": raw_callsign or None,
                    "callsign_missing": callsign_missing,
                    "callsign_source": callsign_source,
                    **extra,
                },
            )
        )

    return alerts


def _normalize_country(value: str | None) -> str | None:
    if not value:
        return None
    text = re.sub(r"\s+", " ", value).strip()
    text = re.sub(r"\bflag\b", "", text, flags=re.I).strip()
    if not text:
        return None
    if len(text) <= 4 and text.isupper():
        return text
    return text.title()


def parse_airnav_alerts(html: str, source_url: str) -> list[AviationAlert]:
    lines = _clean_lines_from_html(html)
    alerts = _parse_airnav_current_section(lines, source_url)
    if not alerts:
        raise AviationParseError("Could not parse any current AirNav alerts")
    return alerts


def parse_fr24_disruptions(
    html: str,
    source_url: str,
    *,
    region: str = "worldwide",
    period: str = "live",
    direction: str = "departures",
) -> list[AirportDisruption]:
    text = _clean_text_from_html(html)
    start_marker = "Rank Flightradar24 disruption index Canceled flights Delayed flights Average delay Name Country"
    end_marker = "Sort by:"
    start_idx = text.find(start_marker)
    if start_idx != -1:
        text = text[start_idx:]
    end_idx = text.find(end_marker)
    if end_idx != -1:
        text = text[:end_idx]

    pattern = re.compile(
        r"(\d+)\.\s+"
        r"([0-9.]+)\s+"
        r"(\d+)\s+\|\s+(\d+)%\s+"
        r"(\d+)\s+\|\s+(\d+)%\s+"
        r"(\d+)\s+min\s+"
        r"(.+?)\s+\(([A-Z0-9]{3,4})\)"
        r"(?:\s+(?:Image:\s+)??([A-Z][A-Z\s\-&]+?(?:\s+flag)?))?"
        r"(?=\s+\d+\.\s+[0-9.]+\s+\d+\s+\|\s+\d+%|\s*$)",
        re.IGNORECASE,
    )

    disruptions: list[AirportDisruption] = []
    for m in pattern.finditer(text):
        country = _normalize_country(m.group(10)) if m.lastindex and m.group(10) else None
        disruptions.append(
            AirportDisruption(
                rank=int(m.group(1)),
                disruption_index=float(m.group(2)),
                canceled_flights=int(m.group(3)),
                canceled_percent=float(m.group(4)),
                delayed_flights=int(m.group(5)),
                delayed_percent=float(m.group(6)),
                average_delay_min=int(m.group(7)),
                airport_name=m.group(8).strip(),
                iata=m.group(9).strip().upper(),
                country=country,
                region=region,
                period=period,
                direction=direction,
                source_name="Flightradar24",
                source_url=source_url,
                extra={},
            )
        )

    if not disruptions:
        raise AviationParseError("Could not parse any FR24 disruptions")

    return disruptions
