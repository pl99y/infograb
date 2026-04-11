from __future__ import annotations

import hashlib
import logging
import re
import time
from html import unescape
from types import SimpleNamespace

from collectors.aviation_fetcher import fetch_aviation_page
from parsers.aviation_parser import parse_airnav_alerts, parse_fr24_disruptions
from storage import (
    get_connection,
    insert_airport_alert,
    insert_airport_disruption,
    record_fetch_run,
    upsert_source,
)

logger = logging.getLogger(__name__)

AIRNAV_ALERTS_URL = "https://www.airnavradar.com/data/alerts"
FR24_WORLD_DISRUPTION_URL = (
    "https://www.flightradar24.com/data/airport-disruption"
    "?continent=worldwide&indices=true&period=live&type=departures"
)
FR24_ASIA_DISRUPTION_URL = (
    "https://www.flightradar24.com/data/airport-disruption"
    "?continent=asia&indices=true&period=live&type=departures"
)


def _clean_html_text(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _norm_cell(cell: str | None) -> str | None:
    if cell is None:
        return None
    s = cell.strip()
    if not s or s == "-":
        return None
    if s.upper() == "LIVE":
        return None
    return s


def _looks_like_time(cell: str) -> bool:
    return bool(re.match(r"^\d{1,2}:\d{2}(?:\s+[A-Z]{2,5})?$", cell.strip()))


def _looks_like_duration(cell: str) -> bool:
    return bool(re.match(r"^\d{2}h\d{2}m$", cell.strip()))


def _looks_like_squawk(cell: str) -> bool:
    return bool(re.match(r"^(7500|7600|7700)\s+-\s+", cell.strip()))


def _looks_like_status(cell: str) -> bool:
    s = cell.strip().lower()
    return (
        s.startswith("landed")
        or s.startswith("departed")
        or s.startswith("status n/a")
        or s.startswith("divert")
        or s.startswith("holding")
        or s.startswith("return")
        or s.startswith("emergency")
    )


def _looks_like_replay(cell: str) -> bool:
    s = cell.lower()
    return "derived from ads-b/radar" in s or "time aircraft arrives at gate derived" in s


def _looks_like_aircraft(cell: str) -> bool:
    s = cell.strip()
    return bool(re.match(r"^[A-Z0-9]{2,5}\s*\(\s*[A-Z0-9-]{3,}\s*\)$", s, re.I))


def _derive_callsign(
    raw_callsign: str | None,
    aircraft_text: str | None,
    departure_airport: str | None,
    arrival_airport: str | None,
) -> tuple[str, bool, str]:
    callsign = _norm_cell(raw_callsign)
    if callsign:
        return callsign, False, "flight_column"

    aircraft = _norm_cell(aircraft_text)
    if aircraft:
        m = re.match(r"^([A-Z0-9]{2,5})\s*\(\s*([A-Z0-9-]{3,})\s*\)$", aircraft, re.I)
        if m:
            aircraft_type = m.group(1).upper().strip()
            registration = m.group(2).upper().strip()
            if registration:
                return registration, True, "aircraft_registration"
            if aircraft_type:
                return aircraft_type, True, "aircraft_type"
        return aircraft, True, "aircraft_text"

    dep = _norm_cell(departure_airport)
    arr = _norm_cell(arrival_airport)
    if dep and arr:
        return f"{dep} -> {arr}", True, "route"
    if dep:
        return dep, True, "departure_airport"
    if arr:
        return arr, True, "arrival_airport"

    return "Unknown Flight", True, "synthetic_unknown"


def _parse_airnav_alerts_rows(html: str, source_url: str):
    trs = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.I | re.S)
    alerts = []

    for tr in trs:
        cells_html = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.I | re.S)
        if not cells_html:
            continue

        cells = [_clean_html_text(c) for c in cells_html]
        if not cells or cells[0].lower() == "date" or len(cells) < 4:
            continue

        event_date_text = _norm_cell(cells[0])
        raw_callsign = _norm_cell(cells[1])
        if not event_date_text:
            continue

        squawk_cell = _norm_cell(cells[-1]) if _looks_like_squawk(cells[-1]) else None
        squawk_code = squawk_cell.split(" - ", 1)[0] if squawk_cell else None
        alert_type = squawk_cell

        departure_time_text = None
        arrival_time_text = None
        duration_text = None
        status_text = None
        aircraft_text = None
        notes = []
        route_cells = []

        middle = cells[2:-1] if squawk_cell else cells[2:]

        for raw_cell in middle:
            if raw_cell.strip().upper() == "LIVE":
                notes.append("LIVE")
                continue

            cell = _norm_cell(raw_cell)
            if not cell:
                continue

            if _looks_like_time(cell):
                if departure_time_text is None:
                    departure_time_text = cell
                elif arrival_time_text is None:
                    arrival_time_text = cell
                else:
                    notes.append(cell)
                continue

            if _looks_like_duration(cell):
                duration_text = cell
                continue

            if _looks_like_replay(cell):
                notes.append(cell)
                continue

            if _looks_like_status(cell):
                status_text = cell if status_text is None else f"{status_text} {cell}"
                continue

            if _looks_like_aircraft(cell):
                aircraft_text = cell
                continue

            route_cells.append(cell)

        departure_airport = _norm_cell(route_cells[0]) if len(route_cells) >= 1 else None
        arrival_airport = _norm_cell(route_cells[1]) if len(route_cells) >= 2 else None
        callsign, callsign_missing, callsign_source = _derive_callsign(raw_callsign, aircraft_text, departure_airport, arrival_airport)

        extra = {
            "parser_version": "airnav_rows_primary_v3",
            "raw_cells": cells,
            "raw_callsign": raw_callsign,
            "callsign_missing": callsign_missing,
            "callsign_source": callsign_source,
        }
        if notes:
            extra["notes"] = notes

        alerts.append(
            SimpleNamespace(
                callsign=callsign,
                status_text=status_text or "Status N/A",
                alert_type=alert_type,
                squawk_code=squawk_code,
                event_date_text=event_date_text,
                departure_time_text=departure_time_text,
                departure_airport=departure_airport,
                arrival_time_text=arrival_time_text,
                arrival_airport=arrival_airport,
                duration_text=duration_text,
                aircraft_text=aircraft_text,
                distance_text=None,
                age_hours=None,
                source_name="AirNav Radar",
                source_url=source_url,
                extra=extra,
            )
        )

    if not alerts:
        raise ValueError("AirNav row parser found no alert rows")

    return alerts


def _alert_dedupe_key(alert) -> str:
    raw = " | ".join(
        [
            alert.callsign or "",
            alert.alert_type or "",
            alert.status_text or "",
            alert.event_date_text or "",
            alert.departure_time_text or "",
            alert.departure_airport or "",
            alert.arrival_time_text or "",
            alert.arrival_airport or "",
            alert.aircraft_text or "",
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _record_failure(
    *,
    db_path: str,
    source_name: str,
    started_at: str,
    error_message: str,
) -> None:
    con = get_connection(db_path)
    try:
        record_fetch_run(
            con=con,
            source_id=None,
            source_name=source_name,
            started_at=started_at,
            finished_at=started_at,
            status="failed",
            items_found=0,
            new_items=0,
            error_message=error_message,
        )
        con.commit()
    finally:
        con.close()


def _fetch_with_retries(source_name: str, url: str, retries: int = 3, sleep_s: float = 2.0):
    last_result = None
    errors = []
    for attempt in range(1, retries + 1):
        result = fetch_aviation_page(source_name, url)
        last_result = result
        if result.success:
            return result, errors
        errors.append(result.error_message or "Unknown fetch error")
        if attempt < retries:
            time.sleep(sleep_s * attempt)
    return last_result, errors


def fetch_airnav_alerts_once(db_path: str = "app.db") -> dict:
    fetch_result, fetch_errors = _fetch_with_retries("AirNav Radar Alerts", AIRNAV_ALERTS_URL, retries=3, sleep_s=2.0)
    if not fetch_result or not fetch_result.success:
        error_message = " | ".join(fetch_errors) if fetch_errors else "Unknown fetch error"
        _record_failure(
            db_path=db_path,
            source_name="Aviation/AirNav Alerts",
            started_at=(fetch_result.fetched_at if fetch_result else ""),
            error_message=error_message,
        )
        return {"alerts_saved": 0, "errors": [error_message]}

    parser_errors: list[str] = []

    try:
        alerts = parse_airnav_alerts(fetch_result.html, AIRNAV_ALERTS_URL)
    except Exception as exc:
        parser_errors.append(f"text_parser_failed: {exc}")
        logger.warning("Primary AirNav parser failed, falling back to row parser: %s", exc)
        try:
            alerts = _parse_airnav_alerts_rows(fetch_result.html, AIRNAV_ALERTS_URL)
            for alert in alerts:
                extra = dict(getattr(alert, "extra", {}) or {})
                extra.setdefault("fallback_reason", str(exc))
                alert.extra = extra
        except Exception as fallback_exc:
            parser_errors.append(f"row_parser_failed: {fallback_exc}")
            error_message = " | ".join(parser_errors)
            _record_failure(
                db_path=db_path,
                source_name="Aviation/AirNav Alerts",
                started_at=fetch_result.fetched_at,
                error_message=error_message,
            )
            return {"alerts_saved": 0, "errors": [error_message]}

    con = get_connection(db_path)
    try:
        source_id = upsert_source(
            con=con,
            name="AirNav Radar Alerts",
            source_type="aviation",
            url=AIRNAV_ALERTS_URL,
            enabled=True,
        )

        # User requirement: every successful aviation refresh must replace the
        # visible list instead of accumulating older cards across fetches.
        con.execute("DELETE FROM airport_alerts")

        saved = 0
        for alert in alerts:
            insert_airport_alert(
                con=con,
                dedupe_key=_alert_dedupe_key(alert),
                callsign=alert.callsign,
                status_text=alert.status_text,
                alert_type=alert.alert_type,
                squawk_code=alert.squawk_code,
                event_date_text=alert.event_date_text,
                departure_time_text=alert.departure_time_text,
                departure_airport=alert.departure_airport,
                arrival_time_text=alert.arrival_time_text,
                arrival_airport=alert.arrival_airport,
                duration_text=alert.duration_text,
                aircraft_text=alert.aircraft_text,
                distance_text=alert.distance_text,
                age_hours=alert.age_hours,
                source_name=alert.source_name,
                source_url=alert.source_url,
                extra=alert.extra,
                fetched_at=fetch_result.fetched_at,
            )
            saved += 1

        record_fetch_run(
            con=con,
            source_id=source_id,
            source_name="Aviation/AirNav Alerts",
            started_at=fetch_result.fetched_at,
            finished_at=fetch_result.fetched_at,
            status="success" if not parser_errors else "partial",
            items_found=len(alerts),
            new_items=saved,
            error_message=" | ".join(parser_errors),
        )
        con.commit()

        return {"alerts_found": len(alerts), "alerts_saved": saved, "errors": parser_errors}
    finally:
        con.close()


def _fetch_and_parse_fr24(url: str, region: str):
    fetch_result = fetch_aviation_page("FR24 Airport Disruptions", url)
    if not fetch_result.success:
        return [], fetch_result.fetched_at, fetch_result.error_message or "Unknown fetch error"

    try:
        disruptions = parse_fr24_disruptions(
            fetch_result.html,
            url,
            region=region,
            period="live",
            direction="departures",
        )
        return disruptions, fetch_result.fetched_at, None
    except Exception as exc:
        return [], fetch_result.fetched_at, str(exc)


def fetch_fr24_disruptions_once(db_path: str = "app.db") -> dict:
    errors: list[str] = []

    disruptions, fetched_at, error = _fetch_and_parse_fr24(FR24_WORLD_DISRUPTION_URL, "worldwide")
    used_region = "worldwide"
    used_url = FR24_WORLD_DISRUPTION_URL

    if not disruptions:
        if error:
            errors.append(f"worldwide: {error}")
        disruptions, fetched_at, error2 = _fetch_and_parse_fr24(FR24_ASIA_DISRUPTION_URL, "asia")
        used_region = "asia"
        used_url = FR24_ASIA_DISRUPTION_URL
        if not disruptions and error2:
            errors.append(f"asia fallback: {error2}")

    if not disruptions:
        _record_failure(
            db_path=db_path,
            source_name="Aviation/FR24 Disruptions",
            started_at=fetched_at,
            error_message=" | ".join(errors) or "Could not fetch/parse FR24 disruptions",
        )
        return {"disruptions_saved": 0, "errors": errors or ["Could not fetch/parse FR24 disruptions"]}

    con = get_connection(db_path)
    try:
        source_id = upsert_source(
            con=con,
            name="FR24 Airport Disruptions",
            source_type="aviation",
            url=used_url,
            enabled=True,
        )

        # Same replacement rule for disruption cards: on a successful fetch we
        # clear the previous snapshot first, then write the current one only.
        con.execute("DELETE FROM airport_disruptions")

        saved = 0
        for item in disruptions:
            insert_airport_disruption(
                con=con,
                region=item.region,
                period=item.period,
                direction=item.direction,
                rank=item.rank,
                airport_name=item.airport_name,
                iata=item.iata,
                country=item.country,
                disruption_index=item.disruption_index,
                canceled_flights=item.canceled_flights,
                canceled_percent=item.canceled_percent,
                delayed_flights=item.delayed_flights,
                delayed_percent=item.delayed_percent,
                average_delay_min=item.average_delay_min,
                source_name=item.source_name,
                source_url=item.source_url,
                extra={**(item.extra or {}), "requested_region": "worldwide", "used_region": used_region},
                fetched_at=fetched_at,
            )
            saved += 1

        record_fetch_run(
            con=con,
            source_id=source_id,
            source_name="Aviation/FR24 Disruptions",
            started_at=fetched_at,
            finished_at=fetched_at,
            status="success" if not errors else "partial",
            items_found=len(disruptions),
            new_items=saved,
            error_message=" | ".join(errors),
        )
        con.commit()

        return {"disruptions_found": len(disruptions), "disruptions_saved": saved, "region_used": used_region, "errors": errors}
    finally:
        con.close()


def fetch_aviation_once(db_path: str = "app.db") -> dict:
    result_alerts = fetch_airnav_alerts_once(db_path=db_path)
    result_disruptions = fetch_fr24_disruptions_once(db_path=db_path)
    return {
        "alerts": result_alerts,
        "disruptions": result_disruptions,
        "errors": result_alerts.get("errors", []) + result_disruptions.get("errors", []),
    }
