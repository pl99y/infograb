from __future__ import annotations

import logging
from typing import Any

from collectors.disaster_fetcher import (
    fetch_disaster_payloads,
    fetch_natural_hazard_payloads,
    fetch_weather_alerts_payloads,
)
from parsers.disaster_parser import (
    parse_disaster_payloads,
    parse_natural_hazard_payloads,
    parse_weather_alerts_payloads,
)
from services.disaster.ongoing import rebuild_disaster_ongoing_from_events
from storage import (
    get_connection,
    initialize_database,
    record_fetch_run,
    upsert_disaster_event,
    upsert_weather_alert,
)

logger = logging.getLogger(__name__)


def _replace_weather_alerts(con, records: list[dict], fetched_at: str | None) -> int:
    con.execute("DELETE FROM weather_alerts")
    saved = 0
    for rec in records:
        upsert_weather_alert(
            con,
            dedupe_key=rec["dedupe_key"],
            source_primary=rec.get("source_primary") or rec.get("source") or "WMO",
            title=rec["title"],
            summary=rec.get("summary"),
            severity_level=rec.get("severity_level"),
            color_level=rec.get("color_level"),
            event_type=rec.get("event_type"),
            location_text=rec.get("location_text"),
            issued_at=rec.get("issued_at"),
            effective_at=rec.get("effective_at"),
            expires_at=rec.get("expires_at"),
            source_url=rec.get("source_url"),
            detail_url=rec.get("detail_url"),
            payload=rec.get("payload"),
            fetched_at=fetched_at,
        )
        saved += 1
    return saved


def _replace_disaster_events(con, records: list[dict], fetched_at: str | None) -> int:
    con.execute("DELETE FROM disaster_events")
    saved = 0
    for rec in records:
        upsert_disaster_event(
            con,
            event_family=rec.get("event_family") or "instant",
            event_type=rec["event_type"],
            severity_level=rec.get("severity_level"),
            severity_color=rec.get("severity_color"),
            title=rec.get("title"),
            summary=rec.get("summary"),
            occurred_at=rec.get("occurred_at"),
            updated_at=rec.get("updated_at"),
            location_text=rec.get("location_text"),
            lat=rec.get("lat"),
            lon=rec.get("lon"),
            source_primary=rec.get("source_primary") or "UNKNOWN",
            source_secondary=rec.get("source_secondary"),
            external_id=rec.get("external_id"),
            external_id_secondary=rec.get("external_id_secondary"),
            dedupe_key=rec["dedupe_key"],
            status=rec.get("status"),
            map_url=rec.get("map_url"),
            payload=rec.get("payload"),
            fetched_at=fetched_at,
        )
        saved += 1
    return saved


def _count_event_types(records: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"earthquake": 0, "tsunami": 0, "volcano": 0, "typhoon": 0, "flood": 0}
    for rec in records:
        event_type = rec.get("event_type")
        if event_type in counts:
            counts[event_type] += 1
    return counts


def _should_preserve_weather_records(records: list[dict], errors: list[str]) -> bool:
    # Weather alerts should not be wiped just because the upstream WMO fetch
    # failed for one cycle. Preserve the previous snapshot when parsing yields
    # nothing *and* the refresh contains source errors.
    return not records and bool(errors)


def _record_failed_run(*, db_path: str, source_name: str, started_at: str | None, exc: Exception) -> None:
    timestamp = started_at or ""
    con = get_connection(db_path)
    try:
        record_fetch_run(
            con=con,
            source_id=None,
            source_name=source_name,
            started_at=timestamp,
            finished_at=timestamp,
            status="failed",
            items_found=0,
            new_items=0,
            error_message=str(exc),
        )
        con.commit()
    finally:
        con.close()


def refresh_natural_hazards_once(*, db_path: str = "app.db") -> dict:
    initialize_database(db_path)
    raw = None
    try:
        raw = fetch_natural_hazard_payloads()
        parsed = parse_natural_hazard_payloads(raw)
    except Exception as exc:
        logger.exception("Natural hazards refresh failed before DB write")
        _record_failed_run(db_path=db_path, source_name="Weather/Alerts", started_at=(raw or {}).get("fetched_at"), exc=exc)
        _record_failed_run(db_path=db_path, source_name="Disaster/Events", started_at=(raw or {}).get("fetched_at"), exc=exc)
        raise

    weather_records = list(parsed.get("weather_alert_records", []) or [])
    disaster_records = list(parsed.get("disaster_event_records", []) or [])
    weather_errors = list(parsed.get("weather_errors", []) or [])
    disaster_errors = list(parsed.get("disaster_errors", []) or [])
    fetched_at = raw.get("fetched_at")

    con = get_connection(db_path)
    try:
        if _should_preserve_weather_records(weather_records, weather_errors):
            weather_saved = 0
            weather_status = "partial"
            weather_errors = list(weather_errors) + ["no weather records parsed; existing weather_alerts preserved"]
        else:
            weather_saved = _replace_weather_alerts(con, weather_records, fetched_at)
            weather_status = "success" if not weather_errors else "partial"

        if disaster_records:
            disaster_saved = _replace_disaster_events(con, disaster_records, fetched_at)
            disaster_status = "success" if not disaster_errors else "partial"
        else:
            disaster_saved = 0
            disaster_errors = list(disaster_errors) + ["no disaster records parsed; existing disaster_events preserved"]
            disaster_status = "partial"

        record_fetch_run(
            con=con,
            source_id=None,
            source_name="Weather/Alerts",
            started_at=fetched_at,
            finished_at=fetched_at,
            status=weather_status,
            items_found=len(weather_records),
            new_items=weather_saved,
            error_message=" | ".join(weather_errors),
        )
        record_fetch_run(
            con=con,
            source_id=None,
            source_name="Disaster/Events",
            started_at=fetched_at,
            finished_at=fetched_at,
            status=disaster_status,
            items_found=len(disaster_records),
            new_items=disaster_saved,
            error_message=" | ".join(disaster_errors),
        )
        con.commit()
    finally:
        con.close()

    ongoing_stats = rebuild_disaster_ongoing_from_events(db_path=db_path)
    return {
        "fetched_at": fetched_at,
        "weather": {
            "alerts_found": len(weather_records),
            "alerts_saved": weather_saved,
            "errors": weather_errors,
        },
        "disaster": {
            "events_found": len(disaster_records),
            "events_saved": disaster_saved,
            "counts": _count_event_types(disaster_records),
            "errors": disaster_errors,
            "ongoing": ongoing_stats,
        },
    }


def fetch_weather_alerts_once(*, db_path: str = "app.db") -> dict:
    initialize_database(db_path)
    raw = None
    try:
        raw = fetch_weather_alerts_payloads()
        parsed = parse_weather_alerts_payloads(raw)
    except Exception as exc:
        logger.exception("Weather alert fetch failed before DB write")
        _record_failed_run(db_path=db_path, source_name="Weather/Alerts", started_at=(raw or {}).get("fetched_at"), exc=exc)
        raise

    records = parsed.get("records", [])
    errors = list(parsed.get("errors", []))
    fetched_at = raw.get("fetched_at")

    con = get_connection(db_path)
    try:
        if _should_preserve_weather_records(records, errors):
            saved = 0
            status = "partial"
            errors = list(errors) + ["no weather records parsed; existing weather_alerts preserved"]
        else:
            saved = _replace_weather_alerts(con, records, fetched_at)
            status = "success" if not errors else "partial"
        record_fetch_run(
            con=con,
            source_id=None,
            source_name="Weather/Alerts",
            started_at=fetched_at,
            finished_at=fetched_at,
            status=status,
            items_found=len(records),
            new_items=saved,
            error_message=" | ".join(errors),
        )
        con.commit()
        return {"alerts_found": len(records), "alerts_saved": saved, "errors": errors, "fetched_at": fetched_at}
    finally:
        con.close()


def fetch_disaster_once(*, db_path: str = "app.db") -> dict:
    initialize_database(db_path)
    raw = None
    try:
        raw = fetch_disaster_payloads()
        parsed = parse_disaster_payloads(raw)
    except Exception as exc:
        logger.exception("Disaster fetch failed before DB write")
        _record_failed_run(db_path=db_path, source_name="Disaster/Events", started_at=(raw or {}).get("fetched_at"), exc=exc)
        raise

    records = parsed.get("records", [])
    errors = list(parsed.get("errors", []))
    fetched_at = raw.get("fetched_at")

    con = get_connection(db_path)
    try:
        if records:
            saved = _replace_disaster_events(con, records, fetched_at)
            status = "success" if not errors else "partial"
        else:
            saved = 0
            errors = list(errors) + ["no disaster records parsed; existing disaster_events preserved"]
            status = "partial"

        record_fetch_run(
            con=con,
            source_id=None,
            source_name="Disaster/Events",
            started_at=fetched_at,
            finished_at=fetched_at,
            status=status,
            items_found=len(records),
            new_items=saved,
            error_message=" | ".join(errors),
        )
        con.commit()
    finally:
        con.close()

    ongoing_stats = rebuild_disaster_ongoing_from_events(db_path=db_path)
    return {
        "events_found": len(records),
        "events_saved": saved,
        "counts": _count_event_types(records),
        "errors": errors,
        "fetched_at": fetched_at,
        "ongoing": ongoing_stats,
    }
