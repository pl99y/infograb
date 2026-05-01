from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone
from html import unescape
from typing import Any

from collectors.aviation_fetcher import ADSBFI_SQUAWK_CODES, adsbfi_squawk_url, fetch_adsbfi_squawk, fetch_aviation_page
from parsers.aviation_parser import parse_adsbfi_squawk_alerts, parse_fr24_disruptions
from storage import (
    get_connection,
    insert_airport_alert,
    insert_airport_disruption,
    record_fetch_run,
    upsert_source,
)

logger = logging.getLogger(__name__)

ADSBFI_SOURCE_NAME = "ADSB.fi Emergency Squawk"
ADSBFI_SOURCE_URL = "https://opendata.adsb.fi/api"
ADSBFI_REQUEST_SLEEP_SECONDS = 1.15
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


def _json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _json_dumps(value: dict[str, Any] | None) -> str:
    return json.dumps(value or {}, ensure_ascii=False, separators=(",", ":"))


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _format_elapsed_ago(then_iso: str | None, now_iso: str | None = None) -> str:
    then = _parse_dt(then_iso)
    now = _parse_dt(now_iso) or datetime.now(timezone.utc)
    if not then:
        return "last detected earlier"
    seconds = max(0, int((now - then).total_seconds()))
    if seconds < 90:
        return f"last detected {seconds}s ago"
    minutes = seconds // 60
    if minutes < 90:
        return f"last detected {minutes}m ago"
    hours = minutes // 60
    if hours < 48:
        rem_minutes = minutes % 60
        if rem_minutes:
            return f"last detected {hours}h {rem_minutes}m ago"
        return f"last detected {hours}h ago"
    days = hours // 24
    return f"last detected {days}d ago"


def _alert_event_key(alert) -> str:
    extra = getattr(alert, "extra", {}) or {}
    hex_id = str(extra.get("hex") or "").strip().lower().lstrip("~")
    callsign = str(alert.callsign or "").strip().upper()
    squawk = str(alert.squawk_code or "").strip()
    identity = hex_id or callsign or "unknown"
    return f"adsbfi|{identity}|{squawk}"


def _alert_dedupe_key(alert) -> str:
    return hashlib.sha1(_alert_event_key(alert).encode("utf-8")).hexdigest()


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


def _record_current_adsbfi_alert(con, *, alert, dedupe_key: str, fetched_at: str) -> bool:
    existing = con.execute(
        "SELECT * FROM airport_alerts WHERE dedupe_key = ?",
        (dedupe_key,),
    ).fetchone()

    if not existing:
        extra = dict(getattr(alert, "extra", {}) or {})
        extra.setdefault("event_key", _alert_event_key(alert))
        extra.setdefault("first_seen_utc", fetched_at)
        extra["last_seen_utc"] = fetched_at
        extra["last_refresh_utc"] = fetched_at
        extra["is_active"] = True
        alert.extra = extra

        insert_airport_alert(
            con=con,
            dedupe_key=dedupe_key,
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
            fetched_at=fetched_at,
        )
        return True

    existing_extra = _json_loads(existing["extra_json"])
    incoming_extra = dict(getattr(alert, "extra", {}) or {})
    first_seen = existing_extra.get("first_seen_utc") or existing["fetched_at"] or fetched_at
    merged_extra = {
        **existing_extra,
        **incoming_extra,
        "event_key": existing_extra.get("event_key") or _alert_event_key(alert),
        "first_seen_utc": first_seen,
        "last_seen_utc": fetched_at,
        "last_refresh_utc": fetched_at,
        "is_active": True,
        "reactivated": bool(existing_extra and existing_extra.get("is_active") is False),
    }

    con.execute(
        """
        UPDATE airport_alerts
        SET callsign = ?,
            status_text = ?,
            alert_type = ?,
            squawk_code = ?,
            departure_time_text = ?,
            departure_airport = ?,
            arrival_time_text = ?,
            arrival_airport = ?,
            duration_text = ?,
            aircraft_text = ?,
            distance_text = ?,
            age_hours = ?,
            source_name = ?,
            source_url = ?,
            extra_json = ?,
            fetched_at = ?
        WHERE dedupe_key = ?
        """,
        (
            alert.callsign,
            alert.status_text,
            alert.alert_type,
            alert.squawk_code,
            alert.departure_time_text,
            alert.departure_airport,
            alert.arrival_time_text,
            alert.arrival_airport,
            alert.duration_text,
            alert.aircraft_text,
            alert.distance_text,
            alert.age_hours,
            alert.source_name,
            alert.source_url,
            _json_dumps(merged_extra),
            fetched_at,
            dedupe_key,
        ),
    )
    return False


def _mark_inactive_adsbfi_alerts(con, *, active_dedupe_keys: set[str], checked_at: str) -> int:
    rows = con.execute(
        """
        SELECT *
        FROM airport_alerts
        WHERE source_name = ?
          AND datetime(fetched_at) >= datetime('now', '-48 hours')
        """,
        (ADSBFI_SOURCE_NAME,),
    ).fetchall()

    updated = 0
    for row in rows:
        dedupe_key = row["dedupe_key"]
        if dedupe_key in active_dedupe_keys:
            continue

        extra = _json_loads(row["extra_json"])
        if extra.get("is_active") is False and extra.get("last_refresh_utc") == checked_at:
            continue

        last_seen = extra.get("last_seen_utc") or row["fetched_at"]
        status_text = _format_elapsed_ago(last_seen, checked_at)
        extra["is_active"] = False
        extra["last_refresh_utc"] = checked_at
        extra.setdefault("last_seen_utc", last_seen)

        con.execute(
            """
            UPDATE airport_alerts
            SET status_text = ?,
                extra_json = ?
            WHERE dedupe_key = ?
            """,
            (status_text, _json_dumps(extra), dedupe_key),
        )
        updated += 1

    return updated


def fetch_adsbfi_emergency_squawk_alerts_once(db_path: str = "app.db") -> dict:
    errors: list[str] = []
    metas: list[dict[str, Any]] = []
    alerts = []
    successful_fetches = 0
    first_started_at: str | None = None
    last_finished_at: str | None = None

    for idx, squawk in enumerate(ADSBFI_SQUAWK_CODES):
        fetch_result = fetch_adsbfi_squawk(squawk)
        if first_started_at is None:
            first_started_at = fetch_result.fetched_at
        last_finished_at = fetch_result.fetched_at

        meta = {
            "squawk": squawk,
            "url": fetch_result.url,
            "ok": fetch_result.success,
            "status_code": fetch_result.status_code,
            "content_type": fetch_result.content_type,
            "fetched_at": fetch_result.fetched_at,
        }

        if not fetch_result.success or fetch_result.payload is None:
            error = fetch_result.error_message or "Unknown ADSB.fi fetch error"
            errors.append(f"{squawk}: {error}")
            meta["error"] = error
            metas.append(meta)
        else:
            successful_fetches += 1
            try:
                parsed = parse_adsbfi_squawk_alerts(
                    fetch_result.payload,
                    fetch_result.url,
                    squawk=squawk,
                    fetched_at=fetch_result.fetched_at,
                )
                alerts.extend(parsed)
                meta["aircraft_count"] = len(parsed)
                meta["total"] = fetch_result.payload.get("total")
                meta["msg"] = fetch_result.payload.get("msg")
                metas.append(meta)
            except Exception as exc:
                error = f"{squawk}: {exc}"
                errors.append(error)
                meta["error"] = str(exc)
                metas.append(meta)

        if idx < len(ADSBFI_SQUAWK_CODES) - 1:
            time.sleep(ADSBFI_REQUEST_SLEEP_SECONDS)

    started_at = first_started_at or datetime.now(timezone.utc).isoformat()
    finished_at = last_finished_at or started_at

    if successful_fetches == 0:
        _record_failure(
            db_path=db_path,
            source_name="Aviation/ADSB.fi Emergency Squawk",
            started_at=started_at,
            error_message=" | ".join(errors) or "Could not fetch ADSB.fi squawk snapshots",
        )
        return {"alerts_found": 0, "alerts_saved": 0, "alerts_updated": 0, "errors": errors}

    # Deduplicate current snapshot by aircraft identity + squawk.
    current_by_key = {}
    for alert in alerts:
        current_by_key[_alert_dedupe_key(alert)] = alert

    con = get_connection(db_path)
    try:
        source_id = upsert_source(
            con=con,
            name=ADSBFI_SOURCE_NAME,
            source_type="aviation",
            url=ADSBFI_SOURCE_URL,
            enabled=True,
        )

        active_keys: set[str] = set()
        saved = 0
        refreshed = 0
        for dedupe_key, alert in current_by_key.items():
            fetched_at = (alert.extra or {}).get("fetched_at") or finished_at
            is_new = _record_current_adsbfi_alert(con, alert=alert, dedupe_key=dedupe_key, fetched_at=fetched_at)
            active_keys.add(dedupe_key)
            if is_new:
                saved += 1
            else:
                refreshed += 1

        inactive_updated = _mark_inactive_adsbfi_alerts(con, active_dedupe_keys=active_keys, checked_at=finished_at)

        record_fetch_run(
            con=con,
            source_id=source_id,
            source_name="Aviation/ADSB.fi Emergency Squawk",
            started_at=started_at,
            finished_at=finished_at,
            status="success" if not errors else "partial",
            items_found=len(current_by_key),
            new_items=saved,
            error_message=" | ".join(errors),
        )
        con.commit()

        return {
            "alerts_found": len(current_by_key),
            "alerts_saved": saved,
            "alerts_refreshed": refreshed,
            "alerts_marked_inactive": inactive_updated,
            "metas": metas,
            "errors": errors,
        }
    finally:
        con.close()


def fetch_airnav_alerts_once(db_path: str = "app.db") -> dict:
    """Backward-compatible alias.

    AirNav's HTML alerts page is now protected by Cloudflare challenge pages in
    many environments. The dashboard now uses ADSB.fi live emergency squawk
    snapshots plus local 48-hour SQLite retention instead.
    """
    return fetch_adsbfi_emergency_squawk_alerts_once(db_path=db_path)


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

        # FR24 disruptions are a current ranking snapshot, so a successful fetch
        # replaces the previous disruption cards.
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
    result_alerts = fetch_adsbfi_emergency_squawk_alerts_once(db_path=db_path)
    result_disruptions = fetch_fr24_disruptions_once(db_path=db_path)
    return {
        "alerts": result_alerts,
        "disruptions": result_disruptions,
        "errors": result_alerts.get("errors", []) + result_disruptions.get("errors", []),
    }
