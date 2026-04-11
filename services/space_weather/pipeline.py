from __future__ import annotations

import asyncio
import logging
from typing import Any

from collectors.common import utc_now_iso
from collectors.space_weather_fetcher import (
    SWPC_3DAY_FORECAST_URL,
    SWPC_3DAY_GEOMAG_URL,
    SWPC_ALERTS_URL,
    fetch_swpc_payloads,
)
from collectors.space_launch_fetcher import (
    SPACE_LAUNCHES_SOURCE_NAME,
    SPACE_LAUNCHES_URL,
    fetch_space_launches,
)
from parsers.space_weather_parser import (
    parse_swpc_alerts_payload,
    parse_swpc_forecast_payloads,
)
from services.shared.translation import translate_batch_with_meta
from storage import (
    get_connection,
    initialize_database,
    insert_swpc_alert,
    insert_swpc_forecast,
    insert_space_launch_snapshot,
    record_fetch_run,
    upsert_source,
)

logger = logging.getLogger(__name__)

SWPC_ALERTS_FETCH_RUN = "SWPC/Alerts"
SWPC_FORECAST_FETCH_RUN = "SWPC/Forecast"
SWPC_FORECAST_KEY = "swpc_default"
ALERTS_MAX_ITEMS = 30
ALERTS_MAX_AGE_DAYS = 5
SPACE_WEATHER_TARGET_LANG = "zh"

ALERT_TEXT_FIELDS = (
    "headline",
    "impacts_text",
    "description_text",
)

FORECAST_TEXT_PATHS = (
    ("geomagnetic", "observed_summary"),
    ("geomagnetic", "expected_summary"),
    ("geomagnetic", "rationale"),
    ("solar_radiation", "observed_summary"),
    ("solar_radiation", "rationale"),
    ("radio_blackout", "observed_summary"),
    ("radio_blackout", "rationale"),
)


def _compact_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _pick_provider(providers: list[str] | None) -> str:
    for provider in providers or []:
        if provider and provider not in {"none", "failed", "pending", "cached"}:
            return provider
    return "none"


def _read_existing_alert_cache(con) -> dict[str, dict[str, str]]:
    rows = con.execute(
        """
        SELECT dedupe_key, headline, impacts_text, description_text
        FROM swpc_alerts
        """
    ).fetchall()

    cache: dict[str, dict[str, str]] = {}
    for row in rows:
        dedupe_key = _compact_text(row["dedupe_key"])
        if not dedupe_key:
            continue
        cache[dedupe_key] = {
            "headline": _compact_text(row["headline"]),
            "impacts_text": _compact_text(row["impacts_text"]),
            "description_text": _compact_text(row["description_text"]),
        }
    return cache


def _read_existing_forecast_cache(con) -> dict[str, Any] | None:
    row = con.execute(
        """
        SELECT panel_json, raw_forecast_text, raw_geomag_text
        FROM swpc_forecasts
        ORDER BY datetime(fetched_at) DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None

    import json

    try:
        panel = json.loads(row["panel_json"]) if row["panel_json"] else None
    except Exception:
        panel = None

    return {
        "panel": panel,
        "raw_forecast_text": row["raw_forecast_text"] or "",
        "raw_geomag_text": row["raw_geomag_text"] or "",
    }


def _reuse_cached_forecast_translations(panel: dict[str, Any], cached_panel: dict[str, Any]) -> bool:
    reused_any = False
    if not isinstance(panel, dict) or not isinstance(cached_panel, dict):
        return reused_any

    for section, field in FORECAST_TEXT_PATHS:
        fresh_value = _compact_text(panel.get(section, {}).get(field))
        cached_value = _compact_text(cached_panel.get(section, {}).get(field))
        if not fresh_value or not cached_value:
            continue
        panel.setdefault(section, {})[field] = cached_value
        reused_any = True

    return reused_any


def _build_translation_plan(
    alerts: list[dict[str, Any]],
    panel: dict[str, Any],
    *,
    existing_alerts: dict[str, dict[str, str]] | None = None,
) -> tuple[list[str], list[tuple[str, Any]], str]:
    texts: list[str] = []
    slots: list[tuple[str, Any]] = []
    provider_hint = "none"
    existing_alerts = existing_alerts or {}

    for index, alert in enumerate(alerts):
        cached = existing_alerts.get(_compact_text(alert.get("dedupe_key"))) or {}
        reused_all_fields = True
        for field in ALERT_TEXT_FIELDS:
            value = _compact_text(alert.get(field))
            if not value:
                continue
            cached_value = _compact_text(cached.get(field))
            if cached_value:
                alert[field] = cached_value
                provider_hint = "cached"
                continue
            reused_all_fields = False
            texts.append(value)
            slots.append(("alert", (index, field)))
        if reused_all_fields and cached:
            provider_hint = "cached"

    if isinstance(panel, dict):
        for section, field in FORECAST_TEXT_PATHS:
            value = _compact_text(panel.get(section, {}).get(field))
            if not value:
                continue
            texts.append(value)
            slots.append(("forecast", (section, field)))

    return texts, slots, provider_hint


def _apply_translation_results(
    alerts: list[dict[str, Any]],
    panel: dict[str, Any],
    slots: list[tuple[str, Any]],
    translations: list[str],
    providers: list[str],
    *,
    provider_hint: str = "none",
) -> str:
    for slot, translated in zip(slots, translations):
        clean = _compact_text(translated)
        if not clean:
            continue

        kind, target = slot
        if kind == "alert":
            index, field = target
            alerts[index][field] = clean
        else:
            section, field = target
            panel.setdefault(section, {})[field] = clean

    provider = _pick_provider(providers)
    if provider == "none" and provider_hint == "cached":
        return "cached"
    return provider or provider_hint or "none"


def _translate_space_weather_content(
    alerts: list[dict[str, Any]],
    panel: dict[str, Any],
    *,
    existing_alerts: dict[str, dict[str, str]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    texts, slots, provider_hint = _build_translation_plan(
        alerts,
        panel,
        existing_alerts=existing_alerts,
    )
    if not texts:
        return alerts, panel, provider_hint or "none"

    try:
        result = asyncio.run(
            translate_batch_with_meta(
                texts,
                SPACE_WEATHER_TARGET_LANG,
            )
        )
        translations = result.get("translations") or texts
        providers = result.get("providers") or ["unknown" for _ in texts]
    except Exception as exc:
        logger.warning("Space weather translation failed; keeping source text: %s", exc)
        translations = texts
        providers = ["failed" for _ in texts]

    provider = _apply_translation_results(
        alerts,
        panel,
        slots,
        translations,
        providers,
        provider_hint=provider_hint,
    )
    return alerts, panel, provider


def _replace_swpc_alerts(con, alerts: list[dict[str, Any]], *, fetched_at: str) -> int:
    con.execute("DELETE FROM swpc_alerts")
    saved = 0
    for alert in alerts:
        insert_swpc_alert(
            con,
            dedupe_key=alert["dedupe_key"],
            product_id=alert.get("product_id"),
            message_code=alert.get("message_code"),
            serial_number=alert.get("serial_number"),
            issue_datetime=alert["issue_datetime"],
            issue_time_text=alert.get("issue_time"),
            headline=alert.get("headline") or "",
            message_type=alert.get("message_type"),
            noaa_scale=alert.get("noaa_scale"),
            details=alert.get("details"),
            impacts_text=alert.get("impacts_text"),
            description_text=alert.get("description_text"),
            message_raw=alert.get("message_raw") or "",
            source_name="SWPC Alerts",
            source_url=SWPC_ALERTS_URL,
            fetched_at=fetched_at,
        )
        saved += 1
    return saved


def _replace_swpc_forecast(con, panel: dict[str, Any], *, fetched_at: str) -> int:
    con.execute("DELETE FROM swpc_forecasts")
    insert_swpc_forecast(
        con,
        forecast_key=SWPC_FORECAST_KEY,
        forecast_issued_at=panel.get("forecast_issued_at"),
        geomag_issued_at=panel.get("geomag_issued_at"),
        panel=panel,
        raw_forecast_text=panel.get("raw_forecast_text") or "",
        raw_geomag_text=panel.get("raw_geomag_text") or "",
        source_name="SWPC Forecast",
        source_url=SWPC_3DAY_FORECAST_URL,
        fetched_at=fetched_at,
    )
    return 1


def ingest_swpc_payloads(raw: dict[str, Any], *, db_path: str = "app.db") -> dict[str, Any]:
    initialize_database(db_path)
    fetched_at = raw.get("fetched_at") or utc_now_iso()

    alerts_payload = raw.get("alerts_payload")
    forecast_text = raw.get("forecast_text")
    geomag_text = raw.get("geomag_text")

    alerts_result = {
        "status": "skipped",
        "saved": 0,
        "found": 0,
        "error": "",
    }
    forecast_result = {
        "status": "skipped",
        "saved": 0,
        "error": "",
    }

    con = get_connection(db_path)
    try:
        alerts_source_id = upsert_source(
            con,
            name="SWPC Alerts",
            source_type="space_weather",
            url=SWPC_ALERTS_URL,
            enabled=True,
        )
        forecast_source_id = upsert_source(
            con,
            name="SWPC Forecast",
            source_type="space_weather",
            url=SWPC_3DAY_FORECAST_URL,
            enabled=True,
        )
        upsert_source(
            con,
            name="SWPC Geomagnetic Forecast",
            source_type="space_weather",
            url=SWPC_3DAY_GEOMAG_URL,
            enabled=True,
        )

        existing_alerts = _read_existing_alert_cache(con)
        existing_forecast = _read_existing_forecast_cache(con)

        parsed_alerts: list[dict[str, Any]] = []
        panel: dict[str, Any] | None = None
        translation_provider = "none"

        if alerts_payload is not None:
            try:
                parsed_alerts = parse_swpc_alerts_payload(
                    alerts_payload,
                    max_items=ALERTS_MAX_ITEMS,
                    max_age_days=ALERTS_MAX_AGE_DAYS,
                )
            except Exception as exc:
                alerts_result.update({"status": "failed", "error": str(exc)})
                record_fetch_run(
                    con=con,
                    source_id=alerts_source_id,
                    source_name=SWPC_ALERTS_FETCH_RUN,
                    started_at=fetched_at,
                    finished_at=fetched_at,
                    status="failed",
                    items_found=0,
                    new_items=0,
                    error_message=str(exc),
                )
        else:
            record_fetch_run(
                con=con,
                source_id=alerts_source_id,
                source_name=SWPC_ALERTS_FETCH_RUN,
                started_at=fetched_at,
                finished_at=fetched_at,
                status="failed",
                items_found=0,
                new_items=0,
                error_message="alerts payload missing",
            )
            alerts_result.update({"status": "failed", "error": "alerts payload missing"})

        if forecast_text is not None and geomag_text is not None:
            try:
                panel = parse_swpc_forecast_payloads(forecast_text, geomag_text)
                panel["raw_forecast_text"] = forecast_text
                panel["raw_geomag_text"] = geomag_text
            except Exception as exc:
                forecast_result.update({"status": "failed", "error": str(exc)})
                record_fetch_run(
                    con=con,
                    source_id=forecast_source_id,
                    source_name=SWPC_FORECAST_FETCH_RUN,
                    started_at=fetched_at,
                    finished_at=fetched_at,
                    status="failed",
                    items_found=0,
                    new_items=0,
                    error_message=str(exc),
                )
        else:
            missing_parts = []
            if forecast_text is None:
                missing_parts.append("forecast_text")
            if geomag_text is None:
                missing_parts.append("geomag_text")
            error_message = f"missing forecast inputs: {', '.join(missing_parts)}"
            forecast_result.update({"status": "failed", "error": error_message})
            record_fetch_run(
                con=con,
                source_id=forecast_source_id,
                source_name=SWPC_FORECAST_FETCH_RUN,
                started_at=fetched_at,
                finished_at=fetched_at,
                status="failed",
                items_found=0,
                new_items=0,
                error_message=error_message,
            )

        if panel and existing_forecast:
            same_forecast_inputs = (
                _compact_text(existing_forecast.get("raw_forecast_text")) == _compact_text(forecast_text)
                and _compact_text(existing_forecast.get("raw_geomag_text")) == _compact_text(geomag_text)
            )
            if same_forecast_inputs and isinstance(existing_forecast.get("panel"), dict):
                cached_panel = existing_forecast["panel"]
                if _reuse_cached_forecast_translations(panel, cached_panel):
                    translation_provider = "cached"

        if parsed_alerts or panel:
            parsed_alerts, panel, combined_provider = _translate_space_weather_content(
                parsed_alerts,
                panel or {},
                existing_alerts=existing_alerts,
            )
            if combined_provider != "none":
                translation_provider = combined_provider

        if alerts_result["status"] != "failed" and alerts_payload is not None:
            try:
                saved = _replace_swpc_alerts(con, parsed_alerts, fetched_at=fetched_at)
                alerts_result.update(
                    {
                        "status": "success",
                        "saved": saved,
                        "found": len(parsed_alerts),
                        "error": "",
                        "translation_provider": translation_provider,
                    }
                )
                record_fetch_run(
                    con=con,
                    source_id=alerts_source_id,
                    source_name=SWPC_ALERTS_FETCH_RUN,
                    started_at=fetched_at,
                    finished_at=fetched_at,
                    status="success",
                    items_found=len(parsed_alerts),
                    new_items=saved,
                    error_message="",
                )
            except Exception as exc:
                alerts_result.update({"status": "failed", "error": str(exc)})
                record_fetch_run(
                    con=con,
                    source_id=alerts_source_id,
                    source_name=SWPC_ALERTS_FETCH_RUN,
                    started_at=fetched_at,
                    finished_at=fetched_at,
                    status="failed",
                    items_found=0,
                    new_items=0,
                    error_message=str(exc),
                )

        if forecast_result["status"] != "failed" and panel:
            try:
                saved = _replace_swpc_forecast(con, panel, fetched_at=fetched_at)
                forecast_result.update(
                    {
                        "status": "success",
                        "saved": saved,
                        "error": "",
                        "translation_provider": translation_provider,
                    }
                )
                record_fetch_run(
                    con=con,
                    source_id=forecast_source_id,
                    source_name=SWPC_FORECAST_FETCH_RUN,
                    started_at=fetched_at,
                    finished_at=fetched_at,
                    status="success",
                    items_found=1,
                    new_items=saved,
                    error_message="",
                )
            except Exception as exc:
                forecast_result.update({"status": "failed", "error": str(exc)})
                record_fetch_run(
                    con=con,
                    source_id=forecast_source_id,
                    source_name=SWPC_FORECAST_FETCH_RUN,
                    started_at=fetched_at,
                    finished_at=fetched_at,
                    status="failed",
                    items_found=0,
                    new_items=0,
                    error_message=str(exc),
                )

        con.commit()
    finally:
        con.close()

    return {
        "fetched_at": fetched_at,
        "alerts": alerts_result,
        "forecast": forecast_result,
    }


def fetch_space_weather_once(*, db_path: str = "app.db") -> dict[str, Any]:
    bundle = fetch_swpc_payloads()
    raw = {
        "fetched_at": bundle.get("fetched_at") or utc_now_iso(),
        "alerts_payload": bundle["alerts"].payload if bundle["alerts"].success else None,
        "forecast_text": bundle["forecast"].content if bundle["forecast"].success else None,
        "geomag_text": bundle["geomag"].content if bundle["geomag"].success else None,
    }

    result = ingest_swpc_payloads(raw, db_path=db_path)

    if not bundle["alerts"].success:
        result["alerts"].update({"status": "failed", "error": bundle["alerts"].error_message})
    if not bundle["forecast"].success or not bundle["geomag"].success:
        errors = [
            part.error_message
            for part in (bundle["forecast"], bundle["geomag"])
            if not part.success and part.error_message
        ]
        if errors:
            result["forecast"].update({"status": "failed", "error": " | ".join(errors)})
    return result


SPACE_LAUNCHES_FETCH_RUN = "Space/Launches"
SPACE_LAUNCHES_SNAPSHOT_KEY = "space_launches_default"
SPACE_LAUNCHES_DEFAULT_LIMIT = 10


def _replace_space_launch_snapshot(con, items: list[dict[str, Any]], *, fetched_at: str) -> int:
    con.execute("DELETE FROM space_launch_snapshots")
    insert_space_launch_snapshot(
        con,
        snapshot_key=SPACE_LAUNCHES_SNAPSHOT_KEY,
        items=items,
        source_name=SPACE_LAUNCHES_SOURCE_NAME,
        source_url=SPACE_LAUNCHES_URL,
        fetched_at=fetched_at,
    )
    return len(items)


def ingest_space_launch_snapshot(raw: dict[str, Any], *, db_path: str = "app.db") -> dict[str, Any]:
    initialize_database(db_path)
    fetched_at = raw.get("fetched_at") or utc_now_iso()
    items = raw.get("items") or []

    con = get_connection(db_path)
    try:
        source_id = upsert_source(
            con,
            name=SPACE_LAUNCHES_SOURCE_NAME,
            source_type="space_launches",
            url=SPACE_LAUNCHES_URL,
            enabled=True,
        )

        cleaned_items: list[dict[str, str]] = []
        for row in list(items)[:SPACE_LAUNCHES_DEFAULT_LIMIT]:
            cleaned_items.append({
                "date": str(row.get("date") or "").strip(),
                "vehicle": str(row.get("vehicle") or "").strip(),
                "launch_site": str(row.get("launch_site") or "").strip(),
                "country": str(row.get("country") or "").strip(),
                "category": str(row.get("category") or "").strip(),
                "outcome": str(row.get("outcome") or "").strip(),
                "actual_payload_capacity": str(row.get("actual_payload_capacity") or "").strip(),
                "starlink_mission": str(row.get("starlink_mission") or "").strip(),
            })

        saved = _replace_space_launch_snapshot(con, cleaned_items, fetched_at=fetched_at)
        record_fetch_run(
            con=con,
            source_id=source_id,
            source_name=SPACE_LAUNCHES_FETCH_RUN,
            started_at=fetched_at,
            finished_at=fetched_at,
            status="success",
            items_found=len(cleaned_items),
            new_items=saved,
            error_message="",
        )
        con.commit()
        return {
            "fetched_at": fetched_at,
            "status": "success",
            "saved": saved,
            "found": len(cleaned_items),
            "items": cleaned_items,
        }
    except Exception as exc:
        try:
            record_fetch_run(
                con=con,
                source_id=source_id,
                source_name=SPACE_LAUNCHES_FETCH_RUN,
                started_at=fetched_at,
                finished_at=fetched_at,
                status="failed",
                items_found=0,
                new_items=0,
                error_message=str(exc),
            )
            con.commit()
        except Exception:
            pass
        raise
    finally:
        con.close()


def fetch_space_launches_once(*, db_path: str = "app.db", limit: int = SPACE_LAUNCHES_DEFAULT_LIMIT) -> dict[str, Any]:
    result = fetch_space_launches(limit=limit)
    if not result.success:
        return {
            "fetched_at": result.fetched_at,
            "status": "failed",
            "saved": 0,
            "found": 0,
            "items": [],
            "error": result.error_message,
            "status_code": result.status_code,
        }

    payload = {
        "fetched_at": result.fetched_at,
        "items": result.items,
    }
    ingested = ingest_space_launch_snapshot(payload, db_path=db_path)
    ingested["status_code"] = result.status_code
    return ingested
