from __future__ import annotations

import logging
from typing import Any

from collectors.mnd_pla_fetcher import (
    MND_PLA_LIST_URL,
    fetch_mnd_pla_detail_page,
    fetch_mnd_pla_list_page,
)
from parsers.mnd_pla_parser import (
    daily_records_to_dicts,
    list_entries_to_dicts,
    parse_mnd_pla_detail_page,
    parse_mnd_pla_list_page,
    summarize_mnd_pla_records,
)
from storage import (
    get_connection,
    initialize_database,
    insert_mnd_pla_daily,
    record_fetch_run,
    upsert_source,
)

logger = logging.getLogger(__name__)

MND_PLA_FETCH_RUN = "MND/PLA Activity"
MND_PLA_DB_SOURCE_NAME = "MND PLA Daily"



def _record_failed_run(*, db_path: str, started_at: str, error_message: str) -> None:
    con = get_connection(db_path)
    try:
        record_fetch_run(
            con=con,
            source_id=None,
            source_name=MND_PLA_FETCH_RUN,
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



def _replace_mnd_pla_daily(con, records: list[dict[str, Any]], *, fetched_at: str) -> int:
    con.execute("DELETE FROM mnd_pla_daily")
    saved = 0
    for record in records:
        insert_mnd_pla_daily(
            con,
            report_date=record.get("published_date"),
            published_date_raw=record.get("published_date_raw"),
            period_start=record.get("period_start"),
            period_end=record.get("period_end"),
            report_period_raw=record.get("report_period_raw"),
            title=record.get("title") or "",
            post_url=record.get("url") or "",
            body=record.get("body") or "",
            activity_text=record.get("activity_text") or "",
            no_aircraft=bool(record.get("no_aircraft")),
            aircraft_total=record.get("aircraft_total"),
            aircraft_intrusion_total=record.get("aircraft_intrusion_total"),
            ship_total=record.get("ship_total"),
            official_ship_total=record.get("official_ship_total"),
            balloon_total=record.get("balloon_total"),
            intrusion_areas=record.get("intrusion_areas") or [],
            source_name=MND_PLA_DB_SOURCE_NAME,
            source_url=record.get("url") or MND_PLA_LIST_URL,
            fetched_at=fetched_at,
        )
        saved += 1
    return saved



def fetch_mnd_pla_once(db_path: str = "app.db", *, limit: int = 7) -> dict[str, Any]:
    initialize_database(db_path)

    list_fetch = fetch_mnd_pla_list_page()
    started_at = list_fetch.fetched_at
    if not list_fetch.success:
        error_message = list_fetch.error_message or "Failed to fetch MND PLA list page."
        logger.error("MND PLA list fetch failed: %s", error_message)
        _record_failed_run(db_path=db_path, started_at=started_at, error_message=error_message)
        return {
            "fetched_at": started_at,
            "status": "failed",
            "records_found": 0,
            "records_saved": 0,
            "errors": [error_message],
            "used_insecure_fallback": list_fetch.used_insecure_fallback,
        }

    try:
        list_entries = parse_mnd_pla_list_page(
            list_fetch.html,
            list_url=list_fetch.url,
            page=1,
        )
    except Exception as exc:
        error_message = f"List parse failed: {exc}"
        logger.exception("MND PLA list parse failed")
        _record_failed_run(db_path=db_path, started_at=started_at, error_message=error_message)
        return {
            "fetched_at": started_at,
            "status": "failed",
            "records_found": 0,
            "records_saved": 0,
            "errors": [error_message],
            "used_insecure_fallback": list_fetch.used_insecure_fallback,
        }

    selected_entries = list_entries[: max(0, int(limit))]
    parsed_records = []
    errors: list[str] = []
    used_insecure = list_fetch.used_insecure_fallback

    for entry in selected_entries:
        detail_fetch = fetch_mnd_pla_detail_page(entry.detail_url)
        used_insecure = used_insecure or detail_fetch.used_insecure_fallback
        if not detail_fetch.success:
            message = f"{entry.detail_url}: {detail_fetch.error_message or 'detail fetch failed'}"
            logger.warning("MND PLA detail fetch failed: %s", message)
            errors.append(message)
            continue

        try:
            parsed = parse_mnd_pla_detail_page(entry.detail_url, detail_fetch.html)
            parsed_records.append(parsed)
        except Exception as exc:
            message = f"{entry.detail_url}: {exc}"
            logger.warning("MND PLA detail parse failed: %s", message)
            errors.append(message)

    records_json = daily_records_to_dicts(parsed_records)
    summary = summarize_mnd_pla_records(records_json, days=limit)

    should_replace = bool(records_json) and len(records_json) == len(selected_entries)

    con = get_connection(db_path)
    try:
        source_id = upsert_source(
            con,
            name=MND_PLA_DB_SOURCE_NAME,
            source_type="mnd_pla",
            url=MND_PLA_LIST_URL,
            enabled=True,
        )

        saved = 0
        if should_replace:
            saved = _replace_mnd_pla_daily(con, records_json, fetched_at=started_at)
            status = "success" if not errors else "partial"
        else:
            status = "partial" if records_json or errors else "failed"
            if not records_json:
                errors = list(errors) + ["no records parsed; existing mnd_pla_daily snapshot preserved"]
            elif len(records_json) != len(selected_entries):
                errors = list(errors) + [
                    f"parsed {len(records_json)} of {len(selected_entries)} requested records; existing snapshot preserved"
                ]

        record_fetch_run(
            con=con,
            source_id=source_id,
            source_name=MND_PLA_FETCH_RUN,
            started_at=started_at,
            finished_at=started_at,
            status=status,
            items_found=len(selected_entries),
            new_items=saved,
            error_message=" | ".join(errors),
        )
        con.commit()
    finally:
        con.close()

    result: dict[str, Any] = {
        "fetched_at": started_at,
        "status": status,
        "records_found": len(selected_entries),
        "records_parsed": len(records_json),
        "records_saved": saved,
        "errors": errors,
        "used_insecure_fallback": used_insecure,
        "list_items": list_entries_to_dicts(selected_entries),
        "records": records_json,
    }
    result.update(summary)
    return result
