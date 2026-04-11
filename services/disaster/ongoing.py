from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.disaster.queries import _keep_disaster_item
from storage import (
    clear_disaster_ongoing,
    get_connection,
    initialize_database,
    record_fetch_run,
    replace_disaster_ongoing_members,
    upsert_disaster_ongoing_group,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        import json
        return json.loads(value)
    except Exception:
        return {}


def _normalize_status(value: str | None) -> str:
    text = (value or "").strip().lower()
    if text in {"active", "watch", "monitoring", "stabilizing", "ended", "current", "high"}:
        return text
    return "active"


def upsert_disaster_ongoing_records(
    records: list[dict[str, Any]],
    *,
    source_name: str = "Disaster/Ongoing",
    db_path: str = "app.db",
) -> dict[str, Any]:
    initialize_database(db_path)
    started_at = _utc_now_iso()
    con = get_connection(db_path)
    try:
        clear_disaster_ongoing(con)
        created = 0
        updated = 0
        processed = 0

        for raw in records:
            group_id, was_created = upsert_disaster_ongoing_group(
                con=con,
                group_key=str(raw["group_key"]),
                event_type=str(raw["event_type"]),
                source_primary=str(raw.get("source_primary") or "official"),
                source_secondary=raw.get("source_secondary"),
                title=str(raw["title"]),
                summary=raw.get("summary"),
                severity_level=raw.get("severity_level"),
                severity_color=raw.get("severity_color"),
                status=_normalize_status(raw.get("status")),
                started_at=raw.get("started_at"),
                updated_at=raw.get("updated_at") or started_at,
                location_text=raw.get("location_text"),
                lat=raw.get("lat"),
                lon=raw.get("lon"),
                country_code=raw.get("country_code"),
                map_url=raw.get("map_url"),
                official_link=raw.get("official_link"),
                payload=raw.get("payload") or {},
            )
            replace_disaster_ongoing_members(
                con=con,
                group_id=group_id,
                event_ids=[int(x) for x in raw.get("member_event_ids", [])],
            )
            processed += 1
            if was_created:
                created += 1
            else:
                updated += 1

        finished_at = _utc_now_iso()
        record_fetch_run(
            con=con,
            source_id=None,
            source_name=source_name,
            started_at=started_at,
            finished_at=finished_at,
            status="success",
            items_found=processed,
            new_items=created,
            error_message="",
        )
        con.commit()
        return {
            "groups_processed": processed,
            "groups_created": created,
            "groups_updated": updated,
            "errors": [],
        }
    except Exception as exc:
        finished_at = _utc_now_iso()
        record_fetch_run(
            con=con,
            source_id=None,
            source_name=source_name,
            started_at=started_at,
            finished_at=finished_at,
            status="failed",
            items_found=0,
            new_items=0,
            error_message=str(exc),
        )
        con.commit()
        return {
            "groups_processed": 0,
            "groups_created": 0,
            "groups_updated": 0,
            "errors": [str(exc)],
        }
    finally:
        con.close()


def rebuild_disaster_ongoing_from_events(*, db_path: str = "app.db") -> dict[str, Any]:
    initialize_database(db_path)
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            SELECT *
            FROM disaster_events
            ORDER BY datetime(COALESCE(occurred_at, updated_at, fetched_at)) DESC, id DESC
            """
        ).fetchall()

        records: list[dict[str, Any]] = []
        for row in rows:
            item = {
                "id": int(row["id"]),
                "event_type": row["event_type"],
                "severity_level": row["severity_level"],
                "severity_color": row["severity_color"],
                "title": row["title"],
                "summary": row["summary"],
                "occurred_at": row["occurred_at"],
                "updated_at": row["updated_at"],
                "fetched_at": row["fetched_at"],
                "location_text": row["location_text"],
                "lat": row["lat"],
                "lon": row["lon"],
                "source_primary": row["source_primary"],
                "source_secondary": row["source_secondary"],
                "status": row["status"],
                "map_url": row["map_url"],
                "payload": _loads(row["payload_json"]),
                "external_id": row["external_id"],
                "external_id_secondary": row["external_id_secondary"],
                "dedupe_key": row["dedupe_key"],
            }
            if not _keep_disaster_item(item):
                continue
            records.append(
                {
                    "group_key": f"{item['event_type']}|{item['dedupe_key']}",
                    "event_type": item["event_type"],
                    "source_primary": item["source_primary"],
                    "source_secondary": item["source_secondary"],
                    "title": item["title"] or item["event_type"],
                    "summary": item["summary"],
                    "severity_level": item["severity_level"],
                    "severity_color": item["severity_color"],
                    "status": item["status"],
                    "started_at": item["occurred_at"],
                    "updated_at": item["updated_at"] or item["fetched_at"],
                    "location_text": item["location_text"],
                    "lat": item["lat"],
                    "lon": item["lon"],
                    "country_code": None,
                    "map_url": item["map_url"],
                    "official_link": item["map_url"],
                    "payload": item["payload"],
                    "member_event_ids": [item["id"]],
                }
            )
    finally:
        con.close()

    return upsert_disaster_ongoing_records(records, db_path=db_path)
