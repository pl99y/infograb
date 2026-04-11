from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from storage import get_connection, initialize_database


def _loads(value: str | None):
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return {}


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
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


def _severity_rank(level: str | None) -> int:
    value = str(level or "").strip().lower()
    return {
        "critical": 0,
        "extreme": 0,
        "red": 0,
        "high": 1,
        "orange": 2,
        "moderate": 2,
        "medium": 2,
        "elevated": 2,
        "green": 3,
        "low": 3,
        "info": 4,
    }.get(value, 5)


def get_weather_alerts(limit: int = 100, db_path: str = "app.db") -> list[dict]:
    initialize_database(db_path)
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            SELECT *
            FROM weather_alerts
            ORDER BY datetime(COALESCE(issued_at, effective_at, fetched_at)) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "dedupe_key": row["dedupe_key"],
                "source_primary": row["source_primary"],
                "title": row["title"],
                "summary": row["summary"],
                "severity_level": row["severity_level"],
                "color_level": row["color_level"],
                "event_type": row["event_type"],
                "location_text": row["location_text"],
                "issued_at": row["issued_at"],
                "effective_at": row["effective_at"],
                "expires_at": row["expires_at"],
                "source_url": row["source_url"],
                "detail_url": row["detail_url"],
                "payload": _loads(row["payload_json"]),
                "fetched_at": row["fetched_at"],
                "published_at": row["issued_at"] or row["effective_at"],
                "updated_at": row["fetched_at"],
                "region_text": row["location_text"] or row["summary"],
                "link_url": row["detail_url"] or row["source_url"],
            }
            for row in rows
        ]
    finally:
        con.close()


def _keep_disaster_item(item: dict) -> bool:
    now_utc = datetime.now(timezone.utc)
    event_type = str(item.get("event_type") or "").strip().lower()
    status = str(item.get("status") or "").strip().lower()
    occurred_dt = _parse_dt(item.get("occurred_at") or item.get("started_at"))
    updated_dt = _parse_dt(item.get("updated_at"))
    fetched_dt = _parse_dt(item.get("fetched_at"))
    ref_dt = updated_dt or fetched_dt or occurred_dt

    if event_type == "earthquake":
        mag = None
        try:
            mag = float((item.get("payload") or {}).get("mag"))
        except Exception:
            mag = None
        if mag is None or mag < 5.0:
            return False
        return ref_dt is not None and ref_dt >= now_utc - timedelta(days=7)

    if event_type == "volcano":
        level = str(item.get("severity_level") or "").strip().lower()
        if level not in {"extreme", "very_high", "high"}:
            return False
        return status in {"active", "current", "watch", "monitoring"} or (
            ref_dt is not None and ref_dt >= now_utc - timedelta(days=7)
        )

    if event_type == "flood":
        level = str(item.get("severity_level") or "").strip().lower()
        if level not in {"red", "orange"}:
            return False
        return ref_dt is not None and ref_dt >= now_utc - timedelta(days=30)

    if event_type == "typhoon":
        if occurred_dt is not None and occurred_dt > now_utc + timedelta(minutes=5):
            return False
        return status in {"active", "current", "watch", "monitoring", "high"} or (
            ref_dt is not None and ref_dt >= now_utc - timedelta(days=7)
        )

    if event_type == "tsunami":
        return status in {"active", "current", "watch", "monitoring", "high"} or (
            ref_dt is not None and ref_dt >= now_utc - timedelta(days=7)
        )

    return ref_dt is not None and ref_dt >= now_utc - timedelta(days=7)


def _row_to_disaster(row):
    return {
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
        "external_id": row["external_id"],
        "external_id_secondary": row["external_id_secondary"],
        "dedupe_key": row["dedupe_key"],
        "status": row["status"],
        "map_url": row["map_url"],
        "payload": _loads(row["payload_json"]),
    }


def _row_to_ongoing(row, member_event_ids: list[int]) -> dict:
    payload = _loads(row["payload_json"])
    item = {
        "id": int(row["id"]),
        "group_key": row["group_key"],
        "event_type": row["event_type"],
        "severity_level": row["severity_level"],
        "severity_color": row["severity_color"],
        "title": row["title"],
        "summary": row["summary"],
        "started_at": row["started_at"],
        "updated_at": row["updated_at"],
        "location_text": row["location_text"],
        "lat": row["lat"],
        "lon": row["lon"],
        "source_primary": row["source_primary"],
        "source_secondary": row["source_secondary"],
        "status": row["status"],
        "map_url": row["map_url"],
        "official_link": row["official_link"],
        "country_code": row["country_code"],
        "payload": payload,
        "member_event_ids": member_event_ids,
    }
    # compatibility aliases for existing front-end expectations
    item["occurred_at"] = row["started_at"]
    item["published_at"] = row["started_at"]
    item["fetched_at"] = row["started_at"]
    return item


def _sort_disaster_items(items: list[dict]) -> list[dict]:
    items.sort(
        key=lambda item: (
            _parse_dt(item.get("occurred_at"))
            or _parse_dt(item.get("started_at"))
            or _parse_dt(item.get("published_at"))
            or _parse_dt(item.get("updated_at"))
            or _parse_dt(item.get("fetched_at"))
            or datetime(1970, 1, 1, tzinfo=timezone.utc),
            -_severity_rank(item.get("severity_level")),
        ),
        reverse=True,
    )
    return items


def get_disaster_instant(limit: int = 100, db_path: str = "app.db") -> list[dict]:
    return get_disaster_ongoing_groups(limit=limit, db_path=db_path)


def get_disaster_events(limit: int = 100, db_path: str = "app.db") -> list[dict]:
    return get_disaster_ongoing_groups(limit=limit, db_path=db_path)


def _load_ongoing_groups(con, limit: int) -> list[dict]:
    rows = con.execute(
        """
        SELECT g.*
        FROM disaster_ongoing_groups g
        ORDER BY datetime(COALESCE(g.started_at, g.created_at, g.updated_at)) DESC, g.id DESC
        LIMIT ?
        """,
        (max(limit * 3, 100),),
    ).fetchall()
    if not rows:
        return []

    group_ids = [int(row["id"]) for row in rows]
    members_by_group: dict[int, list[int]] = {group_id: [] for group_id in group_ids}
    placeholders = ",".join("?" for _ in group_ids)
    member_rows = con.execute(
        f"SELECT group_id, event_id FROM disaster_ongoing_members WHERE group_id IN ({placeholders}) ORDER BY id ASC",
        tuple(group_ids),
    ).fetchall()
    for member_row in member_rows:
        members_by_group.setdefault(int(member_row["group_id"]), []).append(int(member_row["event_id"]))

    items = [_row_to_ongoing(row, members_by_group.get(int(row["id"]), [])) for row in rows]
    items = [item for item in items if _keep_disaster_item(item)]
    return _sort_disaster_items(items)[:limit]


def _load_disaster_events(con, limit: int) -> list[dict]:
    rows = con.execute(
        """
        SELECT *
        FROM disaster_events
        ORDER BY datetime(COALESCE(occurred_at, updated_at, fetched_at)) DESC, id DESC
        LIMIT ?
        """,
        (max(limit * 10, 300),),
    ).fetchall()
    items = [_row_to_disaster(row) for row in rows]
    items = [item for item in items if _keep_disaster_item(item)]
    return _sort_disaster_items(items)[:limit]


def get_disaster_ongoing_groups(limit: int = 100, db_path: str = "app.db") -> list[dict]:
    initialize_database(db_path)
    con = get_connection(db_path)
    try:
        items = _load_ongoing_groups(con, limit=limit)
        if items:
            return items
        return _load_disaster_events(con, limit=limit)
    finally:
        con.close()
