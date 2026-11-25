"""
Aviation query helpers for API / dashboard usage.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from storage import get_connection


def get_airport_alerts(limit: int = 100, db_path: str = "app.db", window_hours: int = 48) -> list[dict]:
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            SELECT *
            FROM airport_alerts
            WHERE datetime(fetched_at) >= datetime('now', ?)
            ORDER BY datetime(fetched_at) DESC, id DESC
            """,
            (f"-{int(window_hours)} hours",),
        ).fetchall()

        items = [dict(r) for r in rows]
        for item in items:
            extra_json = item.pop("extra_json", None)
            try:
                item["extra"] = json.loads(extra_json) if extra_json else {}
            except Exception:
                item["extra"] = {}
        def detected_at(item):
            value = item["extra"].get("last_seen_utc") or item.get("fetched_at")
            try:
                stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)
            except (ValueError, TypeError):
                return datetime.min.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
        items = [item for item in items if detected_at(item) >= cutoff]
        items.sort(key=lambda item: (detected_at(item), item["id"]), reverse=True)
        return items if limit < 0 else items[:limit]
    finally:
        con.close()


def get_airport_disruptions_latest(
    region: str = "worldwide",
    direction: str = "departures",
    limit: int = 50,
    db_path: str = "app.db",
) -> list[dict]:
    con = get_connection(db_path)
    try:
        latest = con.execute(
            """
            SELECT fetched_at
            FROM airport_disruptions
            WHERE region = ? AND direction = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (region, direction),
        ).fetchone()

        if latest:
            rows = con.execute(
                """
                SELECT *
                FROM airport_disruptions
                WHERE region = ? AND direction = ? AND fetched_at = ?
                ORDER BY rank ASC, id ASC
                LIMIT ?
                """,
                (region, direction, latest["fetched_at"], limit),
            ).fetchall()
        else:
            fallback = con.execute(
                """
                SELECT region, fetched_at
                FROM airport_disruptions
                WHERE direction = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (direction,),
            ).fetchone()
            if not fallback:
                return []
            rows = con.execute(
                """
                SELECT *
                FROM airport_disruptions
                WHERE region = ? AND direction = ? AND fetched_at = ?
                ORDER BY rank ASC, id ASC
                LIMIT ?
                """,
                (fallback["region"], direction, fallback["fetched_at"], limit),
            ).fetchall()

        items = [dict(r) for r in rows]
        for item in items:
            extra_json = item.pop("extra_json", None)
            try:
                item["extra"] = json.loads(extra_json) if extra_json else {}
            except Exception:
                item["extra"] = {}
        return items
    finally:
        con.close()


def get_airport_disruptions_history(
    region: str = "worldwide",
    direction: str = "departures",
    limit: int = 200,
    db_path: str = "app.db",
) -> list[dict]:
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            SELECT *
            FROM airport_disruptions
            WHERE region = ? AND direction = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (region, direction, limit),
        ).fetchall()

        if not rows:
            rows = con.execute(
                """
                SELECT *
                FROM airport_disruptions
                WHERE direction = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (direction, limit),
            ).fetchall()

        items = [dict(r) for r in rows]
        for item in items:
            extra_json = item.pop("extra_json", None)
            try:
                item["extra"] = json.loads(extra_json) if extra_json else {}
            except Exception:
                item["extra"] = {}
        return items
    finally:
        con.close()
