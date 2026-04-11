from __future__ import annotations

import json

from storage import get_connection

NEWS_WINDOW_HOURS = 12


def _loads_json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        data = json.loads(value)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def get_news_timeline_latest(*, limit: int = 120, window_hours: int = NEWS_WINDOW_HOURS, db_path: str = "app.db") -> dict:
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            SELECT id,
                   source_key,
                   source_name,
                   channel,
                   topic,
                   title,
                   url,
                   published_at,
                   published_text,
                   author_names_json,
                   list_url,
                   fetched_at,
                   created_at
            FROM news_timeline_items
            WHERE datetime(COALESCE(published_at, fetched_at, created_at)) >= datetime('now', ?)
            ORDER BY datetime(COALESCE(published_at, fetched_at, created_at)) DESC, id DESC
            LIMIT ?
            """,
            (f"-{int(window_hours)} hours", int(limit)),
        ).fetchall()

        items: list[dict] = []
        source_counts: dict[str, int] = {}
        for row in rows:
            item = dict(row)
            item["author_names"] = _loads_json_list(item.pop("author_names_json", None))
            source_key = str(item.get("source_key") or "").strip()
            source_counts[source_key] = source_counts.get(source_key, 0) + 1
            items.append(item)

        updated_row = con.execute(
            """
            SELECT MAX(fetched_at) AS updated_at
            FROM news_timeline_items
            WHERE datetime(COALESCE(published_at, fetched_at, created_at)) >= datetime('now', ?)
            """,
            (f"-{int(window_hours)} hours",),
        ).fetchone()

        return {
            "updated_at": updated_row["updated_at"] if updated_row else None,
            "window_hours": int(window_hours),
            "count": len(items),
            "source_counts": source_counts,
            "items": items,
        }
    finally:
        con.close()
