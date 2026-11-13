"""
Telegram query helpers for API / dashboard usage.
"""
from __future__ import annotations

from collections import defaultdict

from storage import get_connection


def get_recent_posts(limit: int = 50, db_path: str = "app.db") -> list[dict]:
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            WITH ranked AS (
                SELECT items.*, ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY published_at DESC, id DESC) AS source_rank
                FROM items
            )
            SELECT i.id, i.post_url, i.content_text_clean AS text,
                   i.content_text_zh AS text_zh,
                   i.content_text_en AS text_en,
                   i.published_at, i.fetched_at,
                   s.name AS source_name, s.source_type
            FROM ranked i
            JOIN sources s ON i.source_id = s.id
            WHERE s.source_type = ?
            ORDER BY CASE WHEN i.source_rank <= 3 THEN 0 ELSE 1 END, i.published_at DESC, i.id DESC
            LIMIT ?
            """,
            ("telegram", limit),
        ).fetchall()

        items = sorted([dict(r) for r in rows], key=lambda r: (r["published_at"] or "", r["id"]), reverse=True)
        if not items:
            return []

        item_ids = [str(item["id"]) for item in items]
        placeholders = ",".join("?" * len(item_ids))

        media_rows = con.execute(
            f"""
            SELECT item_id, media_type, media_url, local_path
            FROM item_media
            WHERE item_id IN ({placeholders})
            """,
            item_ids,
        ).fetchall()

        media_map = defaultdict(list)
        for media_row in media_rows:
            media_map[media_row["item_id"]].append(dict(media_row))

        for item in items:
            item["media"] = media_map[item["id"]]

        return items
    finally:
        con.close()
