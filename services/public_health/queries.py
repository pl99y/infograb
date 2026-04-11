from __future__ import annotations

from storage import get_connection

PUBLIC_HEALTH_EARLY_WARNING_KEEP_DAYS = 3
PUBLIC_HEALTH_OUTBREAK_KEEP_YEARS = 2


def _fetch_rows(con, category_key: str, limit: int) -> list[dict]:
    if category_key == "early_warning":
        where_clause = f"""
        category_key = ?
          AND datetime(COALESCE(published_at, fetched_at, created_at)) >= datetime('now', '-{PUBLIC_HEALTH_EARLY_WARNING_KEEP_DAYS} days')
        """
    elif category_key == "outbreak_event":
        where_clause = f"""
        category_key = ?
          AND CAST(strftime('%Y', COALESCE(published_at, fetched_at, created_at)) AS INTEGER) >= CAST(strftime('%Y', 'now') AS INTEGER) - {PUBLIC_HEALTH_OUTBREAK_KEEP_YEARS - 1}
        """
    else:
        where_clause = "category_key = ?"

    rows = con.execute(
        f"""
        SELECT id, source_key, source_name, category_key,
               title_raw, title_zh, date_raw, published_at,
               item_url, list_url, rank, translation_provider,
               fetched_at, created_at
        FROM public_health_events
        WHERE {where_clause}
        ORDER BY COALESCE(published_at, fetched_at) DESC, rank ASC, id DESC
        LIMIT ?
        """,
        (category_key, limit),
    ).fetchall()

    output = []
    for row in rows:
        item = dict(row)
        item["title"] = item.get("title_zh") or item.get("title_raw") or ""
        output.append(item)
    return output


def get_public_health_latest(
    *,
    limit_early_warning: int = 80,
    limit_outbreak_events: int = 80,
    db_path: str = "app.db",
) -> dict:
    con = get_connection(db_path)
    try:
        early_warning = _fetch_rows(con, "early_warning", limit_early_warning)
        outbreak_events = _fetch_rows(con, "outbreak_event", limit_outbreak_events)

        row = con.execute(
            "SELECT MAX(fetched_at) AS updated_at FROM public_health_events"
        ).fetchone()
        updated_at = row["updated_at"] if row else None

        return {
            "updated_at": updated_at,
            "early_warning": early_warning,
            "outbreak_events": outbreak_events,
        }
    finally:
        con.close()
