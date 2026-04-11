from __future__ import annotations

import json
from datetime import datetime, timezone

from storage import get_connection
from services.f1.xfeed import get_f1_live_from_feed


def _loads(value: str | None) -> dict:
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return {}


def _get_latest_live_snapshot(db_path: str = "app.db") -> dict | None:
    con = get_connection(db_path)
    try:
        snapshot = con.execute(
            """
            SELECT s.*
            FROM f1_live_snapshots s
            WHERE EXISTS (
                SELECT 1
                FROM f1_live_rows r
                WHERE r.snapshot_id = s.id
            )
            ORDER BY s.id DESC
            LIMIT 1
            """
        ).fetchone()
        if not snapshot:
            return None

        rows = con.execute(
            """
            SELECT position, driver_name, team_name, result_text, extra_json
            FROM f1_live_rows
            WHERE snapshot_id = ?
            ORDER BY position ASC, id ASC
            """,
            (snapshot["id"],),
        ).fetchall()

        session = {
            "title": snapshot["gp_name"],
            "session_name": snapshot["session_type"],
            "round_name": snapshot["gp_name"],
            "rows": [],
            "raw": _loads(snapshot["extra_json"]),
        }
        mapped_rows = []
        for row in rows:
            extra = _loads(row["extra_json"])
            mapped_rows.append(
                {
                    "position": row["position"],
                    "driver": row["driver_name"],
                    "team": row["team_name"],
                    "team_code": extra.get("team_code"),
                    "result_time": row["result_text"],
                    "gap": extra.get("gap"),
                    "laps": extra.get("laps"),
                    "driver_code": extra.get("driver_code"),
                    "driver_slug": extra.get("driver_slug"),
                    "nation": extra.get("nation"),
                    "raw": extra,
                }
            )

        return {
            "mode": "snapshot",
            "headline": snapshot["gp_name"] or "F1",
            "subheadline": "",
            "session_name": snapshot["session_type"],
            "country": None,
            "lap_info": None,
            "page_url": snapshot["source_url"],
            "feed_url": None,
            "round": {"name": snapshot["gp_name"]},
            "strategy": {"mode": "snapshot", "primary_target": None, "fallback_results": None, "best_available_flashscore_target": None},
            "session": session,
            "rows": mapped_rows,
            "session_count": 1,
            "has_data": bool(mapped_rows),
            "error": None,
            "fetched_at": snapshot["fetched_at"],
            "updated_at": snapshot["fetched_at"],
            "snapshot_fetched_at": snapshot["fetched_at"],
            "source": "db_snapshot",
        }
    finally:
        con.close()


def get_f1_live(db_path: str = "app.db") -> dict:
    try:
        result = get_f1_live_from_feed()
        session = result.get("session") or {}
        rows = result.get("rows") or []
        round_info = result.get("round") or {}
        strategy = result.get("strategy") or {}

        payload = {
            "mode": result.get("mode"),
            "headline": session.get("title") or "F1",
            "subheadline": session.get("circuit") or "",
            "session_name": session.get("session_name"),
            "country": session.get("country"),
            "lap_info": session.get("lap_info"),
            "page_url": result.get("page_url"),
            "feed_url": result.get("feed_url"),
            "round": round_info,
            "strategy": {
                "mode": strategy.get("mode"),
                "primary_target": strategy.get("primary_target"),
                "fallback_results": strategy.get("fallback_results"),
                "best_available_flashscore_target": strategy.get("best_available_flashscore_target"),
            },
            "session": session,
            "rows": rows,
            "session_count": result.get("session_count", 0),
            "has_data": bool(session and rows),
            "error": result.get("error"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source": "live_feed",
        }
        if payload["has_data"]:
            return payload
    except Exception as exc:
        payload = {"error": str(exc), "rows": [], "session": None, "has_data": False, "source": "live_feed"}

    fallback = _get_latest_live_snapshot(db_path=db_path)
    if fallback:
        if payload.get("error"):
            fallback["fallback_reason"] = payload["error"]
        return fallback

    return payload


def get_f1_news(limit: int = 20, db_path: str = "app.db") -> list[dict]:
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            SELECT id, title, title_zh, summary, url, published_at, fetched_at,
                   source_name, source_url, category, is_live_text, extra_json, translation_provider
            FROM f1_news_articles
            WHERE lower(title) NOT IN ('news archive', 'archive')
              AND lower(url) NOT LIKE '%/archive%'
            ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        items = []
        for row in rows:
            item = dict(row)
            extra = _loads(item.get("extra_json"))
            title_zh = item.get("title_zh") or extra.get("title_zh") or ""
            provider = item.get("translation_provider") or extra.get("translation_provider") or ("cached" if title_zh else "")
            item["title_raw"] = item.get("title") or ""
            item["title_en"] = item.get("title") or ""
            item["title_zh"] = title_zh
            item["translation_provider"] = provider
            item["post_url"] = item.get("url")
            item["text"] = item.get("summary") or ""
            item["text_en"] = item.get("summary") or ""
            item["text_zh"] = title_zh
            items.append(item)
        return items
    finally:
        con.close()
