from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from storage import get_connection
from services.f1.xfeed import get_f1_live_from_feed


def _loads(value: str | None) -> dict:
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return {}


def _round_date_text(round_info: dict[str, Any] | None) -> str:
    if not isinstance(round_info, dict):
        return ""
    date_text = str(round_info.get("date_text") or "").strip()
    if date_text:
        return date_text
    start = str(round_info.get("start_date") or "").strip()
    end = str(round_info.get("end_date") or "").strip()
    if start and end and start != end:
        return f"{start} to {end}"
    return start or end


def _pick_next_round(strategy: dict[str, Any]) -> dict[str, Any]:
    # In between-rounds mode, primary_target is the next race in the current
    # strategy output. Fall back to best_available for older strategy payloads.
    for key in ("primary_target", "next_round", "best_available_flashscore_target"):
        value = strategy.get(key)
        if isinstance(value, dict) and value:
            return value
    return {}


def _pick_previous_round(strategy: dict[str, Any]) -> dict[str, Any]:
    for key in ("previous_round", "fallback_results"):
        value = strategy.get(key)
        if isinstance(value, dict) and value:
            return value
    return {}


def _between_rounds_payload(*, result: dict[str, Any], payload: dict[str, Any], strategy: dict[str, Any]) -> dict[str, Any]:
    next_round = _pick_next_round(strategy)
    previous_round = _pick_previous_round(strategy)

    next_name = str(next_round.get("name") or "").strip()
    next_dates = _round_date_text(next_round)
    previous_name = str(previous_round.get("name") or "").strip()

    if next_name and next_dates:
        subheadline = f"No live F1 session right now. Next race: {next_name} · {next_dates}."
    elif next_name:
        subheadline = f"No live F1 session right now. Next race: {next_name}."
    else:
        subheadline = "No live F1 session right now."

    # This is a normal state, not an availability failure. The live feed may
    # report "No usable Flashscore round URL" when the current event has ended,
    # but the dashboard should render an idle state instead of a red error.
    payload.update(
        {
            "mode": "between_rounds",
            "headline": "F1",
            "subheadline": subheadline,
            "session_name": None,
            "country": None,
            "lap_info": None,
            "page_url": next_round.get("flashscore_url") or result.get("page_url"),
            "feed_url": None,
            "round": next_round or {},
            "strategy": {
                "mode": "between_rounds",
                "primary_target": strategy.get("primary_target"),
                "fallback_results": strategy.get("fallback_results"),
                "best_available_flashscore_target": strategy.get("best_available_flashscore_target"),
                "previous_round": previous_round or None,
                "next_round": next_round or None,
            },
            "session": {},
            "rows": [],
            "session_count": 0,
            "has_data": False,
            "error": None,
            "message": "No live F1 session right now.",
            "source": "between_rounds",
        }
    )
    return payload


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

        if str(payload.get("mode") or "").lower() == "between_rounds":
            return _between_rounds_payload(result=result, payload=payload, strategy=strategy)

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
