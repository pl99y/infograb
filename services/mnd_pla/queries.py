from __future__ import annotations

import json

from parsers.mnd_pla_parser import summarize_mnd_pla_records
from storage import get_connection



def _loads(value: str | None):
    if not value:
        return []
    try:
        return json.loads(value)
    except Exception:
        return []



def get_mnd_pla_latest(limit: int = 7, db_path: str = "app.db") -> list[dict]:
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            SELECT report_date,
                   published_date_raw,
                   period_start,
                   period_end,
                   report_period_raw,
                   title,
                   post_url,
                   body,
                   activity_text,
                   no_aircraft,
                   aircraft_total,
                   aircraft_intrusion_total,
                   ship_total,
                   official_ship_total,
                   balloon_total,
                   intrusion_areas_json,
                   source_name,
                   source_url,
                   fetched_at
            FROM mnd_pla_daily
            ORDER BY report_date DESC, id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

        items = []
        for row in rows:
            item = dict(row)
            item["published_date"] = item.get("report_date")
            item["no_aircraft"] = bool(item.get("no_aircraft"))
            item["intrusion_areas"] = _loads(item.pop("intrusion_areas_json", None))
            items.append(item)
        return items
    finally:
        con.close()



def get_mnd_pla_dashboard(days: int = 7, db_path: str = "app.db") -> dict:
    items = get_mnd_pla_latest(limit=days, db_path=db_path)
    summary = summarize_mnd_pla_records(items, days=days)

    fetched_values = [str(item.get("fetched_at") or "").strip() for item in items if item.get("fetched_at")]
    fetched_at = max(fetched_values) if fetched_values else None

    return {
        "items": items,
        "summary": summary,
        "fetched_at": fetched_at,
    }
