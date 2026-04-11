from __future__ import annotations

import json

from storage import get_connection



def get_space_weather_alerts(limit: int = 30, db_path: str = "app.db") -> list[dict]:
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            SELECT *
            FROM swpc_alerts
            ORDER BY datetime(issue_datetime) DESC, id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

        items = [dict(row) for row in rows]
        for item in items:
            details_json = item.pop("details_json", None)
            try:
                item["details"] = json.loads(details_json) if details_json else {}
            except Exception:
                item["details"] = {}
        return items
    finally:
        con.close()



def get_space_weather_forecast(db_path: str = "app.db") -> dict:
    con = get_connection(db_path)
    try:
        row = con.execute(
            """
            SELECT *
            FROM swpc_forecasts
            ORDER BY datetime(fetched_at) DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return {
                "forecast_key": None,
                "forecast_issued_at": None,
                "geomag_issued_at": None,
                "fetched_at": None,
                "panel": None,
                "raw_forecast_text": "",
                "raw_geomag_text": "",
            }

        item = dict(row)
        panel_json = item.pop("panel_json", None)
        try:
            item["panel"] = json.loads(panel_json) if panel_json else None
        except Exception:
            item["panel"] = None
        return item
    finally:
        con.close()



def get_space_launches_latest(db_path: str = "app.db") -> dict:
    con = get_connection(db_path)
    try:
        row = con.execute(
            """
            SELECT *
            FROM space_launch_snapshots
            ORDER BY datetime(fetched_at) DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return {
                "snapshot_key": None,
                "fetched_at": None,
                "source_name": None,
                "source_url": None,
                "items": [],
            }

        item = dict(row)
        items_json = item.pop("items_json", None)
        try:
            item["items"] = json.loads(items_json) if items_json else []
        except Exception:
            item["items"] = []
        return item
    finally:
        con.close()
