"""
Energy query helpers for API / dashboard usage.
"""
from __future__ import annotations

import json

from storage import get_connection


ENERGY_ORDER = {
    "wti": 1,
    "brent": 2,
    "murban": 3,
    "natural_gas": 4,
    "gasoline": 5,
    "china": 6,
}


def get_energy_latest(db_path: str = "app.db") -> list[dict]:
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            SELECT e.*
            FROM energy_quotes e
            JOIN (
                SELECT quote_key, MAX(id) AS max_id
                FROM energy_quotes
                GROUP BY quote_key
            ) latest
              ON e.id = latest.max_id
            ORDER BY e.id DESC
            """
        ).fetchall()

        items = [dict(r) for r in rows]
        for item in items:
            extra_json = item.pop("extra_json", None)
            try:
                item["extra"] = json.loads(extra_json) if extra_json else {}
            except Exception:
                item["extra"] = {}

        items.sort(key=lambda x: ENERGY_ORDER.get(x.get("quote_key", ""), 999))
        return items
    finally:
        con.close()


def get_energy_history(
    quote_key: str | None = None,
    limit: int = 200,
    db_path: str = "app.db",
) -> list[dict]:
    con = get_connection(db_path)
    try:
        if quote_key:
            rows = con.execute(
                """
                SELECT *
                FROM energy_quotes
                WHERE quote_key = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (quote_key, limit),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT *
                FROM energy_quotes
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
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
