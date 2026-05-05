from __future__ import annotations

from typing import Any

from services.hotsearch.pipeline import fetch_hotsearch_snapshot


def get_hotsearch_latest(*, db_path: str = "app.db") -> dict[str, Any]:
    # db_path is accepted for consistency with other query helpers.
    # Hotsearch is a lightweight current snapshot and does not need SQLite history.
    return fetch_hotsearch_snapshot(include_ai_digest=True)
