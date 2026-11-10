from __future__ import annotations

from typing import Any

from storage import get_dashboard_snapshot


def get_hotsearch_latest(*, db_path: str = "app.db") -> dict[str, Any] | None:
    return get_dashboard_snapshot("news_hotsearch_latest.json", db_path=db_path)
