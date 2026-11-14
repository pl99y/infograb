from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(os.environ.get("INFOGRAB_DB_PATH", str(PROJECT_ROOT / "runtime" / "app.db")))
DATA_DIR = Path(os.environ.get("INFOGRAB_DATA_DIR", str(PROJECT_ROOT / "docs" / "data"))).resolve()

sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from storage import (  # noqa: E402
    cleanup_old_data,
    cleanup_orphaned_media_files,
    get_connection,
    initialize_database,
    save_dashboard_snapshot,
)
from services.energy.pipeline import fetch_energy_once  # noqa: E402
from services.energy.queries import get_energy_latest  # noqa: E402
from services.market.pipeline import fetch_market_snapshots_once  # noqa: E402
from services.market.queries import get_market_snapshots_latest  # noqa: E402
from services.aviation.pipeline import fetch_aviation_once  # noqa: E402
from services.aviation.queries import get_airport_alerts, get_airport_disruptions_latest  # noqa: E402
from services.disaster.pipeline import refresh_natural_hazards_once  # noqa: E402
from services.disaster.queries import get_weather_alerts, get_disaster_ongoing_groups  # noqa: E402
from services.telegram.pipeline import fetch_telegram_once  # noqa: E402
from services.telegram.queries import get_recent_posts  # noqa: E402
from services.f1.pipeline import fetch_f1_live_once  # noqa: E402
from services.f1.queries import fetch_f1_live_payload, get_f1_live  # noqa: E402
from services.news_timeline.pipeline import fetch_news_timeline_once  # noqa: E402
from services.news_timeline.queries import get_news_timeline_latest  # noqa: E402
from services.hotsearch.pipeline import fetch_hotsearch_snapshot  # noqa: E402
from services.hotsearch.queries import get_hotsearch_latest  # noqa: E402

from scripts.export_status import collect, f1_live_status, hotsearch_status, publish_exports, read_object  # noqa: E402

EXPORT_LABEL = "15m"


def env_bool(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def normalize_telegram(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in items:
        row = dict(item)
        media_rows = []
        for media in item.get("media", []) or []:
            m = dict(media)
            m.pop("local_path", None)
            media_rows.append(m)
        row["media"] = media_rows
        out.append(row)
    return out


def collect_snapshot(
    attempts: dict, filename: str, load: Callable, classify: Callable, *, db_path: str,
) -> None:
    def fetch() -> dict:
        payload = load()
        status = classify(payload)
        if status in {"success", "partial"}:
            save_dashboard_snapshot(filename, payload, db_path=db_path)
        return {"status": status}

    collect(attempts, filename, fetch)


def fetch_all(skip_fetch: bool) -> dict:
    attempts = {}
    if skip_fetch:
        return attempts

    db_path = str(DB_PATH)
    collect(attempts, "energy_latest.json", lambda: fetch_energy_once(db_path=db_path), evidence=("quotes_saved",))
    collect(attempts, "market_latest.json", lambda: fetch_market_snapshots_once(db_path=db_path))
    collect(
        attempts, "aviation", lambda: fetch_aviation_once(db_path=db_path),
        evidence=("alerts_found", "disruptions_found"),
        split={"aviation_alerts.json": "alerts", "aviation_disruptions.json": "disruptions"},
    )
    collect(
        attempts, "natural_hazards", lambda: refresh_natural_hazards_once(db_path=db_path),
        evidence=("alerts_found", "events_found"),
        split={"weather_alerts.json": "weather", "disaster_ongoing.json": "disaster"},
    )
    collect(attempts, "telegram.json", lambda: fetch_telegram_once(db_path=db_path), evidence=("sources_processed",))
    # Keep the existing SQLite snapshot collection; the published live feed has its own status.
    try:
        fetch_f1_live_once(db_path=db_path)
    except Exception as exc:
        print(f"[warn] F1 snapshot collection failed ({type(exc).__name__})")
    collect(attempts, "news_timeline_latest.json", lambda: fetch_news_timeline_once(db_path=db_path), evidence=("items_found",))
    collect_snapshot(
        attempts, "news_hotsearch_latest.json", fetch_hotsearch_snapshot, hotsearch_status, db_path=db_path,
    )
    collect_snapshot(
        attempts, "f1_live.json", lambda: fetch_f1_live_payload(db_path=db_path), f1_live_status, db_path=db_path,
    )
    return attempts


def export_all(attempts: dict) -> None:
    db_path = str(DB_PATH)
    exports = {
        "energy_latest.json": lambda: get_energy_latest(db_path=db_path),
        "market_latest.json": lambda: get_market_snapshots_latest(db_path=db_path),
        "aviation_alerts.json": lambda: get_airport_alerts(limit=30, db_path=db_path),
        "aviation_disruptions.json": lambda: get_airport_disruptions_latest(
            region="worldwide", direction="departures", limit=20, db_path=db_path,
        ),
        "weather_alerts.json": lambda: get_weather_alerts(limit=-1, db_path=db_path),
        "disaster_ongoing.json": lambda: get_disaster_ongoing_groups(limit=80, db_path=db_path),
        "telegram.json": lambda: normalize_telegram(get_recent_posts(limit=50, db_path=db_path, per_source=True)),
        "news_timeline_latest.json": lambda: get_news_timeline_latest(limit=-1, window_hours=12, db_path=db_path),
        "news_hotsearch_latest.json": lambda: (
            get_hotsearch_latest(db_path=db_path)
            or read_object(DATA_DIR / "news_hotsearch_latest.json")
            or {"ok": False, "sources": [], "items": []}
        ),
        "f1_live.json": lambda: (
            get_f1_live(db_path=db_path)
            or read_object(DATA_DIR / "f1_live.json")
            or {"error": "f1_feed_unavailable", "rows": [], "has_data": False}
        ),
    }
    publish_exports(DATA_DIR, EXPORT_LABEL, exports, attempts)


def cleanup_runtime() -> None:
    """Keep the local SQLite DB and old downloaded media from growing forever.

    This runs after JSON export, so cleanup will not remove anything needed by the
    current dashboard output. Set INFOGRAB_CLEANUP=0 to disable it temporarily.
    Set INFOGRAB_VACUUM=0 to skip VACUUM if you need a faster run.
    """
    if not env_bool("INFOGRAB_CLEANUP", True):
        print("[ok] cleanup skipped because INFOGRAB_CLEANUP=0")
        return

    con = get_connection(str(DB_PATH))
    try:
        stats = cleanup_old_data(con)
        media_stats = cleanup_orphaned_media_files(con)
        con.commit()

        print("[ok] cleanup database:", json.dumps(stats, ensure_ascii=False, sort_keys=True))
        print("[ok] cleanup media:", json.dumps(media_stats, ensure_ascii=False, sort_keys=True))

        if env_bool("INFOGRAB_VACUUM", True):
            con.execute("VACUUM")
            print("[ok] vacuumed database")

        con.execute("PRAGMA optimize")
    finally:
        con.close()


def safe_cleanup_runtime(skip_cleanup: bool) -> None:
    if skip_cleanup:
        print("[ok] cleanup skipped by --skip-cleanup")
        return
    try:
        cleanup_runtime()
    except Exception as exc:
        # Cleanup should never block publishing freshly exported JSON.
        print(f"[warn] cleanup failed: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export 15-minute dashboard JSON files into docs/data")
    parser.add_argument("--skip-fetch", action="store_true", help="Write JSON from the existing database without running fetchers first.")
    parser.add_argument("--skip-cleanup", action="store_true", help="Do not prune old database rows or orphan media files after export.")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    initialize_database(str(DB_PATH))

    attempts = fetch_all(skip_fetch=args.skip_fetch)
    export_all(attempts)
    safe_cleanup_runtime(skip_cleanup=args.skip_cleanup)
    print(f"Exported {EXPORT_LABEL} profile JSON into {DATA_DIR}")


if __name__ == "__main__":
    main()

