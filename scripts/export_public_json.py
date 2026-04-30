from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(os.environ.get("INFOGRAB_DB_PATH", str(PROJECT_ROOT / "runtime" / "app.db")))
DATA_DIR = PROJECT_ROOT / "docs" / "data"

sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from storage import (  # noqa: E402
    cleanup_old_data,
    cleanup_orphaned_media_files,
    get_connection,
    initialize_database,
)
from services.energy.pipeline import fetch_energy_once  # noqa: E402
from services.energy.queries import get_energy_latest  # noqa: E402
from services.market.pipeline import fetch_market_snapshots_once  # noqa: E402
from services.market.queries import get_market_snapshots_latest  # noqa: E402
from services.aviation.pipeline import fetch_aviation_once  # noqa: E402
from services.aviation.queries import get_airport_alerts, get_airport_disruptions_latest  # noqa: E402
from services.disaster.pipeline import fetch_weather_alerts_once, fetch_disaster_once  # noqa: E402
from services.disaster.queries import get_weather_alerts, get_disaster_ongoing_groups  # noqa: E402
from services.telegram.pipeline import fetch_telegram_once  # noqa: E402
from services.telegram.queries import get_recent_posts  # noqa: E402
from services.f1.pipeline import fetch_f1_live_once  # noqa: E402
from services.f1.queries import get_f1_live  # noqa: E402
from services.news_timeline.pipeline import fetch_news_timeline_once  # noqa: E402
from services.news_timeline.queries import get_news_timeline_latest  # noqa: E402

TargetFn = Callable[[], Any]
EXPORT_LABEL = "15m"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_bool(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_existing_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def build_export_meta(run_at: str, exported_files: list[str], steps: list[dict[str, Any]]) -> dict[str, Any]:
    path = DATA_DIR / "export_meta.json"
    meta = read_existing_json(path)
    profiles = meta.get("profiles") if isinstance(meta.get("profiles"), dict) else {}
    profiles[EXPORT_LABEL] = {
        "generated_at": run_at,
        "files": {name: run_at for name in exported_files},
        "steps": steps,
    }
    meta["version"] = 2
    meta["generated_at"] = run_at
    meta["last_profile"] = EXPORT_LABEL
    meta["profile"] = EXPORT_LABEL
    meta["steps"] = steps
    meta["profiles"] = profiles
    return meta


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


def safe_run(name: str, fn: TargetFn) -> dict[str, Any]:
    try:
        fn()
        return {"name": name, "ok": True}
    except Exception as exc:
        print(f"[warn] {name}: {exc}")
        return {"name": name, "ok": False, "error": str(exc)}


def fetch_all(skip_fetch: bool) -> list[dict[str, Any]]:
    if skip_fetch:
        return []
    return [
        safe_run("energy", lambda: fetch_energy_once(db_path=str(DB_PATH))),
        safe_run("market", lambda: fetch_market_snapshots_once(db_path=str(DB_PATH))),
        safe_run("aviation", lambda: fetch_aviation_once(db_path=str(DB_PATH))),
        safe_run("weather_alerts", lambda: fetch_weather_alerts_once(db_path=str(DB_PATH))),
        safe_run("disaster", lambda: fetch_disaster_once(db_path=str(DB_PATH))),
        safe_run("telegram", lambda: fetch_telegram_once(db_path=str(DB_PATH))),
        safe_run("f1_live", lambda: fetch_f1_live_once(db_path=str(DB_PATH))),
        safe_run("news_timeline", lambda: fetch_news_timeline_once(db_path=str(DB_PATH))),
    ]


def export_all(steps: list[dict[str, Any]]) -> None:
    run_at = utc_now_iso()
    exports = {
        "energy_latest.json": get_energy_latest(db_path=str(DB_PATH)),
        "market_latest.json": get_market_snapshots_latest(db_path=str(DB_PATH)),
        "aviation_alerts.json": get_airport_alerts(limit=30, db_path=str(DB_PATH)),
        "aviation_disruptions.json": get_airport_disruptions_latest(
            region="worldwide",
            direction="departures",
            limit=20,
            db_path=str(DB_PATH),
        ),
        "weather_alerts.json": get_weather_alerts(limit=60, db_path=str(DB_PATH)),
        "disaster_ongoing.json": get_disaster_ongoing_groups(limit=80, db_path=str(DB_PATH)),
        "telegram.json": normalize_telegram(get_recent_posts(limit=50, db_path=str(DB_PATH))),
        "news_timeline_latest.json": get_news_timeline_latest(limit=120, window_hours=12, db_path=str(DB_PATH)),
        "f1_live.json": get_f1_live(db_path=str(DB_PATH)),
    }

    for filename, payload in exports.items():
        write_json(DATA_DIR / filename, payload)
        print(f"[ok] wrote {filename}")

    write_json(DATA_DIR / "export_meta.json", build_export_meta(run_at, list(exports.keys()), steps))
    print("[ok] wrote export_meta.json")


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

    steps = fetch_all(skip_fetch=args.skip_fetch)
    export_all(steps=steps)
    safe_cleanup_runtime(skip_cleanup=args.skip_cleanup)
    print(f"Exported {EXPORT_LABEL} profile JSON into {DATA_DIR}")


if __name__ == "__main__":
    main()

