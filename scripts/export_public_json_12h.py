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
from services.public_health.pipeline import fetch_public_health_once  # noqa: E402
from services.public_health.queries import get_public_health_latest  # noqa: E402
from services.space_weather.pipeline import fetch_space_weather_once, fetch_space_launches_once  # noqa: E402
from services.space_weather.queries import (  # noqa: E402
    get_space_weather_alerts,
    get_space_weather_forecast,
    get_space_launches_latest,
)
from services.mnd_pla.pipeline import fetch_mnd_pla_once  # noqa: E402
from services.mnd_pla.queries import get_mnd_pla_dashboard  # noqa: E402
from services.f1.pipeline import fetch_f1_news_once  # noqa: E402
from services.f1.queries import get_f1_news  # noqa: E402

TargetFn = Callable[[], Any]
EXPORT_LABEL = "12h"


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
        safe_run("space_weather", lambda: fetch_space_weather_once(db_path=str(DB_PATH))),
        safe_run("space_launches", lambda: fetch_space_launches_once(db_path=str(DB_PATH))),
        safe_run("mnd_pla", lambda: fetch_mnd_pla_once(limit=7, db_path=str(DB_PATH))),
        safe_run("public_health", lambda: fetch_public_health_once(db_path=str(DB_PATH))),
        safe_run("f1_news", lambda: fetch_f1_news_once(db_path=str(DB_PATH))),
    ]


def export_all(steps: list[dict[str, Any]]) -> None:
    run_at = utc_now_iso()
    exports = {
        "space_weather_alerts.json": get_space_weather_alerts(limit=30, db_path=str(DB_PATH)),
        "space_weather_forecast.json": get_space_weather_forecast(db_path=str(DB_PATH)),
        "space_launches_latest.json": get_space_launches_latest(db_path=str(DB_PATH)),
        "mnd_pla_dashboard.json": get_mnd_pla_dashboard(days=7, db_path=str(DB_PATH)),
        "public_health_latest.json": get_public_health_latest(
            limit_early_warning=120,
            limit_outbreak_events=120,
            db_path=str(DB_PATH),
        ),
        "f1_news.json": get_f1_news(limit=24, db_path=str(DB_PATH)),
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
    parser = argparse.ArgumentParser(description="Export 12-hour dashboard JSON files into docs/data")
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

