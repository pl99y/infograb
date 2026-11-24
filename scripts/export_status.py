from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from services.hotsearch.pipeline import sanitize_snapshot_errors


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_object(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def write_json(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(".json.tmp")
    try:
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def collection_status(result: dict, evidence: tuple[str, ...]) -> str:
    if result.get("status") in {"failed", "partial", "skipped"}:
        return result["status"]
    if result.get("errors") or result.get("error") or result.get("ok") is False:
        return "partial" if any(result.get(key) for key in evidence) else "failed"
    return "success"


def collect(
    statuses: dict, filename: str, load: Callable, *,
    evidence: tuple[str, ...] = (), split: dict[str, str] | None = None,
) -> None:
    attempted_at = utc_now_iso()
    try:
        result = load()
    except Exception as exc:
        print(f"[warn] {filename}: collection failed ({type(exc).__name__})")
        result = {"status": "failed"}

    for output, key in (split or {filename: ""}).items():
        details = result.get(key, {"status": "failed"}) if key else result
        status = collection_status(details, evidence)
        statuses[output] = {"status": status, "last_attempt_at": attempted_at}
        print(f"[{status}] {output}: collection")


def hotsearch_status(payload: dict) -> str:
    sources = payload.get("sources", [])
    available = sum(bool(source.get("ok") and source.get("items_count")) for source in sources)
    if not available:
        return "failed"
    return "success" if available == len(sources) and (payload.get("ai_digest", {}).get("ok") or payload.get("ai_digest", {}).get("error") == "disabled") else "partial"


def f1_live_status(payload: dict) -> str:
    if payload.get("error") or payload.get("source") == "db_snapshot":
        return "failed"
    if payload.get("has_data") or payload.get("mode") == "between_rounds":
        return "success"
    return "failed"


def telegram_preview(rows: list) -> dict:
    return {
        "items": rows[:50],
        "sources": list(dict.fromkeys(row["source_name"] for row in rows if row.get("source_name"))),
        "snapshot_at": max((row.get("fetched_at") or "" for row in rows), default=""),
    }


def publish_exports(
    data_dir: Path, profile: str, exports: dict[str, Callable],
    attempts: dict,
) -> None:
    """Publish each dataset independently, retaining the previous snapshot on failure."""
    meta_path = data_dir / "export_meta.json"
    old_meta = read_object(meta_path)
    profiles = old_meta.get("profiles") or {}
    for old_profile in profiles.values():
        # Version 2 sometimes stored raw exception details in these public steps.
        for step in old_profile.get("steps", []):
            step.pop("error", None)
    old_profile = profiles.get(profile, {})
    datasets = old_profile.get("datasets", {}).copy()
    files = old_profile.get("files", {}).copy()

    # A slow publish also carries forward the fast profile's previous hotsearch file.
    hotsearch_path = data_dir / "news_hotsearch_latest.json"
    previous_hotsearch = read_object(hotsearch_path)
    if previous_hotsearch and sanitize_snapshot_errors(previous_hotsearch):
        write_json(hotsearch_path, previous_hotsearch)

    for filename, load in exports.items():
        path = data_dir / filename
        attempt = attempts.get(filename)
        status = attempt["status"] if attempt else "unverified"
        attempted_at = attempt["last_attempt_at"] if attempt else None
        success_at = datasets.get(filename, {}).get("last_success_at")

        if status != "failed" or not path.exists():
            try:
                payload = load()
                if filename == "f1_live.json" and payload.get("error"):
                    payload["error"] = "f1_feed_unavailable"
                if filename == "f1_live.json" and payload.get("fallback_reason"):
                    payload["fallback_reason"] = "f1_feed_unavailable"
                write_json(path, payload)
                files[filename] = utc_now_iso()
            except Exception as exc:
                print(f"[warn] {filename}: export failed ({type(exc).__name__})")
                status = "failed"
                attempted_at = attempted_at or utc_now_iso()

        if status == "success":
            success_at = attempted_at
        if attempted_at or filename not in datasets:
            datasets[filename] = {
                "status": status,
                "last_attempt_at": attempted_at,
                "last_success_at": success_at,
            }
        if status == "failed":
            print(f"[warn] {filename}: {'kept previous snapshot' if path.exists() else 'no snapshot available'}")

    # Fixed-size companion snapshot, derived from the exact full export that
    # survived collection/export failure handling. No additional collection.
    if "telegram.json" in exports:
        full_path = data_dir / "telegram.json"
        if full_path.exists():
            rows = json.loads(full_path.read_text(encoding="utf-8"))
            write_json(data_dir / "telegram_preview.json", telegram_preview(rows))

    published_at = utc_now_iso()
    steps = [
        {"name": name, "ok": row["status"] == "success", "status": row["status"]}
        for name, row in datasets.items()
    ]
    profiles[profile] = {
        "generated_at": published_at,
        "files": files,
        "datasets": datasets,
        "steps": steps,
    }
    write_json(meta_path, {
        "version": 3,
        "generated_at": published_at,
        "last_profile": profile,
        "profile": profile,
        "steps": steps,
        "profiles": profiles,
    })
