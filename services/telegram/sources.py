from __future__ import annotations

import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SOURCES_DIR = PROJECT_ROOT / "sources"
TELEGRAM_SOURCES_FILE = SOURCES_DIR / "telegram.json"


def _read_json_file(file_path: Path) -> Any:
    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {file_path}: {exc}") from exc


def _normalize_source(raw: dict[str, Any], source_type: str, index: int, file_path: Path) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"Item #{index} in {file_path} must be an object")

    name = raw.get("name")
    url = raw.get("url")
    enabled = raw.get("enabled", True)

    if not name or not isinstance(name, str):
        raise ValueError(f"Item #{index} in {file_path} is missing a valid 'name'")

    if not url or not isinstance(url, str):
        raise ValueError(f"Item #{index} in {file_path} is missing a valid 'url'")

    if not isinstance(enabled, bool):
        raise ValueError(f"Item #{index} in {file_path} has non-boolean 'enabled'")

    return {
        "name": name.strip(),
        "url": url.strip(),
        "enabled": enabled,
        "source_type": source_type,
    }


def load_sources(file_path: Path, source_type: str, enabled_only: bool = True) -> list[dict[str, Any]]:
    data = _read_json_file(file_path)

    if not isinstance(data, list):
        raise ValueError(f"{file_path} must contain a JSON array")

    sources: list[dict[str, Any]] = []
    for index, raw in enumerate(data, start=1):
        source = _normalize_source(raw, source_type=source_type, index=index, file_path=file_path)

        if enabled_only and not source["enabled"]:
            continue

        sources.append(source)

    return sources


def load_telegram_sources(enabled_only: bool = True) -> list[dict[str, Any]]:
    return load_sources(
        file_path=TELEGRAM_SOURCES_FILE,
        source_type="telegram",
        enabled_only=enabled_only,
    )
