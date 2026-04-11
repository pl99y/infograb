from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from collectors.common import utc_now_iso

SPACE_LAUNCHES_URL = "https://space-data-api-46930537539.us-central1.run.app/api/public/launches/records"
SPACE_LAUNCHES_SOURCE_NAME = "AEI Launch Records API"
SPACE_LAUNCHES_REFERER = "https://spacedata.aei.org/"
SPACE_LAUNCHES_BASE_PARAMS = [
    ("metric", "launches"),
    ("groupBy", "countryGroup"),
    ("starlink", "include"),
    ("categories", "Orbital"),
    ("categories", "Deep Space"),
]
SPACE_LAUNCHES_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": SPACE_LAUNCHES_REFERER,
    "Accept": "application/json, text/plain, */*",
}
SPACE_LAUNCHES_MIN_YEAR = 1957


@dataclass
class SpaceLaunchFetchResult:
    source_name: str
    url: str
    success: bool
    status_code: int | None
    fetched_at: str
    items: list[dict[str, str]]
    error_message: str = ""
    payload: dict[str, Any] | None = None


CANONICAL_FIELDS = (
    "date",
    "vehicle",
    "launch_site",
    "country",
    "category",
    "outcome",
    "actual_payload_capacity",
    "starlink_mission",
)


def _compact(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "是" if value else "否"
    return " ".join(str(value).split()).strip()


def _format_tons(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        number = float(value)
    except Exception:
        text = _compact(value)
        return text
    if number.is_integer():
        return f"{int(number)} t"
    text = f"{number:.1f}".rstrip("0").rstrip(".")
    return f"{text} t"


def _normalize_vehicle(row: dict[str, Any]) -> str:
    primary = _compact(row.get("lvType"))
    variant = _compact(row.get("variant"))
    parent = _compact(row.get("parentName"))

    if primary and variant and variant.lower() not in primary.lower():
        return f"{primary} {variant}".strip()
    if primary:
        return primary
    if parent and variant:
        return f"{parent} {variant}".strip()
    return parent


def _normalize_payload_capacity(row: dict[str, Any]) -> str:
    parts: list[str] = []

    payload = _format_tons(row.get("payloadMetricTons"))
    leo = _format_tons(row.get("leoMetricTons"))
    sso = _format_tons(row.get("ssoMetricTons"))

    if payload:
        parts.append(f"载荷 {payload}")
    if leo:
        parts.append(f"LEO {leo}")
    if sso:
        parts.append(f"SSO {sso}")

    return " · ".join(parts)


def _normalize_starlink(row: dict[str, Any]) -> str:
    value = row.get("starlinkMission")
    if value in (None, "", False):
        return "否"
    if value is True:
        return "是"
    text = _compact(value)
    return text or "是"


def _normalize_row(row: dict[str, Any]) -> dict[str, str]:
    item = {key: "" for key in CANONICAL_FIELDS}
    item["date"] = _compact(row.get("launchDate"))
    item["vehicle"] = _normalize_vehicle(row)
    item["launch_site"] = _compact(row.get("launchSiteName"))
    item["country"] = _compact(row.get("country") or row.get("countryGroup"))
    item["category"] = _compact(row.get("launchCategory"))
    item["outcome"] = _compact(row.get("launchSuccess"))
    item["actual_payload_capacity"] = _normalize_payload_capacity(row)
    item["starlink_mission"] = _normalize_starlink(row)
    return item


def _request_rows_for_year(year: int, *, timeout: int, url: str) -> tuple[str, int | None, list[dict[str, Any]]]:
    params = [
        *SPACE_LAUNCHES_BASE_PARAMS,
        ("startYear", str(year)),
        ("endYear", str(year)),
    ]
    response = requests.get(
        url,
        params=params,
        headers=SPACE_LAUNCHES_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("rows", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        raise ValueError("Launch API response missing rows list.")
    return response.url, response.status_code, rows


def fetch_space_launches(*, limit: int = 10, timeout: int = 30, url: str = SPACE_LAUNCHES_URL) -> SpaceLaunchFetchResult:
    fetched_at = utc_now_iso()
    target = max(int(limit or 10), 1)
    current_year = datetime.now(timezone.utc).year

    try:
        items: list[dict[str, str]] = []
        seen: set[str] = set()
        final_url = url
        status_code: int | None = None

        for year in range(current_year, SPACE_LAUNCHES_MIN_YEAR - 1, -1):
            final_url, status_code, rows = _request_rows_for_year(year, timeout=timeout, url=url)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                dedupe_key = _compact(row.get("launchTag")) or "|".join([
                    _compact(row.get("launchDate")),
                    _compact(row.get("lvType")),
                    _compact(row.get("launchSiteName")),
                ])
                if not dedupe_key or dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                item = _normalize_row(row)
                if not any(item.values()):
                    continue
                items.append(item)
                if len(items) >= target:
                    break
            if len(items) >= target:
                break

        if not items:
            raise ValueError("Launch API returned no usable rows.")

        return SpaceLaunchFetchResult(
            source_name=SPACE_LAUNCHES_SOURCE_NAME,
            url=final_url,
            success=True,
            status_code=status_code,
            fetched_at=fetched_at,
            items=items,
            error_message="",
            payload=None,
        )
    except Exception as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        return SpaceLaunchFetchResult(
            source_name=SPACE_LAUNCHES_SOURCE_NAME,
            url=url,
            success=False,
            status_code=status_code,
            fetched_at=fetched_at,
            items=[],
            error_message=str(exc),
            payload=None,
        )
