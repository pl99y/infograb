from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from collectors.common import DEFAULT_HEADERS


SWPC_ALERTS_URL = "https://services.swpc.noaa.gov/products/alerts.json"
SWPC_3DAY_FORECAST_URL = "https://services.swpc.noaa.gov/text/3-day-forecast.txt"
SWPC_3DAY_GEOMAG_URL = "https://services.swpc.noaa.gov/text/3-day-geomag-forecast.txt"


@dataclass(frozen=True)
class SwpcFetchResult:
    source_name: str
    source_type: str
    url: str
    success: bool
    status_code: int | None
    content: str
    fetched_at: str
    error_message: str = ""


@dataclass(frozen=True)
class SwpcJsonFetchResult(SwpcFetchResult):
    payload: Any = None



def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()



def _request(url: str, *, timeout: int = 20) -> requests.Response:
    return requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)



def fetch_swpc_json(source_name: str, url: str, *, timeout: int = 20) -> SwpcJsonFetchResult:
    fetched_at = utc_now_iso()
    try:
        response = _request(url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        return SwpcJsonFetchResult(
            source_name=source_name,
            source_type="space_weather",
            url=url,
            success=True,
            status_code=response.status_code,
            content=response.text,
            payload=payload,
            fetched_at=fetched_at,
            error_message="",
        )
    except requests.RequestException as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        return SwpcJsonFetchResult(
            source_name=source_name,
            source_type="space_weather",
            url=url,
            success=False,
            status_code=status_code,
            content="",
            payload=None,
            fetched_at=fetched_at,
            error_message=str(exc),
        )
    except ValueError as exc:
        return SwpcJsonFetchResult(
            source_name=source_name,
            source_type="space_weather",
            url=url,
            success=False,
            status_code=200,
            content="",
            payload=None,
            fetched_at=fetched_at,
            error_message=f"JSON decode error: {exc}",
        )



def fetch_swpc_text(source_name: str, url: str, *, timeout: int = 20) -> SwpcFetchResult:
    fetched_at = utc_now_iso()
    try:
        response = _request(url, timeout=timeout)
        response.raise_for_status()
        return SwpcFetchResult(
            source_name=source_name,
            source_type="space_weather",
            url=url,
            success=True,
            status_code=response.status_code,
            content=response.text,
            fetched_at=fetched_at,
            error_message="",
        )
    except requests.RequestException as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        return SwpcFetchResult(
            source_name=source_name,
            source_type="space_weather",
            url=url,
            success=False,
            status_code=status_code,
            content="",
            fetched_at=fetched_at,
            error_message=str(exc),
        )



def fetch_swpc_payloads(*, timeout: int = 20) -> dict[str, Any]:
    alerts = fetch_swpc_json("SWPC Alerts", SWPC_ALERTS_URL, timeout=timeout)
    forecast = fetch_swpc_text("SWPC 3-Day Forecast", SWPC_3DAY_FORECAST_URL, timeout=timeout)
    geomag = fetch_swpc_text("SWPC 3-Day Geomagnetic Forecast", SWPC_3DAY_GEOMAG_URL, timeout=timeout)

    fetched_candidates = [
        result.fetched_at
        for result in (alerts, forecast, geomag)
        if getattr(result, "fetched_at", None)
    ]
    fetched_at = max(fetched_candidates) if fetched_candidates else utc_now_iso()

    return {
        "fetched_at": fetched_at,
        "alerts": alerts,
        "forecast": forecast,
        "geomag": geomag,
    }
