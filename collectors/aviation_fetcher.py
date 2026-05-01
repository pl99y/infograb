from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote

import requests

from collectors.common import DEFAULT_HEADERS, RawFetchResult, fetch_html, utc_now_iso


ADSBFI_BASE_URL = "https://opendata.adsb.fi/api"
ADSBFI_SQUAWK_CODES = ("7700", "7600", "7500")
ADSBFI_HEADERS = {
    "User-Agent": "InfoGrab-ADSBfi-EmergencySquawk/0.1 (+https://adsb.fi/)",
    "Accept": "application/json,text/plain,*/*",
}


@dataclass
class RawJsonFetchResult:
    source_name: str
    source_type: str
    url: str
    success: bool
    status_code: Optional[int]
    payload: dict[str, Any] | None
    fetched_at: str
    error_message: str = ""
    content_type: str | None = None


def fetch_aviation_page(source_name: str, url: str) -> RawFetchResult:
    success, status_code, html, error_message = fetch_html(url=url)

    return RawFetchResult(
        source_name=source_name,
        source_type="aviation",
        url=url,
        success=success,
        status_code=status_code,
        html=html,
        fetched_at=utc_now_iso(),
        error_message=error_message,
    )


def adsbfi_squawk_url(squawk: str) -> str:
    return f"{ADSBFI_BASE_URL}/v2/sqk/{quote(str(squawk).strip())}"


def fetch_adsbfi_squawk(squawk: str, *, timeout: int = 20) -> RawJsonFetchResult:
    """Fetch aircraft currently broadcasting one emergency squawk code.

    adsb.fi's public API is a live snapshot. It does not provide an AirNav-style
    historical list, so the service layer is responsible for retaining events in
    SQLite for the dashboard's 48-hour window.
    """
    url = adsbfi_squawk_url(squawk)
    fetched_at = utc_now_iso()
    headers = DEFAULT_HEADERS.copy()
    headers.update(ADSBFI_HEADERS)

    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        content_type = response.headers.get("content-type", "")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return RawJsonFetchResult(
                source_name="ADSB.fi Emergency Squawk",
                source_type="aviation",
                url=url,
                success=False,
                status_code=response.status_code,
                payload=None,
                fetched_at=fetched_at,
                error_message="ADSB.fi response was not a JSON object",
                content_type=content_type,
            )

        return RawJsonFetchResult(
            source_name="ADSB.fi Emergency Squawk",
            source_type="aviation",
            url=url,
            success=True,
            status_code=response.status_code,
            payload=payload,
            fetched_at=fetched_at,
            error_message="",
            content_type=content_type,
        )
    except requests.RequestException as exc:
        response = getattr(exc, "response", None)
        return RawJsonFetchResult(
            source_name="ADSB.fi Emergency Squawk",
            source_type="aviation",
            url=url,
            success=False,
            status_code=getattr(response, "status_code", None),
            payload=None,
            fetched_at=fetched_at,
            error_message=str(exc),
            content_type=(response.headers.get("content-type", "") if response is not None else None),
        )
    except ValueError as exc:
        return RawJsonFetchResult(
            source_name="ADSB.fi Emergency Squawk",
            source_type="aviation",
            url=url,
            success=False,
            status_code=None,
            payload=None,
            fetched_at=fetched_at,
            error_message=f"Could not decode JSON: {exc}",
            content_type=None,
        )
