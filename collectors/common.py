from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


@dataclass
class RawFetchResult:
    source_name: str
    source_type: str
    url: str
    success: bool
    status_code: Optional[int]
    html: str
    fetched_at: str
    error_message: str = ""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_html(
    url: str,
    timeout: int = 20,
    headers: Optional[dict] = None,
) -> tuple[bool, Optional[int], str, str]:
    """
    Fetch raw HTML from a URL.

    Returns:
        (success, status_code, html, error_message)
    """
    merged_headers = DEFAULT_HEADERS.copy()
    if headers:
        merged_headers.update(headers)

    try:
        response = requests.get(
            url,
            headers=merged_headers,
            timeout=timeout,
        )
        response.raise_for_status()
        return True, response.status_code, response.text, ""
    except requests.RequestException as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        return False, status_code, "", str(exc)