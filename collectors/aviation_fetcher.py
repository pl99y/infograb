from __future__ import annotations

from collectors.common import RawFetchResult, fetch_html, utc_now_iso


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