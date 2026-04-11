from __future__ import annotations

from urllib.parse import urlparse

from collectors.common import RawFetchResult, fetch_html, utc_now_iso


SINA_SC0_PAGE_URL = "https://gu.sina.cn/ft/hq/nf.php?symbol=sc0"


def _headers_for_energy_url(url: str) -> dict:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()

    if host == "hq.sinajs.cn":
        return {
            "Referer": SINA_SC0_PAGE_URL,
            "Origin": "https://gu.sina.cn",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

    if host == "gu.sina.cn":
        return {
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

    return {}


def fetch_energy_page(source_name: str, url: str) -> RawFetchResult:
    success, status_code, html, error_message = fetch_html(
        url=url,
        headers=_headers_for_energy_url(url),
    )

    return RawFetchResult(
        source_name=source_name,
        source_type="energy",
        url=url,
        success=success,
        status_code=status_code,
        html=html,
        fetched_at=utc_now_iso(),
        error_message=error_message,
    )
