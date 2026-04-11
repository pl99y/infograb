from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import re
from typing import Any
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

FLASHSCORE_OVERVIEW_URL = "https://www.flashscore.com/auto-racing/formula-1/"
AUTOSPORT_F1_NEWS_URL = "https://www.autosport.com/f1/news/"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


@dataclass
class RawFetchResult:
    success: bool
    fetched_at: str
    url: str
    text: str | None = None
    status_code: int | None = None
    error_message: str | None = None
    extra: dict[str, Any] | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fetch_text(url: str, timeout: int = 25) -> RawFetchResult:
    fetched_at = _now_iso()
    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
        if resp.ok:
            return RawFetchResult(success=True, fetched_at=fetched_at, url=url, text=resp.text, status_code=resp.status_code)
        return RawFetchResult(success=False, fetched_at=fetched_at, url=url, status_code=resp.status_code, error_message=f"HTTP {resp.status_code}")
    except Exception as exc:
        logger.warning("F1 fetch failed for %s: %s", url, exc)
        return RawFetchResult(success=False, fetched_at=fetched_at, url=url, error_message=str(exc))


def fetch_flashscore_overview(url: str = FLASHSCORE_OVERVIEW_URL) -> RawFetchResult:
    return _fetch_text(url)


_GRAND_PRIX_RE = re.compile(r"/auto-racing/formula-1/[^\"'#?]+grand-prix/?", re.I)


def discover_flashscore_gp_url(overview_html: str, base_url: str = FLASHSCORE_OVERVIEW_URL) -> str:
    candidates = []
    seen = set()
    for match in _GRAND_PRIX_RE.finditer(overview_html):
        href = match.group(0)
        full = urljoin(base_url, href)
        low = full.lower()
        if any(part in low for part in ["/archive", "/standings", "/results", "/news/"]):
            continue
        if full not in seen:
            seen.add(full)
            candidates.append(full)
    return candidates[0] if candidates else base_url


def fetch_flashscore_gp_page(gp_url: str) -> RawFetchResult:
    return _fetch_text(gp_url)


def fetch_autosport_f1_news_page(url: str = AUTOSPORT_F1_NEWS_URL) -> RawFetchResult:
    return _fetch_text(url)
