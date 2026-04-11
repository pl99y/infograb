from __future__ import annotations

import html as html_lib
import json
import re
from typing import Any
from urllib.parse import urljoin

import requests

from collectors.common import DEFAULT_HEADERS, utc_now_iso

PROMED_URL = "https://www.promedmail.org/"
WHO_LIST_URL = "https://www.who.int/emergencies/disease-outbreak-news"
DEFAULT_WHO_API_URL = (
    "https://www.who.int/api/emergencies/diseaseoutbreaknews"
    "?sf_provider=dynamicProvider372"
    "&sf_culture=en"
    "&$orderby=PublicationDateAndTime%20desc"
    "&$expand=EmergencyEvent"
    "&$select=Title,TitleSuffix,OverrideTitle,UseOverrideTitle,"
    "regionscountries,ItemDefaultUrl,FormattedDate,PublicationDateAndTime"
)
DEFAULT_TIMEOUT = 30


class PublicHealthFetchError(RuntimeError):
    pass


class PublicHealthFetcher:
    def __init__(self, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            **DEFAULT_HEADERS,
            "Accept-Language": "en-US,en;q=0.9",
        })

    def fetch_text(self, url: str, *, headers: dict[str, str] | None = None) -> str:
        response = self.session.get(url, timeout=self.timeout, headers=headers)
        response.raise_for_status()
        return response.text

    def fetch_json(self, url: str, *, headers: dict[str, str] | None = None) -> Any:
        response = self.session.get(url, timeout=self.timeout, headers=headers)
        response.raise_for_status()
        return response.json()


def extract_who_api_url(page_html: str) -> str:
    match = re.search(
        r'hubsfiltering\([^\)]*?,\s*"[^"]+",\s*"([^"]+/api/emergencies/diseaseoutbreaknews\?[^"]+)"',
        page_html,
    )
    if match:
        return urljoin(WHO_LIST_URL, html_lib.unescape(match.group(1)))

    match = re.search(r'("/api/emergencies/diseaseoutbreaknews\?[^"]+")', page_html)
    if match:
        api_path = html_lib.unescape(match.group(1).strip('"'))
        return urljoin(WHO_LIST_URL, api_path)

    return DEFAULT_WHO_API_URL


def fetch_public_health_payloads(timeout: int = DEFAULT_TIMEOUT) -> dict[str, Any]:
    fetcher = PublicHealthFetcher(timeout=timeout)
    fetched_at = utc_now_iso()

    promed_html = fetcher.fetch_text(PROMED_URL)
    who_page_html = fetcher.fetch_text(WHO_LIST_URL)
    who_api_url = extract_who_api_url(who_page_html)
    who_api_payload = fetcher.fetch_json(
        who_api_url,
        headers={
            "Accept": "application/json, text/plain, */*",
            "Referer": WHO_LIST_URL,
            "X-Requested-With": "XMLHttpRequest",
        },
    )

    return {
        "fetched_at": fetched_at,
        "promed": {
            "source_name": "ProMED",
            "source_key": "promed",
            "list_url": PROMED_URL,
            "html": promed_html,
        },
        "who": {
            "source_name": "WHO DON",
            "source_key": "who_don",
            "list_url": WHO_LIST_URL,
            "page_html": who_page_html,
            "api_url": who_api_url,
            "api_payload": who_api_payload,
        },
    }
