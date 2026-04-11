from __future__ import annotations

import hashlib
import html as html_lib
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

PROMED_DATE_RE = re.compile(r"^[A-Z][a-z]{2} [A-Z][a-z]{2} \d{2} \d{4}$")
WHO_ITEM_BASE = "https://www.who.int/emergencies/disease-outbreak-news/item"


class PublicHealthParseError(ValueError):
    pass


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _make_dedupe_key(*parts: str) -> str:
    base = "|".join(_clean_text(p) for p in parts if _clean_text(p))
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _parse_promed_date(value: str) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        dt = datetime.strptime(text, "%a %b %d %Y").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def _parse_who_date(value: str) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        dt = datetime.strptime(text, "%d %B %Y").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def parse_promed_latest_html(html_text: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html_text, "lxml")
    rows: list[dict[str, Any]] = []

    for tr in soup.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        date_raw = _clean_text(tds[0].get_text(" ", strip=True))
        title_raw = _clean_text(tds[1].get_text(" ", strip=True))
        if not date_raw or not title_raw:
            continue
        rows.append(
            {
                "source_key": "promed",
                "source_name": "ProMED",
                "category_key": "early_warning",
                "date_raw": date_raw,
                "published_at": _parse_promed_date(date_raw),
                "title_raw": title_raw,
                "item_url": "",
                "list_url": "https://www.promedmail.org/",
                "rank": len(rows) + 1,
                "payload": {},
            }
        )

    if not rows:
        text = soup.get_text("\n", strip=True)
        lines = [_clean_text(line) for line in text.splitlines() if _clean_text(line)]
        start_idx = -1
        for i in range(len(lines) - 3):
            if lines[i] == "Latest Posts on" and lines[i + 2] == "Date" and lines[i + 3] == "Title":
                start_idx = i + 4
                break
        if start_idx == -1:
            raise PublicHealthParseError("Could not locate ProMED latest-posts block")
        i = start_idx
        while i < len(lines) - 1:
            if not PROMED_DATE_RE.match(lines[i]):
                break
            date_raw = lines[i]
            title_raw = lines[i + 1]
            rows.append(
                {
                    "source_key": "promed",
                    "source_name": "ProMED",
                    "category_key": "early_warning",
                    "date_raw": date_raw,
                    "published_at": _parse_promed_date(date_raw),
                    "title_raw": title_raw,
                    "item_url": "",
                    "list_url": "https://www.promedmail.org/",
                    "rank": len(rows) + 1,
                    "payload": {},
                }
            )
            i += 2

    for row in rows:
        row["dedupe_key"] = _make_dedupe_key(row["source_key"], row["date_raw"], row["title_raw"])

    return rows


def extract_who_region_map(page_html: str) -> dict[str, str]:
    match = re.search(r"var\s+allRegions\s*=\s*JSON\.parse\('(.+?)'\);", page_html, flags=re.DOTALL)
    if not match:
        return {}
    payload = match.group(1)
    payload = payload.encode("utf-8").decode("unicode_escape")
    payload = html_lib.unescape(payload)
    try:
        items = json.loads(payload)
    except json.JSONDecodeError:
        return {}

    output: dict[str, str] = {}
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            key = _clean_text(item.get("Id"))
            value = _clean_text(item.get("Title"))
            if key and value:
                output[key] = value
    return output


def _flatten_who_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("value", "items", "Items", "results", "Results", "data", "Data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _build_who_title(item: dict[str, Any], region_map: dict[str, str]) -> str:
    use_override = bool(item.get("UseOverrideTitle"))
    override_title = _clean_text(item.get("OverrideTitle"))
    if use_override and override_title:
        return override_title

    title = _clean_text(item.get("Title"))
    emergency_event = item.get("EmergencyEvent")
    if isinstance(emergency_event, dict):
        event_title = _clean_text(emergency_event.get("Title"))
        if event_title:
            title = event_title

    region_names: list[str] = []
    for rid in item.get("regionscountries") or []:
        rid_text = _clean_text(rid)
        if rid_text and rid_text in region_map:
            region_names.append(region_map[rid_text])

    title_suffix = _clean_text(item.get("TitleSuffix"))

    if region_names:
        joined = ", ".join(region_names)
        if joined and joined not in title:
            title = f"{title} - {joined}" if title else joined

    if title_suffix and title_suffix not in title:
        title = f"{title} - {title_suffix}" if title else title_suffix

    return _clean_text(title or "Disease Outbreak News")


def _build_who_item_url(raw_value: Any) -> str:
    raw = _clean_text(raw_value)
    if not raw:
        return ""
    if raw.startswith(("http://", "https://")):
        return raw
    if raw.startswith("/emergencies/"):
        return urljoin("https://www.who.int", raw)
    if raw.startswith("/"):
        return urljoin(WHO_ITEM_BASE, raw)
    return f"{WHO_ITEM_BASE}/{raw}"


def parse_who_api_payload(api_payload: Any, page_html: str) -> list[dict[str, Any]]:
    region_map = extract_who_region_map(page_html)
    items = _flatten_who_items(api_payload)
    rows: list[dict[str, Any]] = []

    for item in items:
        date_raw = _clean_text(item.get("FormattedDate"))
        title_raw = _build_who_title(item, region_map)
        if not date_raw and not title_raw:
            continue
        item_url = _build_who_item_url(item.get("ItemDefaultUrl"))
        row = {
            "source_key": "who_don",
            "source_name": "WHO DON",
            "category_key": "outbreak_event",
            "date_raw": date_raw,
            "published_at": _parse_who_date(date_raw),
            "title_raw": title_raw,
            "item_url": item_url,
            "list_url": "https://www.who.int/emergencies/disease-outbreak-news",
            "rank": len(rows) + 1,
            "payload": item,
        }
        row["dedupe_key"] = _make_dedupe_key(row["source_key"], item_url or row["date_raw"], row["title_raw"])
        rows.append(row)

    return rows
