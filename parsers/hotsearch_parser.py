from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def normalize_title_key(title: str) -> str:
    value = clean_text(title).lower()
    value = re.sub(r"[\s　·・,，.。:：;；!！?？'\"“”‘’（）()\[\]【】《》<>]+", "", value)
    return value


def parse_tophub_items(
    html: str,
    *,
    source_id: str,
    source_name: str,
    source_url: str,
    limit: int,
    fetched_at: str,
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")

    rows = soup.select(".rank-all-item table tbody tr")
    if not rows:
        rows = soup.select("table tbody tr")
    if not rows:
        rows = soup.select("tr")

    items: list[dict[str, Any]] = []
    seen: set[str] = set()

    for row in rows:
        if len(items) >= limit:
            break

        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        link = row.select_one("td:nth-of-type(2) a[href]") or row.select_one("a[href]")
        if not link:
            continue

        title = clean_text(link.get_text(" ", strip=True))
        if not title:
            continue

        title_key = normalize_title_key(title)
        if not title_key or title_key in seen:
            continue

        rank_text = clean_text(cells[0].get_text(" ", strip=True))
        rank_match = re.search(r"\d+", rank_text)
        rank = int(rank_match.group(0)) if rank_match else len(items) + 1

        metric_el = row.select_one("td.ws")
        metric = clean_text(metric_el.get_text(" ", strip=True)) if metric_el else ""
        if not metric:
            metric = None

        url = urljoin(source_url, link.get("href") or "")

        items.append(
            {
                "source_id": source_id,
                "source_name": source_name,
                "rank": rank,
                "title": title,
                "title_key": title_key,
                "url": url,
                "metric": metric,
                "fetched_at": fetched_at,
            }
        )
        seen.add(title_key)

    return sorted(items, key=lambda item: item.get("rank") or 9999)
