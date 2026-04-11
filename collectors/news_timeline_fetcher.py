from __future__ import annotations

import time
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,zh-HK;q=0.8,zh-TW;q=0.7,en;q=0.6",
    "Referer": "https://www.google.com/",
}

NEWS_WINDOW_HOURS = 12
SLEEP_BETWEEN_REQUESTS = 0.3
REQUEST_TIMEOUT = 25
UTC_PLUS_8 = timezone(timedelta(hours=8))
HK01_BUCKET_ID = "00000"


ZAOBAO_SECTIONS = [
    {
        "source_key": "zaobao",
        "source_name": "联合早报",
        "channel": "中国",
        "list_url": "https://www.zaobao.com.sg/news/china",
        "path_flag": "/news/china/story",
    },
    {
        "source_key": "zaobao",
        "source_name": "联合早报",
        "channel": "国际",
        "list_url": "https://www.zaobao.com.sg/news/world",
        "path_flag": "/news/world/story",
    },
]

MINGPAO_FEEDS = [
    {
        "source_key": "mingpao",
        "source_name": "明报",
        "channel": "国际",
        "feed_code": "s00005",
        "feed_url": "https://news.mingpao.com/rss/ins/s00005.xml",
    },
    {
        "source_key": "mingpao",
        "source_name": "明报",
        "channel": "两岸国际",
        "feed_code": "s00004",
        "feed_url": "https://news.mingpao.com/rss/ins/s00004.xml",
    },
    {
        "source_key": "mingpao",
        "source_name": "明报",
        "channel": "港闻",
        "feed_code": "s00001",
        "feed_url": "https://news.mingpao.com/rss/ins/s00001.xml",
    },
]

HK01_CHANNELS = [
    {
        "source_key": "hk01",
        "source_name": "香港01",
        "channel": "即時國際",
        "category_id": 19,
    },
    {
        "source_key": "hk01",
        "source_name": "香港01",
        "channel": "即時中國",
        "category_id": 364,
    },
]

ZAOBAO_TIME_RE = re.compile(r"(?:发布|發布)\s*/?\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s+(\d{1,2}):(\d{2})")
ZAOBAO_TIME_RE_ALT = re.compile(r"(?:更新|更[新])\s*/?\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s+(\d{1,2}):(\d{2})")


@dataclass
class NewsFetchPayload:
    fetched_at: str
    cutoff_at: str
    window_hours: int
    items: list[dict[str, Any]]
    errors: list[str]


def clean_text(text: Any) -> str:
    return " ".join(str(text or "").replace("\xa0", " ").split()).strip()


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def fetch_text(session: requests.Session, url: str, timeout: int = REQUEST_TIMEOUT) -> tuple[str, str]:
    response = session.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()

    if not response.encoding or response.encoding.lower() == "iso-8859-1":
        response.encoding = response.apparent_encoding or "utf-8"

    return response.text, response.url


def fetch_json(session: requests.Session, url: str, timeout: int = REQUEST_TIMEOUT) -> tuple[Any, str]:
    response = session.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    return response.json(), response.url


def parse_iso_dt(value: Any) -> datetime | None:
    if not value:
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        raw = int(value)
        if raw > 10_000_000_000:
            return datetime.fromtimestamp(raw / 1000, tz=timezone.utc)
        return datetime.fromtimestamp(raw, tz=timezone.utc)

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.isdigit():
            return parse_iso_dt(int(text))
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            pass
        try:
            return parsedate_to_datetime(text)
        except Exception:
            pass

    return None


def to_iso_utc_str(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def in_last_hours(dt: datetime | None, cutoff_dt: datetime) -> bool:
    if dt is None:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt >= cutoff_dt


def dedupe_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    output: list[dict[str, Any]] = []
    for item in items:
        key = (str(item.get("source_key") or ""), str(item.get("url") or ""))
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def sort_items_desc(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _key(row: dict[str, Any]) -> datetime:
        dt = parse_iso_dt(row.get("published_at"))
        return dt.astimezone(timezone.utc) if dt else datetime(1970, 1, 1, tzinfo=timezone.utc)

    return sorted(items, key=_key, reverse=True)


def build_zaobao_page_url(base_url: str, page_index: int) -> str:
    return base_url if page_index == 0 else f"{base_url}?page={page_index}"


def parse_zaobao_links(list_html: str, final_url: str, path_flag: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(list_html, "lxml")
    seen: set[str] = set()
    links: list[dict[str, str]] = []

    for a in soup.find_all("a", href=True):
        href = clean_text(a.get("href"))
        full_url = urljoin(final_url, href)
        if path_flag not in full_url or full_url in seen:
            continue
        seen.add(full_url)
        links.append({"url": full_url, "title_hint": clean_text(a.get_text(" ", strip=True))})
    return links


def parse_zaobao_article(article_html: str) -> tuple[str, str | None, str]:
    soup = BeautifulSoup(article_html, "lxml")

    title = ""
    h1 = soup.find("h1")
    if h1:
        title = clean_text(h1.get_text(" ", strip=True))
    if not title and soup.title:
        title = clean_text(soup.title.get_text(" ", strip=True)).split("|")[0].strip()

    meta_candidates = [
        ("property", "article:published_time"),
        ("property", "og:published_time"),
        ("name", "publishdate"),
        ("name", "pubdate"),
        ("name", "date"),
        ("itemprop", "datePublished"),
    ]
    for attr_name, attr_value in meta_candidates:
        tag = soup.find("meta", attrs={attr_name: attr_value})
        if tag and tag.get("content"):
            dt = parse_iso_dt(tag["content"])
            if dt:
                return title, to_iso_utc_str(dt), str(tag["content"])

    text = soup.get_text("\n", strip=True)
    match = ZAOBAO_TIME_RE.search(text) or ZAOBAO_TIME_RE_ALT.search(text)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        hour = int(match.group(4))
        minute = int(match.group(5))
        dt = datetime(year, month, day, hour, minute, tzinfo=UTC_PLUS_8)
        return title, to_iso_utc_str(dt), f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}"

    return title, None, ""


def collect_zaobao(session: requests.Session, cutoff_dt: datetime, errors: list[str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for section in ZAOBAO_SECTIONS:
        seen_article_urls: set[str] = set()
        seen_page_signatures: set[tuple[str, ...]] = set()

        for page_index in range(5):
            page_url = build_zaobao_page_url(section["list_url"], page_index)
            try:
                html, final_url = fetch_text(session, page_url)
            except Exception as exc:
                errors.append(f"{section['source_name']} {section['channel']} 列表抓取失败: {exc}")
                break

            links = parse_zaobao_links(html, final_url, section["path_flag"])
            if not links:
                break

            signature = tuple(x["url"] for x in links[:10])
            if signature in seen_page_signatures:
                break
            seen_page_signatures.add(signature)

            page_hits = 0
            page_old = 0
            for link in links:
                article_url = link["url"]
                if article_url in seen_article_urls:
                    continue
                seen_article_urls.add(article_url)
                try:
                    article_html, _ = fetch_text(session, article_url)
                except Exception as exc:
                    errors.append(f"{section['source_name']} 文章抓取失败: {article_url} | {exc}")
                    continue

                title, published_at, published_text = parse_zaobao_article(article_html)
                dt = parse_iso_dt(published_at)
                if not in_last_hours(dt, cutoff_dt):
                    page_old += 1
                    continue

                results.append(
                    {
                        "source_key": section["source_key"],
                        "source_name": section["source_name"],
                        "channel": section["channel"],
                        "topic": None,
                        "title": title or link["title_hint"],
                        "url": article_url,
                        "published_at": published_at,
                        "published_text": published_text,
                        "author_names": [],
                        "list_url": final_url,
                        "payload": {"channel": section["channel"], "list_url": final_url},
                    }
                )
                page_hits += 1
                time.sleep(SLEEP_BETWEEN_REQUESTS)

            if page_hits == 0 and page_old > 0:
                break

    return results


def collect_mingpao(session: requests.Session, cutoff_dt: datetime, errors: list[str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for feed in MINGPAO_FEEDS:
        try:
            xml_text, final_url = fetch_text(session, feed["feed_url"])
        except Exception as exc:
            errors.append(f"{feed['source_name']} {feed['channel']} RSS 抓取失败: {exc}")
            continue

        soup = BeautifulSoup(xml_text, "xml")
        for item in soup.find_all("item"):
            title_tag = item.find("title")
            link_tag = item.find("link")
            pub_tag = item.find("pubDate")
            cat_tag = item.find("category")

            title = clean_text(title_tag.get_text(" ", strip=True)) if title_tag else ""
            url = clean_text(link_tag.get_text(" ", strip=True)) if link_tag else ""
            published_text = clean_text(pub_tag.get_text(" ", strip=True)) if pub_tag else ""
            topic = clean_text(cat_tag.get_text(" ", strip=True)) if cat_tag else ""
            dt = parse_iso_dt(published_text)

            if not title or not url or not in_last_hours(dt, cutoff_dt):
                continue

            results.append(
                {
                    "source_key": feed["source_key"],
                    "source_name": feed["source_name"],
                    "channel": feed["channel"],
                    "topic": topic or None,
                    "title": title,
                    "url": url,
                    "published_at": to_iso_utc_str(dt),
                    "published_text": published_text,
                    "author_names": [],
                    "list_url": final_url,
                    "payload": {"feed_code": feed["feed_code"], "channel": feed["channel"], "topic": topic},
                }
            )

    return results


def build_hk01_api_url(category_id: int, *, limit: int | None = None, offset: int | None = None) -> str:
    params: dict[str, Any] = {"bucketId": HK01_BUCKET_ID}
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    return f"https://web-data.api.hk01.com/v2/feed/category/{category_id}?{urlencode(params)}"


def collect_hk01(session: requests.Session, cutoff_dt: datetime, errors: list[str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for config in HK01_CHANNELS:
        seen_urls: set[str] = set()
        offset: int | None = None

        for _ in range(12):
            api_url = build_hk01_api_url(config["category_id"], limit=20 if offset is None else None, offset=offset)
            try:
                data, final_url = fetch_json(session, api_url)
            except Exception as exc:
                errors.append(f"{config['source_name']} {config['channel']} API 抓取失败: {exc}")
                break

            items = data.get("items") or []
            if not items:
                break

            valid_publish_times: list[int] = []
            for raw_item in items:
                item_data = (raw_item or {}).get("data") or {}
                if item_data.get("mainCategoryId") != config["category_id"]:
                    continue

                title = clean_text(item_data.get("title") or item_data.get("metaTitle") or "")
                url = clean_text(item_data.get("publishUrl") or item_data.get("canonicalUrl") or "")
                publish_time = item_data.get("publishTime")
                if not title or not url or not publish_time:
                    continue

                dt = parse_iso_dt(publish_time)
                if dt is None:
                    continue

                try:
                    valid_publish_times.append(int(publish_time))
                except Exception:
                    pass

                if url in seen_urls:
                    continue
                seen_urls.add(url)
                if not in_last_hours(dt, cutoff_dt):
                    continue

                author_names = [
                    clean_text((author or {}).get("publishName") or "")
                    for author in (item_data.get("authors") or [])
                    if clean_text((author or {}).get("publishName") or "")
                ]
                topic_names = [
                    clean_text(category.get("publishName") or "")
                    for category in (item_data.get("categories") or [])
                    if category.get("categoryId") != config["category_id"] and clean_text(category.get("publishName") or "")
                ]

                results.append(
                    {
                        "source_key": config["source_key"],
                        "source_name": config["source_name"],
                        "channel": config["channel"],
                        "topic": " / ".join(topic_names) if topic_names else None,
                        "title": title,
                        "url": url,
                        "published_at": to_iso_utc_str(dt),
                        "published_text": str(publish_time),
                        "author_names": author_names,
                        "list_url": final_url,
                        "payload": {"main_category_id": config["category_id"], "topics": topic_names},
                    }
                )

            if not valid_publish_times:
                break

            oldest_dt = parse_iso_dt(min(valid_publish_times))
            if oldest_dt and oldest_dt < cutoff_dt:
                break

            next_offset = min(valid_publish_times) - 1
            if next_offset <= 0:
                break
            offset = next_offset
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    return results


def collect_news_timeline(window_hours: int = NEWS_WINDOW_HOURS) -> NewsFetchPayload:
    fetched_at_dt = datetime.now(timezone.utc)
    cutoff_dt = fetched_at_dt - timedelta(hours=window_hours)
    errors: list[str] = []
    session = make_session()

    try:
        items: list[dict[str, Any]] = []
        items.extend(collect_zaobao(session, cutoff_dt, errors))
        items.extend(collect_mingpao(session, cutoff_dt, errors))
        items.extend(collect_hk01(session, cutoff_dt, errors))
        items = sort_items_desc(dedupe_items(items))
        return NewsFetchPayload(
            fetched_at=fetched_at_dt.isoformat(),
            cutoff_at=cutoff_dt.isoformat(),
            window_hours=window_hours,
            items=items,
            errors=errors,
        )
    finally:
        session.close()
