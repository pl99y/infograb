from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
        "Gecko/20100101 Firefox/124.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_REQUESTS = 0.8


@dataclass(frozen=True)
class HotsearchSource:
    source_id: str
    source_name: str
    url: str
    limit: int


HOTSEARCH_SOURCES: list[HotsearchSource] = [
    HotsearchSource(
        source_id="weibo_hot",
        source_name="微博热搜榜",
        url="https://tophub.today/n/KqndgxeLl9",
        limit=20,
    ),
    HotsearchSource(
        source_id="baidu_realtime",
        source_name="百度实时热点",
        url="https://tophub.today/n/Jb0vmloB1G",
        limit=20,
    ),
    HotsearchSource(
        source_id="douyin_hot",
        source_name="抖音热搜榜",
        url="https://tophub.today/n/K7GdaMgdQy",
        limit=20,
    ),
    HotsearchSource(
        source_id="bilibili_hot",
        source_name="哔哩哔哩热搜",
        url="https://tophub.today/n/aqeEk03v9R",
        limit=10,
    ),
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_tophub_page(source: HotsearchSource) -> dict[str, Any]:
    started = time.time()
    meta: dict[str, Any] = {
        "source_id": source.source_id,
        "source_name": source.source_name,
        "source_url": source.url,
        "limit": source.limit,
        "ok": False,
        "status_code": None,
        "final_url": "",
        "content_type": "",
        "error": "",
        "elapsed_sec": None,
        "body_length": 0,
        "html": "",
        "fetched_at": utc_now_iso(),
    }

    try:
        response = requests.get(source.url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        meta["elapsed_sec"] = round(time.time() - started, 3)
        meta["status_code"] = response.status_code
        meta["final_url"] = response.url
        meta["content_type"] = response.headers.get("content-type", "")
        meta["html"] = response.text or ""
        meta["body_length"] = len(meta["html"])

        if response.status_code >= 400:
            meta["error"] = f"HTTP {response.status_code}"
            return meta

        meta["ok"] = True
        return meta
    except Exception as exc:
        meta["elapsed_sec"] = round(time.time() - started, 3)
        meta["error"] = f"{type(exc).__name__}: {exc}"
        return meta


def collect_tophub_hotsearch() -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    for idx, source in enumerate(HOTSEARCH_SOURCES):
        if idx > 0 and SLEEP_BETWEEN_REQUESTS > 0:
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        pages.append(fetch_tophub_page(source))
    return pages
