from __future__ import annotations

import logging
from typing import Any

from collectors.news_timeline_fetcher import (
    HK01_CHANNELS,
    MINGPAO_FEEDS,
    NEWS_WINDOW_HOURS,
    ZAOBAO_SECTIONS,
    collect_news_timeline,
)
from storage import (
    get_connection,
    initialize_database,
    record_fetch_run,
    upsert_news_timeline_item,
    upsert_source,
)

logger = logging.getLogger(__name__)

NEWS_TIMELINE_FETCH_RUN = "News Timeline"
NEWS_TIMELINE_SOURCE_URLS = {
    "zaobao": "https://www.zaobao.com.sg/news/china",
    "mingpao": "https://news.mingpao.com/rss/ins/s00005.xml",
    "hk01": "https://web-data.api.hk01.com/v2/feed/category/19?bucketId=00000&limit=20",
}


def _ensure_sources(con) -> dict[str, int]:
    return {
        "zaobao": upsert_source(con, name="联合早报", source_type="news_timeline", url=NEWS_TIMELINE_SOURCE_URLS["zaobao"], enabled=True),
        "mingpao": upsert_source(con, name="明报", source_type="news_timeline", url=NEWS_TIMELINE_SOURCE_URLS["mingpao"], enabled=True),
        "hk01": upsert_source(con, name="香港01", source_type="news_timeline", url=NEWS_TIMELINE_SOURCE_URLS["hk01"], enabled=True),
    }


def fetch_news_timeline_once(db_path: str = "app.db") -> dict[str, Any]:
    initialize_database(db_path)
    payload = collect_news_timeline(window_hours=NEWS_WINDOW_HOURS)

    total_new = 0
    saved = 0
    counts_by_source: dict[str, int] = {"zaobao": 0, "mingpao": 0, "hk01": 0}
    con = get_connection(db_path)

    try:
        source_ids = _ensure_sources(con)
        for item in payload.items:
            source_key = str(item.get("source_key") or "").strip()
            if source_key in counts_by_source:
                counts_by_source[source_key] += 1
            _, is_new = upsert_news_timeline_item(
                con,
                source_key=source_key,
                source_name=str(item.get("source_name") or "").strip(),
                source_id=source_ids.get(source_key),
                channel=str(item.get("channel") or "").strip() or None,
                topic=str(item.get("topic") or "").strip() or None,
                title=str(item.get("title") or "").strip(),
                url=str(item.get("url") or "").strip(),
                published_at=item.get("published_at"),
                published_text=str(item.get("published_text") or "").strip() or None,
                author_names=item.get("author_names") or [],
                list_url=str(item.get("list_url") or "").strip() or None,
                payload=item.get("payload") or item,
                fetched_at=payload.fetched_at,
            )
            saved += 1
            if is_new:
                total_new += 1

        record_fetch_run(
            con=con,
            source_id=None,
            source_name=NEWS_TIMELINE_FETCH_RUN,
            started_at=payload.fetched_at,
            finished_at=payload.fetched_at,
            status="partial" if payload.errors else "success",
            items_found=len(payload.items),
            new_items=total_new,
            error_message=" | ".join(payload.errors),
        )
        con.commit()
    finally:
        con.close()

    return {
        "fetched_at": payload.fetched_at,
        "cutoff_at": payload.cutoff_at,
        "window_hours": payload.window_hours,
        "items_found": len(payload.items),
        "items_saved": saved,
        "items_new": total_new,
        "counts_by_source": counts_by_source,
        "errors": payload.errors,
        "source_channels": {
            "zaobao": [row["channel"] for row in ZAOBAO_SECTIONS],
            "mingpao": [row["channel"] for row in MINGPAO_FEEDS],
            "hk01": [row["channel"] for row in HK01_CHANNELS],
        },
    }
