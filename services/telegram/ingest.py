from __future__ import annotations

import hashlib
import re
from pathlib import Path
from urllib.parse import urlparse

import requests

from storage import (
    get_connection,
    insert_item_if_new,
    insert_media_if_new,
    record_fetch_run,
    update_media_download,
    upsert_source,
)


MEDIA_ROOT = Path("raw") / "media"
IMAGE_DIR = MEDIA_ROOT / "images"


def ensure_media_dirs() -> None:
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)


def clean_whitespace(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def clean_telegram_text(text: str) -> str:
    text = clean_whitespace(text)

    promo_patterns = [
        r"\s*\|\s*Socials\s*\|\s*Donate\s*\|\s*Advertising\s*$",
        r"\s*\|\s*Donate\s*\|\s*Advertising\s*$",
        r"ℹ️📰 Subscribe to.*$",
        r"📱.*Telegram.*📱",
        r"Subscribe to.*👉.*$",
        r"Join us on Telegram.*$",
        r"Follow us on.*$",
        r"http[s]?://t\.me/[\w_]+",
    ]

    for pattern in promo_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()

    return clean_whitespace(text)


def normalize_telegram_post(post: dict) -> dict | None:
    text_raw = post.get("text", "") or ""
    text_clean = clean_telegram_text(text_raw)
    post_url = post.get("post_url", "") or ""
    published_at = post.get("published_at", "") or ""
    media_items = post.get("media_items", []) or []

    media_items = [
        item for item in media_items
        if isinstance(item, dict) and item.get("type") and item.get("url")
    ]

    if not post_url:
        return None

    if not text_raw and not media_items:
        return None

    return {
        "post_url": post_url,
        "content_text_raw": text_raw,
        "content_text_clean": text_clean,
        "published_at": published_at,
        "media_items": media_items,
    }


def guess_extension(media_url: str) -> str:
    parsed = urlparse(media_url)
    suffix = Path(parsed.path).suffix.lower()
    return suffix if suffix else ".jpg"


def download_media_file(
    media_type: str,
    media_url: str,
    item_id: int,
    media_index: int,
    timeout: int = 30,
) -> tuple[str | None, str]:
    if media_type != "image":
        return None, "skipped"

    ensure_media_dirs()

    file_hash = hashlib.sha1(media_url.encode("utf-8")).hexdigest()[:12]
    ext = guess_extension(media_url)
    filename = f"item_{item_id}_{media_index}_{file_hash}{ext}"
    file_path = IMAGE_DIR / filename

    if file_path.exists():
        return str(file_path), "downloaded"

    try:
        response = requests.get(media_url, timeout=timeout, stream=True)
        response.raise_for_status()

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        return str(file_path), "downloaded"
    except Exception:
        return None, "failed"


def ingest_telegram_posts(
    source: dict,
    posts: list[dict],
    fetched_at: str,
    db_path: str = "app.db",
    download_media: bool = False,
) -> dict:
    con = get_connection(db_path)

    started_at = fetched_at

    stats = {
        "items_total": 0,
        "items_new": 0,
        "media_total": 0,
        "media_new": 0,
        "media_downloaded": 0,
        "media_failed": 0,
        "media_skipped": 0,
    }

    try:
        source_id = upsert_source(
            con=con,
            name=source["name"],
            source_type=source["source_type"],
            url=source["url"],
            enabled=source.get("enabled", True),
        )

        normalized_posts: list[dict] = []
        for post in posts:
            normalized = normalize_telegram_post(post)
            if normalized:
                normalized_posts.append(normalized)

        stats["items_total"] = len(normalized_posts)

        for normalized in normalized_posts:
            item_id, created_new = insert_item_if_new(
                con=con,
                source_id=source_id,
                post_url=normalized["post_url"],
                content_text_raw=normalized["content_text_raw"],
                content_text_clean=normalized["content_text_clean"],
                published_at=normalized["published_at"],
                fetched_at=fetched_at,
            )

            if created_new:
                stats["items_new"] += 1

            media_items = normalized["media_items"]
            stats["media_total"] += len(media_items)

            for idx, media in enumerate(media_items, start=1):
                media_id, media_new = insert_media_if_new(
                    con=con,
                    item_id=item_id,
                    media_type=media["type"],
                    media_url=media["url"],
                )

                if media_new:
                    stats["media_new"] += 1

                if download_media and media_new:
                    local_path, download_status = download_media_file(
                        media_type=media["type"],
                        media_url=media["url"],
                        item_id=item_id,
                        media_index=idx,
                    )

                    update_media_download(
                        con=con,
                        media_id=media_id,
                        local_path=local_path,
                        download_status=download_status,
                    )

                    if download_status == "downloaded":
                        stats["media_downloaded"] += 1
                    elif download_status == "failed":
                        stats["media_failed"] += 1
                    elif download_status == "skipped":
                        stats["media_skipped"] += 1

        record_fetch_run(
            con=con,
            source_id=source_id,
            source_name=source["name"],
            started_at=started_at,
            finished_at=fetched_at,
            status="success",
            items_found=stats["items_total"],
            new_items=stats["items_new"],
            error_message="",
        )

        con.commit()
        return stats

    except Exception as exc:
        con.rollback()

        record_fetch_run(
            con=con,
            source_id=None,
            source_name=source.get("name", ""),
            started_at=started_at,
            finished_at=fetched_at,
            status="failed",
            items_found=0,
            new_items=0,
            error_message=str(exc),
        )
        con.commit()
        raise
    finally:
        con.close()