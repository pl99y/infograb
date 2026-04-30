"""
Telegram pipeline — load enabled sources, fetch pages, parse posts, ingest them into SQLite,
and pretranslate the newest feed items for the dashboard.
"""
from __future__ import annotations

import asyncio
import logging

from collectors.telegram_fetcher import fetch_telegram_page
from parsers.telegram_parser import parse_telegram_page
from services.shared.translation import translate_batch_with_meta
from services.telegram.ingest import ingest_telegram_posts
from services.telegram.sources import load_telegram_sources
from storage import get_connection

logger = logging.getLogger(__name__)

TG_PRETRANSLATE_LIMIT = 50
# Optional safety trim for very long Telegram posts. Set to 0 to disable trimming.
TG_PRETRANSLATE_TEXT_CHAR_LIMIT = 0


def _load_recent_untranslated_posts(limit: int, db_path: str) -> list[dict]:
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            SELECT i.id, i.content_text_clean AS text
            FROM items i
            JOIN sources s ON i.source_id = s.id
            WHERE s.source_type = ?
              AND i.content_text_clean IS NOT NULL
              AND TRIM(i.content_text_clean) <> ''
              AND (i.content_text_zh IS NULL OR TRIM(i.content_text_zh) = '')
            ORDER BY COALESCE(i.published_at, i.fetched_at) DESC, i.id DESC
            LIMIT ?
            """,
            ("telegram", int(limit)),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def _prepare_tg_text(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return ""
    if TG_PRETRANSLATE_TEXT_CHAR_LIMIT and len(cleaned) > TG_PRETRANSLATE_TEXT_CHAR_LIMIT:
        return cleaned[:TG_PRETRANSLATE_TEXT_CHAR_LIMIT].rstrip()
    return cleaned


def _save_pretranslations(rows: list[dict], translations: list[str], providers: list[str], db_path: str) -> int:
    updates: list[tuple[str, int]] = []
    for row, translated, provider in zip(rows, translations, providers):
        text = (translated or "").strip()
        provider_name = (provider or "").strip().lower()
        if not text:
            continue
        if provider_name in {"failed", "none", "pending"}:
            continue
        updates.append((text, int(row["id"])))

    if not updates:
        return 0

    con = get_connection(db_path)
    try:
        con.executemany(
            "UPDATE items SET content_text_zh = ? WHERE id = ?",
            updates,
        )
        con.commit()
        return len(updates)
    finally:
        con.close()


def _pretranslate_recent_telegram_posts(limit: int = TG_PRETRANSLATE_LIMIT, db_path: str = "app.db") -> int:
    rows = _load_recent_untranslated_posts(limit=limit, db_path=db_path)
    if not rows:
        return 0

    texts = [_prepare_tg_text(row.get("text") or "") for row in rows]

    try:
        result = asyncio.run(
            translate_batch_with_meta(
                texts,
                "zh",
            )
        )
    except Exception as exc:
        logger.warning("Telegram pretranslation failed; leaving source text only: %s", exc)
        return 0

    translations = result.get("translations") or []
    providers = result.get("providers") or []
    if len(translations) != len(rows) or len(providers) != len(rows):
        logger.warning(
            "Telegram pretranslation returned mismatched lengths: rows=%d translations=%d providers=%d",
            len(rows),
            len(translations),
            len(providers),
        )
        return 0

    updated = _save_pretranslations(rows, translations, providers, db_path=db_path)
    if updated:
        logger.info("Telegram pretranslated %d/%d recent feed items", updated, len(rows))
    return updated


def fetch_telegram_once(db_path: str = "app.db") -> dict:
    results = {"sources_processed": 0, "total_new": 0, "pretranslated": 0, "errors": []}

    try:
        sources = load_telegram_sources(enabled_only=True)
    except Exception as exc:
        logger.error("Failed to load Telegram sources: %s", exc)
        results["errors"].append(str(exc))
        return results

    for source in sources:
        try:
            fetch_result = fetch_telegram_page(
                source_name=source["name"],
                url=source["url"],
            )

            if not fetch_result.success:
                logger.warning(
                    "Fetch failed for %s: %s",
                    source["name"],
                    fetch_result.error_message,
                )
                results["errors"].append(f"{source['name']}: {fetch_result.error_message}")
                continue

            parsed = parse_telegram_page(fetch_result.html)
            posts = parsed.get("posts", [])

            stats = ingest_telegram_posts(
                source=source,
                posts=posts,
                fetched_at=fetch_result.fetched_at,
                db_path=db_path,
                download_media=False,
            )

            results["sources_processed"] += 1
            results["total_new"] += stats.get("items_new", 0)

            logger.info("Telegram [%s]: %d new items", source["name"], stats.get("items_new", 0))

        except Exception as exc:
            logger.error("Error processing %s: %s", source["name"], exc)
            results["errors"].append(f"{source['name']}: {exc}")

    try:
        results["pretranslated"] = _pretranslate_recent_telegram_posts(
            limit=TG_PRETRANSLATE_LIMIT,
            db_path=db_path,
        )
    except Exception as exc:
        logger.warning("Telegram post-ingest pretranslation failed: %s", exc)
        results["errors"].append(f"telegram pretranslate: {exc}")

    return results
