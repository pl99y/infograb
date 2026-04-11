from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
import re
import sqlite3
import threading
import time
from typing import Any

from collectors.f1_fetcher import (
    AUTOSPORT_F1_NEWS_URL,
    FLASHSCORE_OVERVIEW_URL,
    discover_flashscore_gp_url,
    fetch_autosport_f1_news_page,
    fetch_flashscore_gp_page,
    fetch_flashscore_overview,
)
from parsers.f1_parser import F1NewsArticle, parse_autosport_f1_news_page, parse_flashscore_gp_page
from services.shared.translation import translate_batch_with_meta
from storage import get_connection, initialize_database

logger = logging.getLogger(__name__)

F1_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS f1_live_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gp_name TEXT NOT NULL,
    session_type TEXT NOT NULL,
    session_status TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    extra_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS f1_live_rows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    driver_name TEXT NOT NULL,
    team_name TEXT,
    result_text TEXT,
    extra_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (snapshot_id) REFERENCES f1_live_snapshots(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS f1_news_articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    published_at TEXT,
    summary TEXT,
    url TEXT NOT NULL,
    normalized_url TEXT NOT NULL UNIQUE,
    category TEXT,
    is_live_text INTEGER NOT NULL DEFAULT 0,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    extra_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

F1_WRITE_LOCK = threading.Lock()
F1_NEWS_KEEP_MAX = 20
F1_LIVE_KEEP_HOURS = 24
F1_LIVE_KEEP_MAX_SNAPSHOTS = 180
F1_TITLE_TARGET_LANG = "zh"


def _prune_f1_live_history(
    con: sqlite3.Connection,
    *,
    latest_gp_hours: int = F1_LIVE_KEEP_HOURS,
    keep_max_snapshots: int = F1_LIVE_KEEP_MAX_SNAPSHOTS,
) -> dict[str, int]:
    latest = con.execute(
        """
        SELECT gp_name
        FROM f1_live_snapshots
        ORDER BY COALESCE(fetched_at, created_at) DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if not latest or not latest["gp_name"]:
        return {"other_gp_deleted": 0, "stale_latest_gp_deleted": 0, "overflow_latest_gp_deleted": 0, "total_deleted": 0}

    latest_gp_name = latest["gp_name"]
    deleted_other_gp = con.execute(
        "DELETE FROM f1_live_snapshots WHERE gp_name <> ?",
        (latest_gp_name,),
    ).rowcount

    deleted_stale_latest_gp = con.execute(
        f"""
        DELETE FROM f1_live_snapshots
        WHERE gp_name = ?
          AND datetime(COALESCE(fetched_at, created_at)) < datetime('now', '-{latest_gp_hours} hours')
        """,
        (latest_gp_name,),
    ).rowcount

    deleted_overflow_latest_gp = con.execute(
        """
        DELETE FROM f1_live_snapshots
        WHERE gp_name = ?
          AND id NOT IN (
              SELECT id
              FROM f1_live_snapshots
              WHERE gp_name = ?
              ORDER BY COALESCE(fetched_at, created_at) DESC, id DESC
              LIMIT ?
          )
        """,
        (latest_gp_name, latest_gp_name, keep_max_snapshots),
    ).rowcount

    total_deleted = deleted_other_gp + deleted_stale_latest_gp + deleted_overflow_latest_gp
    return {
        "other_gp_deleted": deleted_other_gp,
        "stale_latest_gp_deleted": deleted_stale_latest_gp,
        "overflow_latest_gp_deleted": deleted_overflow_latest_gp,
        "total_deleted": total_deleted,
    }


def _prune_f1_news_to_max(con: sqlite3.Connection, keep_max: int = F1_NEWS_KEEP_MAX) -> int:
    if keep_max <= 0:
        cur = con.execute("DELETE FROM f1_news_articles")
        return cur.rowcount

    cur = con.execute(
        """
        DELETE FROM f1_news_articles
        WHERE id NOT IN (
            SELECT id
            FROM f1_news_articles
            ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
            LIMIT ?
        )
        """,
        (keep_max,),
    )
    return cur.rowcount


def ensure_f1_tables(con: sqlite3.Connection) -> None:
    con.executescript(F1_SCHEMA_SQL)


def _ensure_f1_news_translation_columns(con: sqlite3.Connection) -> None:
    cols = {str(row[1]) for row in con.execute("PRAGMA table_info(f1_news_articles)").fetchall()}
    if "title_zh" not in cols:
        con.execute("ALTER TABLE f1_news_articles ADD COLUMN title_zh TEXT")
    if "translation_provider" not in cols:
        con.execute("ALTER TABLE f1_news_articles ADD COLUMN translation_provider TEXT")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _translate_f1_titles(con: sqlite3.Connection, articles: list[F1NewsArticle]) -> str:
    if not articles:
        return "none"

    existing_rows = con.execute(
        """
        SELECT normalized_url, title, title_zh, translation_provider
        FROM f1_news_articles
        WHERE title_zh IS NOT NULL AND TRIM(title_zh) <> ''
        """
    ).fetchall()
    cached_by_url = {
        _normalize_text(row["normalized_url"]): (_normalize_text(row["title_zh"]), _normalize_text(row["translation_provider"]) or "cached")
        for row in existing_rows
        if _normalize_text(row["normalized_url"]) and _normalize_text(row["title_zh"])
    }
    cached_by_title = {
        _normalize_text(row["title"]): (_normalize_text(row["title_zh"]), _normalize_text(row["translation_provider"]) or "cached")
        for row in existing_rows
        if _normalize_text(row["title"]) and _normalize_text(row["title_zh"])
    }

    provider_best = "none"
    translation_inputs: list[str] = []
    translated_by_text: dict[str, tuple[str, str]] = {}

    for article in articles:
        cached = cached_by_url.get(_normalize_text(article.normalized_url)) or cached_by_title.get(_normalize_text(article.title))
        if cached is not None:
            article.extra["title_zh"] = cached[0]
            article.extra["translation_provider"] = cached[1]
            if cached[1] not in {"", "none", "unknown", "cached", "failed"}:
                provider_best = cached[1]
            continue

        title = _normalize_text(article.title)
        if not title:
            article.extra["title_zh"] = ""
            article.extra["translation_provider"] = "none"
            continue

        if title not in translated_by_text:
            translated_by_text[title] = ("", "pending")
            translation_inputs.append(title)

    if translation_inputs:
        try:
            result = asyncio.run(
                translate_batch_with_meta(
                    translation_inputs,
                    F1_TITLE_TARGET_LANG,
                )
            )
            translations = result.get("translations") or translation_inputs
            providers = result.get("providers") or ["unknown" for _ in translation_inputs]
        except Exception as exc:
            logger.warning("F1 title translation failed; using source titles: %s", exc)
            translations = translation_inputs
            providers = ["failed" for _ in translation_inputs]

        for source_text, translated, provider in zip(translation_inputs, translations, providers):
            clean = _normalize_text(translated) or source_text
            provider_name = _normalize_text(provider) or "unknown"
            translated_by_text[source_text] = (clean, provider_name)
            if provider_name not in {"", "none", "unknown", "cached", "failed"}:
                provider_best = provider_name

    for article in articles:
        if article.extra.get("title_zh"):
            continue
        translated, provider = translated_by_text.get(_normalize_text(article.title), (_normalize_text(article.title), "failed"))
        article.extra["title_zh"] = translated
        article.extra["translation_provider"] = provider

    return provider_best


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _prepare_connection(con: sqlite3.Connection) -> None:
    try:
        con.execute("PRAGMA busy_timeout = 30000")
    except Exception:
        pass


def _db_retry(fn, *, retries: int = 4, sleep_s: float = 0.8):
    last_exc: Exception | None = None
    for i in range(retries):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            last_exc = exc
            if "database is locked" not in str(exc).lower() or i == retries - 1:
                raise
            time.sleep(sleep_s * (i + 1))
    if last_exc:
        raise last_exc


def _insert_live_snapshot(con: sqlite3.Connection, *, snapshot: dict[str, Any], rows: list[dict[str, Any]]) -> int:
    cur = con.execute(
        """
        INSERT INTO f1_live_snapshots (
            gp_name, session_type, session_status, source_name, source_url, fetched_at, extra_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot["gp_name"],
            snapshot["session_type"],
            snapshot["session_status"],
            snapshot["source_name"],
            snapshot["source_url"],
            snapshot["fetched_at"],
            json.dumps(snapshot.get("extra") or {}, ensure_ascii=False),
        ),
    )
    snapshot_id = int(cur.lastrowid)
    for row in rows:
        con.execute(
            """
            INSERT INTO f1_live_rows (snapshot_id, position, driver_name, team_name, result_text, extra_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                row["position"],
                row["driver_name"],
                row.get("team_name"),
                row.get("result_text"),
                json.dumps(row.get("extra") or {}, ensure_ascii=False),
            ),
        )
    return snapshot_id


def _fallback_rows_from_overview_text(overview_text: str) -> list[dict[str, Any]]:
    compact = re.sub(r"\s+", " ", overview_text or "").strip()
    pattern = re.compile(
        r"(\d{1,2})\.\s+([A-Z][A-Za-z.'\- ]+?)\s+([A-Z][A-Za-z0-9 &'\\-]+?)\s+(\+?[0-9:.]+|DNF|DNS|DSQ|RET|OUT|NC|Stopped|Finished)",
        re.I,
    )
    rows: list[dict[str, Any]] = []
    seen: set[int] = set()
    for m in pattern.finditer(compact):
        pos = int(m.group(1))
        if pos in seen:
            continue
        rows.append(
            {
                "position": pos,
                "driver_name": m.group(2).strip(),
                "team_name": m.group(3).strip(),
                "result_text": m.group(4).strip(),
                "extra": {"parsed_from": "overview_fallback"},
            }
        )
        seen.add(pos)
    rows.sort(key=lambda x: x["position"])
    return rows


def _latest_non_empty_snapshot(con: sqlite3.Connection) -> dict | None:
    row = con.execute(
        """
        SELECT s.*
        FROM f1_live_snapshots s
        WHERE EXISTS (
            SELECT 1
            FROM f1_live_rows r
            WHERE r.snapshot_id = s.id
        )
        ORDER BY s.id DESC
        LIMIT 1
        """
    ).fetchone()
    return dict(row) if row else None


def fetch_f1_live_once(db_path: str = "app.db") -> dict:
    initialize_database(db_path)

    def _write_live() -> dict:
        con = get_connection(db_path)
        _prepare_connection(con)
        try:
            ensure_f1_tables(con)

            overview = fetch_flashscore_overview(FLASHSCORE_OVERVIEW_URL)
            if not overview.success or not overview.text:
                return {"snapshots_saved": 0, "rows_saved": 0, "errors": [overview.error_message or "overview fetch failed"]}

            gp_url = discover_flashscore_gp_url(overview.text, FLASHSCORE_OVERVIEW_URL)
            gp_page = fetch_flashscore_gp_page(gp_url)
            if not gp_page.success or not gp_page.text:
                return {"snapshots_saved": 0, "rows_saved": 0, "errors": [gp_page.error_message or "gp fetch failed"]}

            snapshot = parse_flashscore_gp_page(gp_page.text, gp_page.url)
            fetched_at = gp_page.fetched_at or _utcnow()

            rows_payload = [
                {
                    "position": row.position,
                    "driver_name": row.driver_name,
                    "team_name": row.team_name,
                    "result_text": row.result_text,
                    "extra": row.extra,
                }
                for row in snapshot.rows
            ]

            if not rows_payload:
                overview_rows = _fallback_rows_from_overview_text(overview.text or "")
                if overview_rows:
                    rows_payload = overview_rows
                    snapshot.extra = {**(snapshot.extra or {}), "fallback_used": "overview_text"}

            if not rows_payload:
                old = _latest_non_empty_snapshot(con)
                if old:
                    return {
                        "snapshots_saved": 0,
                        "rows_saved": 0,
                        "gp_name": old.get("gp_name"),
                        "session_type": old.get("session_type"),
                        "session_status": old.get("session_status"),
                        "errors": [],
                        "fallback_to_previous": True,
                    }

            snapshot_payload = {
                "gp_name": snapshot.gp_name,
                "session_type": snapshot.session_type,
                "session_status": snapshot.session_status,
                "source_name": snapshot.source_name,
                "source_url": snapshot.source_url,
                "fetched_at": fetched_at,
                "extra": snapshot.extra,
            }

            _insert_live_snapshot(con, snapshot=snapshot_payload, rows=rows_payload)
            prune_stats = _prune_f1_live_history(con)
            con.commit()
            return {
                "snapshots_saved": 1,
                "rows_saved": len(rows_payload),
                "gp_name": snapshot.gp_name,
                "session_type": snapshot.session_type,
                "session_status": snapshot.session_status,
                "old_snapshots_deleted": prune_stats["total_deleted"],
                "old_other_gp_snapshots_deleted": prune_stats["other_gp_deleted"],
                "old_same_gp_stale_snapshots_deleted": prune_stats["stale_latest_gp_deleted"],
                "old_same_gp_overflow_snapshots_deleted": prune_stats["overflow_latest_gp_deleted"],
                "errors": [],
            }
        finally:
            con.close()

    try:
        with F1_WRITE_LOCK:
            return _db_retry(_write_live)
    except Exception as exc:
        logger.exception("F1 live fetch failed")
        return {"snapshots_saved": 0, "rows_saved": 0, "errors": [str(exc)]}


def _upsert_news_article(con: sqlite3.Connection, article: F1NewsArticle, fetched_at: str) -> bool:
    existing = con.execute(
        "SELECT id FROM f1_news_articles WHERE normalized_url = ?",
        (article.normalized_url,),
    ).fetchone()
    payload_json = json.dumps(article.extra or {}, ensure_ascii=False)
    if existing:
        con.execute(
            """
            UPDATE f1_news_articles
            SET title = ?, title_zh = ?, published_at = ?, summary = ?, url = ?, category = ?, is_live_text = ?,
                source_name = ?, source_url = ?, fetched_at = ?, extra_json = ?, translation_provider = ?
            WHERE normalized_url = ?
            """,
            (
                article.title,
                article.extra.get("title_zh") or "",
                article.published_at,
                article.summary,
                article.url,
                article.category,
                int(article.is_live_text),
                article.source_name,
                article.source_url,
                fetched_at,
                payload_json,
                article.extra.get("translation_provider"),
                article.normalized_url,
            ),
        )
        return False

    con.execute(
        """
        INSERT INTO f1_news_articles (
            title, title_zh, published_at, summary, url, normalized_url, category, is_live_text,
            source_name, source_url, fetched_at, extra_json, translation_provider
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            article.title,
            article.extra.get("title_zh") or "",
            article.published_at,
            article.summary,
            article.url,
            article.normalized_url,
            article.category,
            int(article.is_live_text),
            article.source_name,
            article.source_url,
            fetched_at,
            payload_json,
            article.extra.get("translation_provider"),
        ),
    )
    return True


def fetch_f1_news_once(db_path: str = "app.db") -> dict:
    initialize_database(db_path)

    def _write_news() -> dict:
        con = get_connection(db_path)
        _prepare_connection(con)
        try:
            ensure_f1_tables(con)
            _ensure_f1_news_translation_columns(con)

            news_page = fetch_autosport_f1_news_page(AUTOSPORT_F1_NEWS_URL)
            if not news_page.success or not news_page.text:
                return {"articles_saved": 0, "articles_new": 0, "errors": [news_page.error_message or "news page fetch failed"]}

            articles = parse_autosport_f1_news_page(news_page.text, news_page.url)
            translation_provider = _translate_f1_titles(con, articles)
            fetched_at = news_page.fetched_at or _utcnow()
            new_count = 0
            for article in articles:
                if _upsert_news_article(con, article, fetched_at=fetched_at):
                    new_count += 1
            deleted_count = _prune_f1_news_to_max(con, keep_max=F1_NEWS_KEEP_MAX)
            con.commit()
            return {
                "articles_saved": len(articles),
                "articles_new": new_count,
                "articles_deleted": deleted_count,
                "articles_kept_max": F1_NEWS_KEEP_MAX,
                "translation_provider": translation_provider,
                "errors": [],
            }
        finally:
            con.close()

    try:
        with F1_WRITE_LOCK:
            return _db_retry(_write_news)
    except Exception as exc:
        logger.exception("F1 news fetch failed")
        return {"articles_saved": 0, "articles_new": 0, "errors": [str(exc)]}


def fetch_f1_once(db_path: str = "app.db") -> dict:
    live = fetch_f1_live_once(db_path=db_path)
    news = fetch_f1_news_once(db_path=db_path)
    return {"live": live, "news": news}
