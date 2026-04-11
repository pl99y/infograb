from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from collectors.common import utc_now_iso
from collectors.public_health_fetcher import PROMED_URL, WHO_LIST_URL, fetch_public_health_payloads
from parsers.public_health_parser import parse_promed_latest_html, parse_who_api_payload
from services.shared.translation import translate_batch_with_meta
from storage import (
    get_connection,
    initialize_database,
    record_fetch_run,
    upsert_public_health_event,
    upsert_source,
)

logger = logging.getLogger(__name__)
PUBLIC_HEALTH_TARGET_LANG = "zh"
PUBLIC_HEALTH_EARLY_WARNING_KEEP_DAYS = 3
PUBLIC_HEALTH_OUTBREAK_KEEP_YEARS = 2

# Public-health feeds often arrive in all-caps telegraph style. That degrades
# language detection / translation quality on translator backends, so we send
# a translator-friendlier normalized variant while still storing the original raw
# title and preserving key acronyms / pathogen codes.
_PUBLIC_HEALTH_PRESERVE_UPPER_TOKENS = {
    "AIDS",
    "CDC",
    "COVID",
    "COVID-19",
    "EU",
    "HPAI",
    "HIV",
    "LPAI",
    "MERS",
    "MPOX",
    "SARS",
    "TB",
    "UK",
    "UN",
    "USA",
    "US",
    "UAE",
    "WHO",
}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _filter_early_warning_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=PUBLIC_HEALTH_EARLY_WARNING_KEEP_DAYS)
    kept: list[dict[str, Any]] = []
    for row in rows:
        published_dt = _parse_iso_datetime(row.get("published_at"))
        if published_dt is None or published_dt >= cutoff:
            kept.append(row)
    return kept


def _filter_outbreak_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    min_year = datetime.now(timezone.utc).year - (PUBLIC_HEALTH_OUTBREAK_KEEP_YEARS - 1)
    kept: list[dict[str, Any]] = []
    for row in rows:
        published_dt = _parse_iso_datetime(row.get("published_at"))
        if published_dt is None or published_dt.year >= min_year:
            kept.append(row)
    return kept


def _looks_code_like(token: str) -> bool:
    token = _normalize_text(token).upper()
    if not token:
        return False
    if re.fullmatch(r"H\d+N\d+", token):
        return True
    if re.fullmatch(r"[A-Z]{1,4}\d+[A-Z0-9-]*", token):
        return True
    if re.fullmatch(r"[A-Z]+-\d+", token):
        return True
    return False


def _looks_mostly_uppercase_latin(text: str) -> bool:
    letters = [c for c in text if c.isalpha() and c.isascii()]
    if len(letters) < 8:
        return False

    uppercase = sum(1 for c in letters if c.isupper())
    lowercase = sum(1 for c in letters if c.islower())
    if uppercase == 0:
        return False
    if lowercase == 0:
        return True
    return uppercase / max(1, uppercase + lowercase) >= 0.75


def _latin_alpha_ratio(text: str) -> float:
    letters = [c for c in text if c.isalpha()]
    if not letters:
        return 0.0
    latin = sum(1 for c in letters if c.isascii())
    return latin / len(letters)


def _looks_suspicious_cached_translation(title_raw: str, title_zh: str) -> bool:
    raw = _normalize_text(title_raw)
    translated = _normalize_text(title_zh)
    if not translated:
        return True

    if raw and translated.casefold() == raw.casefold():
        return True
    if re.search(r"[-–—]{3,}", translated):
        return True

    latin_words = re.findall(r"[A-Za-z]{4,}", translated)
    if _looks_mostly_uppercase_latin(raw):
        if _latin_alpha_ratio(translated) >= 0.35 and len(latin_words) >= 2:
            return True
        if re.search(r"\b[A-Z]{4,}(?:\s+[A-Z]{4,})+\b", translated):
            return True

    return False


def _normalize_title_for_translation(text: str) -> str:
    text = _normalize_text(text)
    if not text:
        return ""

    text = re.sub(r"[–—]+", "-", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*-\s*", " - ", text)
    text = text.strip(" -")

    if not _looks_mostly_uppercase_latin(text):
        return text

    def repl(match: re.Match[str]) -> str:
        token = match.group(0)
        token_upper = token.upper()

        if not any(ch.isalpha() for ch in token_upper):
            return token
        if token_upper in _PUBLIC_HEALTH_PRESERVE_UPPER_TOKENS or _looks_code_like(token_upper):
            return token_upper
        return token_upper.capitalize()

    return re.sub(r"[A-Z0-9][A-Z0-9/-]*", repl, text)


def _postprocess_translated_title(text: str) -> str:
    text = _normalize_text(text)
    if not text:
        return ""

    text = re.sub(r"[-–—]{3,}", " - ", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*-\s*", " - ", text)

    for token in sorted(_PUBLIC_HEALTH_PRESERVE_UPPER_TOKENS, key=len, reverse=True):
        text = re.sub(fr"\b{re.escape(token)}\b", token, text, flags=re.IGNORECASE)

    text = re.sub(
        r"\b[hH]\s*(\d+)\s*[nN]\s*(\d+)\b",
        lambda m: f"H{m.group(1)}N{m.group(2)}",
        text,
    )

    return _normalize_text(text)


def _load_existing_title_cache(con) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str]]]:
    rows = con.execute(
        """
        SELECT dedupe_key, title_raw, title_zh, translation_provider
        FROM public_health_events
        WHERE title_zh IS NOT NULL AND TRIM(title_zh) <> ''
        """
    ).fetchall()

    by_dedupe: dict[str, tuple[str, str]] = {}
    by_title: dict[str, tuple[str, str]] = {}
    for row in rows:
        title_raw = _normalize_text(row["title_raw"])
        title_zh = _normalize_text(row["title_zh"])
        provider = _normalize_text(row["translation_provider"]) or "cached"
        dedupe_key = _normalize_text(row["dedupe_key"])

        if _looks_suspicious_cached_translation(title_raw, title_zh):
            continue

        if dedupe_key and title_zh:
            by_dedupe[dedupe_key] = (title_zh, provider)
        if title_raw and title_zh and title_raw not in by_title:
            by_title[title_raw] = (title_zh, provider)
    return by_dedupe, by_title


def _translate_titles(con, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    if not rows:
        return rows, "none"

    cached_by_dedupe, cached_by_title = _load_existing_title_cache(con)
    translation_inputs: list[str] = []
    pending_rows: list[tuple[dict[str, Any], str]] = []
    translated_by_text: dict[str, tuple[str, str]] = {}
    best_provider = "none"

    for row in rows:
        title_raw = _normalize_text(row.get("title_raw"))
        dedupe_key = _normalize_text(row.get("dedupe_key"))

        cached = None
        if dedupe_key:
            cached = cached_by_dedupe.get(dedupe_key)
        if cached is None and title_raw:
            cached = cached_by_title.get(title_raw)

        if cached is not None:
            row["title_zh"] = cached[0]
            row["translation_provider"] = cached[1]
            if cached[1] not in {"", "none", "failed", "pending", "unknown", "cached"}:
                best_provider = cached[1]
            continue

        if not title_raw:
            row["title_zh"] = ""
            row["translation_provider"] = "none"
            continue

        normalized_input = _normalize_title_for_translation(title_raw)
        row["_translation_input"] = normalized_input
        pending_rows.append((row, normalized_input))
        if normalized_input not in translated_by_text:
            translated_by_text[normalized_input] = ("", "pending")
            translation_inputs.append(normalized_input)

    if translation_inputs:
        try:
            result = asyncio.run(
                translate_batch_with_meta(
                    translation_inputs,
                    PUBLIC_HEALTH_TARGET_LANG,
                )
            )
            translations = result.get("translations") or translation_inputs
            providers = result.get("providers") or ["unknown" for _ in translation_inputs]
        except Exception as exc:
            logger.warning("Public health translation failed; using source titles: %s", exc)
            translations = translation_inputs
            providers = ["failed" for _ in translation_inputs]

        for source_text, translated, provider in zip(translation_inputs, translations, providers):
            clean = _postprocess_translated_title(translated) or source_text
            provider_name = _normalize_text(provider) or "unknown"
            translated_by_text[source_text] = (clean, provider_name)
            if provider_name not in {"", "none", "failed", "pending", "unknown", "cached"}:
                best_provider = provider_name

    for row, translation_input in pending_rows:
        translated, provider = translated_by_text.get(translation_input, (translation_input, "failed"))
        row["title_zh"] = translated or row.get("title_raw") or translation_input
        row["translation_provider"] = provider
        row.pop("_translation_input", None)

    return rows, best_provider


def _ingest_rows(con, rows: list[dict[str, Any]], *, fetched_at: str) -> dict[str, int]:
    counts = {"total": 0, "promed": 0, "who_don": 0}
    for row in rows:
        _, is_new = upsert_public_health_event(
            con,
            dedupe_key=row["dedupe_key"],
            source_key=row["source_key"],
            source_name=row["source_name"],
            category_key=row["category_key"],
            title_raw=row.get("title_raw") or "",
            title_zh=row.get("title_zh") or "",
            date_raw=row.get("date_raw") or "",
            published_at=row.get("published_at"),
            item_url=row.get("item_url") or "",
            list_url=row.get("list_url") or "",
            rank=row.get("rank"),
            payload=row.get("payload"),
            translation_provider=row.get("translation_provider"),
            fetched_at=fetched_at,
        )
        if is_new:
            counts["total"] += 1
            counts[row["source_key"]] = counts.get(row["source_key"], 0) + 1
    return counts


def fetch_public_health_once(*, db_path: str = "app.db") -> dict[str, Any]:
    initialize_database(db_path)
    started_at = utc_now_iso()
    raw = fetch_public_health_payloads()
    fetched_at = raw.get("fetched_at") or started_at

    promed_rows = _filter_early_warning_rows(parse_promed_latest_html(raw["promed"]["html"]))
    who_rows = _filter_outbreak_rows(parse_who_api_payload(raw["who"]["api_payload"], raw["who"]["page_html"]))

    all_rows = promed_rows + who_rows

    con = get_connection(db_path)
    try:
        all_rows, translation_provider = _translate_titles(con, all_rows)

        promed_source_id = upsert_source(
            con,
            name="ProMED",
            source_type="public_health",
            url=PROMED_URL,
            enabled=True,
        )
        who_source_id = upsert_source(
            con,
            name="WHO DON",
            source_type="public_health",
            url=WHO_LIST_URL,
            enabled=True,
        )

        new_counts = _ingest_rows(con, all_rows, fetched_at=fetched_at)
        finished_at = utc_now_iso()

        record_fetch_run(
            con,
            source_id=promed_source_id,
            source_name="ProMED",
            started_at=started_at,
            finished_at=finished_at,
            status="success",
            items_found=len(promed_rows),
            new_items=new_counts.get("promed", 0),
            error_message="",
        )
        record_fetch_run(
            con,
            source_id=who_source_id,
            source_name="WHO DON",
            started_at=started_at,
            finished_at=finished_at,
            status="success",
            items_found=len(who_rows),
            new_items=new_counts.get("who_don", 0),
            error_message="",
        )
        con.commit()

        latest_published_at = max((row.get("published_at") or "" for row in all_rows), default="")
        return {
            "status": "success",
            "started_at": started_at,
            "finished_at": finished_at,
            "fetched_at": fetched_at,
            "translation_provider": translation_provider,
            "promed_count": len(promed_rows),
            "who_count": len(who_rows),
            "items_found": len(all_rows),
            "new_items": new_counts.get("total", 0),
            "promed_new": new_counts.get("promed", 0),
            "who_new": new_counts.get("who_don", 0),
            "latest_published_at": latest_published_at,
            "who_api_url": raw["who"].get("api_url"),
        }
    except Exception as exc:
        finished_at = utc_now_iso()
        try:
            record_fetch_run(
                con,
                source_id=None,
                source_name="Public Health",
                started_at=started_at,
                finished_at=finished_at,
                status="failed",
                items_found=0,
                new_items=0,
                error_message=str(exc),
            )
            con.commit()
        except Exception:
            pass
        raise
    finally:
        con.close()
