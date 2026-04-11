"""Batch translation helpers backed by the Gemini API."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import time
from typing import Any, List

import httpx

logger = logging.getLogger(__name__)

GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = "gemini-3.1-flash-lite-preview"

# Global behavior defaults.
# New board pipelines can simply call translate_batch_with_meta(texts, "zh").
DEFAULT_REQUESTS_PER_CALL = 1
DEFAULT_MAX_CONCURRENCY = 1
DEFAULT_ALLOW_SINGLE_FALLBACK = False

# Safety and compatibility caps.
REQUEST_TIMEOUT = 45.0
REQUEST_RETRIES = 3
MAX_REQUESTS_PER_CALL_CAP = 3
LEGACY_HARD_MAX_BATCH_SIZE = 1000

# Gemini free tier is easy to hit on RPM.
MIN_SECONDS_BETWEEN_REQUESTS = 4.2
RATE_LIMIT_FALLBACK_WAIT = 35.0
_PROVIDER_NAME = "gemini:3.1-flash-lite"
_SYSTEM_PROMPT = (
    "You are a translation engine. "
    "Translate directly. "
    "Return only the translation result in the required JSON format. "
    "Do not explain, do not add notes, do not wrap in markdown."
)

_REQUEST_GATE = asyncio.Lock()
_NEXT_REQUEST_TS = 0.0


def _require_api_key() -> str:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is not set")
    return GEMINI_API_KEY


class GeminiRateLimitError(RuntimeError):
    def __init__(self, message: str, retry_after: float) -> None:
        super().__init__(message)
        self.retry_after = max(0.0, float(retry_after))


def _normalize_target_lang(target_lang: str) -> str:
    if target_lang == "zh":
        return "zh"
    if target_lang == "en":
        return "en"
    return target_lang


def _target_lang_label(target_lang: str) -> str:
    normalized = _normalize_target_lang(target_lang)
    if normalized == "zh":
        return "Simplified Chinese"
    if normalized == "en":
        return "English"
    return normalized


def _script_ratio(text: str, checker) -> float:
    chars = [c for c in text if not c.isspace()]
    if not chars:
        return 0.0
    matched = sum(1 for c in chars if checker(c))
    return matched / len(chars)


def _detect_source_lang(text: str) -> str:
    if not text or not text.strip():
        return "en"

    if any(c in text for c in "іїєґІЇЄҐ"):
        return "uk"

    zh_ratio = _script_ratio(text, lambda c: "一" <= c <= "鿿")
    ja_ratio = _script_ratio(text, lambda c: "぀" <= c <= "ヿ")
    ru_ratio = _script_ratio(text, lambda c: "Ѐ" <= c <= "ӿ")
    ar_ratio = _script_ratio(text, lambda c: "؀" <= c <= "ۿ")

    if ja_ratio > 0.15:
        return "ja"
    if zh_ratio > 0.20:
        return "zh"
    if ar_ratio > 0.20:
        return "ar"
    if ru_ratio > 0.20:
        return "ru"

    return "en"


def _is_target_lang(text: str, target_lang: str) -> bool:
    if not text or not text.strip():
        return True
    return _detect_source_lang(text) == _normalize_target_lang(target_lang)


def _chunk_list(items: list[int], size: int) -> list[list[int]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _build_prompt(texts: list[str], target_lang: str) -> str:
    return (
        f"Translate this JSON array to {_target_lang_label(target_lang)}. "
        "Return only a JSON array of strings in the same order and same length. "
        "Do not explain. Preserve names, numbers, URLs, emojis, hashtags, cashtags, and formatting unless translation requires otherwise.\n"
        f"{json.dumps(texts, ensure_ascii=False)}"
    )


def _clean_model_text(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _extract_text_from_response(data: dict[str, Any]) -> str:
    candidates: list[str] = []
    for cand in data.get("candidates", []):
        if not isinstance(cand, dict):
            continue
        content = cand.get("content")
        if not isinstance(content, dict):
            continue
        for part in content.get("parts", []):
            if isinstance(part, dict):
                maybe_text = part.get("text")
                if isinstance(maybe_text, str) and maybe_text.strip():
                    candidates.append(maybe_text)
    merged = "\n".join(candidates).strip()
    if merged:
        return merged
    raise RuntimeError(f"Gemini response had no readable text. keys={sorted(data.keys())}")


def _extract_json_candidate(raw_text: str) -> str:
    cleaned = _clean_model_text(raw_text)
    if not cleaned:
        return cleaned
    if cleaned[0] in "[{":
        return cleaned
    arr_match = re.search(r"\[.*\]", cleaned, flags=re.DOTALL)
    obj_match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if arr_match:
        return arr_match.group(0).strip()
    if obj_match:
        return obj_match.group(0).strip()
    return cleaned


def _normalize_line_item(line: str) -> str:
    line = line.strip()
    line = re.sub(r"^[-*•]\s*", "", line)
    line = re.sub(r"^\d+[.)\-:]\s*", "", line)
    return line.strip()


def _parse_translations(raw_text: str, expected_count: int) -> list[str]:
    candidate = _extract_json_candidate(raw_text)

    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, list):
            result = [str(item) for item in parsed]
            if len(result) == expected_count:
                return result
        elif isinstance(parsed, dict):
            translations = parsed.get("translations")
            if isinstance(translations, list):
                result = [str(item) for item in translations]
                if len(result) == expected_count:
                    return result
    except Exception:
        pass

    lines = [_normalize_line_item(line) for line in candidate.splitlines()]
    lines = [line for line in lines if line]
    if len(lines) == expected_count:
        return lines

    if expected_count == 1 and candidate.strip():
        return [candidate.strip()]

    raise RuntimeError(
        f"Gemini returned an unparsable translation payload for {expected_count} inputs: {raw_text[:500]}"
    )


def _extract_retry_after_seconds(text: str) -> float:
    match = re.search(r"retry in\s+([0-9]+(?:\.[0-9]+)?)s", text, flags=re.IGNORECASE)
    if match:
        try:
            return max(0.0, float(match.group(1)))
        except Exception:
            pass
    return RATE_LIMIT_FALLBACK_WAIT


async def _respect_request_gap() -> None:
    global _NEXT_REQUEST_TS
    async with _REQUEST_GATE:
        now = time.monotonic()
        if _NEXT_REQUEST_TS > now:
            await asyncio.sleep(_NEXT_REQUEST_TS - now)
            now = time.monotonic()
        _NEXT_REQUEST_TS = now + MIN_SECONDS_BETWEEN_REQUESTS


def _bump_request_gate(delay_seconds: float) -> None:
    global _NEXT_REQUEST_TS
    target = time.monotonic() + max(0.0, delay_seconds)
    if target > _NEXT_REQUEST_TS:
        _NEXT_REQUEST_TS = target


async def _post_generate_content(client: httpx.AsyncClient, payload: dict[str, Any]) -> dict[str, Any]:
    await _respect_request_gap()
    resp = await client.post(
        f"{GEMINI_API_BASE.rstrip('/')}/models/{GEMINI_MODEL}:generateContent",
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )

    if resp.status_code == 429:
        text = resp.text[:2000]
        retry_after = _extract_retry_after_seconds(text)
        _bump_request_gate(retry_after)
        raise GeminiRateLimitError(f"HTTP 429: {text}", retry_after)

    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:2000]}")

    return resp.json()


async def _call_gemini_batch(client: httpx.AsyncClient, texts: list[str], target_lang: str) -> list[str]:
    payload = {
        "systemInstruction": {
            "parts": [{"text": _SYSTEM_PROMPT}],
        },
        "contents": [
            {
                "role": "user",
                "parts": [{"text": _build_prompt(texts, target_lang)}],
            }
        ],
        "generationConfig": {
            "temperature": 0,
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "ARRAY",
                "items": {"type": "STRING"},
            },
            "thinkingConfig": {
                "thinkingBudget": 0,
            },
        },
    }

    last_exc: Exception | None = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            data = await _post_generate_content(client, payload)
            raw_text = _extract_text_from_response(data)
            return _parse_translations(raw_text, expected_count=len(texts))
        except GeminiRateLimitError as exc:
            last_exc = exc
            logger.warning(
                "Gemini batch hit rate limit (attempt %s/%s): retry_after=%.1fs",
                attempt,
                REQUEST_RETRIES,
                exc.retry_after,
            )
            if attempt < REQUEST_RETRIES:
                await asyncio.sleep(exc.retry_after)
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Gemini batch failed (attempt %s/%s) [%s]: %s",
                attempt,
                REQUEST_RETRIES,
                type(exc).__name__,
                exc,
            )
            if attempt < REQUEST_RETRIES:
                await asyncio.sleep(0.75 * attempt)

    if last_exc is None:
        raise RuntimeError("Gemini batch failed for unknown reason")
    raise RuntimeError(f"Gemini batch failed after retries: {type(last_exc).__name__}: {last_exc}")


async def _call_gemini_single(client: httpx.AsyncClient, text: str, target_lang: str) -> str:
    translated = await _call_gemini_batch(client, [text], target_lang)
    return translated[0]


def _legacy_batch_size_to_request_count(pending_count: int, batch_size: int, hard_max_batch_size: int | None) -> int:
    size = max(1, int(batch_size))
    if hard_max_batch_size is not None:
        size = min(size, max(1, int(hard_max_batch_size)))
    if pending_count <= 0:
        return 1
    return max(1, math.ceil(pending_count / size))


def _resolve_request_plan(
    pending_count: int,
    *,
    batch_size: int | None,
    hard_max_batch_size: int | None,
    request_count: int | None,
) -> tuple[int, int]:
    """Return (effective_request_count, effective_batch_size)."""
    if pending_count <= 0:
        return 1, 1

    if request_count is not None:
        effective_request_count = max(1, min(int(request_count), MAX_REQUESTS_PER_CALL_CAP))
        return effective_request_count, max(1, math.ceil(pending_count / effective_request_count))

    # Backward compatibility: if legacy batch params are explicitly passed, honor them.
    if batch_size is not None or hard_max_batch_size is not None:
        legacy_batch_size = batch_size if batch_size is not None else LEGACY_HARD_MAX_BATCH_SIZE
        effective_request_count = _legacy_batch_size_to_request_count(
            pending_count,
            legacy_batch_size,
            hard_max_batch_size,
        )
        effective_request_count = max(1, min(effective_request_count, MAX_REQUESTS_PER_CALL_CAP))
        return effective_request_count, max(1, math.ceil(pending_count / effective_request_count))

    effective_request_count = max(1, min(DEFAULT_REQUESTS_PER_CALL, MAX_REQUESTS_PER_CALL_CAP))
    return effective_request_count, max(1, math.ceil(pending_count / effective_request_count))


def _should_fallback_to_singles(exc: Exception) -> bool:
    return not isinstance(exc, GeminiRateLimitError)


async def translate_batch_with_meta(
    texts: List[str],
    target_lang: str,
    *,
    batch_size: int | None = None,
    max_concurrency: int | None = None,
    hard_max_batch_size: int | None = None,
    allow_single_fallback: bool | None = None,
    request_count: int | None = None,
) -> dict:
    if not texts:
        return {"translations": [], "providers": []}

    effective_concurrency = max(1, int(max_concurrency or DEFAULT_MAX_CONCURRENCY))
    effective_allow_single_fallback = (
        DEFAULT_ALLOW_SINGLE_FALLBACK
        if allow_single_fallback is None
        else bool(allow_single_fallback)
    )

    results: list[str] = list(texts)
    providers: list[str] = ["none" for _ in texts]

    pending_indices: list[int] = []
    for i, text in enumerate(texts):
        if not text or not text.strip():
            providers[i] = "none"
            continue
        if _is_target_lang(text, target_lang):
            providers[i] = "none"
            continue
        pending_indices.append(i)
        providers[i] = "pending"

    if not pending_indices:
        return {"translations": results, "providers": providers}

    effective_request_count, effective_batch_size = _resolve_request_plan(
        len(pending_indices),
        batch_size=batch_size,
        hard_max_batch_size=hard_max_batch_size,
        request_count=request_count,
    )
    index_batches = _chunk_list(pending_indices, effective_batch_size)

    logger.info(
        "Translation plan: pending=%s requests=%s batch_size=%s concurrency=%s single_fallback=%s",
        len(pending_indices),
        len(index_batches),
        effective_batch_size,
        effective_concurrency,
        effective_allow_single_fallback,
    )

    semaphore = asyncio.Semaphore(effective_concurrency)

    async with httpx.AsyncClient(
        headers={
            "x-goog-api-key": _require_api_key(),
            "Content-Type": "application/json",
            "User-Agent": "InfoGraber/1.0",
        },
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        http2=False,
    ) as client:

        async def run_one_batch(batch_indices: list[int]) -> tuple[list[int], list[str], str]:
            batch_texts = [texts[idx] for idx in batch_indices]
            async with semaphore:
                try:
                    translated = await _call_gemini_batch(client, batch_texts, target_lang)
                    return batch_indices, translated, _PROVIDER_NAME
                except Exception as batch_exc:
                    if effective_allow_single_fallback and _should_fallback_to_singles(batch_exc):
                        logger.warning(
                            "Gemini batch group failed; falling back to singles for %s items: %s",
                            len(batch_indices),
                            batch_exc,
                        )
                        single_results: list[str] = []
                        for idx in batch_indices:
                            try:
                                single_results.append(await _call_gemini_single(client, texts[idx], target_lang))
                            except Exception as single_exc:
                                logger.error(
                                    "Gemini single translation failed for index %s [%s]: %s",
                                    idx,
                                    type(single_exc).__name__,
                                    single_exc,
                                )
                                single_results.append("")
                        return batch_indices, single_results, _PROVIDER_NAME

                    logger.warning(
                        "Gemini batch group failed without single fallback for %s items: %s",
                        len(batch_indices),
                        batch_exc,
                    )
                    return batch_indices, ["" for _ in batch_indices], "failed"

        batch_results = await asyncio.gather(
            *(run_one_batch(batch) for batch in index_batches),
            return_exceptions=True,
        )

    for item in batch_results:
        if isinstance(item, Exception):
            logger.error("Gemini translation batch task failed: %s", item)
            continue
        batch_indices, translated_texts, provider = item
        for idx, translated in zip(batch_indices, translated_texts):
            clean = str(translated).strip()
            if clean:
                results[idx] = clean
                providers[idx] = provider
            else:
                results[idx] = texts[idx]
                providers[idx] = "failed"

    for idx in pending_indices:
        if providers[idx] == "pending":
            results[idx] = texts[idx]
            providers[idx] = "failed"

    return {"translations": results, "providers": providers}


async def translate_batch(
    texts: List[str],
    target_lang: str,
    *,
    batch_size: int | None = None,
    max_concurrency: int | None = None,
    hard_max_batch_size: int | None = None,
    allow_single_fallback: bool | None = None,
    request_count: int | None = None,
) -> List[str]:
    data = await translate_batch_with_meta(
        texts,
        target_lang,
        batch_size=batch_size,
        max_concurrency=max_concurrency,
        hard_max_batch_size=hard_max_batch_size,
        allow_single_fallback=allow_single_fallback,
        request_count=request_count,
    )
    return data["translations"]
