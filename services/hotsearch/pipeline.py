from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests

from collectors.hotsearch_fetcher import collect_tophub_hotsearch
from parsers.hotsearch_parser import parse_tophub_items, normalize_title_key

logger = logging.getLogger(__name__)

GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
GEMINI_MODEL = os.getenv("HOTSEARCH_GEMINI_MODEL", os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview"))
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

AI_TIMEOUT = 45
AI_RETRIES = 2


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_model_text(text: str) -> str:
    value = (text or "").strip()
    if value.startswith("```"):
        value = re.sub(r"^```(?:json)?\s*", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s*```$", "", value)
    return value.strip()


def _extract_model_text(data: dict[str, Any]) -> str:
    chunks: list[str] = []
    for cand in data.get("candidates", []) or []:
        content = cand.get("content") if isinstance(cand, dict) else None
        if not isinstance(content, dict):
            continue
        for part in content.get("parts", []) or []:
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                chunks.append(part["text"])
    return "\n".join(chunks).strip()


def _extract_json_object(text: str) -> dict[str, Any]:
    cleaned = _clean_model_text(text)
    if not cleaned:
        raise ValueError("empty model response")
    try:
        data = json.loads(cleaned)
        return data if isinstance(data, dict) else {"raw": data}
    except Exception:
        pass

    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if not match:
        raise ValueError("model response did not contain a JSON object")
    data = json.loads(match.group(0))
    return data if isinstance(data, dict) else {"raw": data}


def _fallback_digest(reason: str = "") -> dict[str, Any]:
    return {
        "ok": False,
        "summary": "",
        "priority_items": [],
        "no_major_news": None,
        "error": reason,
        "model": GEMINI_MODEL,
    }


def _build_ai_prompt(items: list[dict[str, Any]]) -> str:
    compact_items = [
        {
            "source": item.get("source_name"),
            "rank": item.get("rank"),
            "title": item.get("title"),
            "metric": item.get("metric"),
        }
        for item in items
    ]

    return (
        "你是一个中文新闻热点筛选助手。下面是来自微博热搜、百度实时热点、哔哩哔哩热搜的榜单标题。\n"
        "请只根据标题本身判断当前热搜中最值得关注的公共新闻，不要编造标题之外的事实。\n"
        "优先关注：重大伤亡事故、公共安全、灾害、公共卫生、政策监管、国际局势、经济金融、科技产业重大变化。\n"
        "降低权重：明星八卦、娱乐营销、饭圈、普通体育赛果、生活梗、节日互动、广告软文。\n"
        "如果没有明显值得关注的公共新闻，请明确 no_major_news=true，并用一句话说明。\n"
        "请严格返回 JSON object，不要 markdown，不要解释。格式：\n"
        "{\n"
        "  \"summary\": \"一句话到三句话中文摘要\",\n"
        "  \"no_major_news\": false,\n"
        "  \"priority_items\": [\n"
        "    {\"title\": \"标题\", \"source\": \"来源\", \"rank\": 1, \"category\": \"公共安全/国际/经济/科技/社会/其他\", \"reason\": \"为什么值得关注\"}\n"
        "  ],\n"
        "  \"noise_note\": \"可选：娱乐/营销类噪音概况\"\n"
        "}\n\n"
        f"热搜条目 JSON：\n{json.dumps(compact_items, ensure_ascii=False)}"
    )


def generate_hotsearch_ai_digest(items: list[dict[str, Any]]) -> dict[str, Any]:
    if not items:
        return _fallback_digest("no hotsearch items")
    if not GEMINI_API_KEY:
        return _fallback_digest("GEMINI_API_KEY is not set")

    prompt = _build_ai_prompt(items)
    url = f"{GEMINI_API_BASE}/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "topP": 0.8,
            "maxOutputTokens": 1200,
        },
    }

    last_error = ""
    for attempt in range(1, AI_RETRIES + 1):
        try:
            response = requests.post(url, json=payload, timeout=AI_TIMEOUT)
            if response.status_code >= 400:
                last_error = f"HTTP {response.status_code}: {response.text[:500]}"
                if response.status_code in {429, 500, 502, 503, 504} and attempt < AI_RETRIES:
                    time.sleep(4 * attempt)
                    continue
                return _fallback_digest(last_error)

            data = response.json()
            model_text = _extract_model_text(data)
            parsed = _extract_json_object(model_text)

            priority_items = parsed.get("priority_items")
            if not isinstance(priority_items, list):
                priority_items = []

            return {
                "ok": True,
                "summary": str(parsed.get("summary") or "").strip(),
                "priority_items": priority_items[:6],
                "no_major_news": bool(parsed.get("no_major_news", False)),
                "noise_note": str(parsed.get("noise_note") or "").strip(),
                "model": GEMINI_MODEL,
                "error": "",
            }
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < AI_RETRIES:
                time.sleep(4 * attempt)

    return _fallback_digest(last_error)


def _dedupe_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = item.get("title_key") or normalize_title_key(str(item.get("title") or ""))
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        public_item = dict(item)
        public_item.pop("title_key", None)
        deduped.append(public_item)
    return deduped


def fetch_hotsearch_snapshot(*, include_ai_digest: bool = True) -> dict[str, Any]:
    generated_at = utc_now_iso()
    pages = collect_tophub_hotsearch()

    source_payloads: list[dict[str, Any]] = []
    merged_items: list[dict[str, Any]] = []

    for page in pages:
        html = page.pop("html", "") or ""
        source_id = str(page.get("source_id") or "")
        source_name = str(page.get("source_name") or "")
        source_url = str(page.get("source_url") or "")
        limit = int(page.get("limit") or 0)

        items: list[dict[str, Any]] = []
        parse_error = ""
        if page.get("ok"):
            try:
                items = parse_tophub_items(
                    html,
                    source_id=source_id,
                    source_name=source_name,
                    source_url=source_url,
                    limit=limit,
                    fetched_at=str(page.get("fetched_at") or generated_at),
                )
            except Exception as exc:
                parse_error = f"{type(exc).__name__}: {exc}"

        public_meta = {
            key: value
            for key, value in page.items()
            if key not in {"final_url", "content_type", "body_length", "elapsed_sec"}
        }
        public_meta["parse_error"] = parse_error
        public_meta["items_count"] = len(items)
        public_meta["items"] = [
            {k: v for k, v in item.items() if k != "title_key"}
            for item in items
        ]

        source_payloads.append(public_meta)
        merged_items.extend(items)

    merged_items = _dedupe_items(merged_items)
    ai_digest = generate_hotsearch_ai_digest(merged_items) if include_ai_digest else _fallback_digest("AI digest disabled")

    return {
        "generated_at": generated_at,
        "ok": any(bool(source.get("ok")) for source in source_payloads),
        "source_name": "TopHub 今日热榜",
        "sources": source_payloads,
        "merged_count": len(merged_items),
        "items": merged_items,
        "ai_digest": ai_digest,
    }
