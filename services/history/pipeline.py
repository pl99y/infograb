"""Daily packed generation with bounded retries under the shared dashboard lock."""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from collectors.history_fetcher import fetch_history_events
from parsers.history_parser import clean_text, parse_history_events
from services.history.prompt import PROMPT_VERSION, SYSTEM_PROMPT
from storage import get_dashboard_snapshot, save_dashboard_snapshot

MODEL = "gemini-3.5-flash-lite"
CACHE = "history_today_v1"
LOCAL_TIME = ZoneInfo("Asia/Shanghai")
MAX_CANDIDATES = 32
DISPLAY_HOURS = 6
RETRY_MINUTES = (15, 30, 60, 60)
FORBIDDEN = ("谁能想到", "这波", "拉满", "妥妥", "开启新篇章", "第一件事", "从此", "后来成为",
             "还没开始", "尚未开始", "比当事人", "紧张", "心想", "没想到", "嫌弃", "突破天际")


def choose_candidates(events, day):
    """Prefer China, leaving room for foreign events."""
    def rank(event):
        shuffle = hashlib.sha256(f"{day}|{event['id']}".encode()).hexdigest()
        return (event["language"] != "zh", shuffle)

    china = sorted((e for e in events if e["china"]), key=rank)
    world = sorted((e for e in events if not e["china"]), key=rank)
    selected = china[:20] + world[:12]
    selected_ids = {e["id"] for e in selected}
    remainder = sorted((e for e in events if e["id"] not in selected_ids), key=rank)
    return (selected + remainder)[:MAX_CANDIDATES]


def generate_history(candidates, api_key):
    response = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent",
        headers={"x-goog-api-key": api_key},
        json={"systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
              "contents": [{"parts": [{"text": json.dumps(candidates, ensure_ascii=False)}]}],
              "generationConfig": {"responseMimeType": "application/json", "maxOutputTokens": 8192,
                                   "thinkingConfig": {"thinkingLevel": "MEDIUM"}}},
        timeout=(5, 75),
    )
    response.raise_for_status()
    payload = response.json()
    returned_model = payload.get("modelVersion", "")
    if returned_model and returned_model != MODEL:
        raise ValueError("History model version mismatch")
    result = payload["candidates"][0]
    if result.get("finishReason") != "STOP":
        raise ValueError("Incomplete history response")
    parts = result["content"]["parts"]
    generated = json.loads("".join(p.get("text", "") for p in parts if not p.get("thought")))
    return validate_history(generated, candidates)


def validate_history(generated, candidates):
    if not isinstance(generated, dict) or not isinstance(generated.get("items"), list):
        raise ValueError("History response must contain items")
    lookup = {e["id"]: e for e in candidates}
    cards = []
    used = set()
    used_topics = []
    for row in generated["items"][:12]:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str):
            continue
        event = lookup.get(row["id"])
        if not event or event["id"] in used:
            continue
        title, comment = row.get("title"), row.get("comment")
        if not isinstance(title, str) or not isinstance(comment, str):
            continue
        title, comment = clean_text(title), clean_text(comment)
        if not 6 <= len(title) <= 24 or not 6 <= len(comment) <= 48:
            continue
        if not re.search(r"[\u3400-\u9fff]", title + comment) or any(word in title + comment for word in FORBIDDEN):
            continue
        fact = event["fact"]
        if event["language"] != "zh":
            translation = row.get("fact_zh")
            if not isinstance(translation, str) or not re.search(r"[\u3400-\u9fff]", translation):
                continue
            fact = clean_text(translation, 700)
        evidence = json.dumps(event, ensure_ascii=False)
        if any(number not in evidence for number in re.findall(r"\d+(?:\.\d+)?", title + comment + fact)):
            continue
        support = row.get("support_titles")
        if not isinstance(support, list):
            continue
        pages = [p for p in event["pages"] if p["title"] in support]
        if not pages:
            continue
        topics = {p["qid"] for p in event["pages"] if p.get("qid")}
        if any(year == event["year"] and topics & previous for year, previous in used_topics):
            continue
        cards.append({"id": event["id"], "year": event["year"], "month_day": event["month_day"],
                      "title": title, "comment": comment, "fact": fact, "china": event["china"],
                      "sources": [{"title": p["title"], "url": p["url"]} for p in pages[:3]],
                      "source_language": event["language"]})
        used.add(event["id"])
        used_topics.append((event["year"], topics))
        if len(cards) == 4:
            break
    if not cards:
        raise ValueError("No history cards passed validation")
    return sorted(cards, key=lambda card: not card["china"])


def factual_fallback(candidates):
    """Use today's Chinese source facts without inventing a translation or joke."""
    cards, used_topics = [], []
    for event in candidates:
        if event.get("language") != "zh":
            continue
        topics = {p["qid"] for p in event["pages"] if p.get("qid")}
        if any(year == event["year"] and topics & previous for year, previous in used_topics):
            continue
        cards.append({"id": event["id"], "year": event["year"], "month_day": event["month_day"],
                      "title": event["fact"], "comment": "", "fact": event["fact"],
                      "china": event["china"], "source_language": "zh",
                      "sources": [{"title": p["title"], "url": p["url"]} for p in event["pages"][:3]]})
        used_topics.append((event["year"], topics))
        if len(cards) == 4:
            break
    return cards


def failure_reason(error):
    # Never log request URLs, provider bodies, or credentials.
    if isinstance(error, requests.HTTPError):
        return f"HTTP {getattr(error.response, 'status_code', 'unknown')}"
    if isinstance(error, requests.RequestException):
        return type(error).__name__
    known = {"History model version mismatch", "Incomplete history response",
             "History response must contain items", "No history cards passed validation",
             "No usable history candidates", "History API key unavailable"}
    return str(error) if str(error) in known else type(error).__name__


def _load_days(db_path, today):
    """Migrate the old single-day snapshot and keep today/tomorrow only."""
    state = get_dashboard_snapshot(CACHE, db_path=db_path) or {}
    tomorrow = (today + timedelta(days=1)).isoformat()
    allowed = {today.isoformat(), tomorrow}
    days = state.get("days")
    if not isinstance(days, dict):
        days = {}
        if state.get("date") in allowed or state.get("raw_date") in allowed:
            date = state.get("date")
            if date not in allowed or state.get("generation_status") == "fallback":
                date = state.get("raw_date") or date
            if date in allowed:
                record = {key: value for key, value in state.items() if key not in ("seen", "display_slot")}
                if record.get("date") != date:
                    record.pop("cards", None)
                    record["date"] = date
                    record["generation_status"] = "fallback"
                days[date] = record
    days = {date: record for date, record in days.items() if date in allowed and isinstance(record, dict)}
    for record in days.values():
        record.pop("seen", None)
        record.pop("display_slot", None)
        if isinstance(record.get("cards"), list):
            record["cards"] = record["cards"][:4]
        if isinstance(record.get("raw"), list):
            record["raw"] = record["raw"][:MAX_CANDIDATES]
        if record.get("generation_status") == "generated":
            record.pop("raw", None)
            record.pop("raw_date", None)
    return days


def _save_days(days, db_path):
    save_dashboard_snapshot(CACHE, {"version": 2, "days": days}, db_path=db_path)


def _refresh_day(days, day, now, db_path):
    date = day.isoformat()
    record = days.setdefault(date, {"date": date})
    if record.get("generation_status") == "generated" and record.get("cards"):
        return {"status": "success", "count": len(record["cards"])}
    count = record.get("attempt_count", 1 if record.get("last_attempt_at") else 0)
    if count >= 1 + len(RETRY_MINUTES):
        return {"status": "partial" if record.get("cards") else "failed"}
    attempted = datetime.fromisoformat(record["last_attempt_at"]) if record.get("last_attempt_at") else None
    due = (datetime.fromisoformat(record["next_attempt_at"]) if record.get("next_attempt_at")
           else attempted + timedelta(minutes=RETRY_MINUTES[max(0, count - 1)]) if attempted else None)
    if due is not None and now < due:
        return {"status": "partial" if record.get("cards") else "failed"}
    record.update(last_attempt_at=now.isoformat(), attempt_count=count + 1, next_attempt_at=None)
    _save_days(days, db_path)
    try:
        candidates = record.get("raw", [])
        if not candidates:
            candidates = choose_candidates(parse_history_events(fetch_history_events(day), day), date)
            if not candidates:
                raise ValueError("No usable history candidates")
            record["raw"] = candidates
            _save_days(days, db_path)
        key = os.getenv("GEMINI_API_KEY", "").strip()
        if not key:
            raise ValueError("History API key unavailable")
        cards = generate_history(candidates, key)
    except (requests.RequestException, ValueError, KeyError, IndexError, TypeError) as error:
        if not record.get("cards"):
            record["cards"] = factual_fallback(record.get("raw", []))
        record.update(generation_status="fallback", model=None, last_error=failure_reason(error))
        if record["cards"]:
            record.setdefault("generated_at", now.isoformat())
        if record["attempt_count"] <= len(RETRY_MINUTES):
            record["next_attempt_at"] = (now + timedelta(minutes=RETRY_MINUTES[record["attempt_count"] - 1])).isoformat()
        _save_days(days, db_path)
        print(f"[warn] History {date} unavailable ({record['last_error']}); attempt {record['attempt_count']}/5")
        return {"status": "partial" if record.get("cards") else "failed"}
    record.update(cards=cards, generated_at=now.isoformat(), model=MODEL,
                  prompt_version=PROMPT_VERSION, generation_status="generated", last_error=None,
                  next_attempt_at=None, selected_index=record.get("selected_index", 0))
    record.pop("raw", None)
    record.pop("raw_date", None)
    _save_days(days, db_path)
    print(f"[ok] History {date} generated {len(cards)} cards in one packed request")
    return {"status": "success", "count": len(cards)}


def refresh_history_once(*, db_path, now=None, retry_only=False, prepare_tomorrow=False):
    """Prepare tomorrow at the evening long run; fast runs publish rollover/retry."""
    now = now or datetime.now(timezone.utc)
    local = now.astimezone(LOCAL_TIME)
    today = local.date()
    tomorrow = today + timedelta(days=1)
    days = _load_days(db_path, today)
    targets = [today]
    if (not retry_only and local.hour >= 20) or prepare_tomorrow:
        targets.append(tomorrow)
    elif retry_only and days.get(tomorrow.isoformat(), {}).get("last_attempt_at"):
        targets.append(tomorrow)
    for day in targets:
        record = days.get(day.isoformat(), {})
        if retry_only and not record.get("last_attempt_at") and record.get("generation_status") != "generated":
            continue
        _refresh_day(days, day, now, db_path)
    current = days.get(today.isoformat(), {})
    if not retry_only and current.get("cards"):
        # Matches the existing 02/08/14/20 long-update schedule; midnight starts at card zero.
        current["selected_index"] = min(len(current["cards"]) - 1, max(0, (local.hour - 2) // 6))
    _save_days(days, db_path)
    return {"status": "success" if current.get("generation_status") == "generated" else
            "partial" if current.get("cards") else "failed", "count": len(current.get("cards", []))}


def get_history_today(*, db_path, now=None):
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(LOCAL_TIME).date()
    days = _load_days(db_path, today)
    def public(record):
        return {key: record.get(key, default) for key, default in (
            ("date", ""), ("generated_at", ""), ("selected_index", 0), ("cards", []))}
    result = public(days.get(today.isoformat(), {"date": today.isoformat()}))
    result["days"] = {date: public(record) for date, record in days.items() if record.get("cards")}
    return result
