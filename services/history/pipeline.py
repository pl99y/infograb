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


def choose_candidates(events, seen, day):
    """Prefer China, leave room for foreign events, and rotate unseen material."""
    def rank(event):
        shuffle = hashlib.sha256(f"{day}|{event['id']}".encode()).hexdigest()
        return (event["id"] in seen, event["language"] != "zh", shuffle)

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


def _prune_seen(seen, day):
    cutoff = (day - timedelta(days=90)).isoformat()
    recent = {key: date for key, date in seen.items() if isinstance(date, str) and cutoff <= date <= day.isoformat()}
    return dict(sorted(recent.items(), key=lambda pair: pair[1], reverse=True)[:360])


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


def _use_fallback(state, today, now, slot):
    if state.get("date") == today:
        return state.get("generation_status") == "fallback"
    cards = factual_fallback(state.get("raw", [])) if state.get("raw_date") == today else []
    if not cards:
        return False
    state.update(date=today, cards=cards, selected_index=0, display_slot=slot,
                 generated_at=now.isoformat(), generation_status="fallback", model=None)
    return True


def refresh_history_once(*, db_path, now=None, retry_only=False):
    """Long jobs start/rotate; fast jobs only retry, both under the shared flock."""
    now = now or datetime.now(timezone.utc)
    day = now.astimezone(LOCAL_TIME).date()
    today = day.isoformat()
    slot = int(now.timestamp()) // (DISPLAY_HOURS * 3600)
    state = get_dashboard_snapshot(CACHE, db_path=db_path) or {}
    state["seen"] = _prune_seen(state.get("seen", {}), day)
    if state.get("date") == today and state.get("cards"):
        if not retry_only:
            previous_slot = state.get("display_slot", slot)
            state["selected_index"] = min(len(state["cards"]) - 1,
                                          state.get("selected_index", 0) + max(0, slot - previous_slot))
            state["display_slot"] = slot
            save_dashboard_snapshot(CACHE, state, db_path=db_path)
        if state.get("generation_status") != "fallback":
            return {"status": "success", "count": len(state["cards"])}

    last_attempt = state.get("last_attempt_at")
    attempted = datetime.fromisoformat(last_attempt) if last_attempt else None
    attempted_today = attempted is not None and attempted.astimezone(LOCAL_TIME).date() == day
    # Older snapshots have no counter; their last attempt counts as the initial one.
    count = state.get("attempt_count", 1) if attempted_today else 0
    due = None
    if attempted_today:
        due = (datetime.fromisoformat(state["next_attempt_at"]) if state.get("next_attempt_at")
               else attempted + timedelta(minutes=RETRY_MINUTES[min(max(count - 1, 0), len(RETRY_MINUTES) - 1)]))
    if ((retry_only and not attempted_today) or count >= 1 + len(RETRY_MINUTES)
            or (due is not None and now < due)):
        if _use_fallback(state, today, now, slot):
            save_dashboard_snapshot(CACHE, state, db_path=db_path)
        return {"status": "partial" if state.get("cards") else "failed"}
    state.update(last_attempt_at=now.isoformat(), attempt_count=count + 1, next_attempt_at=None)
    save_dashboard_snapshot(CACHE, state, db_path=db_path)
    try:
        candidates = state.get("raw", []) if state.get("raw_date") == today else []
        if not candidates:
            raw = fetch_history_events(day)
            events = parse_history_events(raw, day)
            candidates = choose_candidates(events, state["seen"], today)
            if not candidates:
                raise ValueError("No usable history candidates")
            state.update(raw_date=today, raw=candidates)
            save_dashboard_snapshot(CACHE, state, db_path=db_path)
        key = os.getenv("GEMINI_API_KEY", "").strip()
        if not key:
            raise ValueError("History API key unavailable")
        cards = generate_history(candidates, key)
    except (requests.RequestException, ValueError, KeyError, IndexError, TypeError) as error:
        fallback = _use_fallback(state, today, now, slot)
        state["last_error"] = failure_reason(error)
        if state["attempt_count"] <= len(RETRY_MINUTES):
            state["next_attempt_at"] = (now + timedelta(
                minutes=RETRY_MINUTES[state["attempt_count"] - 1])).isoformat()
        save_dashboard_snapshot(CACHE, state, db_path=db_path)
        print(f"[warn] History generation unavailable ({state['last_error']}); "
              + ("using today's source facts" if fallback else "no current Chinese source facts")
              + f"; daily attempt {state['attempt_count']}/{1 + len(RETRY_MINUTES)}")
        return {"status": "partial" if state.get("cards") else "failed"}

    selected_index = state.get("selected_index", 0) if state.get("date") == today else 0
    state.update(date=today, cards=cards, selected_index=min(len(cards) - 1, selected_index), display_slot=slot,
                 generated_at=now.isoformat(), model=MODEL, prompt_version=PROMPT_VERSION,
                 generation_status="generated", last_error=None, next_attempt_at=None)
    state["seen"].update({card["id"]: today for card in cards})
    state["seen"] = _prune_seen(state["seen"], day)
    save_dashboard_snapshot(CACHE, state, db_path=db_path)
    print(f"[ok] History generated {len(cards)} cards in one request")
    return {"status": "success", "count": len(cards)}


def get_history_today(*, db_path):
    state = get_dashboard_snapshot(CACHE, db_path=db_path) or {}
    return {key: state.get(key, default) for key, default in (
        ("date", ""), ("generated_at", ""), ("selected_index", 0), ("cards", []),
    )}
