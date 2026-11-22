"""One cached weather line per long-update slot, under the existing shared lock."""
import json
import os
import re
from datetime import datetime, timezone

import requests

from collectors.local_weather_fetcher import fetch_local_weather
from parsers.local_weather_parser import summarize_weather
from services.history.pipeline import failure_reason
from services.local_weather.prompt import PROMPT_VERSION, SYSTEM_PROMPT
from storage import get_dashboard_snapshot, save_dashboard_snapshot

MODEL = "gemini-3.5-flash-lite"
CACHE = "local_weather_line_v1"


def validate_line(text, summary):
    if not isinstance(text, str):
        raise ValueError("Missing weather line")
    text = re.sub(r"\s+", " ", text).strip()
    if not 8 <= len(text) <= 72 or not re.search(r"[\u3400-\u9fff]", text):
        raise ValueError("Invalid weather line length")
    forbidden = ["珠海", "台风", "雷电", "雷暴", "积水", "冰雹", "暴雪", "结冰", "半熔", "融化", "口水", "痰", "低云", "云层很低", "云层压得", "空气开始挂水珠", "降雨概率", "降水概率"]
    for tag, words in {"hot": ("酷热", "炎热", "酷暑"), "cold": ("严寒", "冻伤"),
                       "heavy_rain": ("暴雨", "倾盆"), "strong_wind": ("狂风",),
                       "sunny": ("暴晒", "紫外线", "晒伤")}.items():
        if tag not in summary["tags"]: forbidden.extend(words)
    if any(word in text for word in forbidden):
        raise ValueError("Unsupported weather claim")
    return text


def generate_line(summary, api_key):
    response = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent",
        headers={"x-goog-api-key": api_key},
        json={"systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
              "contents": [{"parts": [{"text": json.dumps(summary, ensure_ascii=False)}]}],
              "generationConfig": {"responseMimeType": "application/json", "maxOutputTokens": 4096,
                                   "thinkingConfig": {"thinkingLevel": "MEDIUM"}}}, timeout=(5, 75))
    response.raise_for_status()
    payload = response.json()
    if payload.get("modelVersion", MODEL) != MODEL:
        raise ValueError("Weather model version mismatch")
    candidate = payload["candidates"][0]
    if candidate.get("finishReason") != "STOP":
        raise ValueError("Incomplete weather response")
    result = json.loads("".join(p.get("text", "") for p in candidate["content"]["parts"] if not p.get("thought")))
    return validate_line(result.get("text"), summary)


def fallback_line(summary):
    tags = summary["tags"]
    if "strong_wind" in tags: return "风有点忙，发型今天建议采用随缘管理。"
    if "heavy_rain" in tags: return "天空正在集中办理泼水业务，出门这事可以再议。"
    if "hot" in tags: return "室外正在认真加热，出门可以先跟空调道个别。"
    if "cold" in tags: return "空气开始认真降温，被窝的挽留暂时有理有据。"
    if "rain_disagreement" in tags: return "几家说下，几家说不下。伞已申请成为薛定谔的伞。"
    if summary["rain_models"]: return "天空正在安排降水，带伞这事暂时不需要哲学讨论。"
    if "humid" in tags: return "空气湿度正在冲业绩，晾衣服需要一点耐心。"
    return "天气暂时挑不出毛病，可以趁它还没反悔出去走走。"


def refresh_local_weather_once(*, db_path, now=None):
    now = now or datetime.now(timezone.utc)
    slot = int(now.timestamp()) // 21600
    state = get_dashboard_snapshot(CACHE, db_path=db_path) or {}
    if state.get("attempted_slot") == slot:
        return {"status": state.get("last_attempt_status", "partial")}
    state.update(attempted_slot=slot, last_attempt_status="failed")
    save_dashboard_snapshot(CACHE, state, db_path=db_path)
    try:
        latitude = float(os.environ["INFOGRAB_WEATHER_LATITUDE"])
        longitude = float(os.environ["INFOGRAB_WEATHER_LONGITUDE"])
        if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
            raise ValueError("Invalid weather point")
        raw, metadata = fetch_local_weather(latitude, longitude)
        summary = summarize_weather(raw, metadata, now)
    except (requests.RequestException, ValueError, TypeError, KeyError, IndexError) as error:
        print(f"[warn] Local weather data unavailable ({failure_reason(error)})")
        return {"status": "failed"}
    status = "generated"
    try:
        key = os.environ.get("GEMINI_API_KEY", "").strip()
        if not key: raise ValueError("Weather API key unavailable")
        text = generate_line(summary, key)
    except (requests.RequestException, ValueError, TypeError, KeyError, IndexError, AttributeError) as error:
        print(f"[warn] Local weather editor unavailable ({failure_reason(error)}); using rule-based line")
        text, status = fallback_line(summary), "fallback"
    state.update(text=text, valid_until=summary["valid_until"], generated_at=now.isoformat(),
                 status=status, model=MODEL if status == "generated" else None,
                 last_attempt_status="success" if status == "generated" else "partial",
                 prompt_version=PROMPT_VERSION, summary=summary)
    save_dashboard_snapshot(CACHE, state, db_path=db_path)
    print(f"[ok] Local weather line: {summary['model_count']} fresh models; {status}")
    return {"status": "success" if status == "generated" else "partial"}


def get_local_weather(*, db_path, now=None):
    now = now or datetime.now(timezone.utc)
    state = get_dashboard_snapshot(CACHE, db_path=db_path) or {}
    try:
        if datetime.fromisoformat(state.get("valid_until", "")) <= now: return {}
    except (ValueError, TypeError):
        return {}
    # No coordinates, city, raw data, or private configuration in the public export.
    return {key: state.get(key, "") for key in ("text", "generated_at", "valid_until")}
