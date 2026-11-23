"""Ground a shared AI selection in actual current panel items; alert colors default to shadow mode."""
import hashlib
import html
import json
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from collectors.focus_news_fetcher import fetch_focus_news
from services.disaster.queries import get_disaster_ongoing_groups, get_weather_alerts
from services.news_timeline.queries import get_news_timeline_latest
from services.telegram.queries import get_recent_posts
from services.public_health.queries import get_public_health_latest
from services.market.queries import get_market_snapshots_latest
from services.energy.queries import get_energy_latest
from services.commodities.pipeline import get_commodities
from services.space_weather.queries import get_space_weather_alerts, get_space_weather_forecast, get_space_launches_latest
from services.f1.queries import get_f1_news
from services.mnd_pla.queries import get_mnd_pla_latest
from services.aviation.queries import get_airport_alerts, get_airport_disruptions_latest
from storage import get_dashboard_snapshot, save_dashboard_snapshot

CACHE = "current_focus_v1"
OFFICIAL = ("who.int", "un.org", "usgs.gov", "tsunami.gov", "noaa.gov", "jma.go.jp", "gov.cn", "hko.gov.hk", "gdacs.org")
NEWS_ORIGINS = {"zaobao.com.sg": "zaobao", "bbc.com": "bbc", "bbc.co.uk": "bbc",
                "feeds.bbci.co.uk": "bbc", "news.mingpao.com": "mingpao", "hk01.com": "hk01"}
CRITICAL_TYPES = {"major_power_war", "nuclear_use", "global_financial_disruption", "global_health_emergency", "multinational_catastrophe"}
UNCERTAIN = re.compile(r"疑似|传闻|未证实|谣言|演习|回顾|假设|否认|可能|\b(?:unconfirmed|rumou?r|drill|simulation|anniversary|denies|could|may)\b", re.I)


def clean(value, limit=300):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", str(value or "")))).strip()[:limit]


def safe_url(value):
    try:
        url = urlparse(value or "")
        return value if url.scheme in ("http", "https") and url.hostname and not url.username else ""
    except (TypeError, ValueError):
        return ""


def origin_of(row):
    host = (urlparse(row.get("url") or "").hostname or "").lower().removeprefix("www.")
    # Reprints explicitly credited to the same wire count as a single origin.
    credit = clean(row.get("authors"))
    for pattern, origin in ((r"Reuters|路透", "reuters"), (r"Associated Press|美联社", "ap"), (r"AFP|法新社", "afp")):
        if re.search(pattern, credit, re.I): return origin
    official = next((domain for domain in OFFICIAL if host == domain or host.endswith("." + domain)), None)
    return "official:" + official if official else NEWS_ORIGINS.get(host, host)


def is_official(row):
    host = (urlparse(row.get("url") or "").hostname or "").lower()
    return any(host == domain or host.endswith("." + domain) for domain in OFFICIAL)


def normalize_candidates(rows, now):
    result, seen = [], set()
    for row in rows:
        title = clean(row.get("title"), 220)
        url = safe_url(row.get("url"))
        try:
            published = datetime.fromisoformat(str(row.get("published_at") or "").replace("Z", "+00:00"))
            if published.tzinfo is None: published = published.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if not title or not url or not -600 <= (now - published).total_seconds() <= 86400:
            continue
        if re.search(r"casino|sportsbook|rainbet|抽奖|博彩", title, re.I): continue
        key = origin_of(row) + ":" + re.sub(r"\W+", "", title.casefold())
        if key in seen: continue
        seen.add(key)
        item = {"id": hashlib.sha256((url + title).encode()).hexdigest()[:20], "title": title,
                "snippet": clean(row.get("snippet"), 350), "url": url,
                "source_name": clean(row.get("source_name"), 60), "source_kind": row.get("source_kind", "news"),
                "published_at": published.isoformat(), "score": int(row.get("score", 1)),
                "authors": clean(row.get("authors"), 150)}
        item["official"] = is_official(item)
        item["origin"] = origin_of(item)
        result.append(item)
    result.sort(key=lambda row: (row["score"], row["published_at"]), reverse=True)
    # Keep representation from the panels instead of letting a large feed crowd them out.
    selected, counts = [], {}
    for row in result:
        kind = row["source_kind"]
        if counts.get(kind, 0) < 2:
            selected.append(row); counts[kind] = counts.get(kind, 0) + 1
    selected = selected[:40]
    chosen_ids = {row["id"] for row in selected}
    for row in result:
        if len(selected) == 40: break
        if row["id"] in chosen_ids: continue
        kind = row["source_kind"]
        if counts.get(kind, 0) >= (16 if kind == "news" else 8): continue
        selected.append(row); counts[kind] = counts.get(kind, 0) + 1
    return sorted(selected, key=lambda row: (row["score"], row["published_at"]), reverse=True)


def build_focus_candidates(*, db_path, now=None):
    now = now or datetime.now(timezone.utc)
    rows = fetch_focus_news()
    for row in rows: row["score"] = 4
    news = get_news_timeline_latest(limit=35, db_path=db_path).get("items", [])
    rows += [{"title": r.get("title"), "url": r.get("url"), "published_at": r.get("published_at"),
              "source_name": r.get("source_name"), "source_kind": "news", "score": 4,
              "authors": r.get("author_names")} for r in news]
    rows += [{"title": r.get("text_zh") or r.get("text"), "url": r.get("post_url"),
              "published_at": r.get("published_at"), "source_name": r.get("source_name"),
              "source_kind": "telegram", "score": 2} for r in get_recent_posts(limit=16, db_path=db_path)]
    for kind, data in (("disaster", get_disaster_ongoing_groups(limit=15, db_path=db_path)),
                       ("weather", get_weather_alerts(limit=15, db_path=db_path))):
        for r in data:
            payload = r.get("payload") or {}
            display = r.get("display_zh") or {}
            rows.append({"title": " · ".join(filter(None, [display.get("title") or r.get("title"), display.get("region") or r.get("location_text")])),
                         "snippet": display.get("summary") or r.get("summary"),
                         "url": r.get("source_url") or r.get("link_url") or r.get("map_url") or payload.get("url"),
                         "published_at": r.get("occurred_at") or r.get("published_at") or r.get("issued_at"),
                         "source_name": r.get("source_name") or kind, "source_kind": kind, "score": 3})
    health = get_public_health_latest(limit_early_warning=8, limit_outbreak_events=8, db_path=db_path)
    for r in health.get("early_warning", []) + health.get("outbreak_events", []):
        rows.append({"title": r.get("title_zh") or r.get("title"), "url": r.get("item_url") or r.get("list_url"),
                     "published_at": r.get("published_at"), "source_name": r.get("source_name"), "source_kind": "health", "score": 3})
    for kind, quotes, threshold in (("market", get_market_snapshots_latest(db_path=db_path), 3),
                                    ("energy", get_energy_latest(db_path=db_path), 7),
                                    ("commodity", get_commodities(db_path=db_path), 7)):
        for r in quotes:
            change = r.get("change_percent")
            if not isinstance(change, (int, float)) or abs(change) < threshold or r.get("status") in ("stale", "unavailable"):
                continue
            symbol = r.get("symbol", "")
            if kind == "market" and not r.get("quote_time"):
                continue
            rows.append({"title": f"{r.get('name') or r.get('key')} 报价较前收盘变化 {change:+.2f}%",
                         "url": "https://finance.yahoo.com/quote/" + str(symbol) if symbol else "https://www.oilprice.com/",
                         "published_at": r.get("quote_time") or r.get("timestamp_text") or r.get("fetched_at"),
                         "source_name": r.get("source") or kind, "source_kind": kind, "score": 4})
    # Less urgent panels remain eligible without displacing breaking news.
    for kind, getter in (("f1", lambda: get_f1_news(limit=4, db_path=db_path)),
                         ("space", lambda: get_space_weather_alerts(limit=6, db_path=db_path))):
        for r in getter():
            rows.append({"title": r.get("title_zh") or r.get("title") or r.get("headline") or r.get("message"),
                         "snippet": r.get("impacts_text") or r.get("description_text") or r.get("summary"),
                         "url": r.get("url") or r.get("source_url") or r.get("link_url") or
                                ("https://www.swpc.noaa.gov/products/alerts-watches-and-warnings" if kind == "space" else ""),
                         "published_at": r.get("published_at") or r.get("issue_datetime"),
                         "source_name": r.get("source_name") or kind, "source_kind": kind, "score": 1})
    for r in get_mnd_pla_latest(limit=1, db_path=db_path):
        rows.append({"title": r.get("title"), "snippet": r.get("activity_text") or r.get("body"),
                     "url": r.get("post_url") or r.get("source_url"), "published_at": r.get("period_end") or r.get("report_date"),
                     "source_name": r.get("source_name"), "source_kind": "taiwan_strait", "score": 2})
    for r in get_airport_alerts(limit=4, db_path=db_path) + get_airport_disruptions_latest(limit=4, db_path=db_path):
        if r.get("age_hours") and r["age_hours"] > 24: continue
        if "canceled_percent" in r and (r.get("canceled_percent") or 0) < 20 and (r.get("average_delay_min") or 0) < 60: continue
        title = r.get("title") or r.get("airport_name") or f"{r.get('callsign', '')} 应答机信号 {r.get('squawk_code') or ''}"
        rows.append({"title": title, "snippet": r.get("status_text") or r.get("description") or r.get("delay_note"),
                     "url": r.get("source_url"), "published_at": r.get("published_at") or r.get("fetched_at"),
                     "source_name": r.get("source_name"), "source_kind": "aviation", "score": 1})
    launches = get_space_launches_latest(db_path=db_path)
    for r in launches.get("items", [])[:3]:
        rows.append({"title": r.get("title") or r.get("name"), "url": r.get("url") or launches.get("source_url"),
                     "published_at": r.get("net") or r.get("window_start"), "source_name": launches.get("source_name"),
                     "source_kind": "launch", "score": 1})
    return normalize_candidates(rows, now)


def assess_alert(selection, candidates):
    lookup = {r["id"]: r for r in candidates}
    ids = list(dict.fromkeys(i for i in selection.get("support_ids", []) if isinstance(i, str)))[:6]
    evidence = [lookup[i] for i in ids if i in lookup]
    eligible = [r for r in evidence if r["source_kind"] != "telegram"]
    origins = {r["origin"] for r in eligible if r["origin"]}
    official = [r for r in eligible if r["official"]]
    independent_news = {r["origin"] for r in eligible if r["source_kind"] == "news" and not r["official"]}
    uncertain = any(UNCERTAIN.search(r["title"]) for r in evidence)
    proposed = selection.get("level", "normal")
    critical = (proposed == "critical" and selection.get("global_impact") is True
                and selection.get("event_type") in CRITICAL_TYPES and bool(official)
                and len(independent_news) >= 2 and len(origins) >= 3 and not uncertain)
    major = proposed in ("major", "critical") and len(origins) >= 2 and not uncertain
    return {"shadow_level": "critical" if critical else "major" if major else "normal",
            "proposed_level": proposed if proposed in ("normal", "major", "critical") else "normal",
            "origins": sorted(origins), "official_count": len(official), "uncertain": uncertain,
            "evidence": [{k: r[k] for k in ("id", "title", "url", "source_name", "origin")} for r in evidence]}


def store_focus(candidates, selection, *, db_path, now=None):
    now = now or datetime.now(timezone.utc)
    lookup = {row["id"]: row for row in candidates}
    selection = selection if isinstance(selection, dict) else {}
    chosen = lookup.get(selection.get("id")) if isinstance(selection.get("id"), str) else None
    valid_ai = chosen is not None
    if not chosen:
        # A Telegram rumor cannot become the default headline when the editor fails.
        eligible = [r for r in candidates if r["source_kind"] not in ("telegram", "f1")]
        chosen = next((r for r in eligible if re.search(r"[\u3400-\u9fff]", r["title"])), eligible[0] if eligible else None)
    if not chosen:
        previous = get_dashboard_snapshot(CACHE, db_path=db_path) or {}
        if previous.get("alert_level") in ("major", "critical"):
            # A collection outage is not evidence that a confirmed emergency ended.
            return
        save_dashboard_snapshot(CACHE, {"generated_at": now.isoformat(), "text": "", "audit": []}, db_path=db_path)
        return
    text = clean(selection.get("text"), 100) if valid_ai else ""
    evidence_text = chosen["title"] + chosen["snippet"]
    if not text or any(n not in evidence_text for n in re.findall(r"\d+(?:\.\d+)?", text)):
        text, valid_ai = chosen["title"][:100], False
    selection = {**selection, "support_ids": selection.get("support_ids", []) if isinstance(selection.get("support_ids"), list) else []}
    if chosen["id"] not in selection["support_ids"]:
        selection["support_ids"] = [chosen["id"], *selection["support_ids"]]
    if not valid_ai: selection.update(level="normal", global_impact=False)
    assessment = assess_alert(selection, candidates)
    state = get_dashboard_snapshot(CACHE, db_path=db_path) or {}
    cutoff = (now - timedelta(days=2)).isoformat()
    audit = [r for r in state.get("audit", []) if r.get("at", "") >= cutoff]
    signature = chosen["id"] + ":" + assessment["shadow_level"]
    if not audit or audit[-1].get("signature") != signature:
        audit.append({"at": now.isoformat(), "signature": signature, "assessment": assessment})
    colors = os.getenv("INFOGRAB_ALERT_COLORS", "0").strip() == "1"
    save_dashboard_snapshot(CACHE, {"generated_at": now.isoformat(), "valid_until": (now + timedelta(minutes=45)).isoformat(),
        "text": text, "url": chosen["url"], "source_name": chosen["source_name"], "source_time": chosen["published_at"],
        "status": "selected" if valid_ai else "fallback", "assessment": assessment,
        "alert_level": assessment["shadow_level"] if colors else "normal", "shadow_mode": not colors,
        "audit": audit[-48:]}, db_path=db_path)


def fetch_hotsearch_bundle(*, db_path):
    from services.hotsearch.pipeline import fetch_hotsearch_snapshot
    try:
        candidates = build_focus_candidates(db_path=db_path)
    except Exception as error:
        print(f"[warn] Focus candidates unavailable ({type(error).__name__})")
        candidates = []
    snapshot = fetch_hotsearch_snapshot(focus_candidates=candidates)
    selection = snapshot.get("ai_digest", {}).pop("focus", None)
    try:
        store_focus(candidates, selection, db_path=db_path)
    except Exception as error:
        print(f"[warn] Focus export unavailable ({type(error).__name__})")
    return snapshot


def get_focus(*, db_path, now=None):
    now = now or datetime.now(timezone.utc)
    state = get_dashboard_snapshot(CACHE, db_path=db_path) or {}
    try:
        expired = datetime.fromisoformat(state.get("valid_until", ""))
        if expired <= now:
            if state.get("alert_level") in ("major", "critical") and now - expired < timedelta(days=1):
                return {**{k: state.get(k, "") for k in ("url", "source_name", "source_time", "generated_at")},
                        "text": "此前重大事件状态待核实 · " + state.get("text", "")[:70], "alert_level": "major",
                        "valid_until": min(now + timedelta(minutes=30), expired + timedelta(days=1)).isoformat()}
            return {}
    except (ValueError, TypeError): return {}
    return {key: state.get(key, "") for key in ("text", "url", "source_name", "source_time", "generated_at", "valid_until", "alert_level")}
