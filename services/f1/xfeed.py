from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import re
from typing import Any

import requests

from services.f1.rounds import resolve_round_strategy, extract_page_metadata


HEADERS_BASE = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

XFSIGN = "SW9D1eZo"


@dataclass
class F1Row:
    position: int | None
    driver: str | None
    team: str | None
    team_code: str | None
    result_time: str | None
    gap: str | None
    laps: int | None
    driver_code: str | None
    driver_slug: str | None
    nation: str | None
    raw: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class F1Session:
    session_id: str
    title: str | None
    session_name: str | None
    round_name: str | None
    country: str | None
    start_ts: int | None
    end_ts: int | None
    circuit: str | None
    lap_info: str | None
    standings_label: str | None
    event_path: str | None
    rows: list[F1Row]
    raw: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "title": self.title,
            "session_name": self.session_name,
            "round_name": self.round_name,
            "country": self.country,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "circuit": self.circuit,
            "lap_info": self.lap_info,
            "standings_label": self.standings_label,
            "event_path": self.event_path,
            "rows": [r.as_dict() for r in self.rows],
            "raw": self.raw,
        }


def _fetch_text(url: str, headers: dict[str, str] | None = None, timeout: int = 30) -> str:
    resp = requests.get(url, headers=headers or HEADERS_BASE, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _record_to_dict(record: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for field in record.split("¬"):
        if "÷" not in field:
            continue
        key, value = field.split("÷", 1)
        out[key.strip()] = value.strip()
    return out


def _parse_zn_block(value: str | None) -> tuple[int | None, int | None, str | None, str | None]:
    if not value:
        return None, None, None, None
    parts = value.split("|")
    start_ts = _to_int(parts[0]) if len(parts) >= 1 else None
    end_ts = _to_int(parts[1]) if len(parts) >= 2 else None
    circuit = parts[2].strip() if len(parts) >= 3 and parts[2].strip() else None
    lap_info = parts[-1].strip() if len(parts) >= 6 and parts[-1].strip() else None
    return start_ts, end_ts, circuit, lap_info


def extract_legacy_feed_meta_from_page(html: str) -> dict[str, Any]:
    compact = re.sub(r"\s+", " ", html)

    legacy_tournament_id = None
    m = re.search(r'tournament_id\s*=\s*"([^"]+)"', compact)
    if m:
        legacy_tournament_id = m.group(1)

    country_id = None
    m = re.search(r'country_id\s*=\s*([0-9]+)', compact)
    if m:
        country_id = m.group(1)

    default_tz = None
    m = re.search(r'default_tz\s*=\s*([0-9-]+)', compact)
    if m:
        default_tz = m.group(1)

    myleagues_country = None
    m = re.search(r'getToggleIcon\("32_([0-9]+)_[A-Za-z0-9]+"\s*,\s*true\)', compact)
    if m:
        myleagues_country = m.group(1)

    return {
        "legacy_tournament_id": legacy_tournament_id,
        "country_id": country_id,
        "default_tz": default_tz,
        "myleagues_country": myleagues_country,
    }


def build_tournament_feed_url(
    *,
    sport_id: int,
    country_id: str,
    legacy_tournament_id: str,
    default_tz: str,
    language: str = "en",
    project_type_id: str = "1",
    origin: str = "https://www.flashscore.com",
) -> str:
    return (
        f"{origin}/x/feed/"
        f"t_{sport_id}_{country_id}_{legacy_tournament_id}_{default_tz}_{language}_{project_type_id}"
    )


def fetch_round_page_and_feed(round_url: str) -> dict[str, Any]:
    page_html = _fetch_text(round_url)
    page_meta = extract_page_metadata(page_html)
    legacy_meta = extract_legacy_feed_meta_from_page(page_html)

    sport_id = page_meta.get("sport_id") or 32
    country_id = legacy_meta.get("country_id") or legacy_meta.get("myleagues_country") or "5860"
    legacy_tournament_id = legacy_meta.get("legacy_tournament_id")
    default_tz = legacy_meta.get("default_tz") or "2"

    if not legacy_tournament_id:
        raise ValueError("Could not extract legacy_tournament_id from round page")

    feed_url = build_tournament_feed_url(
        sport_id=int(sport_id),
        country_id=str(country_id),
        legacy_tournament_id=str(legacy_tournament_id),
        default_tz=str(default_tz),
        language="en",
        project_type_id="1",
        origin="https://www.flashscore.com",
    )

    headers = dict(HEADERS_BASE)
    headers["x-fsign"] = XFSIGN
    feed_text = _fetch_text(feed_url, headers=headers, timeout=30)

    return {
        "page_url": round_url,
        "feed_url": feed_url,
        "page_meta": page_meta,
        "legacy_meta": legacy_meta,
        "feed_text": feed_text,
    }


def parse_tournament_feed(feed_text: str) -> list[F1Session]:
    raw_records = [part for part in feed_text.split("¬~") if part.strip()]
    dict_records = [_record_to_dict(rec) for rec in raw_records]

    session_map: dict[str, F1Session] = {}

    for rec in dict_records:
        session_id = rec.get("ZC")
        if not session_id:
            continue

        start_ts, end_ts, circuit, lap_info = _parse_zn_block(rec.get("ZN"))

        session = F1Session(
            session_id=session_id,
            title=rec.get("ZA"),
            session_name=rec.get("ZAE"),
            round_name=rec.get("ZAF"),
            country=rec.get("ZY"),
            start_ts=start_ts,
            end_ts=end_ts,
            circuit=circuit,
            lap_info=lap_info,
            standings_label=rec.get("ZRB"),
            event_path=rec.get("ZL"),
            rows=[],
            raw=rec,
        )
        session_map[session_id] = session

    session_ids = list(session_map.keys())

    for rec in dict_records:
        aa = rec.get("AA")
        driver = rec.get("AE")
        if not aa or not driver:
            continue

        matched_session_id = None
        for sid in session_ids:
            if aa.endswith(sid):
                matched_session_id = sid
                break
        if not matched_session_id:
            continue

        row = F1Row(
            position=_to_int(rec.get("CX")),
            driver=driver,
            team=rec.get("NA"),
            team_code=rec.get("NB"),
            result_time=rec.get("ND"),
            gap=rec.get("NG"),
            laps=_to_int(rec.get("NC")),
            driver_code=rec.get("WM"),
            driver_slug=rec.get("WU"),
            nation=rec.get("FU") or rec.get("CC"),
            raw=rec,
        )
        session_map[matched_session_id].rows.append(row)

    sessions = list(session_map.values())
    for s in sessions:
        s.rows.sort(key=lambda r: (999999 if r.position is None else r.position, r.driver or ""))

    sessions.sort(key=lambda s: (s.end_ts or 0, s.start_ts or 0))
    return sessions


def choose_display_session(sessions: list[F1Session], mode: str) -> F1Session | None:
    if not sessions:
        return None

    now_ts = int(datetime.now(timezone.utc).timestamp())

    if mode == "live":
        live_candidates = [
            s for s in sessions
            if s.start_ts is not None and s.end_ts is not None and s.start_ts <= now_ts <= s.end_ts
        ]
        if live_candidates:
            live_candidates.sort(key=lambda s: (s.start_ts or 0, s.end_ts or 0), reverse=True)
            return live_candidates[0]

        started = [s for s in sessions if s.start_ts is not None and s.start_ts <= now_ts]
        if started:
            started.sort(key=lambda s: (s.start_ts or 0, s.end_ts or 0), reverse=True)
            return started[0]

    completed = [s for s in sessions if s.rows]
    if completed:
        completed.sort(key=lambda s: (s.end_ts or 0, s.start_ts or 0), reverse=True)
        return completed[0]

    sessions.sort(key=lambda s: (s.end_ts or 0, s.start_ts or 0), reverse=True)
    return sessions[0]


def get_f1_live_from_feed() -> dict[str, Any]:
    strategy = resolve_round_strategy()

    if strategy.get("mode") == "live":
        target_round = strategy.get("current_round") or strategy.get("best_available_flashscore_target")
    else:
        target_round = strategy.get("fallback_results") or strategy.get("best_available_flashscore_target")

    if not target_round or not target_round.get("flashscore_url"):
        return {
            "mode": strategy.get("mode"),
            "error": "No usable Flashscore round URL found for feed fetch",
            "strategy": strategy,
            "round": None,
            "session": None,
            "rows": [],
        }

    fetched = fetch_round_page_and_feed(target_round["flashscore_url"])
    sessions = parse_tournament_feed(fetched["feed_text"])
    chosen = choose_display_session(sessions, strategy.get("mode") or "between_rounds")

    return {
        "mode": strategy.get("mode"),
        "strategy": strategy,
        "round": target_round,
        "page_url": fetched["page_url"],
        "feed_url": fetched["feed_url"],
        "page_meta": fetched["page_meta"],
        "legacy_meta": fetched["legacy_meta"],
        "session": chosen.as_dict() if chosen else None,
        "rows": [r.as_dict() for r in (chosen.rows if chosen else [])],
        "session_count": len(sessions),
    }
