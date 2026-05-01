from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import re
from typing import Any
from urllib.parse import urljoin

import requests

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

FLASHSCORE_HOME_URL = "https://www.flashscore.com/auto-racing/formula-1/"
FORMULA1_SCHEDULE_URL_TEMPLATE = "https://www.formula1.com/en/racing/{year}"

MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

NAME_TO_SLUG = {
    "Australia": "australian-grand-prix",
    "China": "chinese-grand-prix",
    "Japan": "japanese-grand-prix",
    "Bahrain": "bahrain-grand-prix",
    "Saudi Arabia": "saudi-arabian-grand-prix",
    "Miami": "united-states-grand-prix-miami",
    "Emilia-Romagna": "emilia-romagna-grand-prix",
    "Monaco": "monaco-grand-prix",
    "Spain": "spanish-grand-prix-barcelona",
    "Canada": "canadian-grand-prix",
    "Austria": "austrian-grand-prix",
    "Great Britain": "british-grand-prix",
    "Belgium": "belgian-grand-prix",
    "Hungary": "hungarian-grand-prix",
    "Netherlands": "dutch-grand-prix",
    "Italy": "italian-grand-prix",
    "Azerbaijan": "azerbaijan-grand-prix",
    "Singapore": "singapore-grand-prix",
    "United States": "united-states-grand-prix",
    "Mexico": "mexican-grand-prix",
    "São Paulo": "brazilian-grand-prix",
    "Sao Paulo": "brazilian-grand-prix",
    "Las Vegas": "las-vegas-grand-prix",
    "Qatar": "qatar-grand-prix",
    "Abu Dhabi": "abu-dhabi-grand-prix",
    "France": "france-grand-prix",
    "Russia": "russian-grand-prix",
}



# Flashscore does not always use the same Grand Prix naming convention as
# formula1.com. The current recurring example is Miami: formula1.com labels it
# "Miami", while Flashscore exposes it as "united-states-grand-prix-miami".
FLASHSCORE_SLUG_ALIASES: dict[str, list[str]] = {
    "miami-grand-prix": ["united-states-grand-prix-miami"],
    "united-states-grand-prix-miami": ["miami-grand-prix"],
    "mexico-city-grand-prix": ["mexican-grand-prix"],
    "mexican-grand-prix": ["mexico-city-grand-prix"],
    "sao-paulo-grand-prix": ["brazilian-grand-prix"],
    "s-o-paulo-grand-prix": ["brazilian-grand-prix"],
    "brazilian-grand-prix": ["sao-paulo-grand-prix", "s-o-paulo-grand-prix"],
    "spanish-grand-prix": ["spanish-grand-prix-barcelona"],
    "spanish-grand-prix-barcelona": ["spanish-grand-prix"],
}

DISPLAY_NAME_OVERRIDES = {
    "Australia": "Australian Grand Prix",
    "China": "Chinese Grand Prix",
    "Japan": "Japan Grand Prix",
    "Bahrain": "Bahrain Grand Prix",
    "Saudi Arabia": "Saudi Arabian Grand Prix",
    "Miami": "Miami Grand Prix",
    "Emilia-Romagna": "Emilia-Romagna Grand Prix",
    "Monaco": "Monaco Grand Prix",
    "Spain": "Spanish Grand Prix",
    "Canada": "Canadian Grand Prix",
    "Austria": "Austrian Grand Prix",
    "Great Britain": "British Grand Prix",
    "Belgium": "Belgian Grand Prix",
    "Hungary": "Hungarian Grand Prix",
    "Netherlands": "Dutch Grand Prix",
    "Italy": "Italian Grand Prix",
    "Azerbaijan": "Azerbaijan Grand Prix",
    "Singapore": "Singapore Grand Prix",
    "United States": "United States Grand Prix",
    "Mexico": "Mexico City Grand Prix",
    "São Paulo": "São Paulo Grand Prix",
    "Sao Paulo": "São Paulo Grand Prix",
    "Las Vegas": "Las Vegas Grand Prix",
    "Qatar": "Qatar Grand Prix",
    "Abu Dhabi": "Abu Dhabi Grand Prix",
    "France": "French Grand Prix",
    "Russia": "Russian Grand Prix",
}


def _display_name(name: str) -> str:
    cleaned = str(name or "").strip()
    if not cleaned:
        return cleaned
    if re.search(r"grand\s+prix", cleaned, flags=re.I):
        return cleaned
    return DISPLAY_NAME_OVERRIDES.get(cleaned, f"{cleaned} Grand Prix")


@dataclass
class RoundInfo:
    round_number: int
    name: str
    date_text: str
    start_date: str | None
    end_date: str | None
    slug: str
    flashscore_url: str | None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _fetch_text(url: str, timeout: int = 30) -> str:
    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def _slugify_name(name: str) -> str:
    if name in NAME_TO_SLUG:
        return NAME_TO_SLUG[name]
    slug = name.lower().strip()
    slug = slug.replace("&", "and")
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    if not slug.endswith("grand-prix"):
        slug = f"{slug}-grand-prix"
    return slug


def _parse_date_text(date_text: str, year: int) -> tuple[str | None, str | None]:
    text = re.sub(r"\s+", " ", date_text.strip()).upper()

    m = re.fullmatch(r"(\d{2})\s*-\s*(\d{2})\s+([A-Z]{3})", text)
    if m:
        d1 = int(m.group(1))
        d2 = int(m.group(2))
        month = MONTHS.get(m.group(3))
        if month:
            return date(year, month, d1).isoformat(), date(year, month, d2).isoformat()

    m = re.fullmatch(r"(\d{2})\s+([A-Z]{3})\s*-\s*(\d{2})\s+([A-Z]{3})", text)
    if m:
        d1 = int(m.group(1))
        m1 = MONTHS.get(m.group(2))
        d2 = int(m.group(3))
        m2 = MONTHS.get(m.group(4))
        if m1 and m2:
            return date(year, m1, d1).isoformat(), date(year, m2, d2).isoformat()

    return None, None


def extract_flashscore_home_links(html: str) -> list[str]:
    links = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.I)
    gp_links = []
    seen = set()
    for link in links:
        if "/auto-racing/formula-1/" not in link:
            continue
        if "news" in link:
            continue
        full = urljoin(FLASHSCORE_HOME_URL, link)
        if full not in seen:
            seen.add(full)
            gp_links.append(full)
    return gp_links


def _link_map_from_home(home_links: list[str]) -> dict[str, str]:
    mapping = {}
    for link in home_links:
        slug = link.rstrip("/").split("/")[-1]
        if slug and slug != "formula-1":
            mapping[slug] = link
    return mapping


def _candidate_slugs_for_round(raw_name: str, primary_slug: str) -> list[str]:
    candidates: list[str] = []

    def add(value: str | None) -> None:
        if not value:
            return
        value = str(value).strip().strip("/")
        if value and value not in candidates:
            candidates.append(value)

    add(primary_slug)
    add(NAME_TO_SLUG.get(raw_name))

    for key in list(candidates):
        for alias in FLASHSCORE_SLUG_ALIASES.get(key, []):
            add(alias)

    raw_lower = str(raw_name or "").lower()
    if raw_lower == "miami" or "miami" in primary_slug:
        add("united-states-grand-prix-miami")
    if "mexico" in raw_lower or "mexico" in primary_slug:
        add("mexican-grand-prix")
    if "são paulo" in raw_lower or "sao paulo" in raw_lower or "sao-paulo" in primary_slug:
        add("brazilian-grand-prix")
    if raw_lower == "spain" or primary_slug == "spanish-grand-prix":
        add("spanish-grand-prix-barcelona")

    return candidates


def _resolve_flashscore_url(raw_name: str, primary_slug: str, link_map: dict[str, str]) -> str | None:
    for slug in _candidate_slugs_for_round(raw_name, primary_slug):
        if slug in link_map:
            return link_map[slug]

    # Conservative fuzzy fallback: if exactly one Flashscore slug contains the
    # distinctive part of the Formula1 schedule name, use it. This catches
    # city-style labels such as Miami without accidentally mapping generic names.
    tokens = [
        t for t in re.split(r"[^a-z0-9]+", primary_slug.lower())
        if t and t not in {"grand", "prix"}
    ]
    distinctive = [t for t in tokens if len(t) >= 4]
    if distinctive:
        matches = [
            (slug, url) for slug, url in link_map.items()
            if all(token in slug for token in distinctive)
        ]
        if len(matches) == 1:
            return matches[0][1]

    return None


def parse_formula1_schedule_rounds(html: str, year: int, home_links: list[str] | None = None) -> list[RoundInfo]:
    compact = re.sub(r"\s+", " ", html)
    pattern = re.compile(
        r"ROUND\s*(\d+)</span>.*?group-hover/schedule-card:underline\">([^<]+)</span><span[^>]*>(\d{2}(?:\s+[A-Z]{3})?\s*-\s*\d{2}\s+[A-Z]{3})</span>",
        re.I,
    )

    link_map = _link_map_from_home(home_links or [])
    rounds: list[RoundInfo] = []
    seen = set()

    for m in pattern.finditer(compact):
        round_number = int(m.group(1))
        raw_name = m.group(2).strip()
        name = _display_name(raw_name)
        date_text = m.group(3).strip()
        if round_number in seen:
            continue
        seen.add(round_number)

        slug = _slugify_name(raw_name)
        start_date, end_date = _parse_date_text(date_text, year)
        rounds.append(
            RoundInfo(
                round_number=round_number,
                name=name,
                date_text=date_text,
                start_date=start_date,
                end_date=end_date,
                slug=slug,
                flashscore_url=_resolve_flashscore_url(raw_name, slug, link_map),
            )
        )

    rounds.sort(key=lambda x: x.round_number)
    return rounds


def extract_page_metadata(html: str) -> dict[str, Any]:
    compact = re.sub(r"\s+", " ", html)

    tournament_id = None
    m = re.search(r'window\.tournamentId\s*=\s*"([^"]+)"', compact)
    if m:
        tournament_id = m.group(1)

    sport_id = None
    m = re.search(r"sportId\s*=\s*([0-9]+)", compact)
    if m:
        sport_id = int(m.group(1))

    project_id = None
    # page often contains project_id":"2"
    m = re.search(r'"project_id":"([^"]+)"', compact)
    if m:
        project_id = m.group(1)
    if project_id is None:
        # fallback to known value for flashscore web
        project_id = "2"

    geo_ip = None
    m = re.search(r'"geo_ip":"([^"]+)"', compact)
    if m:
        geo_ip = m.group(1)

    return {
        "tournament_id": tournament_id,
        "sport_id": sport_id,
        "project_id": project_id,
        "geo_ip": geo_ip,
    }


def resolve_round_strategy(*, year: int | None = None, today: date | None = None) -> dict[str, Any]:
    if year is None:
        year = datetime.now(timezone.utc).year
    if today is None:
        today = datetime.now(timezone.utc).date()

    schedule_url = FORMULA1_SCHEDULE_URL_TEMPLATE.format(year=year)
    schedule_html = _fetch_text(schedule_url)
    flashscore_home_html = _fetch_text(FLASHSCORE_HOME_URL)

    home_links = extract_flashscore_home_links(flashscore_home_html)
    rounds = parse_formula1_schedule_rounds(schedule_html, year, home_links=home_links)

    current_round = None
    previous_round = None
    next_round = None

    for rnd in rounds:
        if rnd.start_date and rnd.end_date:
            start_d = date.fromisoformat(rnd.start_date)
            end_d = date.fromisoformat(rnd.end_date)
            if start_d <= today <= end_d:
                current_round = rnd
            elif end_d < today:
                previous_round = rnd
            elif start_d > today and next_round is None:
                next_round = rnd

    mode = "between_rounds"
    primary_target = next_round
    fallback_results = previous_round
    if current_round is not None:
        mode = "live"
        primary_target = current_round
        fallback_results = previous_round

    best_available_flashscore_target = None
    for candidate in (current_round, previous_round, next_round):
        if candidate and candidate.flashscore_url:
            best_available_flashscore_target = candidate
            break

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "year": year,
        "today": today.isoformat(),
        "mode": mode,
        "primary_target": primary_target.as_dict() if primary_target else None,
        "fallback_results": fallback_results.as_dict() if fallback_results else None,
        "current_round": current_round.as_dict() if current_round else None,
        "previous_round": previous_round.as_dict() if previous_round else None,
        "next_round": next_round.as_dict() if next_round else None,
        "best_available_flashscore_target": best_available_flashscore_target.as_dict() if best_available_flashscore_target else None,
        "rounds": [r.as_dict() for r in rounds],
        "flashscore_home_links_sample": home_links[:40],
        "schedule_url": schedule_url,
        "flashscore_home_url": FLASHSCORE_HOME_URL,
    }
