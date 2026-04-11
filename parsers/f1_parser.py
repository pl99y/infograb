from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import html
import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

from bs4 import BeautifulSoup

FLASHCORE_BASE = "https://www.flashscore.com"

TEAM_NAMES = {
    "mercedes", "ferrari", "mclaren", "red bull", "red bull racing", "rb", "williams",
    "aston martin", "haas", "sauber", "alpine", "racing bulls", "kick sauber",
    "alphatauri", "alfa romeo", "renault", "toro rosso", "audi", "cadillac"
}

FILTER_TITLE_PATTERNS = [
    re.compile(r"^live\s*:", re.I),
    re.compile(r"\blive text\b", re.I),
    re.compile(r"\bin photos\b", re.I),
    re.compile(r"\bratings\b", re.I),
]


@dataclass
class F1LiveRow:
    position: int
    driver_name: str
    team_name: str | None
    result_text: str
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class F1LiveSnapshot:
    gp_name: str
    session_type: str
    session_status: str
    source_name: str
    source_url: str
    rows: list[F1LiveRow] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class F1NewsArticle:
    title: str
    published_at: str | None
    summary: str | None
    url: str
    normalized_url: str
    category: str | None
    is_live_text: bool
    source_name: str
    source_url: str
    extra: dict[str, Any] = field(default_factory=dict)


class F1ParseError(ValueError):
    pass


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_f1_url(url: str) -> str:
    parsed = urlparse(url)
    qs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", urlencode(qs), ""))


def _extract_title(soup: BeautifulSoup) -> str:
    title = _clean_text(soup.title.text if soup.title else "")
    if title:
        return title
    og = soup.find("meta", attrs={"property": "og:title"})
    return _clean_text(og.get("content") if og else "")


def _detect_session_type(text: str) -> str:
    low = text.lower()
    if re.search(r"\bqualifying\b|\bsprint qualifying\b", low):
        return "qualifying"
    if re.search(r"\bsprint\b", low):
        return "sprint"
    if re.search(r"\brace\b|\bgrand prix results\b|\bclassification\b", low):
        return "race"
    return "unknown"


def _detect_session_status(text: str) -> str:
    low = text.lower()
    if any(term in low for term in ["live", "in progress", "lap ", "green flag"]):
        return "live"
    if any(term in low for term in ["red flag", "stopped", "suspended", "interrupted", "postponed"]):
        return "stopped"
    if any(term in low for term in ["final result", "classification", "finished", "ended", "full results"]):
        return "finished"
    if any(term in low for term in ["starts at", "scheduled", "not started", "upcoming"]):
        return "scheduled"
    return "unknown"


def _detect_gp_name(title_text: str, page_text: str) -> str:
    for text in (title_text, page_text):
        m = re.search(r"([A-Z][A-Za-z\- ]+ Grand Prix)", text)
        if m:
            return _clean_text(m.group(1))
    return "Formula 1"


def _looks_like_result_token(value: str) -> bool:
    v = value.strip()
    return bool(
        re.fullmatch(r"(?:\+?[0-9:.]+|DNF|DNS|DSQ|RET|OUT|NC|No time|Stopped|Finished)", v, re.I)
        or re.fullmatch(r"\+\d+[.:]\d+", v)
        or re.fullmatch(r"\d+:\d{2}(?::\d{2})?\.\d+", v)
        or re.fullmatch(r"Q\d", v)
    )


def _looks_like_driver(value: str) -> bool:
    v = value.strip()
    if not v or len(v) < 3:
        return False
    if _looks_like_result_token(v):
        return False
    if re.fullmatch(r"[A-Z]{2,4}", v):
        return False
    return bool(re.search(r"[A-Za-z]", v))


def _looks_like_team(value: str) -> bool:
    low = value.strip().lower()
    if not low:
        return False
    if low in TEAM_NAMES:
        return True
    return any(team in low for team in TEAM_NAMES)


def _parse_rows_from_tables(soup: BeautifulSoup) -> list[F1LiveRow]:
    results: list[F1LiveRow] = []
    seen_positions: set[int] = set()

    for tr in soup.find_all("tr"):
        cells = [_clean_text(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
        cells = [c for c in cells if c]
        if len(cells) < 3:
            continue

        m = re.fullmatch(r"(\d{1,2})", cells[0])
        if not m:
            continue
        position = int(m.group(1))
        if position in seen_positions:
            continue

        driver_name = None
        team_name = None
        result_text = None

        for idx, cell in enumerate(cells[1:], start=1):
            if driver_name is None and _looks_like_driver(cell) and not _looks_like_team(cell):
                driver_name = cell
                continue
            if driver_name is not None and team_name is None and _looks_like_team(cell):
                team_name = cell
                continue

        for cell in reversed(cells):
            if _looks_like_result_token(cell):
                result_text = cell
                break
        if result_text is None:
            result_text = cells[-1]

        if driver_name:
            results.append(
                F1LiveRow(
                    position=position,
                    driver_name=driver_name,
                    team_name=team_name,
                    result_text=result_text,
                    extra={"raw_cells": cells},
                )
            )
            seen_positions.add(position)

    return results


def _parse_rows_from_text(page_text: str) -> list[F1LiveRow]:
    # Fallback for pages that flatten table rows into plain text.
    compact = re.sub(r"\s+", " ", page_text)
    pattern = re.compile(
        r"(\d{1,2})\.\s+([A-Z][A-Za-z.'\- ]+?)\s+([A-Z][A-Za-z0-9 &'\-]+?)\s+(\+?[0-9:.]+|DNF|DNS|DSQ|RET|OUT|NC|Stopped|Finished)",
        re.I,
    )
    results: list[F1LiveRow] = []
    seen_positions: set[int] = set()
    for m in pattern.finditer(compact):
        pos = int(m.group(1))
        if pos in seen_positions:
            continue
        driver = _clean_text(m.group(2))
        team = _clean_text(m.group(3))
        result_text = _clean_text(m.group(4))
        results.append(
            F1LiveRow(
                position=pos,
                driver_name=driver,
                team_name=team if _looks_like_team(team) else None,
                result_text=result_text,
                extra={"raw_match": m.group(0)},
            )
        )
        seen_positions.add(pos)
    return results


def parse_flashscore_gp_page(html_text: str, source_url: str) -> F1LiveSnapshot:
    soup = BeautifulSoup(html_text, "html.parser")
    page_text = _clean_text(soup.get_text(" ", strip=True))
    title_text = _extract_title(soup)

    gp_name = _detect_gp_name(title_text, page_text)
    session_type = _detect_session_type(title_text + " " + page_text)
    session_status = _detect_session_status(title_text + " " + page_text)

    rows = _parse_rows_from_tables(soup)
    if not rows:
        rows = _parse_rows_from_text(page_text)

    rows = [row for row in rows if row.position > 0]
    rows.sort(key=lambda x: x.position)

    if session_type not in {"qualifying", "sprint", "race"}:
        raise F1ParseError("No supported core session detected on Flashscore GP page")

    return F1LiveSnapshot(
        gp_name=gp_name,
        session_type=session_type,
        session_status=session_status,
        source_name="Flashscore",
        source_url=source_url,
        rows=rows,
        extra={"title_text": title_text},
    )


def _parse_relative_time_to_utc(value: str | None) -> str | None:
    if not value:
        return None

    text = _clean_text(value).lower()
    now = datetime.now(timezone.utc)

    m = re.fullmatch(r"(\d+)\s*([smhdw])", text)
    if not m:
        return None

    amount = int(m.group(1))
    unit = m.group(2)
    if unit == "s":
        delta_seconds = amount
    elif unit == "m":
        delta_seconds = amount * 60
    elif unit == "h":
        delta_seconds = amount * 3600
    elif unit == "d":
        delta_seconds = amount * 86400
    else:
        delta_seconds = amount * 7 * 86400

    ts = now.timestamp() - delta_seconds
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()



def _is_filtered_news_title(title: str) -> bool:
    cleaned = _clean_text(title)
    return any(pattern.search(cleaned) for pattern in FILTER_TITLE_PATTERNS)

def _normalize_autosport_title(raw_text: str) -> str:
    text = _clean_text(raw_text)
    if not text:
        return ""

    # Flattened Autosport anchors often look like:
    #   <title> Formula 1 F1 Formula 1 4 h <title>
    dup_match = re.match(
        r"(.+?)\s+(?:Formula 1|F1)(?:\s+(?:Formula 1|F1))*\s+\d+\s*[smhdw]\s+(.+)",
        text,
        flags=re.I,
    )
    if dup_match:
        left = _clean_text(dup_match.group(1))
        right = _clean_text(dup_match.group(2))
        if left and right and left.lower() == right.lower():
            text = left
        elif right:
            text = right
        elif left:
            text = left

    text = re.sub(r"^(?:(?:Formula 1|F1)\s+)+", "", text, flags=re.I)
    text = re.sub(r"\s+\d+\s*[smhdw]$", "", text, flags=re.I)
    text = re.sub(r"\s+\d{1,2}:\d{2}$", "", text)
    return _clean_text(text)


def _is_autosport_news_href(href: str) -> bool:
    low = (href or "").lower()
    if "/f1/news/" not in low:
        return False
    if low.rstrip("/") == "/f1/news":
        return False
    if any(token in low for token in ["/video/", "/live/", "/drivers/", "/teams/", "/calendar/", "/results/", "/archive/"]):
        return False
    return True


def _normalize_iso_value(value: str | None) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return text


def _extract_autosport_title(anchor) -> str:
    for selector in (".ms-item__title", ".ms-item__thumb-title", "h2", "h3", "h4"):
        node = anchor.select_one(selector)
        if node:
            title = _normalize_autosport_title(node.get_text(" ", strip=True))
            if title:
                return title

    title = _normalize_autosport_title(anchor.get_text(" ", strip=True))
    if title:
        return title

    aria_label = _clean_text(anchor.get("aria-label"))
    return _normalize_autosport_title(aria_label) if aria_label else ""


def _extract_autosport_published_at(anchor) -> str | None:
    candidates = [anchor]
    parent = anchor.parent
    depth = 0
    while parent is not None and depth < 2:
        candidates.append(parent)
        parent = parent.parent
        depth += 1

    for node in candidates:
        time_tag = node.find("time") if getattr(node, "find", None) else None
        if not time_tag:
            continue

        datetime_attr = _clean_text(time_tag.get("datetime")) if hasattr(time_tag, "get") else ""
        normalized = _normalize_iso_value(datetime_attr)
        if normalized:
            return normalized

        relative_text = _clean_text(time_tag.get_text(" ", strip=True))
        normalized = _parse_relative_time_to_utc(relative_text)
        if normalized:
            return normalized

    relative_sources = []
    for node in candidates:
        try:
            relative_sources.append(_clean_text(node.get_text(" ", strip=True)))
        except Exception:
            pass
    combined = " ".join(part for part in relative_sources if part)
    m = re.search(r"(\d+\s*[smhdw])", combined, re.I)
    if m:
        return _parse_relative_time_to_utc(m.group(1))
    return None


def _parse_autosport_jsonld_articles(soup: BeautifulSoup, source_url: str) -> list[F1NewsArticle]:
    items: list[F1NewsArticle] = []
    seen: set[str] = set()

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(" ", strip=True)
        raw = (raw or "").strip()
        if not raw:
            continue

        try:
            payload = json.loads(raw)
        except Exception:
            continue

        def walk(node: Any):
            if isinstance(node, list):
                for child in node:
                    yield from walk(child)
                return
            if isinstance(node, dict):
                yield node
                for child in node.values():
                    yield from walk(child)

        for node in walk(payload):
            if not isinstance(node, dict):
                continue
            node_type = str(node.get("@type", "") or "")
            if "NewsArticle" not in node_type and "Article" not in node_type:
                continue

            url = _clean_text(node.get("url") or "")
            title = _normalize_autosport_title(node.get("headline") or node.get("name") or "")
            if not url or not title or not _is_autosport_news_href(url):
                continue
            if _is_filtered_news_title(title):
                continue

            normalized_url = normalize_f1_url(urljoin(source_url, url))
            if normalized_url in seen:
                continue
            seen.add(normalized_url)

            published_at = _clean_text(node.get("datePublished") or "") or None
            if published_at and published_at.endswith("Z"):
                published_at = published_at.replace("Z", "+00:00")

            items.append(
                F1NewsArticle(
                    title=title,
                    published_at=published_at,
                    summary=None,
                    url=urljoin(source_url, url),
                    normalized_url=normalized_url,
                    category="Formula 1",
                    is_live_text=False,
                    source_name="Autosport",
                    source_url=source_url,
                    extra={"parsed_from": "jsonld"},
                )
            )

    return items


def parse_autosport_f1_news_page(html_text: str, source_url: str) -> list[F1NewsArticle]:
    soup = BeautifulSoup(html_text, "html.parser")

    jsonld_items = _parse_autosport_jsonld_articles(soup, source_url)
    if jsonld_items:
        return jsonld_items

    items: list[F1NewsArticle] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = _clean_text(anchor.get("href"))
        if not _is_autosport_news_href(href):
            continue

        title = _extract_autosport_title(anchor)
        if not title or len(title) < 12 or _is_filtered_news_title(title):
            continue
        if title.lower() in {"news archive", "archive"}:
            continue

        full_url = urljoin(source_url, href)
        normalized_url = normalize_f1_url(full_url)
        if normalized_url in seen:
            continue
        seen.add(normalized_url)

        published_at = _extract_autosport_published_at(anchor)

        items.append(
            F1NewsArticle(
                title=title,
                published_at=published_at,
                summary=None,
                url=full_url,
                normalized_url=normalized_url,
                category="Formula 1",
                is_live_text=False,
                source_name="Autosport",
                source_url=source_url,
                extra={"parsed_from": "html_anchor"},
            )
        )

    if not items:
        raise F1ParseError("Could not parse Autosport F1 news page")

    return items
