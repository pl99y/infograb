from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from collectors.mnd_pla_fetcher import MND_PLA_BASE_URL


class MndPlaParseError(ValueError):
    pass


AREA_CANONICAL_MAP = {
    "北部": "北部",
    "中部": "中部",
    "南部": "南部",
    "東部": "東部",
    "西南部": "西南",
    "西南": "西南",
    "東南部": "東南",
    "東南": "東南",
}


@dataclass
class MndPlaListEntry:
    page: int
    list_url: str
    detail_url: str
    list_date_raw: Optional[str]
    published_date: Optional[str]
    title_from_list: str
    views: Optional[int]


@dataclass
class MndPlaDailyRecord:
    url: str
    title: str
    published_date_raw: Optional[str]
    published_date: Optional[str]
    report_period_raw: Optional[str]
    period_start: Optional[str]
    period_end: Optional[str]
    body: str
    activity_text: str
    no_aircraft: bool
    aircraft_total: Optional[int]
    aircraft_intrusion_total: Optional[int]
    intrusion_areas: list[str]
    ship_total: Optional[int]
    official_ship_total: Optional[int]
    balloon_total: Optional[int]



def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = text.replace("\u3000", " ")
    text = re.sub(r"\r", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()



def roc_parts_to_iso(roc_year: int, month: int, day: int) -> Optional[str]:
    ad_year = roc_year + 1911
    try:
        return datetime(ad_year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None



def roc_dot_date_to_iso(date_str: str | None) -> Optional[str]:
    if not date_str:
        return None

    match = re.fullmatch(r"(\d{2,3})\.(\d{1,2})\.(\d{1,2})", date_str.strip())
    if not match:
        return None

    return roc_parts_to_iso(int(match.group(1)), int(match.group(2)), int(match.group(3)))



def parse_mnd_pla_list_page(
    html: str,
    *,
    list_url: str,
    page: int = 1,
    base_url: str = MND_PLA_BASE_URL,
) -> list[MndPlaListEntry]:
    soup = BeautifulSoup(html or "", "html.parser")
    entries: list[MndPlaListEntry] = []
    seen_urls: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        detail_url = urljoin(base_url, href)
        if not re.search(r"/news/plaact/\d+$", detail_url):
            continue
        if detail_url in seen_urls:
            continue
        seen_urls.add(detail_url)

        text = clean_text(anchor.get_text(" ", strip=True))
        if not text:
            continue

        date_match = re.search(r"(\d{2,3}\.\d{1,2}\.\d{1,2})", text)
        views_match = re.search(r"點閱次數[:：]\s*(\d+)\s*次", text)

        list_date_raw = date_match.group(1) if date_match else None
        published_date = roc_dot_date_to_iso(list_date_raw)
        views = int(views_match.group(1)) if views_match else None

        title = text
        if list_date_raw:
            title = title.replace(list_date_raw, "", 1).strip()
        title = re.sub(r"點閱次數[:：]\s*\d+\s*次", "", title).strip()

        entries.append(
            MndPlaListEntry(
                page=page,
                list_url=list_url,
                detail_url=detail_url,
                list_date_raw=list_date_raw,
                published_date=published_date,
                title_from_list=title,
                views=views,
            )
        )

    return entries



def extract_title(soup: BeautifulSoup, page_text: str) -> str:
    for tag_name in ("h1", "h2", "h3"):
        tag = soup.find(tag_name)
        if tag:
            text = clean_text(tag.get_text(" ", strip=True))
            if text:
                return text

    for line in (x.strip() for x in page_text.splitlines() if x.strip()):
        if "中共解放軍臺海周邊海、空域動態" in line:
            return line

    return ""



def extract_published_date_raw(page_text: str) -> Optional[str]:
    match = re.search(r"\b(\d{2,3}\.\d{1,2}\.\d{1,2})\b", page_text)
    return match.group(1) if match else None



def extract_body(page_text: str) -> str:
    match = re.search(
        r"(一、.*?)(?=\n\s*關鍵字[:：]|\n\s*Share\b|\n\s*下載專區|\Z)",
        page_text,
        flags=re.S,
    )
    if match:
        return clean_text(match.group(1))

    match = re.search(
        r"((?:一、)?日期[:：].*?)(?=\n\s*關鍵字[:：]|\n\s*Share\b|\n\s*下載專區|\Z)",
        page_text,
        flags=re.S,
    )
    if match:
        return clean_text(match.group(1))

    return ""



def extract_activity_text(body: str) -> str:
    match = re.search(r"(二、活動動態[:：].*?)(?=\n\s*三、|\Z)", body, flags=re.S)
    if match:
        return clean_text(match.group(1))

    match = re.search(r"(活動動態[:：].*?)(?=\n\s*三、|\Z)", body, flags=re.S)
    if match:
        return clean_text(match.group(1))

    return ""



def extract_period_raw(body: str) -> Optional[str]:
    match = re.search(r"一、日期[:：]\s*(.*?)(?=\n\s*二、|\Z)", body, flags=re.S)
    if match:
        return clean_text(match.group(1))
    return None



def extract_period_start_end(period_raw: str | None) -> tuple[Optional[str], Optional[str]]:
    if not period_raw:
        return None, None

    pattern = re.compile(
        r"(?:(?:中華民國)?\s*(?P<roc_year>\d{2,3})年\s*)?"
        r"(?P<month>\d{1,2})月\s*(?P<day>\d{1,2})日"
    )
    matches = list(pattern.finditer(period_raw))
    if not matches:
        return None, None

    parsed_dates: list[str] = []
    inferred_year: Optional[int] = None

    for match in matches:
        year_text = match.group("roc_year")
        month = int(match.group("month"))
        day = int(match.group("day"))
        if year_text:
            inferred_year = int(year_text)
        if inferred_year is None:
            continue
        iso_value = roc_parts_to_iso(inferred_year, month, day)
        if iso_value:
            parsed_dates.append(iso_value)

    if not parsed_dates:
        return None, None

    start = parsed_dates[0]
    end = parsed_dates[1] if len(parsed_dates) >= 2 else None
    return start, end



def normalize_area_token(token: str) -> Optional[str]:
    token = clean_text(token).replace(" ", "")
    if not token:
        return None
    return AREA_CANONICAL_MAP.get(token)



def parse_areas_from_text(text: str) -> list[str]:
    segment = ""
    match = re.search(r"進入(.+?)空域", text)
    if match:
        segment = clean_text(match.group(1))
    else:
        segment = clean_text(text)

    segment = segment.replace("逾越中線", "")
    segment = segment.replace("，", "、")
    segment = segment.replace(",", "、")
    segment = segment.replace("及", "、")
    segment = segment.replace("與", "、")
    raw_tokens = [x.strip() for x in segment.split("、") if x.strip()]

    normalized: list[str] = []
    seen: set[str] = set()
    for token in raw_tokens:
        area = normalize_area_token(token)
        if area and area not in seen:
            seen.add(area)
            normalized.append(area)
    return normalized



def extract_intrusion_info(activity_text: str) -> tuple[Optional[int], list[str]]:
    if not activity_text:
        return None, []

    for part in re.findall(r"[（(]([^）)]+)[）)]", activity_text):
        if "空域" not in part:
            continue
        count_match = re.search(r"(\d+)\s*架(?:次)?", part)
        intrusion_total = int(count_match.group(1)) if count_match else None
        return intrusion_total, parse_areas_from_text(part)

    match = re.search(
        r"(逾越中線進入.*?空域.*?\d+\s*架(?:次)?|進入.*?空域.*?\d+\s*架(?:次)?)",
        activity_text,
    )
    if match:
        part = match.group(1)
        count_match = re.search(r"(\d+)\s*架(?:次)?", part)
        intrusion_total = int(count_match.group(1)) if count_match else None
        return intrusion_total, parse_areas_from_text(part)

    return None, []



def extract_no_aircraft(body: str, activity_text: str) -> bool:
    if "未偵獲共機" in body:
        return True

    if activity_text:
        has_aircraft = "共機" in activity_text
        has_other_activity = (
            "共艦" in activity_text
            or "公務船" in activity_text
            or "持續在臺海周邊活動" in activity_text
        )
        if (not has_aircraft) and has_other_activity:
            return True

    return False



def extract_aircraft_total(activity_text: str, no_aircraft: bool) -> Optional[int]:
    match = re.search(r"偵獲共機\s*(\d+)\s*架(?:次)?", activity_text)
    if match:
        return int(match.group(1))
    return 0 if no_aircraft else None



def extract_ship_total(activity_text: str) -> Optional[int]:
    match = re.search(r"共艦\s*(\d+)\s*艘", activity_text)
    return int(match.group(1)) if match else None



def extract_official_ship_total(activity_text: str) -> Optional[int]:
    match = re.search(r"公務船\s*(\d+)\s*艘", activity_text)
    return int(match.group(1)) if match else None



def extract_balloon_total(body: str) -> Optional[int]:
    patterns = [
        r"空飄氣球(?:活動)?[:：]?\s*.*?計偵獲\s*(\d+)\s*顆",
        r"中共空飄氣球.*?計偵獲\s*(\d+)\s*顆",
        r"空飄氣球.*?偵獲\s*(\d+)\s*顆",
    ]
    for pattern in patterns:
        match = re.search(pattern, body, flags=re.S)
        if match:
            return int(match.group(1))
    return None



def parse_mnd_pla_detail_page(url: str, html: str) -> MndPlaDailyRecord:
    soup = BeautifulSoup(html or "", "html.parser")
    page_text = clean_text(soup.get_text("\n", strip=True))
    if not page_text:
        raise MndPlaParseError(f"Empty detail page: {url}")

    title = extract_title(soup, page_text)
    published_date_raw = extract_published_date_raw(page_text)
    published_date = roc_dot_date_to_iso(published_date_raw)

    body = extract_body(page_text)
    activity_text = extract_activity_text(body)
    report_period_raw = extract_period_raw(body)
    period_start, period_end = extract_period_start_end(report_period_raw)

    no_aircraft = extract_no_aircraft(body, activity_text)
    aircraft_total = extract_aircraft_total(activity_text, no_aircraft)
    aircraft_intrusion_total, intrusion_areas = extract_intrusion_info(activity_text)
    ship_total = extract_ship_total(activity_text)
    official_ship_total = extract_official_ship_total(activity_text)
    balloon_total = extract_balloon_total(body)

    return MndPlaDailyRecord(
        url=url,
        title=title,
        published_date_raw=published_date_raw,
        published_date=published_date,
        report_period_raw=report_period_raw,
        period_start=period_start,
        period_end=period_end,
        body=body,
        activity_text=activity_text,
        no_aircraft=no_aircraft,
        aircraft_total=aircraft_total,
        aircraft_intrusion_total=aircraft_intrusion_total,
        intrusion_areas=intrusion_areas,
        ship_total=ship_total,
        official_ship_total=official_ship_total,
        balloon_total=balloon_total,
    )



def summarize_mnd_pla_records(records: list[dict], days: int = 7) -> dict:
    usable = [record for record in records if record.get("published_date")]
    usable.sort(key=lambda item: item["published_date"], reverse=True)
    recent = usable[:days]

    area_counts: dict[str, int] = {}
    no_aircraft_dates: list[str] = []

    for record in recent:
        if record.get("no_aircraft") and record.get("published_date"):
            no_aircraft_dates.append(record["published_date"])
        for area in record.get("intrusion_areas") or []:
            area_counts[area] = area_counts.get(area, 0) + 1

    return {
        "days_counted": len(recent),
        "aircraft_total_sum": sum(int(record.get("aircraft_total") or 0) for record in recent),
        "aircraft_intrusion_total_sum": sum(int(record.get("aircraft_intrusion_total") or 0) for record in recent),
        "ship_total_sum": sum(int(record.get("ship_total") or 0) for record in recent),
        "official_ship_total_sum": sum(int(record.get("official_ship_total") or 0) for record in recent),
        "balloon_total_sum": sum(int(record.get("balloon_total") or 0) for record in recent),
        "no_aircraft_days": len(no_aircraft_dates),
        "no_aircraft_dates": no_aircraft_dates,
        "area_counts": area_counts,
        "recent_dates": [record["published_date"] for record in recent],
    }



def list_entries_to_dicts(entries: list[MndPlaListEntry]) -> list[dict]:
    return [asdict(entry) for entry in entries]



def daily_records_to_dicts(records: list[MndPlaDailyRecord]) -> list[dict]:
    return [asdict(record) for record in records]
