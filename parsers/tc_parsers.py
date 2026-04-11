from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, List, Optional

try:
    import requests
except Exception:
    requests = None

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None


@dataclass
class ParsedItem:
    source: str
    key: str
    title: str
    text: str
    item_type: str  # disturbance | active_systems | warning | forecast_advisory
    metadata: dict

    @property
    def normalized_text(self) -> str:
        return normalize_for_hash(self.text)

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.normalized_text.encode("utf-8")).hexdigest()[:16]


NOAA_TITLE_RE = re.compile(
    r"^\s*(?:\d+\.\s+)?(?P<title>[^\n:]{3,200}?)(?:\s*\((?P<invest>[A-Z]{2}\d{2})\))?:\s*$",
    re.M,
)
JTWC_INVEST_RE = re.compile(r"\bINVEST\s+(\d{2}[A-Z])\b", re.I)
JTWC_SYSTEM_TAG_RE = re.compile(r"\(\s*(?:(?P<prefix>INVEST|SS)\s+)?(?P<id>\d{2}[A-Z])\s*\)", re.I)
JTWC_SUBTROP_SECTION_RE = re.compile(
    r"C\.\s*SUBTROPICAL\s+SYSTEM\s+SUMMARY:\s*(?P<body>.*?)(?=\s*(?:\d+\.\s+[^:]+AREA\s*\([^)]*\):)|//\s*NNNN|\Z)",
    re.I | re.S,
)
JTWC_BASIN_SECTION_RE = re.compile(
    r"(?P<num>\d+)\.\s+(?P<title>[^:]+AREA\s*\([^)]*\)):\s*"
    r"(?P<body>.*?)(?=(?:\n?\s*\d+\.\s+[^:]+AREA\s*\([^)]*\):)|//\s*NNNN|\Z)",
    re.I | re.S,
)
JTWC_DIST_SECTION_RE = re.compile(
    r"B\.\s*TROPICAL\s+DISTURBANCE\s+SUMMARY:\s*(?P<body>.*?)(?=\s*C\.\s*SUBTROPICAL\s+SYSTEM\s+SUMMARY:|\Z)",
    re.I | re.S,
)
JTWC_TC_SECTION_RE = re.compile(
    r"A\.\s*TROPICAL\s+CYCLONE\s+SUMMARY:\s*(?P<body>.*?)(?=\s*B\.\s*TROPICAL\s+DISTURBANCE\s+SUMMARY:|\Z)",
    re.I | re.S,
)
JTWC_ITEM_SPLIT_RE = re.compile(r"(?=\(\d+\)\s)")
JTWC_TC_STORM_RE = re.compile(
    r"(?P<storm_class>TROPICAL\s+DEPRESSION|TROPICAL\s+STORM|SEVERE\s+TROPICAL\s+STORM|SUPER\s+TYPHOON|TYPHOON|HURRICANE|TROPICAL\s+CYCLONE)\s+"
    r"(?P<storm_id>\d{2}[A-Z])(?:\s*\((?P<storm_name>[^)]+)\))?",
    re.I,
)

NOAA_START_RE = re.compile(r"(?:Special\s+)?Tropical\s+Weather\s+Outlook", re.I)
NOAA_BASIN_RE = re.compile(r"^\s*For the .*?:\s*$", re.M)
NOAA_ACTIVE_RE = re.compile(r"^\s*Active\s+Systems:\s*$", re.M)
NOAA_NO_FORM_RE = re.compile(
    r"(?:Tropical cyclone formation|No tropical cyclones are)\s+.*?next\s+(?:5|7)\s+days\.",
    re.I,
)
NOAA_FOOTER_RE = re.compile(r"^\s*(?:&&|\$\$|Forecaster\b|NNNN\b)", re.M)
NOAA_48H_RE = re.compile(r"\*\s*Formation chance through 48 hours\.*\s*(low|medium|high)\.*\s*(near 0|\d+)\s*percent\.", re.I)
NOAA_7D_RE = re.compile(r"\*\s*Formation chance through (?:5|7) days\.*\s*(low|medium|high)\.*\s*(near 0|\d+)\s*percent\.", re.I)

NHC_FSTADV_START_RE = re.compile(r"^[ \t]*[A-Z0-9 /\-]+FORECAST/ADVISORY NUMBER\s+\S+\s*$", re.M)
NHC_FSTADV_CYCLONE_RE = re.compile(r"^(?P<storm_title>[A-Z0-9 /\-]+?)\s+FORECAST/ADVISORY NUMBER\s+(?P<num>\S+)\s*$", re.M)
NHC_FSTADV_CENTER_RE = re.compile(
    r"CENTER LOCATED NEAR\s+(?P<lat>[\d.]+[NS])\s+(?P<lon>[\d.]+[EW])\s+AT\s+(?P<fix>\d{2}/\d{4}Z)",
    re.I,
)
NHC_FSTADV_MOVE_RE = re.compile(
    r"PRESENT MOVEMENT TOWARD THE\s+(.+?)\s+OR\s+(\d{1,3})\s+DEGREES\s+AT\s+(\d+)\s+KT",
    re.I,
)
NHC_FSTADV_PRESSURE_RE = re.compile(r"ESTIMATED MINIMUM CENTRAL PRESSURE\s+(\d+)\s+MB", re.I)
NHC_FSTADV_WIND_RE = re.compile(r"MAX SUSTAINED WINDS\s+(\d+)\s+KT\s+WITH GUSTS TO\s+(\d+)\s+KT", re.I)
NHC_FSTADV_NEXT_RE = re.compile(r"NEXT ADVISORY AT\s+(\d{2}/\d{4}Z)", re.I)
NHC_FSTADV_FORECAST_POINT_RE = re.compile(
    r"(?:(?:FORECAST|OUTLOOK)\s+VALID\s+(?P<valid1>\d{2}/\d{4}Z)\s+(?P<lat1>[\d.]+[NS])\s+(?P<lon1>[\d.]+[EW])\s*\n\s*MAX WIND\s+(?P<wind1>\d+)\s+KT)"
    r"|(?:\s*(?P<hours>\d{1,3})\s+HRS,\s+VALID AT:\s*\n\s*(?P<valid2>\d{6}Z)\s+---\s+(?P<lat2>[\d.]+[NS])\s+(?P<lon2>[\d.]+[EW])\s*\n\s*MAX SUSTAINED WINDS\s*-\s*(?P<wind2>\d+)\s+KT,\s*GUSTS\s+(?P<gust2>\d+)\s+KT)",
    re.I,
)

JTWC_WARN_TITLE_RE = re.compile(
    r"^\s*(?:1\.\s*)?(?P<class>[A-Z ]+?)\s+(?P<storm_id>\d{2}[A-Z])\s*\((?P<name>[^)]+)\)\s+WARNING NR\s+(?P<num>\S+)",
    re.M,
)
JTWC_WARN_POS_RE = re.compile(r"WARNING POSITION:\s*\n\s*(?P<fix>\d{6}Z)\s*---\s*NEAR\s+(?P<lat>[\d.]+[NS])\s+(?P<lon>[\d.]+[EW])", re.I)
JTWC_WARN_MOVE_RE = re.compile(r"MOVEMENT PAST SIX HOURS\s*-\s*(\d{1,3})\s+DEGREES\s+AT\s+(\d+)\s+KTS", re.I)
JTWC_WARN_WIND_RE = re.compile(r"MAX SUSTAINED WINDS\s*-\s*(\d+)\s+KT,\s*GUSTS\s+(\d+)\s+KT", re.I)
JTWC_WARN_PRESSURE_RE = re.compile(r"MINIMUM\s+CENTRAL\s+PRESSURE(?:\s+AT\s+\d{6}Z)?\s+IS\s+(\d+)\s+MB", re.I)
JTWC_WARN_WAVE_RE = re.compile(r"MAXIMUM\s+(?:SIGNIFICANT|SIGNIF(?:-\s*)?ICANT)\s+WAVE\s+HEIGHT(?:\s+AT\s+\d{6}Z)?\s+IS\s+(\d+)\s+FEET", re.I)
JTWC_WARN_NEXT_RE = re.compile(r"NEXT WARNINGS? AT\s+([^/]+)//", re.I)
JTWC_WARN_FORECAST_RE = re.compile(
    r"(?P<hours>\d{1,3})\s+HRS,\s+VALID AT:\s*\n\s*(?P<valid>\d{6}Z)\s*---\s*(?P<lat>[\d.]+[NS])\s+(?P<lon>[\d.]+[EW])\s*\n\s*MAX SUSTAINED WINDS\s*-\s*(?P<wind>\d+)\s+KT,\s*GUSTS\s+(?P<gust>\d+)\s+KT",
    re.I,
)


def pause_exit(code: int = 0) -> None:
    try:
        input("\nPress Enter to exit...")
    except EOFError:
        pass
    raise SystemExit(code)


def normalize_ws(text: str) -> str:
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")
    text = text.replace("\r", "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_for_hash(text: str) -> str:
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")
    text = text.replace("\r", "")
    text = text.replace("\xa0", " ")
    text = text.replace("-\n", "-")
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("&", " and ")
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def latlon_to_signed(value: str) -> float:
    value = value.strip().upper()
    hemi = value[-1]
    num = float(value[:-1])
    if hemi in {"S", "W"}:
        num *= -1
    return num


def clean_text_extract(raw_html_or_text: str, start_re: re.Pattern[str], cut_markers: Optional[list[str]] = None) -> str:
    if "<html" not in raw_html_or_text.lower() and "<HTML" not in raw_html_or_text:
        return normalize_ws(raw_html_or_text)

    if BeautifulSoup is None:
        text = re.sub(r"<[^>]+>", "\n", raw_html_or_text)
    else:
        soup = BeautifulSoup(raw_html_or_text, "html.parser")
        text = soup.get_text("\n")

    text = text.replace("\r", "")
    m_start = start_re.search(text)
    if not m_start:
        return normalize_ws(text)
    text = text[m_start.start():]

    if cut_markers:
        cut_positions = [text.find(marker) for marker in cut_markers if marker in text]
        cut_positions = [p for p in cut_positions if p >= 0]
        if cut_positions:
            text = text[: min(cut_positions)]
    return normalize_ws(text)


def extract_nhc_pre_text(raw_html_or_text: str) -> str:
    return clean_text_extract(
        raw_html_or_text,
        NOAA_START_RE,
        ["Standard version of this page", "Quick Links and Additional Resources", "Search   NWS All NOAA", "Search NWS All NOAA"],
    )


def extract_nhc_fstadv_text(raw_html_or_text: str) -> str:
    return clean_text_extract(
        raw_html_or_text,
        NHC_FSTADV_START_RE,
        ["Quick Links and Additional Resources", "Search   NWS All NOAA", "Search NWS All NOAA"],
    )


def extract_jtwc_tc_summary_metadata(chunk: str) -> dict:
    meta: dict = {}

    storm_match = JTWC_TC_STORM_RE.search(chunk)
    if storm_match:
        storm_class = re.sub(r"\s+", " ", storm_match.group("storm_class") or "").strip().upper()
        meta["storm_class"] = storm_class
        meta["storm_id"] = storm_match.group("storm_id").upper()
        storm_name = re.sub(r"\s+", " ", storm_match.group("storm_name") or "").strip().upper()
        if storm_name:
            compact_name = storm_name.replace("-", "")
            spelled_number_words = {
                "ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE",
                "TEN", "ELEVEN", "TWELVE", "THIRTEEN", "FOURTEEN", "FIFTEEN", "SIXTEEN",
                "SEVENTEEN", "EIGHTEEN", "NINETEEN", "TWENTY", "TWENTYONE", "TWENTYTWO",
                "TWENTYTHREE", "TWENTYFOUR", "TWENTYFIVE", "TWENTYSIX", "TWENTYSEVEN",
                "TWENTYEIGHT", "TWENTYNINE", "THIRTY", "THIRTYONE",
            }
            if compact_name not in spelled_number_words:
                meta["storm_name"] = storm_name

    fix_match = re.search(r"AT\s+(\d{2}[A-Z]{3}\d{2})\s+(\d{4})Z", chunk, re.I)
    if fix_match:
        meta["fix_date"] = fix_match.group(1).upper()
        meta["fix_time"] = f"{fix_match.group(1).upper()} {fix_match.group(2)}Z"

    loc_match = re.search(r"LOCATED\s+NEAR\s+([\d.]+[NS])\s+([\d.]+[EW])", chunk, re.I)
    if loc_match:
        meta["lat"] = latlon_to_signed(loc_match.group(1))
        meta["lon"] = latlon_to_signed(loc_match.group(2))

    motion_match = re.search(
        r"TRACKED\s+([A-Z\-]+)WARD\s+AT\s+(\d+)\s+KNOTS?\s+OVER\s+THE\s+PAST\s+SIX\s+HOURS",
        chunk,
        re.I,
    )
    if motion_match:
        meta["movement_text"] = motion_match.group(1).upper() + "WARD"
        try:
            meta["movement_kt"] = int(motion_match.group(2))
        except Exception:
            pass

    wind_match = re.search(
        r"MAXIMUM\s+SUSTAINED\s+SURFACE\s+WINDS\s+WERE\s+ESTIMATED\s+AT\s+(\d+)\s+KNOTS(?:\s+GUSTING\s+TO\s+(\d+)\s+KNOTS)?",
        chunk,
        re.I,
    )
    if wind_match:
        meta["wind_kt"] = int(wind_match.group(1))
        if wind_match.group(2):
            meta["gust_kt"] = int(wind_match.group(2))

    pressure_match = re.search(r"MINIMUM\s+SEA\s+LEVEL\s+PRESSURE\s+IS\s+ESTIMATED\s+TO\s+BE\s+NEAR\s+(\d+)\s+MB", chunk, re.I)
    if pressure_match:
        meta["pressure_mb"] = int(pressure_match.group(1))

    warning_ref_match = re.search(r"SEE\s+REF\s+[A-Z]\s+\(([^)]+)\)", chunk, re.I)
    if warning_ref_match:
        meta["warning_reference"] = warning_ref_match.group(1).upper()

    return meta


def _parse_jtwc_tc_summary_body(tc_body: str, source: str, basin_title: str) -> List[ParsedItem]:
    items: List[ParsedItem] = []
    tc_body = normalize_ws(tc_body)
    if not tc_body or tc_body.upper().startswith("NONE."):
        return items

    raw_chunks = [c.strip() for c in JTWC_ITEM_SPLIT_RE.split(tc_body) if c.strip()]
    for chunk in raw_chunks:
        if re.search(r"^\(\d+\)\s+NO\s+OTHER\s+TROPICAL\s+CYCLONES\.?$", chunk, re.I):
            continue
        if chunk.upper().startswith("NONE"):
            continue
        if not JTWC_TC_STORM_RE.search(chunk):
            continue

        meta = {"basin_section": basin_title} | extract_jtwc_tc_summary_metadata(chunk)
        storm_id = meta.get("storm_id")
        if not storm_id:
            continue

        key = f"{source}|{slugify(basin_title)}|tc_{storm_id.lower()}"
        storm_name = meta.get("storm_name")
        storm_class = str(meta.get("storm_class") or "TROPICAL CYCLONE").title()
        title = f"{storm_class} {storm_id}"
        if storm_name:
            title += f" ({storm_name.title()})"

        items.append(ParsedItem(source=source, key=key, title=title, text=chunk, item_type="warning", metadata=meta))
    return items


def extract_jtwc_disturbance_metadata(chunk: str) -> dict:
    meta: dict = {}

    tag_match = JTWC_SYSTEM_TAG_RE.search(chunk)
    if tag_match:
        system_prefix = (tag_match.group("prefix") or "").upper()
        system_id = tag_match.group("id").upper()
        if system_prefix == "INVEST":
            meta["invest_id"] = system_id
        else:
            meta["system_prefix"] = system_prefix or None
            meta["system_id"] = system_id

    invest_match = JTWC_INVEST_RE.search(chunk)
    if invest_match:
        meta["invest_id"] = invest_match.group(1).upper()

    if re.search(r"\bSUBTROPICAL\s+CYCLONE\b", chunk, re.I):
        meta["system_class"] = "SUBTROPICAL CYCLONE"

    loc_matches = list(re.finditer(r"(?:LOCATED|PERSISTED)\s+NEAR\s+([\d.]+[NS])\s+([\d.]+[EW])", chunk, re.I))
    if not loc_matches:
        loc_matches = list(re.finditer(r"\bNEAR\s+([\d.]+[NS])\s+([\d.]+[EW])", chunk, re.I))
    if loc_matches:
        last = loc_matches[-1]
        meta["lat"] = latlon_to_signed(last.group(1))
        meta["lon"] = latlon_to_signed(last.group(2))

    wind_match = re.search(r"MAXIMUM\s+SUSTAINED\s+SURFACE\s+WINDS\s+ARE\s+ESTIMATED\s+AT\s+(\d+)\s+TO\s+(\d+)\s+KNOTS", chunk, re.I)
    if wind_match:
        meta["wind_kt_min"] = int(wind_match.group(1))
        meta["wind_kt_max"] = int(wind_match.group(2))
        try:
            meta["wind_kt"] = max(int(wind_match.group(1)), int(wind_match.group(2)))
        except Exception:
            pass

    pressure_match = re.search(r"MINIMUM\s+SEA\s+LEVEL\s+PRESSURE\s+IS\s+ESTIMATED\s+TO\s+BE\s+NEAR\s+(\d+)\s+MB", chunk, re.I)
    if pressure_match:
        meta["pressure_mb"] = int(pressure_match.group(1))

    dev_match = re.search(
        r"THE\s+POTENTIAL\s+FOR\s+THE\s+DEVELOPMENT\s+OF\s+A\s+SIGNIFICANT\s+TROPICAL\s+CYCLONE.*?(?:IS\s+)?(UPGRADED TO|DOWNGRADED TO|ASSESSED AS|REMAINS)\s+(LOW|MEDIUM|HIGH)",
        chunk,
        re.I | re.S,
    )
    if dev_match:
        meta["development_phrase"] = dev_match.group(1).upper()
        meta["development_level"] = dev_match.group(2).upper()
    return meta


def _parse_jtwc_disturbance_body(dist_body: str, source: str, basin_title: str) -> List[ParsedItem]:
    items: List[ParsedItem] = []
    dist_body = normalize_ws(dist_body)
    if not dist_body or dist_body.upper().startswith("NONE."):
        return items

    raw_chunks = [c.strip() for c in JTWC_ITEM_SPLIT_RE.split(dist_body) if c.strip()]
    for chunk in raw_chunks:
        if re.search(r"^\(\d+\)\s+NO\s+OTHER\s+SUSPECT\s+AREAS\.?$", chunk, re.I):
            continue
        if chunk.upper().startswith("NONE"):
            continue
        if re.search(r"NOW\s+THE\s+SUBJECT\s+OF\s+A\s+TROPICAL\s+CYCLONE\s+WARNING", chunk, re.I):
            continue

        meta = {"basin_section": basin_title} | extract_jtwc_disturbance_metadata(chunk)
        if not re.search(r"AREA\s+OF\s+CONVECTION", chunk, re.I) and "invest_id" not in meta:
            continue

        invest = meta.get("invest_id")
        if invest:
            key = f"{source}|{slugify(basin_title)}|invest_{invest.lower()}"
            title = f"INVEST {invest}"
        else:
            first_clause = re.split(r"[\.;:]", chunk, maxsplit=1)[0]
            key = f"{source}|{slugify(basin_title)}|{slugify(first_clause)[:80]}"
            title = first_clause[:120]

        items.append(ParsedItem(source=source, key=key, title=title, text=chunk, item_type="disturbance", metadata=meta))
    return items


def _parse_jtwc_subtropical_body(sub_body: str, source: str, basin_title: str) -> List[ParsedItem]:
    items: List[ParsedItem] = []
    sub_body = normalize_ws(sub_body)
    if not sub_body or sub_body.upper().startswith("NONE."):
        return items

    raw_chunks = [c.strip() for c in JTWC_ITEM_SPLIT_RE.split(sub_body) if c.strip()]
    for chunk in raw_chunks:
        if re.search(r"^\(\d+\)\s+NO\s+OTHER\s+SUBTROPICAL\s+SYSTEMS?\.?$", chunk, re.I):
            continue
        if chunk.upper().startswith("NONE"):
            continue

        meta = {"basin_section": basin_title} | extract_jtwc_disturbance_metadata(chunk)
        meta.setdefault("system_class", "SUBTROPICAL SYSTEM")

        if not re.search(r"AREA\s+OF\s+CONVECTION", chunk, re.I) and not meta.get("system_id") and not meta.get("invest_id"):
            continue

        system_id = str(meta.get("system_id") or meta.get("invest_id") or "").upper()
        system_prefix = str(meta.get("system_prefix") or ("INVEST" if meta.get("invest_id") else "SS")).upper()

        if system_id:
            key_prefix = (system_prefix or "system").lower()
            key = f"{source}|{slugify(basin_title)}|{key_prefix}_{system_id.lower()}"
            title = f"{system_prefix} {system_id}" if system_prefix else system_id
        else:
            first_clause = re.split(r"[\.;:]", chunk, maxsplit=1)[0]
            key = f"{source}|{slugify(basin_title)}|{slugify(first_clause)[:80]}"
            title = first_clause[:120]

        items.append(ParsedItem(source=source, key=key, title=title, text=chunk, item_type="disturbance", metadata=meta))
    return items


def parse_jtwc_disturbance_summary(raw_text: str, source: str) -> List[ParsedItem]:
    text = normalize_ws(raw_text)
    items: List[ParsedItem] = []
    for basin_match in JTWC_BASIN_SECTION_RE.finditer(text):
        basin_title = basin_match.group("title").strip()
        basin_body = basin_match.group("body").strip()

        tc_match = JTWC_TC_SECTION_RE.search(basin_body)
        if tc_match:
            items.extend(_parse_jtwc_tc_summary_body(tc_match.group("body"), source, basin_title))

        dist_match = JTWC_DIST_SECTION_RE.search(basin_body)
        if dist_match:
            items.extend(_parse_jtwc_disturbance_body(dist_match.group("body"), source, basin_title))

        subtrop_match = JTWC_SUBTROP_SECTION_RE.search(basin_body)
        if subtrop_match:
            items.extend(_parse_jtwc_subtropical_body(subtrop_match.group("body"), source, basin_title))

    if not items:
        for idx, tc_match in enumerate(JTWC_TC_SECTION_RE.finditer(text), start=1):
            items.extend(_parse_jtwc_tc_summary_body(tc_match.group("body"), source, f"unknown_basin_{idx}"))
        for idx, dist_match in enumerate(JTWC_DIST_SECTION_RE.finditer(text), start=1):
            items.extend(_parse_jtwc_disturbance_body(dist_match.group("body"), source, f"unknown_basin_{idx}"))
        for idx, subtrop_match in enumerate(JTWC_SUBTROP_SECTION_RE.finditer(text), start=1):
            items.extend(_parse_jtwc_subtropical_body(subtrop_match.group("body"), source, f"unknown_basin_{idx}"))
    return items


def _strip_footer(text: str) -> str:
    m = NOAA_FOOTER_RE.search(text)
    return text[: m.start()].rstrip() if m else text.rstrip()


def _extract_active_block(body: str) -> tuple[Optional[str], str]:
    m = NOAA_ACTIVE_RE.search(body)
    if not m:
        return None, body
    after = body[m.end():].lstrip("\n")
    next_title = NOAA_TITLE_RE.search(after)
    footer = NOAA_FOOTER_RE.search(after)
    no_form = NOAA_NO_FORM_RE.search(after)
    end_positions = [len(after)]
    if next_title:
        end_positions.append(next_title.start())
    if footer:
        end_positions.append(footer.start())
    if no_form:
        end_positions.append(no_form.start())
    end = min(end_positions)
    return after[:end].strip() or None, after[end:].lstrip()


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _extract_active_names(active_block: str) -> tuple[List[str], List[str]]:
    block = re.sub(r"\s+", " ", active_block).strip()
    started_patterns = [
        r"is issuing advisories on\s+(.+?)(?=,\s+located|\.|\s+and\s+on)",
        r"also issuing advisories on\s+(.+?)(?=,\s+located|\.|\s+and\s+on)",
        r",\s*and\s+on\s+(.+?)(?=,\s+located|\.)",
    ]
    final_patterns = [
        r"has issued the (?:final|last) advisory on\s+(.+?)(?=,\s+located|\.)",
        r",\s*and\s+has issued the (?:final|last) advisory on\s+(.+?)(?=,\s+located|\.)",
    ]
    started = []
    final = []
    for pat in started_patterns:
        started.extend(re.findall(pat, block, flags=re.I))
    for pat in final_patterns:
        final.extend(re.findall(pat, block, flags=re.I))
    started = _dedupe_preserve_order([re.sub(r"\s+", " ", n).strip() for n in started])
    final = _dedupe_preserve_order([re.sub(r"\s+", " ", n).strip() for n in final])
    return started, final


def parse_noaa_two(raw_html_or_text: str, source: str) -> List[ParsedItem]:
    text = _strip_footer(extract_nhc_pre_text(raw_html_or_text))
    items: List[ParsedItem] = []
    basin_match = NOAA_BASIN_RE.search(text)
    if not basin_match:
        return items
    body = text[basin_match.end():].strip()
    if NOAA_NO_FORM_RE.search(body) and not NOAA_ACTIVE_RE.search(body) and not NOAA_TITLE_RE.search(body):
        return items

    active_block, remainder = _extract_active_block(body)
    if active_block:
        clean_started, clean_final = _extract_active_names(active_block)
        summary_title = "Active Systems"
        if clean_started:
            summary_title += ": " + "; ".join(clean_started)
        elif clean_final:
            summary_title += ": " + "; ".join(clean_final)
        items.append(ParsedItem(source=source, key=f"{source}|active_systems", title=summary_title, text=active_block, item_type="active_systems", metadata={"active_started": clean_started, "active_final": clean_final}))

    remainder = remainder.strip()
    if NOAA_NO_FORM_RE.search(remainder) and not NOAA_TITLE_RE.search(remainder):
        return items

    matches = list(NOAA_TITLE_RE.finditer(remainder))
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(remainder)
        block = remainder[start:end].strip()
        if not NOAA_48H_RE.search(block):
            continue
        title = match.group("title").strip()
        invest = match.group("invest")
        key = f"{source}|{invest.lower()}" if invest else f"{source}|{slugify(title)}"
        meta = {}
        m48 = NOAA_48H_RE.search(block)
        m7d = NOAA_7D_RE.search(block)
        if m48:
            meta["chance_48h_level"] = m48.group(1).upper()
            meta["chance_48h_pct"] = m48.group(2)
        if m7d:
            meta["chance_7d_level"] = m7d.group(1).upper()
            meta["chance_7d_pct"] = m7d.group(2)
        if invest:
            meta["invest_id"] = invest.upper()
        items.append(ParsedItem(source=source, key=key, title=(f"{title} ({invest})" if invest else title), text=block, item_type="disturbance", metadata=meta))
    return items


def parse_jtwc_warning(raw_text: str, source: str) -> List[ParsedItem]:
    text = normalize_ws(raw_text)
    title_match = JTWC_WARN_TITLE_RE.search(text)
    if not title_match:
        return []
    storm_class = re.sub(r"\s+", " ", title_match.group("class")).strip().upper()
    storm_id = title_match.group("storm_id").upper()
    storm_name = re.sub(r"\s+", " ", title_match.group("name")).strip().upper()
    warning_nr = title_match.group("num").upper()
    meta = {"storm_class": storm_class, "storm_id": storm_id, "storm_name": storm_name, "warning_nr": warning_nr}
    pos_match = JTWC_WARN_POS_RE.search(text)
    if pos_match:
        meta["fix_time"] = pos_match.group("fix")
        meta["lat"] = latlon_to_signed(pos_match.group("lat"))
        meta["lon"] = latlon_to_signed(pos_match.group("lon"))
    move_match = JTWC_WARN_MOVE_RE.search(text)
    if move_match:
        meta["movement_deg"] = int(move_match.group(1))
        meta["movement_kt"] = int(move_match.group(2))
    wind_match = JTWC_WARN_WIND_RE.search(text)
    if wind_match:
        meta["wind_kt"] = int(wind_match.group(1))
        meta["gust_kt"] = int(wind_match.group(2))
    pressure_match = JTWC_WARN_PRESSURE_RE.search(text)
    if pressure_match:
        meta["pressure_mb"] = int(pressure_match.group(1))
    wave_match = JTWC_WARN_WAVE_RE.search(text)
    if wave_match:
        meta["wave_height_ft"] = int(wave_match.group(1))
    next_match = JTWC_WARN_NEXT_RE.search(text)
    if next_match:
        times = re.findall(r"\d{6}Z", next_match.group(1))
        if times:
            meta["next_warning_times"] = times
    forecasts = []
    for m in JTWC_WARN_FORECAST_RE.finditer(text):
        forecasts.append({"hours": int(m.group("hours")), "valid": m.group("valid"), "lat": latlon_to_signed(m.group("lat")), "lon": latlon_to_signed(m.group("lon")), "wind_kt": int(m.group("wind")), "gust_kt": int(m.group("gust"))})
    if forecasts:
        meta["forecast_points"] = forecasts
    key = f"{source}|{storm_id.lower()}|warning_{warning_nr.lower()}"
    title = f"{storm_class.title()} {storm_id} ({storm_name.title()})"
    return [ParsedItem(source=source, key=key, title=title, text=text, item_type="warning", metadata=meta)]


def parse_nhc_forecast_advisory(raw_html_or_text: str, source: str) -> List[ParsedItem]:
    text = _strip_footer(extract_nhc_fstadv_text(raw_html_or_text))
    cyclone_match = NHC_FSTADV_CYCLONE_RE.search(text)
    if not cyclone_match:
        return []
    storm_title = re.sub(r"\s+", " ", cyclone_match.group("storm_title")).strip()
    adv_num = cyclone_match.group("num")
    meta = {"storm_title": storm_title, "advisory_number": adv_num}
    storm_id_match = re.search(r"\b([A-Z]{2}\d{6})\b", text)
    if storm_id_match:
        meta["storm_id"] = storm_id_match.group(1)
    center_match = NHC_FSTADV_CENTER_RE.search(text)
    if center_match:
        meta["fix_time"] = center_match.group("fix")
        meta["lat"] = latlon_to_signed(center_match.group("lat"))
        meta["lon"] = latlon_to_signed(center_match.group("lon"))
    move_match = NHC_FSTADV_MOVE_RE.search(text)
    if move_match:
        meta["movement_text"] = move_match.group(1).strip()
        meta["movement_deg"] = int(move_match.group(2))
        meta["movement_kt"] = int(move_match.group(3))
    pressure_match = NHC_FSTADV_PRESSURE_RE.search(text)
    if pressure_match:
        meta["pressure_mb"] = int(pressure_match.group(1))
    wind_match = NHC_FSTADV_WIND_RE.search(text)
    if wind_match:
        meta["wind_kt"] = int(wind_match.group(1))
        meta["gust_kt"] = int(wind_match.group(2))
    next_match = NHC_FSTADV_NEXT_RE.search(text)
    if next_match:
        meta["next_advisory"] = next_match.group(1)
    forecast_points = []
    for m in NHC_FSTADV_FORECAST_POINT_RE.finditer(text):
        if m.group("valid1"):
            forecast_points.append({"valid": m.group("valid1"), "lat": latlon_to_signed(m.group("lat1")), "lon": latlon_to_signed(m.group("lon1")), "wind_kt": int(m.group("wind1"))})
        else:
            forecast_points.append({"hours": int(m.group("hours")), "valid": m.group("valid2"), "lat": latlon_to_signed(m.group("lat2")), "lon": latlon_to_signed(m.group("lon2")), "wind_kt": int(m.group("wind2")), "gust_kt": int(m.group("gust2"))})
    if forecast_points:
        meta["forecast_points"] = forecast_points
    storm_id_for_key = meta.get("storm_id", slugify(storm_title))
    key = f"{source}|{storm_id_for_key.lower()}|adv_{str(adv_num).lower()}"
    return [ParsedItem(source=source, key=key, title=storm_title.title(), text=text, item_type="forecast_advisory", metadata=meta)]


def parse_bulletin(raw_text: str, source: str) -> List[ParsedItem]:
    source_l = source.lower()
    if source_l in {"jtwc_abio", "jtwc_abpw"}:
        return parse_jtwc_disturbance_summary(raw_text, source)
    if source_l in {"nhc_at", "nhc_ep", "cphc_cp", "noaa_at", "noaa_ep", "noaa_cp"}:
        return parse_noaa_two(raw_text, source)
    if source_l.startswith("jtwc_warning") or source_l in {"jtwc_warning", "jtwc_warn"}:
        return parse_jtwc_warning(raw_text, source)
    if source_l.startswith("nhc_fstadv") or source_l in {"nhc_advisory", "nhc_forecast_advisory", "cphc_fstadv"}:
        return parse_nhc_forecast_advisory(raw_text, source)
    raise ValueError(f"Unsupported source: {source}")


def diff_current(previous: List[ParsedItem], current: List[ParsedItem]) -> dict:
    prev_map = {x.key: x for x in previous}
    cur_map = {x.key: x for x in current}
    added, updated, unchanged, removed = [], [], [], []
    for key, item in cur_map.items():
        old = prev_map.get(key)
        if old is None:
            added.append(item)
        elif old.content_hash != item.content_hash:
            updated.append(item)
        else:
            unchanged.append(item)
    for key, item in prev_map.items():
        if key not in cur_map:
            removed.append(item)
    return {
        "added": [asdict(x) | {"content_hash": x.content_hash} for x in added],
        "updated": [asdict(x) | {"content_hash": x.content_hash} for x in updated],
        "unchanged": [asdict(x) | {"content_hash": x.content_hash} for x in unchanged],
        "removed": [asdict(x) | {"content_hash": x.content_hash} for x in removed],
    }


JTWC_ABIO_REAL = """ABIO10 PGTW 311800 MSGID/GENADMIN/JOINT TYPHOON WRNCEN PEARL HARBOR HI// SUBJ/SIGNIFICANT TROPICAL WEATHER ADVISORY FOR THE INDIAN OCEAN/311800ZMAR2026-011800ZAPR2026// RMKS/ 1. NORTH INDIAN OCEAN AREA (MALAY PENINSULA WEST TO COAST OF AFRICA): A. TROPICAL CYCLONE SUMMARY: NONE. B. TROPICAL DISTURBANCE SUMMARY: NONE. C. SUBTROPICAL SYSTEM SUMMARY: NONE. 2. SOUTH INDIAN OCEAN AREA (135E WEST TO COAST OF AFRICA): A. TROPICAL CYCLONE SUMMARY: NONE. B. TROPICAL DISTURBANCE SUMMARY: (1) THE AREA OF CONVECTION (INVEST 99S) PREVIOUSLY LOCATED NEAR 9.6S 76.1E IS NOW LOCATED NEAR 8.6S 75.9E, APPROXIMATELY 213 NM EAST-SOUTHEAST OF DIEGO GARCIA. ANIMATED ENHANCED INFRARED IMAGERY DEPICTS A DEVELOPING LOW LEVEL CIRCULATION CENTER (LLCC) PARTIALLY OBSCURED BY FLARING CONVECTION. A 311629Z METOP-B ASCAT PASS REVEALED 15-20 KNOT WINDS WITHIN THE EASTERN SEMICIRCLE OF THE SYSTEM. ENVIRONMENTAL ANALYSIS REVEALS FAVORABLE CONDITIONS FOR DEVELOPMENT WITH LOW VERTICAL WIND SHEAR (10-15 KTS), WARM SEA SURFACE TEMPERATURES (29 TO 30 C), AND MODERATE POLEWARD OUTFLOW. DETERMINISTIC AND ENSEMBLE MODELS ARE IN GOOD AGREEMENT OF 99S FURTHER DEVELOPING AND TAKING A SOUTH-SOUTHWESTWARD TRACK OVER THE NEXT 48 HOURS. MAXIMUM SUSTAINED SURFACE WINDS ARE ESTIMATED AT 18 TO 23 KNOTS. MINIMUM SEA LEVEL PRESSURE IS ESTIMATED TO BE NEAR 1005 MB. THE POTENTIAL FOR THE DEVELOPMENT OF A SIGNIFICANT TROPICAL CYCLONE WITHIN THE NEXT 24 HOURS IS UPGRADED TO MEDIUM. (2) NO OTHER SUSPECT AREAS. C. SUBTROPICAL SYSTEM SUMMARY: NONE.// NNNN"""
JTWC_ABPW_EMPTY = """ABPW10 PGTW 310600 MSGID/GENADMIN/JOINT TYPHOON WRNCEN PEARL HARBOR HI// SUBJ/SIGNIFICANT TROPICAL WEATHER ADVISORY FOR THE WESTERN AND SOUTH PACIFIC OCEANS/310600ZMAR2026-010600ZAPR2026// RMKS/ 1. WESTERN NORTH PACIFIC AREA (180 TO MALAY PENINSULA): A. TROPICAL CYCLONE SUMMARY: NONE. B. TROPICAL DISTURBANCE SUMMARY: NONE. C. SUBTROPICAL SYSTEM SUMMARY: NONE. 2. SOUTH PACIFIC AREA (WEST COAST OF SOUTH AMERICA TO 135 EAST): A. TROPICAL CYCLONE SUMMARY: NONE. B. TROPICAL DISTURBANCE SUMMARY: NONE. C. SUBTROPICAL SYSTEM SUMMARY: NONE.// NNNN"""
JTWC_ABPW_OLD = """ABPW10 PGTW 050140 MSGID/GENADMIN/JOINT TYPHOON WRNCEN PEARL HARBOR HI// SUBJ/SIGNIFICANT TROPICAL WEATHER ADVISORY FOR THE WESTERN AND SOUTH PACIFIC OCEANS/050140ZJUN2025-060600ZJUN2025// RMKS/ 1. WESTERN NORTH PACIFIC AREA (180 TO MALAY PENINSULA): A. TROPICAL CYCLONE SUMMARY: NONE. B. TROPICAL DISTURBANCE SUMMARY: (1) AN AREA OF CONVECTION (INVEST 92W) HAS PERSISTED NEAR 17.4N 116.8E, APPROXIMATELY 258 NM SOUTHWEST OF HONG KONG. ENVIRONMENTAL ANALYSIS INDICATES MARGINALLY FAVORABLE CONDITIONS FOR DEVELOPMENT. MAXIMUM SUSTAINED SURFACE WINDS ARE ESTIMATED AT 13 TO 18 KNOTS. MINIMUM SEA LEVEL PRESSURE IS ESTIMATED TO BE NEAR 1004 MB. THE POTENTIAL FOR THE DEVELOPMENT OF A SIGNIFICANT TROPICAL CYCLONE WITHIN THE NEXT 24 HOURS IS ASSESSED AS LOW. (2) NO OTHER SUSPECT AREAS. C. SUBTROPICAL SYSTEM SUMMARY: NONE. 2. SOUTH PACIFIC AREA (WEST COAST OF SOUTH AMERICA TO 135 EAST): A. TROPICAL CYCLONE SUMMARY: NONE. B. TROPICAL DISTURBANCE SUMMARY: NONE. C. SUBTROPICAL SYSTEM SUMMARY: NONE.// NNNN"""
NOAA_EMPTY_HTML = """<html><body><h2>Atlantic Tropical Weather Outlook (Text)</h2><pre>\n000\nABNT20 KNHC 302304\nTWOAT\n\nTropical Weather Outlook\nNWS National Hurricane Center Miami FL\n700 PM EST Sun Nov 30 2025\n\nFor the North Atlantic...Caribbean Sea and the Gulf of America:\n\nTropical cyclone formation is not expected during the next 7 days.\nThis is the last regularly scheduled Tropical Weather Outlook of the\n2025 Atlantic Hurricane Season. Routine issuance of the Tropical\nWeather Outlook will resume on May 15, 2026. During the off-season,\nSpecial Tropical Weather Outlooks will be issued as conditions\nwarrant.\n\n$$\nForecaster Bucci\n</pre></body></html>"""
NOAA_ACTIVE_MULTI = """<html><body><pre>\nZCZC HFOTWOCP ALL\nTTAA00 PHFO DDHHMM\n\nTropical Weather Outlook\nNWS Central Pacific Hurricane Center Honolulu HI\nIssued by NWS National Hurricane Center Miami FL\n200 AM HST Tue Sep 2 2025\n\nFor the central North Pacific...between 140W and 180W:\n\nActive Systems:\nThe National Hurricane Center is issuing advisories on Hurricane\nKiko, located in the east Pacific basin well east of the Hawaiian\nIslands. Kiko is expected to cross into the central Pacific basin\nover the weekend.\nThe National Hurricane Center is also issuing advisories on\nTropical Depression Twelve-E, located in the east Pacific basin\nabout 200 miles west-southwest of the southwestern coast of Mexico.\n\nTropical cyclone formation is not expected during the next 7 days.\n\n&&\nPublic Advisories on Tropical Depression Twelve-E are issued under\nWMO header WTPZ32 KNHC and under AWIPS header MIATCPEP2.\n$$\nForecaster Hagen\nNNNN\n</pre></body></html>"""
NOAA_SPECIAL_TWO = """<html><body><pre>\nABNT20 KNHC 041548\nTWOAT\n\nSpecial Tropical Weather Outlook\nNWS National Hurricane Center Miami FL\n1150 AM EDT Tue Oct 4 2022\n\nSpecial Tropical Weather Outlook to update discussion of the\ntropical wave east of the Windward Islands\n\nFor the North Atlantic...Caribbean Sea and the Gulf of America:\nEastern Tropical Atlantic (AL91):\nA broad low pressure system located a few hundred miles\nwest-southwest of the Cabo Verde Islands continues to produce a\nlarge area of showers and thunderstorms.\n* Formation chance through 48 hours...high...80 percent.\n* Formation chance through 7 days...high...80 percent.\nEast of the Windward Islands (AL92):\nUpdated: Visible satellite images and recent satellite-derived\nwind data suggest that a broad low-level circulation could be\nforming in association with the tropical wave located a few hundred\nmiles east of the southern Windward Islands.\n* Formation chance through 48 hours...medium...40 percent.\n* Formation chance through 7 days...high...70 percent.\n$$\nForecaster Berg\nNNNN\n</pre></body></html>"""
JTWC_WARNING_FIXTURE = """WTPN31 PGTW 172100\nMSGID/GENADMIN/NAVPACMETOCCEN PEARL HARBOR HI/JTWC//\nSUBJ/TROPICAL CYCLONE WARNING//\nRMKS/\n1. TYPHOON 02W (CHANCHU) WARNING NR 037\n   01 ACTIVE TROPICAL CYCLONE IN NORTHWESTPAC\n   MAX SUSTAINED WINDS BASED ON ONE-MINUTE AVERAGE\n    ---\n   WARNING POSITION:\n   171800Z --- NEAR 23.4N 117.0E\n     MOVEMENT PAST SIX HOURS - 020 DEGREES AT 10 KTS\n     POSITION ACCURATE TO WITHIN 060 NM\n     POSITION BASED ON CENTER LOCATED BY SATELLITE\n   PRESENT WIND DISTRIBUTION:\n   MAX SUSTAINED WINDS - 075 KT, GUSTS 090 KT\n   BECOMING EXTRATROPICAL\n   REPEAT POSIT: 23.4N 117.0E\n    ---\n   FORECASTS:\n   12 HRS, VALID AT:\n   180600Z --- 26.3N 118.7E\n   MAX SUSTAINED WINDS - 045 KT, GUSTS 055 KT\n   EXTRATROPICAL\n   VECTOR TO 24 HR POSIT: 030 DEG/ 17 KTS\n    ---\n   24 HRS, VALID AT:\n   181800Z --- 29.2N 120.8E\n   MAX SUSTAINED WINDS - 030 KT, GUSTS 040 KT\n   EXTRATROPICAL\n    ---\nREMARKS:\n172100Z POSITION NEAR 24.1N 117.4E.\nTYPHOON (TY) 02W (CHANCHU), LOCATED APPROXIMATELY 170 NM EAST-\nNORTHEAST OF HONG KONG, HAS TRACKED NORTH-NORTHEASTWARD AT 10 KNOTS\nOVER THE PAST 06 HOURS. MAXIMUM SIGNIFICANT WAVE HEIGHT AT 171800Z IS 17 FEET. NEXT WARNINGS AT 180300Z,\n180900Z AND 181500Z.//"""
NHC_FSTADV_FIXTURE = """TROPICAL DEPRESSION NINE FORECAST/ADVISORY NUMBER   1\nNWS NATIONAL HURRICANE CENTER MIAMI FL       AL092022\n0900 UTC FRI SEP 23 2022\n\nTHERE ARE NO COASTAL WATCHES OR WARNINGS IN EFFECT.\n\nTROPICAL DEPRESSION CENTER LOCATED NEAR 13.9N  68.6W AT 23/0900Z\nPOSITION ACCURATE WITHIN  40 NM\n\nPRESENT MOVEMENT TOWARD THE WEST-NORTHWEST OR 290 DEGREES AT  11 KT\nESTIMATED MINIMUM CENTRAL PRESSURE 1006 MB\nMAX SUSTAINED WINDS  30 KT WITH GUSTS TO  40 KT.\n\nFORECAST VALID 23/1800Z 14.4N  70.2W\nMAX WIND  35 KT...GUSTS  45 KT.\n34 KT... 40NE   0SE   0SW  30NW.\nFORECAST VALID 24/0600Z 14.7N  72.6W\nMAX WIND  35 KT...GUSTS  45 KT.\n34 KT... 40NE   0SE   0SW  40NW.\n\nFORECAST VALID 24/1800Z 14.8N  75.0W\nMAX WIND  35 KT...GUSTS  45 KT.\n34 KT... 50NE   0SE   0SW  40NW.\n\nOUTLOOK VALID 27/0600Z 22.6N  82.6W\nMAX WIND  90 KT...GUSTS 110 KT.\n\nOUTLOOK VALID 28/0600Z 26.0N  82.3W\nMAX WIND  95 KT...GUSTS 115 KT.\nREQUEST FOR 3 HOURLY SHIP REPORTS WITHIN 300 MILES OF 13.9N  68.6W\n\nNEXT ADVISORY AT 23/1500Z\n\n$$\nFORECASTER PAPIN\nNNNN"""
FIXTURES = {
    "jtwc_abio_real": ("jtwc_abio", JTWC_ABIO_REAL),
    "jtwc_abpw_empty": ("jtwc_abpw", JTWC_ABPW_EMPTY),
    "jtwc_abpw_old": ("jtwc_abpw", JTWC_ABPW_OLD),
    "noaa_empty_html": ("nhc_at", NOAA_EMPTY_HTML),
    "noaa_active_multi": ("cphc_cp", NOAA_ACTIVE_MULTI),
    "noaa_special_two": ("nhc_at", NOAA_SPECIAL_TWO),
    "jtwc_warning": ("jtwc_warning", JTWC_WARNING_FIXTURE),
    "nhc_fstadv": ("nhc_fstadv", NHC_FSTADV_FIXTURE),
}
LIVE_SOURCE_URLS = {
    "jtwc_abio": "https://www.metoc.navy.mil/jtwc/products/abioweb.txt",
    "jtwc_abpw": "https://www.metoc.navy.mil/jtwc/products/abpwweb.txt",
    "nhc_at": "https://www.nhc.noaa.gov/text/MIATWOAT.shtml?text=",
    "nhc_ep": "https://www.nhc.noaa.gov/text/MIATWOEP.shtml?text=",
    "cphc_cp": "https://www.nhc.noaa.gov/text/HFOTWOCP.shtml?text=",
}


def fetch_url(url: str, timeout: int = 20) -> str:
    if requests is None:
        raise RuntimeError("requests is not installed")
    resp = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0 tc-bulletin-parser-v3"})
    resp.raise_for_status()
    return resp.text


def dump_items(items: Iterable[ParsedItem]) -> str:
    return json.dumps([asdict(x) | {"content_hash": x.content_hash} for x in items], ensure_ascii=False, indent=2)

def _fmt_coord(value: float, lat: bool) -> str:
    hemi = ("N" if value >= 0 else "S") if lat else ("E" if value >= 0 else "W")
    return f"{abs(value):.1f}{hemi}"


def _fmt_location(meta: dict) -> Optional[str]:
    if "lat" in meta and "lon" in meta:
        return f"{_fmt_coord(meta['lat'], True)} {_fmt_coord(meta['lon'], False)}"
    return None


def _fmt_wind(meta: dict) -> Optional[str]:
    if "wind_kt_min" in meta and "wind_kt_max" in meta:
        return f"{meta['wind_kt_min']}-{meta['wind_kt_max']} kt"
    if "wind_kt" in meta and "gust_kt" in meta:
        return f"{meta['wind_kt']} kt (gust {meta['gust_kt']})"
    if "wind_kt" in meta:
        return f"{meta['wind_kt']} kt"
    return None


def _fmt_next(meta: dict) -> Optional[str]:
    if meta.get("next_warning_times"):
        return ", ".join(meta["next_warning_times"])
    if meta.get("next_advisory"):
        return meta["next_advisory"]
    return None


def _summary_for_item(item: ParsedItem) -> str:
    m = item.metadata
    if item.item_type == "disturbance" and item.source.startswith("jtwc"):
        parts = []
        loc = _fmt_location(m)
        if loc:
            parts.append(loc)
        if m.get("wind_kt_min") is not None and m.get("wind_kt_max") is not None:
            parts.append(f"winds {m['wind_kt_min']}-{m['wind_kt_max']} kt")
        if m.get("pressure_mb") is not None:
            parts.append(f"{m['pressure_mb']} mb")
        if m.get("development_level"):
            parts.append(m['development_level'])
        return " | ".join(parts) if parts else item.title
    if item.item_type == "disturbance":
        parts = []
        if m.get("chance_48h_pct") is not None:
            parts.append(f"48h {m['chance_48h_pct']}% {m.get('chance_48h_level','')}")
        if m.get("chance_7d_pct") is not None:
            parts.append(f"7d {m['chance_7d_pct']}% {m.get('chance_7d_level','')}")
        return " | ".join(parts) if parts else item.title
    if item.item_type == "active_systems":
        started = m.get("active_started") or []
        final = m.get("active_final") or []
        parts = []
        if started:
            parts.append("active: " + "; ".join(started))
        if final:
            parts.append("final: " + "; ".join(final))
        return " | ".join(parts) if parts else item.title
    if item.item_type in {"warning", "forecast_advisory"}:
        parts = []
        loc = _fmt_location(m)
        if loc:
            parts.append(loc)
        wind = _fmt_wind(m)
        if wind:
            parts.append(f"wind {wind}")
        if m.get("pressure_mb") is not None:
            parts.append(f"{m['pressure_mb']} mb")
        nxt = _fmt_next(m)
        if nxt:
            parts.append(f"next {nxt}")
        return " | ".join(parts) if parts else item.title
    return item.title


def to_platform_rows(items: Iterable[ParsedItem]) -> list[dict]:
    rows = []
    for item in items:
        m = item.metadata
        row = {
            "source": item.source,
            "key": item.key,
            "item_type": item.item_type,
            "title": item.title,
            "status": m.get("development_level") or m.get("storm_class") or ("Active Systems" if item.item_type == "active_systems" else None),
            "location": _fmt_location(m),
            "wind": _fmt_wind(m),
            "pressure_mb": m.get("pressure_mb"),
            "risk_48h": (f"{m.get('chance_48h_pct')}% {m.get('chance_48h_level')}" if m.get('chance_48h_pct') is not None else None),
            "risk_7d": (f"{m.get('chance_7d_pct')}% {m.get('chance_7d_level')}" if m.get('chance_7d_pct') is not None else None),
            "next_update": _fmt_next(m),
            "summary": _summary_for_item(item),
            "content_hash": item.content_hash,
            "raw_text": item.text,
            "metadata": m,
        }
        rows.append(row)
    return rows


def dump_platform_rows(items: Iterable[ParsedItem]) -> str:
    return json.dumps(to_platform_rows(items), ensure_ascii=False, indent=2)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parse JTWC/NOAA tropical cyclone bulletins and warning text.")
    parser.add_argument("--fetch", help="Fetch a known live alias or a full URL")
    parser.add_argument("--file", help="Parse a local file")
    parser.add_argument("--source", help="Source type for --file or full URL")
    parser.add_argument("--fixture", help="Run one embedded fixture")
    parser.add_argument("--list-fixtures", action="store_true")
    parser.add_argument("--list-live", action="store_true")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--platform", action="store_true", help="Output platform-style table rows instead of raw parsed items")
    parser.add_argument("--platform-demo", action="store_true", help="Show platform-style rows for all embedded fixtures")
    return parser


def run_demo() -> None:
    print("=" * 88)
    print("EMBEDDED FIXTURE RESULTS")
    print("=" * 88)
    out = {}
    for name, (source, raw) in FIXTURES.items():
        out[name] = [asdict(x) | {"content_hash": x.content_hash} for x in parse_bulletin(raw, source)]
    print(json.dumps(out, ensure_ascii=False, indent=2))

    print("\n" + "=" * 88)
    print("UPSERT DIFF DEMO (same JTWC key, line-wrap changes only => unchanged after normalized hash)")
    print("=" * 88)
    old_items = parse_bulletin(JTWC_ABIO_REAL, "jtwc_abio")
    live_style = JTWC_ABIO_REAL.replace("EAST-SOUTHEAST", "EAST-\nSOUTHEAST").replace("ARE ESTIMATED AT 18 TO 23 KNOTS", "ARE \nESTIMATED AT 18 TO 23 KNOTS")
    cur_items = parse_bulletin(live_style, "jtwc_abio")
    print(json.dumps(diff_current(old_items, cur_items), ensure_ascii=False, indent=2))



def run_platform_demo() -> None:
    print("=" * 88)
    print("PLATFORM-STYLE ROWS")
    print("=" * 88)
    out = {}
    for name, (source, raw) in FIXTURES.items():
        out[name] = to_platform_rows(parse_bulletin(raw, source))
    print(json.dumps(out, ensure_ascii=False, indent=2))

def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.list_fixtures:
        print(json.dumps(sorted(FIXTURES.keys()), indent=2))
        pause_exit(0)
    if args.list_live:
        print(json.dumps(LIVE_SOURCE_URLS, indent=2))
        pause_exit(0)
    if args.platform_demo:
        run_platform_demo()
        pause_exit(0)
    if args.demo or (not args.fetch and not args.file and not args.fixture):
        run_demo()
        pause_exit(0)
    if args.fixture:
        if args.fixture not in FIXTURES:
            print(f"Unknown fixture: {args.fixture}")
            pause_exit(1)
        source, raw = FIXTURES[args.fixture]
        items = parse_bulletin(raw, source)
        print(dump_platform_rows(items) if args.platform else dump_items(items))
        pause_exit(0)
    if args.file:
        if not args.source:
            print("--source is required with --file")
            pause_exit(1)
        raw = Path(args.file).read_text(encoding="utf-8", errors="ignore")
        print(dump_items(parse_bulletin(raw, args.source)))
        pause_exit(0)
    if args.fetch:
        if args.fetch in LIVE_SOURCE_URLS:
            source = args.fetch
            raw = fetch_url(LIVE_SOURCE_URLS[args.fetch])
        else:
            if not args.source:
                print("When --fetch is a full URL, --source is required.")
                pause_exit(1)
            source = args.source
            raw = fetch_url(args.fetch)
        items = parse_bulletin(raw, source)
        print(dump_platform_rows(items) if args.platform else dump_items(items))
        pause_exit(0)


if __name__ == "__main__":
    main()
