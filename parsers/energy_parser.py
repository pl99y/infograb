from __future__ import annotations

import html as html_lib
import json
import re
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any


class EnergyParseError(ValueError):
    pass


@dataclass
class EnergyQuote:
    quote_key: str
    name: str
    price: float | None
    unit: str | None
    change: float | None
    change_percent: float | None
    timestamp_text: str | None
    source_name: str
    source_url: str
    provider_used: str
    is_delayed: bool | None
    delay_note: str | None
    extra: dict[str, Any] = field(default_factory=dict)


UNIT_MAP = {
    "wti": "USD/bbl",
    "brent": "USD/bbl",
    "murban": "USD/bbl",
    "natural_gas": "USD/MMBtu",
    "gasoline": "USD/gal",
    "china": "CNY/bbl",
}

DISPLAY_NAME_MAP = {
    "wti": "WTI Crude",
    "brent": "Brent Crude",
    "murban": "Murban Crude",
    "natural_gas": "Natural Gas",
    "gasoline": "Gasoline",
}

OILPRICE_LAST_JSON_ID_MAP = {
    "wti": "45",
    "brent": "46",
    "murban": "4464",
    "natural_gas": "51",
    "gasoline": "53",
}


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def parse_tradingview_quote(
    *,
    quote_key: str,
    name: str,
    html: str,
    source_url: str,
) -> EnergyQuote:
    text = re.sub(r"\s+", " ", html)

    match = re.search(
        r"The current price of .*? is\s*([+-]?[0-9][0-9,]*\.?[0-9]*)\s*([A-Z$€¥/ ]+?)\s*[—-]\s*it has\s*(risen|fallen|increased|decreased|dropped|hasn't changed|has not changed)\s*([0-9][0-9,]*\.?[0-9]*)?%?\s*in the past 24 hours",
        text,
        re.IGNORECASE,
    )

    price = None
    unit = None
    pct = None
    verb = None

    if match:
        price = _to_float(match.group(1))
        unit = match.group(2).strip().replace("\xa0", " ")
        verb = match.group(3).lower().strip()
        pct = _to_float(match.group(4)) if match.group(4) else 0.0

    if price is None:
        raise EnergyParseError(f"Could not parse TradingView quote for {quote_key}")

    if verb in {"fallen", "decreased", "dropped"} and pct is not None:
        pct = -abs(pct)
    elif verb in {"hasn't changed", "has not changed"}:
        pct = 0.0

    return EnergyQuote(
        quote_key=quote_key,
        name=name,
        price=price,
        unit=unit,
        change=None,
        change_percent=pct,
        timestamp_text=None,
        source_name="TradingView",
        source_url=source_url,
        provider_used="tradingview",
        is_delayed=None,
        delay_note=None,
        extra={},
    )



def _timestamp_to_iso(value: Any) -> str | None:
    timestamp = _to_float(value)
    if timestamp is None:
        return str(value).strip() if value not in (None, "") else None
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
    except Exception:
        return str(value).strip()


def _first_last_price_row(value: Any) -> dict[str, Any] | None:
    if isinstance(value, list) and value:
        first = value[0]
        return first if isinstance(first, dict) else None
    if isinstance(value, dict):
        return value
    return None


def parse_oilprice_last_json_quote(
    *,
    quote_key: str,
    name: str,
    json_text: str,
    source_url: str,
    data_source_url: str | None = None,
    metadata_json_text: str | None = None,
) -> EnergyQuote:
    """
    Parse OilPrice's dynamic widget JSON.

    OilPrice's HTML table can contain an old server-rendered snapshot. The page then
    refreshes rows client-side from:
      https://s3.amazonaws.com/oilprice.com/widgets/oilprices/all/last.json

    This parser reads that dynamic JSON first, while optionally using
    blend_cache.json only for labels/source/delay metadata.
    """
    blend_id = OILPRICE_LAST_JSON_ID_MAP.get(quote_key)
    if not blend_id:
        raise EnergyParseError(f"Unsupported OilPrice last.json quote_key: {quote_key}")

    try:
        payload = json.loads(json_text or "{}")
    except Exception as exc:
        raise EnergyParseError(f"Could not decode OilPrice last.json: {exc}") from exc

    if not isinstance(payload, dict):
        raise EnergyParseError("OilPrice last.json root is not a JSON object")

    raw_quote = payload.get(blend_id)
    if not isinstance(raw_quote, dict):
        raise EnergyParseError(f"OilPrice last.json missing blend id {blend_id} for {quote_key}")

    price = _to_float(raw_quote.get("price"))
    change = _to_float(raw_quote.get("change"))
    change_percent = _to_float(raw_quote.get("change_percent"))
    timestamp_text = _timestamp_to_iso(raw_quote.get("time"))

    if price is None:
        raise EnergyParseError(f"OilPrice last.json blend id {blend_id} has no usable price")

    metadata: dict[str, Any] = {}
    if metadata_json_text:
        try:
            metadata_payload = json.loads(metadata_json_text)
            if isinstance(metadata_payload, dict) and isinstance(metadata_payload.get(blend_id), dict):
                metadata = metadata_payload[blend_id]
        except Exception:
            metadata = {}

    metadata_last_price = _first_last_price_row(metadata.get("last_price")) if metadata else None
    delay_note = None
    if metadata:
        delay_note = str(metadata.get("update_text") or "").strip() or None

    return EnergyQuote(
        quote_key=quote_key,
        name=name,
        price=price,
        unit=UNIT_MAP.get(quote_key),
        change=change,
        change_percent=change_percent,
        timestamp_text=timestamp_text,
        source_name="OilPrice.com",
        source_url=source_url,
        provider_used="oilprice_last_json",
        is_delayed=True if delay_note else None,
        delay_note=delay_note,
        extra={
            "oilprice_blend_id": blend_id,
            "oilprice_time": raw_quote.get("time"),
            "data_source_url": data_source_url or source_url,
            "metadata_source_url": "https://s3.amazonaws.com/oilprice.com/oilprices/blend_cache.json" if metadata else None,
            "metadata_blend_name": metadata.get("blend_name") if metadata else None,
            "metadata_spreadsheet_name": metadata.get("spreadsheet_name") if metadata else None,
            "metadata_source": metadata.get("source") if metadata else None,
            "metadata_update_text": delay_note,
            "metadata_last_price_timestamp": metadata.get("last_price_timestamp") if metadata else None,
            "metadata_last_price": metadata_last_price,
            "parsed_from": "oilprice_last_json",
        },
    )

def parse_oilprice_charts_quote(
    *,
    quote_key: str,
    name: str,
    html: str,
    source_url: str,
) -> EnergyQuote:
    display_name = DISPLAY_NAME_MAP.get(quote_key)
    if not display_name:
        raise EnergyParseError(f"Unsupported oilprice quote_key: {quote_key}")

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.I | re.S)
    if not rows:
        raise EnergyParseError("OilPrice charts page has no table rows")

    target_row = None
    matched_row_name = None

    for row_html in rows:
        row_text = re.sub(r"<[^>]+>", " ", row_html)
        row_text = re.sub(r"\s+", " ", row_text).strip()

        if display_name.lower() in row_text.lower():
            target_row = row_html
            matched_row_name = display_name
            break

        # small tolerance for gasoline labels
        if quote_key == "gasoline" and "gasoline" in row_text.lower():
            target_row = row_html
            matched_row_name = row_text[:80]
            break

    if not target_row:
        raise EnergyParseError(f"Could not find OilPrice row for {quote_key}")

    price_match = re.search(r"class=['\"]last_price['\"][^>]*data-price=['\"]([^'\"]+)['\"]", target_row, flags=re.I)
    if not price_match:
        price_match = re.search(r"class=['\"]last_price['\"][^>]*>([^<]+)<", target_row, flags=re.I)

    change_match = re.search(r"class=['\"][^'\"]*flat_change_cell[^'\"]*['\"][^>]*>([^<]+)<", target_row, flags=re.I)
    pct_match = re.search(r"class=['\"][^'\"]*percent_change_cell[^'\"]*['\"][^>]*>([^<%]+)%", target_row, flags=re.I)
    delay_match = re.search(r"class=['\"][^'\"]*blend_update_text[^'\"]*['\"][^>]*>\(([^)]+)\)", target_row, flags=re.I)

    price = _to_float(price_match.group(1)) if price_match else None
    change = _to_float(change_match.group(1)) if change_match else None
    change_percent = _to_float(pct_match.group(1)) if pct_match else None
    delay_note = delay_match.group(1).strip() if delay_match else None

    if price is None:
        cleaned = re.sub(r"<[^>]+>", " ", target_row)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        raise EnergyParseError(f"OilPrice row found for {quote_key} but price parse failed: {cleaned[:240]}")

    return EnergyQuote(
        quote_key=quote_key,
        name=name,
        price=price,
        unit=UNIT_MAP.get(quote_key),
        change=change,
        change_percent=change_percent,
        timestamp_text=None,
        source_name="OilPrice.com",
        source_url=source_url,
        provider_used="oilprice_charts",
        is_delayed=True,
        delay_note=delay_note or "Delayed on page.",
        extra={
            "matched_name": matched_row_name,
            "parsed_from": "oilprice_table_row",
        },
    )


def _flatten_group_text(rows: list[dict]) -> str:
    parts: list[str] = []
    for row in rows:
        for k, v in row.items():
            parts.append(str(k))
            if v is not None:
                parts.append(str(v))
    return " ".join(parts).lower()


def _score_crude_group(rows: list[dict]) -> int:
    text = _flatten_group_text(rows)
    score = 0

    if "crude oil" in text:
        score += 100
    if "原油" in text:
        score += 100

    if re.search(r"\bsc\b", text):
        score += 120
    if re.search(r"\bsc\d{3,}\b", text):
        score += 120

    if "scfis" in text:
        score -= 50

    for row in rows:
        for field_name in (
            "PRODUCTGROUPID",
            "PRODUCTID",
            "INSTRUMENTID",
            "PRODUCTNAME",
            "PRODUCTGROUPNAME",
            "PRODUCTGROUPENGNAME",
            "PRODUCTENGNAME",
        ):
            val = str(row.get(field_name, "") or "").strip().lower()
            if not val:
                continue

            if val == "sc":
                score += 200
            elif re.fullmatch(r"sc\d{3,}", val):
                score += 180
            elif "crude" in val:
                score += 120
            elif "oil" in val:
                score += 40
            elif "原油" in val:
                score += 120
            elif "scfis" in val:
                score -= 80

    return score


def _format_date_yyyymmdd(date_str: str) -> str:
    if isinstance(date_str, str) and re.fullmatch(r"\d{8}", date_str):
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return str(date_str)


def _extract_time_show(time_item: Any) -> str | None:
    if isinstance(time_item, dict):
        for key in ("TIMESHOW", "TIME_SHOW", "timeShow", "timeshow"):
            value = time_item.get(key)
            if value:
                return str(value).strip()
        return None

    if isinstance(time_item, str):
        value = time_item.strip()
        return value or None

    return None




def _normalize_ine_html_text(html: str) -> str:
    text = html or ""
    text = text.replace("：", ":")
    text = text.replace("−", "-")
    text = text.replace("—", "-")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()




def _extract_signed_number(value: str | None) -> float | None:
    if not value:
        return None
    match = re.search(r"([+-]?\d+(?:\.\d+)?)", str(value))
    if not match:
        return None
    try:
        return float(match.group(1))
    except Exception:
        return None



def _normalize_visible_text(html: str) -> str:
    text = html_lib.unescape(html or "")
    text = text.replace("\xa0", " ")
    text = text.replace("：", ":")
    text = text.replace("−", "-")
    text = text.replace("—", "-")
    text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_sina_sc0_visible_quote(text: str) -> tuple[float | None, float | None, float | None]:
    compact = _normalize_visible_text(text)

    patterns = [
        re.compile(r"(?:上海)?原油(?:连续)?\s*([0-9]+(?:\.[0-9]+)?)\s*([+-][0-9]+(?:\.[0-9]+)?)%\s*SC0", re.I),
        re.compile(r"SC0\s*(?:APP下载\s*)?(?:上海)?原油(?:连续)?\s*([0-9]+(?:\.[0-9]+)?)\s*([+-][0-9]+(?:\.[0-9]+)?)%", re.I),
        re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*([+-][0-9]+(?:\.[0-9]+)?)%\s*SC0", re.I),
        re.compile(r"(?:上海)?原油(?:连续)?\s*([0-9]+(?:\.[0-9]+)?)", re.I),
    ]

    for pattern in patterns:
        match = pattern.search(compact)
        if not match:
            continue
        price = _to_float(match.group(1))
        change_percent = _to_float(match.group(2)) if match.lastindex and match.lastindex >= 2 else None
        if price is not None:
            return price, None, change_percent

    return None, None, None

def _looks_like_timestamp_token(value: str) -> bool:
    value = str(value or '').strip()
    if not value:
        return False
    if re.search(r'\d{4}-\d{2}-\d{2}', value):
        return True
    if re.search(r'\d{2}:\d{2}(?::\d{2})?', value):
        return True
    if re.fullmatch(r'\d{8}', value):
        return True
    return False


def _pick_sina_price_change(parts: list[str]) -> tuple[float | None, float | None, float | None, str | None]:
    timestamp_text = None
    numeric_tokens: list[tuple[float, str]] = []
    pct_candidates: list[float] = []

    for raw in parts:
        token = str(raw or '').strip()
        if not token:
            continue
        if _looks_like_timestamp_token(token):
            timestamp_text = timestamp_text or token
            continue

        if '%' in token:
            pct = _extract_signed_number(token)
            if pct is not None:
                pct_candidates.append(pct)
            continue

        if re.fullmatch(r'[A-Za-z_]+\d+[A-Za-z_]*', token):
            continue

        num = _extract_signed_number(token)
        if num is None:
            continue
        numeric_tokens.append((num, token))

    price = None
    change = None
    change_percent = pct_candidates[0] if pct_candidates else None

    for num, token in numeric_tokens:
        if token.startswith('+') or token.startswith('-'):
            continue
        if 50 <= num <= 2000:
            price = num
            break
    if price is None and numeric_tokens:
        price = numeric_tokens[0][0]

    if price is not None:
        for num, token in numeric_tokens:
            if token.startswith('+') or token.startswith('-'):
                if change is None and abs(num) <= 300:
                    change = num
                elif change_percent is None and abs(num) <= 30:
                    change_percent = num
                continue

            if num == price:
                continue
            if change is None and abs(num) <= 300:
                change = num
                continue
            if change_percent is None and abs(num) <= 30:
                change_percent = num

    return price, change, change_percent, timestamp_text


def parse_sina_hq_quote(
    *,
    quote_key: str,
    name: str,
    html: str,
    source_url: str,
) -> EnergyQuote:
    text = html or ''
    match = re.search(r'var\s+hq_str_(?:nf_)?SC0\s*=\s*"([^"]*)"', text, flags=re.I)
    if not match:
        match = re.search(r'hq_str_(?:nf_)?SC0\s*=\s*"([^"]*)"', text, flags=re.I)
    if not match:
        raise EnergyParseError('Could not find hq_str for Sina SC0 quote response')

    raw = match.group(1)
    parts = [part.strip() for part in raw.split(',')]

    time_token = parts[1] if len(parts) > 1 else None
    open_price = _to_float(parts[2]) if len(parts) > 2 else None
    high_price = _to_float(parts[3]) if len(parts) > 3 else None
    low_price = _to_float(parts[4]) if len(parts) > 4 else None
    bid_price = _to_float(parts[6]) if len(parts) > 6 else None
    ask_price = _to_float(parts[7]) if len(parts) > 7 else None
    last_price = _to_float(parts[8]) if len(parts) > 8 else None
    settlement_price = _to_float(parts[9]) if len(parts) > 9 else None
    prev_settlement = _to_float(parts[10]) if len(parts) > 10 else None

    price = last_price
    if price is None:
        price = bid_price
    if price is None:
        price = ask_price
    if price is None:
        price, _, _, _ = _pick_sina_price_change(parts)
    if price is None:
        raise EnergyParseError('Found Sina SC0 quote response but could not determine latest price')

    change = None
    change_percent = None
    if prev_settlement not in (None, 0):
        change = round(price - prev_settlement, 3)
        change_percent = round((change / prev_settlement) * 100, 2)
    elif settlement_price not in (None, 0):
        change = round(price - settlement_price, 3)
        change_percent = round((change / settlement_price) * 100, 2)

    timestamp_text = None
    if isinstance(time_token, str) and re.fullmatch(r'\d{6}', time_token):
        timestamp_text = f"{time_token[:2]}:{time_token[2:4]}:{time_token[4:6]}"
    elif isinstance(time_token, str) and time_token.strip():
        timestamp_text = time_token.strip()

    return EnergyQuote(
        quote_key=quote_key,
        name=name,
        price=price,
        unit=UNIT_MAP.get(quote_key),
        change=change,
        change_percent=change_percent,
        timestamp_text=timestamp_text,
        source_name='Sina Futures',
        source_url=source_url,
        provider_used='sina_hq_direct',
        is_delayed=True,
        delay_note='Parsed from direct Sina HQ quote response.',
        extra={
            'parsed_from': 'sina_hq_direct',
            'raw_parts': parts[:16],
            'open_price': open_price,
            'high_price': high_price,
            'low_price': low_price,
            'bid_price': bid_price,
            'ask_price': ask_price,
            'last_price': last_price,
            'settlement_price': settlement_price,
            'prev_settlement': prev_settlement,
        },
    )


def parse_sina_china_crude_quote(
    *,
    quote_key: str,
    name: str,
    html: str,
    source_url: str,
) -> EnergyQuote:
    text = html or ""

    variable_patterns = [
        r'var\s+hq_str_(?:nf_)?sc0\s*=\s*"([^"]+)"',
        r'hq_str_(?:nf_)?sc0\s*=\s*"([^"]+)"',
    ]
    for var_pattern in variable_patterns:
        match = re.search(var_pattern, text, flags=re.I)
        if not match:
            continue

        raw = match.group(1)
        parts = [part.strip() for part in raw.split(',') if part.strip()]
        price = None
        change = None
        change_percent = None
        timestamp_text = None

        numeric_candidates = [(_extract_signed_number(part), part) for part in parts]
        numeric_only = [num for num, _ in numeric_candidates if num is not None]
        if numeric_only:
            price = numeric_only[0]
        if len(numeric_only) >= 2:
            change = numeric_only[1]
        if len(numeric_only) >= 3:
            change_percent = numeric_only[2]

        for part in parts:
            if re.search(r"\d{2}:\d{2}:\d{2}", part) or re.search(r"\d{4}-\d{2}-\d{2}", part):
                timestamp_text = part
                break

        if price is not None:
            return EnergyQuote(
                quote_key=quote_key,
                name=name,
                price=price,
                unit=UNIT_MAP.get(quote_key),
                change=change,
                change_percent=change_percent,
                timestamp_text=timestamp_text,
                source_name="Sina Futures",
                source_url=source_url,
                provider_used="sina_nf_html",
                is_delayed=True,
                delay_note="Parsed from Sina futures mobile quote page.",
                extra={"parsed_from": "hq_str_sc0", "raw_parts": parts[:12]},
            )

    compact = re.sub(r"\s+", " ", text)
    label_patterns = {
        "price": [
            r"(?:最新价|最新|现价|价格)[:：]?\s*<[^>]*>\s*([+-]?\d+(?:\.\d+)?)",
            r"(?:最新价|最新|现价|价格)[:：]?\s*([+-]?\d+(?:\.\d+)?)",
        ],
        "change": [r"(?:涨跌额|涨跌)[:：]?\s*([+-]?\d+(?:\.\d+)?)"],
        "change_percent": [r"(?:涨跌幅|幅度)[:：]?\s*([+-]?\d+(?:\.\d+)?)%"],
        "timestamp": [r"(?:更新时间|时间)[:：]?\s*([0-9:\- ]{8,})"],
    }

    extracted: dict[str, str] = {}
    for key, patterns in label_patterns.items():
        for fallback_pattern in patterns:
            match = re.search(fallback_pattern, compact, flags=re.I)
            if match:
                extracted[key] = match.group(1).strip()
                break

    if "price" not in extracted:
        json_patterns = [
            ("price", r'"(?:price|last|lastPrice|trade|newPrice)"\s*:\s*"?([+-]?\d+(?:\.\d+)?)"?'),
            ("change", r'"(?:change|updown|zd)"\s*:\s*"?([+-]?\d+(?:\.\d+)?)"?'),
            ("change_percent", r'"(?:changepercent|percent|changePercent|zdf)"\s*:\s*"?([+-]?\d+(?:\.\d+)?)"?'),
        ]
        for key, json_pattern in json_patterns:
            match = re.search(json_pattern, text, flags=re.I)
            if match:
                extracted.setdefault(key, match.group(1).strip())

    price = _to_float(extracted.get("price"))
    change = _to_float(extracted.get("change"))
    change_percent = _to_float(extracted.get("change_percent"))
    timestamp_text = extracted.get("timestamp")

    if price is None:
        price, change, change_percent = _extract_sina_sc0_visible_quote(text)

    if price is None:
        raise EnergyParseError("Could not parse Sina China crude quote page")

    return EnergyQuote(
        quote_key=quote_key,
        name=name,
        price=price,
        unit=UNIT_MAP.get(quote_key),
        change=change,
        change_percent=change_percent,
        timestamp_text=timestamp_text,
        source_name="Sina Futures",
        source_url=source_url,
        provider_used="sina_nf_html",
        is_delayed=True,
        delay_note="Parsed from Sina futures mobile quote page.",
        extra={"parsed_from": "label_json_or_visible_text_fallback"},
    )


def parse_ine_homepage_quote(
    *,
    quote_key: str,
    name: str,
    html: str,
    source_url: str,
) -> EnergyQuote:
    # backward-compatible alias for earlier pipeline code
    return parse_sina_china_crude_quote(
        quote_key=quote_key,
        name=name,
        html=html,
        source_url=source_url,
    )
def parse_ine_markerprice_json(
    *,
    quote_key: str,
    name: str,
    data: dict,
    source_url: str,
    date_used: str,
) -> EnergyQuote:
    rows = data.get("o_curMarkerPrice")
    times = data.get("o_curMarkerTime")

    if not isinstance(rows, list) or not rows:
        raise EnergyParseError("INE markerprice JSON missing o_curMarkerPrice")

    if not isinstance(times, list):
        times = []

    groups: list[list[dict]] = []
    for i in range(0, len(rows), 3):
        group = rows[i:i + 3]
        if group:
            groups.append(group)

    if not groups:
        raise EnergyParseError("INE markerprice JSON contains no grouped rows")

    best_group = None
    best_score = -10**9

    for group in groups:
        if not all(isinstance(x, dict) for x in group):
            continue
        score = _score_crude_group(group)
        if score > best_score:
            best_score = score
            best_group = group

    if not best_group or best_score < 50:
        raise EnergyParseError("Could not identify crude oil group in INE markerprice JSON")

    chosen_idx = None
    chosen_row = None

    for idx in range(len(best_group) - 1, -1, -1):
        row = best_group[idx]
        nvlflag = row.get("NVLFLAG")
        price = _to_float(row.get("MARKERPRICE"))
        if price is not None and str(nvlflag) in {"1", "1.0", "True", "true"}:
            chosen_idx = idx
            chosen_row = row
            break

    if chosen_row is None:
        for idx in range(len(best_group) - 1, -1, -1):
            row = best_group[idx]
            price = _to_float(row.get("MARKERPRICE"))
            if price is not None:
                chosen_idx = idx
                chosen_row = row
                break

    if chosen_row is None:
        raise EnergyParseError("INE crude group found but no valid MARKERPRICE")

    price = _to_float(chosen_row.get("MARKERPRICE"))
    change = _to_float(chosen_row.get("CHG"))

    time_text = None
    if chosen_idx is not None and 0 <= chosen_idx < len(times):
        time_text = _extract_time_show(times[chosen_idx])
    if not time_text and times:
        time_text = _extract_time_show(times[-1])

    formatted_date = _format_date_yyyymmdd(date_used)
    timestamp_text = f"{formatted_date} {time_text}".strip() if time_text else formatted_date

    product_group_id = str(chosen_row.get("PRODUCTGROUPID", "") or "")
    product_id = str(chosen_row.get("PRODUCTID", "") or "")
    instrument_id = str(chosen_row.get("INSTRUMENTID", "") or "")
    precision = chosen_row.get("precision")

    return EnergyQuote(
        quote_key=quote_key,
        name=name,
        price=price,
        unit="CNY/bbl",
        change=change,
        change_percent=None,
        timestamp_text=timestamp_text,
        source_name="Shanghai International Energy Exchange",
        source_url=source_url,
        provider_used="ine_markerprice",
        is_delayed=False,
        delay_note="Official INE daily marker price file, not an intraday live tick feed.",
        extra={
            "date_used": date_used,
            "product_group_id": product_group_id,
            "product_id": product_id,
            "instrument_id": instrument_id,
            "precision": precision,
            "available_times": times,
        },
    )
