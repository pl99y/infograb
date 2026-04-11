from __future__ import annotations

import logging

from collectors.energy_fetcher import fetch_energy_page
from collectors.common import utc_now_iso
from parsers.energy_parser import (
    parse_oilprice_charts_quote,
    parse_sina_china_crude_quote,
    parse_sina_hq_quote,
    parse_tradingview_quote,
)
from storage import (
    get_connection,
    insert_energy_quote,
    record_fetch_run,
    upsert_source,
)

logger = logging.getLogger(__name__)

OILPRICE_CHARTS_URL = "https://oilprice.com/oil-price-charts/"
SINA_CHINA_CRUDE_PAGE_URL = "https://gu.sina.cn/ft/hq/nf.php?symbol=sc0"
SINA_CHINA_CRUDE_QUOTE_URL = "https://hq.sinajs.cn/list=nf_SC0"

ENERGY_SOURCES = [
    {"quote_key": "wti", "name": "WTI Crude", "kind": "oilprice_charts", "url": OILPRICE_CHARTS_URL, "fallback_urls": []},
    {"quote_key": "brent", "name": "Brent Crude", "kind": "oilprice_charts", "url": OILPRICE_CHARTS_URL, "fallback_urls": []},
    {"quote_key": "murban", "name": "Murban Crude", "kind": "oilprice_charts", "url": OILPRICE_CHARTS_URL, "fallback_urls": []},
    {"quote_key": "natural_gas", "name": "Natural Gas", "kind": "oilprice_charts", "url": OILPRICE_CHARTS_URL, "fallback_urls": []},
    {"quote_key": "gasoline", "name": "Gasoline (RBOB)", "kind": "oilprice_charts", "url": OILPRICE_CHARTS_URL, "fallback_urls": []},
    {"quote_key": "china", "name": "China Crude Oil Futures (INE SC)", "kind": "sina_hq", "url": SINA_CHINA_CRUDE_QUOTE_URL, "display_url": SINA_CHINA_CRUDE_PAGE_URL, "fallback_urls": []},
]


def _select_sources(keys: list[str] | None = None) -> list[dict]:
    if not keys:
        return list(ENERGY_SOURCES)
    wanted = set(keys)
    return [item for item in ENERGY_SOURCES if item["quote_key"] in wanted]


def _insert_quote_and_log(*, db_path: str, source_def: dict, fetched_at: str, quote) -> None:
    con = get_connection(db_path)
    try:
        source_id = upsert_source(
            con=con,
            name=source_def["name"],
            source_type="energy",
            url=source_def["url"],
            enabled=True,
        )

        insert_energy_quote(
            con=con,
            quote_key=quote.quote_key,
            name=quote.name,
            price=quote.price,
            unit=quote.unit,
            change=quote.change,
            change_percent=quote.change_percent,
            timestamp_text=quote.timestamp_text,
            source_name=quote.source_name,
            source_url=quote.source_url,
            provider_used=quote.provider_used,
            is_delayed=quote.is_delayed,
            delay_note=quote.delay_note,
            extra=quote.extra,
            fetched_at=fetched_at,
        )

        record_fetch_run(
            con=con,
            source_id=source_id,
            source_name=f"Energy/{source_def['name']}",
            started_at=fetched_at,
            finished_at=fetched_at,
            status="success",
            items_found=1,
            new_items=1,
            error_message="",
        )
        con.commit()
    finally:
        con.close()


def _record_failure(*, db_path: str, source_name: str, started_at: str, error_message: str) -> None:
    con = get_connection(db_path)
    try:
        record_fetch_run(
            con=con,
            source_id=None,
            source_name=f"Energy/{source_name}",
            started_at=started_at,
            finished_at=started_at,
            status="failed",
            items_found=0,
            new_items=0,
            error_message=error_message,
        )
        con.commit()
    finally:
        con.close()


def _parse_energy_source(source_def: dict, html: str, source_url: str):
    if source_def["kind"] == "tradingview":
        return parse_tradingview_quote(
            quote_key=source_def["quote_key"],
            name=source_def["name"],
            html=html,
            source_url=source_url,
        )
    if source_def["kind"] == "oilprice_charts":
        return parse_oilprice_charts_quote(
            quote_key=source_def["quote_key"],
            name=source_def["name"],
            html=html,
            source_url=source_url,
        )
    if source_def["kind"] == "sina_hq":
        return parse_sina_hq_quote(
            quote_key=source_def["quote_key"],
            name=source_def["name"],
            html=html,
            source_url=source_def.get("display_url") or source_url,
        )
    if source_def["kind"] == "sina_nf_html":
        return parse_sina_china_crude_quote(
            quote_key=source_def["quote_key"],
            name=source_def["name"],
            html=html,
            source_url=source_url,
        )
    raise RuntimeError(f"Unknown HTML energy source kind: {source_def['kind']}")


def fetch_energy_once(db_path: str = "app.db", keys: list[str] | None = None) -> dict:
    results = {"sources_processed": 0, "quotes_saved": 0, "errors": []}

    shared_oilprice_html = None
    shared_oilprice_fetched_at = None
    wanted_sources = _select_sources(keys)

    for source_def in wanted_sources:
        fetched_at = utc_now_iso()

        if source_def["kind"] == "oilprice_charts":
            if shared_oilprice_html is None:
                primary_result = fetch_energy_page(source_name="OilPrice Charts", url=OILPRICE_CHARTS_URL)
                if not primary_result.success:
                    error_message = primary_result.error_message or "Unknown fetch error"
                    for s in [x for x in wanted_sources if x["kind"] == "oilprice_charts"]:
                        results["errors"].append(f"{s['name']}: {error_message}")
                        _record_failure(db_path=db_path, source_name=s["name"], started_at=primary_result.fetched_at, error_message=error_message)
                    return results
                shared_oilprice_html = primary_result.html
                shared_oilprice_fetched_at = primary_result.fetched_at

            try:
                quote = _parse_energy_source(source_def, shared_oilprice_html, source_def["url"])
                _insert_quote_and_log(db_path=db_path, source_def=source_def, fetched_at=shared_oilprice_fetched_at or fetched_at, quote=quote)
                results["sources_processed"] += 1
                results["quotes_saved"] += 1
                continue
            except Exception as exc:
                error_message = str(exc)
                logger.error("Energy parse/save failed for %s: %s", source_def["name"], error_message)
                results["errors"].append(f"{source_def['name']}: {error_message}")
                _record_failure(db_path=db_path, source_name=source_def["name"], started_at=shared_oilprice_fetched_at or fetched_at, error_message=error_message)
                continue

        primary_result = fetch_energy_page(source_name=source_def["name"], url=source_def["url"])
        if not primary_result.success:
            error_message = primary_result.error_message or "Unknown fetch error"
            logger.error("Energy fetch failed for %s: %s", source_def["name"], error_message)
            results["errors"].append(f"{source_def['name']}: {error_message}")
            _record_failure(db_path=db_path, source_name=source_def["name"], started_at=primary_result.fetched_at, error_message=error_message)
            continue

        try:
            quote = _parse_energy_source(source_def, primary_result.html, source_def["url"])
        except Exception as exc:
            error_message = str(exc)
            logger.error("Energy parse/save failed for %s: %s", source_def["name"], error_message)
            results["errors"].append(f"{source_def['name']}: {error_message}")
            _record_failure(db_path=db_path, source_name=source_def["name"], started_at=primary_result.fetched_at, error_message=error_message)
            continue

        _insert_quote_and_log(db_path=db_path, source_def=source_def, fetched_at=primary_result.fetched_at, quote=quote)
        results["sources_processed"] += 1
        results["quotes_saved"] += 1

    return results
