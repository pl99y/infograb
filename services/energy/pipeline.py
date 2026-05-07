from __future__ import annotations

import logging

from collectors.energy_fetcher import fetch_energy_page
from collectors.common import utc_now_iso
from parsers.energy_parser import (
    parse_oilprice_charts_quote,
    parse_oilprice_last_json_quote,
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
OILPRICE_LAST_JSON_URL = "https://s3.amazonaws.com/oilprice.com/widgets/oilprices/all/last.json"
OILPRICE_BLEND_CACHE_URL = "https://s3.amazonaws.com/oilprice.com/oilprices/blend_cache.json"
SINA_CHINA_CRUDE_PAGE_URL = "https://gu.sina.cn/ft/hq/nf.php?symbol=sc0"
SINA_CHINA_CRUDE_QUOTE_URL = "https://hq.sinajs.cn/list=nf_SC0"

ENERGY_SOURCES = [
    {"quote_key": "wti", "name": "WTI Crude", "kind": "oilprice_last_json", "url": OILPRICE_LAST_JSON_URL, "display_url": OILPRICE_CHARTS_URL, "metadata_url": OILPRICE_BLEND_CACHE_URL, "fallback_url": OILPRICE_CHARTS_URL},
    {"quote_key": "brent", "name": "Brent Crude", "kind": "oilprice_last_json", "url": OILPRICE_LAST_JSON_URL, "display_url": OILPRICE_CHARTS_URL, "metadata_url": OILPRICE_BLEND_CACHE_URL, "fallback_url": OILPRICE_CHARTS_URL},
    {"quote_key": "murban", "name": "Murban Crude", "kind": "oilprice_last_json", "url": OILPRICE_LAST_JSON_URL, "display_url": OILPRICE_CHARTS_URL, "metadata_url": OILPRICE_BLEND_CACHE_URL, "fallback_url": OILPRICE_CHARTS_URL},
    {"quote_key": "natural_gas", "name": "Natural Gas", "kind": "oilprice_last_json", "url": OILPRICE_LAST_JSON_URL, "display_url": OILPRICE_CHARTS_URL, "metadata_url": OILPRICE_BLEND_CACHE_URL, "fallback_url": OILPRICE_CHARTS_URL},
    {"quote_key": "gasoline", "name": "Gasoline (RBOB)", "kind": "oilprice_last_json", "url": OILPRICE_LAST_JSON_URL, "display_url": OILPRICE_CHARTS_URL, "metadata_url": OILPRICE_BLEND_CACHE_URL, "fallback_url": OILPRICE_CHARTS_URL},
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
    if source_def["kind"] == "oilprice_last_json":
        return parse_oilprice_last_json_quote(
            quote_key=source_def["quote_key"],
            name=source_def["name"],
            json_text=html,
            source_url=source_def.get("display_url") or OILPRICE_CHARTS_URL,
            data_source_url=source_def["url"],
            metadata_json_text=source_def.get("_metadata_json"),
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


def _fetch_oilprice_html_fallback(*, cache: dict) -> tuple[str | None, str | None, str | None]:
    if cache.get("html") is not None or cache.get("html_error") is not None:
        return cache.get("html"), cache.get("html_fetched_at"), cache.get("html_error")

    result = fetch_energy_page(source_name="OilPrice Charts", url=OILPRICE_CHARTS_URL)
    if result.success:
        cache["html"] = result.html
        cache["html_fetched_at"] = result.fetched_at
        cache["html_error"] = None
    else:
        cache["html"] = None
        cache["html_fetched_at"] = result.fetched_at
        cache["html_error"] = result.error_message or "Unknown OilPrice HTML fetch error"
    return cache.get("html"), cache.get("html_fetched_at"), cache.get("html_error")


def _parse_oilprice_html_fallback(source_def: dict, html: str):
    return parse_oilprice_charts_quote(
        quote_key=source_def["quote_key"],
        name=source_def["name"],
        html=html,
        source_url=source_def.get("display_url") or OILPRICE_CHARTS_URL,
    )


def fetch_energy_once(db_path: str = "app.db", keys: list[str] | None = None) -> dict:
    results = {"sources_processed": 0, "quotes_saved": 0, "errors": []}

    shared_oilprice_last_json = None
    shared_oilprice_last_fetched_at = None
    shared_oilprice_last_error = None
    shared_oilprice_metadata_json = None
    shared_oilprice_metadata_attempted = False
    oilprice_html_cache: dict = {}

    wanted_sources = _select_sources(keys)

    for source_def in wanted_sources:
        fetched_at = utc_now_iso()

        if source_def["kind"] == "oilprice_last_json":
            if shared_oilprice_last_json is None and shared_oilprice_last_error is None:
                primary_result = fetch_energy_page(source_name="OilPrice Last JSON", url=OILPRICE_LAST_JSON_URL)
                if primary_result.success:
                    shared_oilprice_last_json = primary_result.html
                    shared_oilprice_last_fetched_at = primary_result.fetched_at
                else:
                    shared_oilprice_last_error = primary_result.error_message or "Unknown OilPrice last.json fetch error"
                    shared_oilprice_last_fetched_at = primary_result.fetched_at

            if not shared_oilprice_metadata_attempted:
                metadata_result = fetch_energy_page(source_name="OilPrice Blend Cache", url=OILPRICE_BLEND_CACHE_URL)
                if metadata_result.success:
                    shared_oilprice_metadata_json = metadata_result.html
                else:
                    logger.warning("OilPrice blend metadata fetch failed: %s", metadata_result.error_message)
                shared_oilprice_metadata_attempted = True

            try:
                if not shared_oilprice_last_json:
                    raise RuntimeError(shared_oilprice_last_error or "OilPrice last.json unavailable")

                source_def_with_metadata = dict(source_def)
                source_def_with_metadata["_metadata_json"] = shared_oilprice_metadata_json
                quote = _parse_energy_source(source_def_with_metadata, shared_oilprice_last_json, source_def["url"])
                _insert_quote_and_log(
                    db_path=db_path,
                    source_def=source_def,
                    fetched_at=shared_oilprice_last_fetched_at or fetched_at,
                    quote=quote,
                )
                results["sources_processed"] += 1
                results["quotes_saved"] += 1
                continue
            except Exception as primary_exc:
                logger.warning("OilPrice last.json parse failed for %s; falling back to HTML: %s", source_def["name"], primary_exc)
                html, html_fetched_at, html_error = _fetch_oilprice_html_fallback(cache=oilprice_html_cache)
                if not html:
                    error_message = f"last.json failed: {primary_exc}; HTML fallback failed: {html_error}"
                    results["errors"].append(f"{source_def['name']}: {error_message}")
                    _record_failure(
                        db_path=db_path,
                        source_name=source_def["name"],
                        started_at=shared_oilprice_last_fetched_at or html_fetched_at or fetched_at,
                        error_message=error_message,
                    )
                    continue

                try:
                    quote = _parse_oilprice_html_fallback(source_def, html)
                    quote.extra = dict(quote.extra or {})
                    quote.extra["fallback_reason"] = str(primary_exc)
                    _insert_quote_and_log(
                        db_path=db_path,
                        source_def=source_def,
                        fetched_at=html_fetched_at or fetched_at,
                        quote=quote,
                    )
                    results["sources_processed"] += 1
                    results["quotes_saved"] += 1
                    continue
                except Exception as fallback_exc:
                    error_message = f"last.json failed: {primary_exc}; HTML fallback parse failed: {fallback_exc}"
                    logger.error("Energy parse/save failed for %s: %s", source_def["name"], error_message)
                    results["errors"].append(f"{source_def['name']}: {error_message}")
                    _record_failure(
                        db_path=db_path,
                        source_name=source_def["name"],
                        started_at=html_fetched_at or fetched_at,
                        error_message=error_message,
                    )
                    continue

        if source_def["kind"] == "oilprice_charts":
            html, html_fetched_at, html_error = _fetch_oilprice_html_fallback(cache=oilprice_html_cache)
            if not html:
                error_message = html_error or "Unknown OilPrice HTML fetch error"
                results["errors"].append(f"{source_def['name']}: {error_message}")
                _record_failure(db_path=db_path, source_name=source_def["name"], started_at=html_fetched_at or fetched_at, error_message=error_message)
                continue

            try:
                quote = _parse_oilprice_html_fallback(source_def, html)
                _insert_quote_and_log(db_path=db_path, source_def=source_def, fetched_at=html_fetched_at or fetched_at, quote=quote)
                results["sources_processed"] += 1
                results["quotes_saved"] += 1
                continue
            except Exception as exc:
                error_message = str(exc)
                logger.error("Energy parse/save failed for %s: %s", source_def["name"], error_message)
                results["errors"].append(f"{source_def['name']}: {error_message}")
                _record_failure(db_path=db_path, source_name=source_def["name"], started_at=html_fetched_at or fetched_at, error_message=error_message)
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
