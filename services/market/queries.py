"""
Market snapshot service.

- Live fetch helpers use Yahoo Finance / Eastmoney adapters directly.
- Latest query helpers read the most recent successful snapshot per index from SQLite.
"""
from __future__ import annotations

import logging

from collectors.market_snapshot_fetcher import (
    EastmoneyMarketSnapshotAdapter,
    MarketSnapshotSpec,
    YahooMarketSnapshotAdapter,
)
from storage import get_connection

logger = logging.getLogger(__name__)


MARKET_SNAPSHOT_SPECS: list[MarketSnapshotSpec] = [
    MarketSnapshotSpec(
        key="nasdaq",
        name="NASDAQ Composite",
        symbol="^IXIC",
        provider="yahoo_finance",
    ),
    MarketSnapshotSpec(
        key="sp500",
        name="S&P 500",
        symbol="^GSPC",
        provider="yahoo_finance",
    ),
    MarketSnapshotSpec(
        key="dowjones",
        name="Dow Jones Industrial Average",
        symbol="^DJI",
        provider="yahoo_finance",
    ),
    MarketSnapshotSpec(
        key="a_share_sse",
        name="上证指数",
        symbol="000001",
        provider="eastmoney",
        secid="1.000001",
    ),
    MarketSnapshotSpec(
        key="nikkei225",
        name="Nikkei 225",
        symbol="^N225",
        provider="yahoo_finance",
    ),
    MarketSnapshotSpec(
        key="hangseng",
        name="Hang Seng Index",
        symbol="^HSI",
        provider="yahoo_finance",
    ),
]


_MARKET_SPEC_ORDER = {spec.key: idx for idx, spec in enumerate(MARKET_SNAPSHOT_SPECS)}


def _build_adapters(timeout: int = 15) -> dict[str, object]:
    return {
        "yahoo_finance": YahooMarketSnapshotAdapter(timeout=timeout),
        "eastmoney": EastmoneyMarketSnapshotAdapter(timeout=timeout),
    }


def get_market_snapshots_with_meta(timeout: int = 15) -> dict:
    """
    Fetch all configured market snapshots live.

    Returns:
        {
          "items": [...successful normalized snapshots...],
          "errors": [...failed items...]
        }

    Error handling strategy:
    - Each index is fetched independently.
    - If one index fails, the others still return normally.
    - Failed items are collected into the errors list.
    - get_market_snapshots() keeps the public output minimal by returning only items.
    """
    adapters = _build_adapters(timeout=timeout)
    items: list[dict] = []
    errors: list[dict] = []

    try:
        for spec in MARKET_SNAPSHOT_SPECS:
            adapter = adapters[spec.provider]
            try:
                item = adapter.fetch_one(spec)  # type: ignore[attr-defined]
                items.append(item)
            except Exception as exc:
                logger.error("Market snapshot fetch failed for %s: %s", spec.key, exc)
                errors.append(
                    {
                        "key": spec.key,
                        "name": spec.name,
                        "symbol": spec.symbol,
                        "source": spec.provider,
                        "error": str(exc),
                    }
                )
    finally:
        for adapter in adapters.values():
            try:
                adapter.close()  # type: ignore[attr-defined]
            except Exception:
                pass

    items.sort(key=lambda item: _MARKET_SPEC_ORDER.get(item.get("key"), 999))
    return {"items": items, "errors": errors}


def get_market_snapshots(timeout: int = 15) -> list[dict]:
    return get_market_snapshots_with_meta(timeout=timeout)["items"]


def get_market_snapshots_latest(db_path: str = "app.db") -> list[dict]:
    """
    Return the latest successful stored snapshot for each configured key.

    API-facing helper: includes fetched_at so the frontend can show panel update time.
    """
    con = get_connection(db_path)
    try:
        rows = con.execute(
            """
            SELECT ms.snapshot_key AS key,
                   ms.name,
                   ms.symbol,
                   ms.price,
                   ms.change_percent,
                   ms.source_name AS source,
                   ms.fetched_at
            FROM market_snapshots ms
            INNER JOIN (
                SELECT snapshot_key, MAX(id) AS max_id
                FROM market_snapshots
                GROUP BY snapshot_key
            ) latest
                ON latest.snapshot_key = ms.snapshot_key
               AND latest.max_id = ms.id
            """
        ).fetchall()

        items = [dict(row) for row in rows]
        items.sort(key=lambda item: _MARKET_SPEC_ORDER.get(item.get("key"), 999))
        return items
    finally:
        con.close()
