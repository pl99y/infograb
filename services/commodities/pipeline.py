"""Six latest futures quotes in one bounded snapshot, not an expanding price history."""
from datetime import datetime, timezone

from collectors.market_snapshot_fetcher import YahooMarketSnapshotAdapter, MarketSnapshotSpec
from storage import get_dashboard_snapshot, save_dashboard_snapshot

CACHE = "commodities_latest_v1"
SPECS = (("gold", "黄金", "GC=F", "美元/金衡盎司"),
         ("silver", "白银", "SI=F", "美元/金衡盎司"),
         ("copper", "铜", "HG=F", "美元/磅"),
         ("corn", "玉米", "ZC=F", "美分/蒲式耳"),
         ("soybean", "大豆", "ZS=F", "美分/蒲式耳"),
         ("wheat", "小麦", "ZW=F", "美分/蒲式耳"))


def fetch_commodities_once(*, db_path):
    now = datetime.now(timezone.utc).isoformat()
    previous = get_dashboard_snapshot(CACHE, db_path=db_path) or {}
    old = {row["quote_key"]: row for row in previous.get("items", [])}
    items, successes = [], 0
    adapter = YahooMarketSnapshotAdapter(timeout=12)
    try:
        for key, name, symbol, unit in SPECS:
            try:
                quote = adapter.fetch_one(MarketSnapshotSpec(key, name, symbol, "yahoo_finance"))
                before = old.get(key, {})
                current_contract = quote.get("contract_name")
                rolled = bool(current_contract and before.get("contract_name") and before["contract_name"] != current_contract)
                items.append({**quote, "quote_key": key, "name": name, "unit": unit,
                              "fetched_at": now, "timestamp_text": quote.get("quote_time"),
                              "status": "available", "contract_rolled": rolled,
                              "change_percent": None if rolled else quote.get("change_percent")})
                successes += 1
            except Exception as error:
                print(f"[warn] Commodity {key} unavailable ({type(error).__name__})")
                items.append({**old.get(key, {}), "quote_key": key, "name": name, "unit": unit,
                              "symbol": symbol, "status": "stale" if key in old else "unavailable"})
    finally:
        adapter.close()
    save_dashboard_snapshot(CACHE, {"generated_at": now, "items": items}, db_path=db_path)
    return {"status": "success" if successes == len(SPECS) else "partial" if successes else "failed"}


def get_commodities(*, db_path):
    return (get_dashboard_snapshot(CACHE, db_path=db_path) or {}).get("items", [])
