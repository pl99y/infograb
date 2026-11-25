"""Six latest futures quotes in one bounded snapshot, not an expanding price history."""
from datetime import datetime, timezone
import math

from collectors.market_snapshot_fetcher import YahooMarketSnapshotAdapter, MarketSnapshotSpec
from storage import get_dashboard_snapshot, save_dashboard_snapshot

CACHE = "commodities_latest_v1"
# NIST Handbook 133, Appendix E: international troy ounce in grams.
TROY_OUNCE_GRAMS = 31.1034768
PRECIOUS_METALS = {"gold", "silver"}


def usable_fx(fx, now=None):
    now = now or datetime.now(timezone.utc)
    try:
        rate = float(fx["rate"])
        stamp = datetime.fromisoformat(fx["quote_time"].replace("Z", "+00:00"))
        age = (now - stamp).total_seconds()
        return math.isfinite(rate) and rate > 0 and -300 <= age <= 7 * 86400
    except (KeyError, TypeError, ValueError):
        return False

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
    fx = previous.get("fx") or {}
    fx_ok = False
    adapter = YahooMarketSnapshotAdapter(timeout=12)
    try:
        try:
            quote = adapter.fetch_one(MarketSnapshotSpec("usd_cny", "USD/CNY", "CNY=X", "yahoo_finance"))
            candidate = {"rate": quote["price"], "quote_time": quote.get("quote_time"), "fetched_at": now, "source": "Yahoo Finance USD/CNY"}
            if not usable_fx(candidate):
                raise ValueError("Invalid or expired USD/CNY quote")
            fx, fx_ok = candidate, True
        except Exception as error:
            print(f"[warn] USD/CNY unavailable ({type(error).__name__}); checking last quote")
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
    save_dashboard_snapshot(CACHE, {"generated_at": now, "items": items, "fx": fx}, db_path=db_path)
    return {"status": "success" if successes == len(SPECS) and fx_ok else "partial" if successes else "failed"}


def get_commodities(*, db_path):
    snapshot = get_dashboard_snapshot(CACHE, db_path=db_path) or {}
    fx = snapshot.get("fx") or {}
    valid_fx = usable_fx(fx)
    items = []
    for raw in snapshot.get("items", []):
        item = dict(raw)
        if item.get("quote_key") in PRECIOUS_METALS:
            item.update(display_price=None, display_unit="人民币/克", fx=fx if valid_fx else None,
                        conversion_status="unavailable")
            if valid_fx and item.get("price") is not None:
                price = float(item["price"]) * float(fx["rate"]) / TROY_OUNCE_GRAMS
                if math.isfinite(price) and price > 0:
                    item.update(display_price=round(price, 2), conversion_status="available")
        items.append(item)
    return items
