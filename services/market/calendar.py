"""Serve a small UTC schedule; browser computes current status without new requests."""
import json
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

MAPPING = {"nasdaq": "XNYS", "sp500": "XNYS", "dowjones": "XNYS",
           "a_share_sse": "XSHG", "nikkei225": "XTKS", "hangseng": "XHKG"}


@lru_cache(maxsize=1)
def _calendars():
    return json.loads((Path(__file__).resolve().parents[2] / "sources/market_sessions.json").read_text(encoding="utf-8"))


def get_market_calendar(now=None):
    now = now or datetime.now(timezone.utc)
    start, end = (now - timedelta(days=2)).date(), (now + timedelta(days=16)).date()
    source = _calendars()
    calendars = {}
    for name, data in source["calendars"].items():
        calendars[name] = {**{k: data[k] for k in ("timezone", "valid_from", "valid_until")},
                          "sessions": [row for row in data["sessions"] if start.isoformat() <= row["date"] <= end.isoformat()]}
    return {"generated_at": now.isoformat(), "expires_at": (now + timedelta(days=7)).isoformat(),
            "mapping": MAPPING, "calendars": calendars, "basis": source["generator"]}
