from __future__ import annotations

from dataclasses import dataclass
import logging
import math
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests

from collectors.common import DEFAULT_HEADERS

logger = logging.getLogger(__name__)


YAHOO_CHART_URL_TEMPLATE = (
    "https://query1.finance.yahoo.com/v8/finance/chart/"
    "{symbol}?interval=1d&range=1d&includePrePost=false&events=div,splits"
)

EASTMONEY_QUOTE_URL_TEMPLATE = (
    "https://push2.eastmoney.com/api/qt/stock/get"
    "?secid={secid}&fields=f57,f58,f43,f60,f170,f169,f86,f124"
    "&fltt=2&invt=2&ut=fa5fd1943c7b386f172d6893dbfba10b"
)


@dataclass(frozen=True)
class MarketSnapshotSpec:
    key: str
    name: str
    symbol: str
    provider: str
    secid: str | None = None


class MarketSnapshotAdapterError(RuntimeError):
    pass



def _coerce_float(value: Any, *, field_name: str) -> float:
    if value is None or value == "":
        raise MarketSnapshotAdapterError(f"Missing field: {field_name}")

    try:
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("Non-finite quote")
        return result
    except (TypeError, ValueError) as exc:
        raise MarketSnapshotAdapterError(
            f"Invalid numeric field {field_name}: {value!r}"
        ) from exc


class YahooMarketSnapshotAdapter:
    source_name = "yahoo_finance"

    def __init__(self, *, timeout: int = 15, session: requests.Session | None = None) -> None:
        self.timeout = timeout
        self.session = session or requests.Session()

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def fetch_one(self, spec: MarketSnapshotSpec) -> dict:
        url = YAHOO_CHART_URL_TEMPLATE.format(symbol=quote(spec.symbol, safe=""))

        response = self.session.get(url, headers=DEFAULT_HEADERS, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()

        chart = payload.get("chart") or {}
        error = chart.get("error")
        if error:
            raise MarketSnapshotAdapterError(str(error))

        results = chart.get("result") or []
        if not results:
            raise MarketSnapshotAdapterError("Yahoo returned no result")

        meta = results[0].get("meta") or {}
        price = _coerce_float(meta.get("regularMarketPrice"), field_name="meta.regularMarketPrice")
        # chartPreviousClose is the chart-range baseline, not necessarily yesterday's close.
        if meta.get("regularMarketChangePercent") is not None:
            change_percent = round(_coerce_float(meta["regularMarketChangePercent"], field_name="regularMarketChangePercent"), 2)
        else:
            previous_close = _coerce_float(meta.get("previousClose", meta.get("chartPreviousClose")), field_name="previousClose")
            if previous_close == 0:
                raise MarketSnapshotAdapterError("Previous close is zero")
            change_percent = round((price - previous_close) / previous_close * 100, 2)

        return {
            "key": spec.key,
            "name": spec.name,
            "symbol": spec.symbol,
            "price": price,
            "change_percent": change_percent,
            "source": self.source_name,
            "quote_time": datetime.fromtimestamp(meta["regularMarketTime"], timezone.utc).isoformat() if meta.get("regularMarketTime") else None,
            "contract_name": str(meta.get("shortName") or "")[:120],
        }


class EastmoneyMarketSnapshotAdapter:
    source_name = "eastmoney"

    def __init__(self, *, timeout: int = 15, session: requests.Session | None = None) -> None:
        self.timeout = timeout
        self.session = session or requests.Session()

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def fetch_one(self, spec: MarketSnapshotSpec) -> dict:
        if not spec.secid:
            raise MarketSnapshotAdapterError("Eastmoney spec is missing secid")

        url = EASTMONEY_QUOTE_URL_TEMPLATE.format(secid=quote(spec.secid, safe=""))
        headers = {
            **DEFAULT_HEADERS,
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "application/json,text/plain,*/*",
        }

        response = self.session.get(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()

        data = payload.get("data")
        if not data:
            raise MarketSnapshotAdapterError("Eastmoney returned empty data")

        symbol = str(data.get("f57") or spec.symbol)
        name = str(data.get("f58") or spec.name)
        price = _coerce_float(data.get("f43"), field_name="data.f43")
        change_percent = round(_coerce_float(data.get("f170"), field_name="data.f170"), 2)
        epoch = data.get("f124") or data.get("f86")
        quote_time = datetime.fromtimestamp(epoch, timezone.utc).isoformat() if isinstance(epoch, (int, float)) and 946684800 < epoch < 10000000000 else None

        return {
            "key": spec.key,
            "name": name,
            "symbol": symbol,
            "price": price,
            "change_percent": change_percent,
            "source": self.source_name,
            "quote_time": quote_time,
        }
