"""Developer-only calendar generation; exchange_calendars is not a runtime dependency.

Build with exchange-calendars==4.13.3 in a temporary Python >=3.10 environment.
Unavailable future holiday coverage remains unavailable, never guessed as weekdays.
"""
import json
from datetime import timedelta
from datetime import datetime, timezone
from pathlib import Path

import exchange_calendars as xcals
import pandas as pd


def build(year=2026):
    result = {"built_at": datetime.now(timezone.utc).isoformat(),
              "generator": f"exchange_calendars {xcals.__version__}", "calendars": {}}
    for name in ("XNYS", "XSHG", "XTKS", "XHKG"):
        end_year = year + 1
        while end_year >= year:
            try:
                calendar = xcals.get_calendar(name, start=f"{year}-01-01", end=f"{end_year}-12-31")
                break
            except ValueError:
                end_year -= 1
        if end_year < year:
            raise ValueError(f"No current-year calendar: {name}")
        sessions = []
        for date, row in calendar.schedule.iterrows():
            session = {"date": date.date().isoformat(), **{
                key: None if pd.isna(row[key]) else row[key].isoformat()
                for key in ("open", "break_start", "break_end", "close")}}
            if name == "XHKG":
                session["auction_start"] = session["close"]
                session["close"] = (row["close"] + timedelta(minutes=10)).isoformat()
            sessions.append(session)
        result["calendars"][name] = {"timezone": str(calendar.tz),
            "valid_from": f"{year}-01-01", "valid_until": f"{end_year}-12-31", "sessions": sessions}
    target = Path(__file__).resolve().parents[1] / "sources" / "market_sessions.json"
    target.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print({name: (cal["valid_until"], len(cal["sessions"])) for name, cal in result["calendars"].items()})


if __name__ == "__main__":
    build()
