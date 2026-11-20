"""Bounded Wikimedia On This Day requests, without model-side browsing."""
from datetime import date

import requests


def fetch_history_events(day: date) -> dict:
    sources = []
    with requests.Session() as session:
        session.headers["User-Agent"] = "InfoGrab/1.0 (personal history dashboard; pl99y/infograb)"
        for language in ("zh", "en"):
            url = f"https://{language}.wikipedia.org/api/rest_v1/feed/onthisday/events/{day:%m/%d}"
            source = {"language": language, "url": url, "ok": False, "events": []}
            try:
                response = session.get(url, timeout=(5, 25))
                response.raise_for_status()
                events = response.json().get("events")
                if not isinstance(events, list) or not events:
                    raise ValueError("History feed contains no events")
                source.update(ok=True, events=events[:150])
            except (requests.RequestException, ValueError, TypeError, AttributeError):
                # Do not expose response bodies or exception URLs to public exports.
                print(f"[warn] History {language} source unavailable")
            sources.append(source)
    return {"sources": sources}
