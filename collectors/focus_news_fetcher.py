"""A bounded pair of international feeds, fetched serially without article archives."""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

import requests

FEEDS = (("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
         ("UN News", "https://news.un.org/feed/subscribe/en/news/all/rss.xml"))


def fetch_focus_news():
    rows = []
    for source, url in FEEDS:
        try:
            response = requests.get(url, headers={"User-Agent": "InfoGrab/1.0 (personal news dashboard)"}, timeout=(4, 12))
            response.raise_for_status()
            root = ET.fromstring(response.content)
            for item in root.findall(".//item")[:20]:
                published = item.findtext("pubDate")
                if not published:
                    continue
                date = parsedate_to_datetime(published).astimezone(timezone.utc)
                rows.append({"source_name": source, "source_kind": "news", "title": item.findtext("title") or "",
                             "url": item.findtext("link") or "", "published_at": date.isoformat(),
                             "snippet": (item.findtext("description") or "")[:500]})
        except (requests.RequestException, ET.ParseError, ValueError, TypeError, OverflowError):
            print(f"[warn] Focus news feed unavailable: {source}")
    return rows
