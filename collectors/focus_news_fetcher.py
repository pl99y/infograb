"""A small international RSS pool, fetched serially without article archives."""
from datetime import timezone
from email.utils import parsedate_to_datetime
import re
import xml.etree.ElementTree as ET

import requests

FEEDS = (("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
         ("UN News", "https://news.un.org/feed/subscribe/en/news/all/rss.xml"),
         ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
         ("France 24", "https://www.france24.com/en/rss"))
ITEMS_PER_FEED = 12

# Editorial triage, not confirmation of an emergency. Severity is checked later.
LOW_INTEREST = re.compile(r"\b(?:actor|actress|singer|celebrity|football|soccer|cricket|tennis|oscars?|grammys?)\b", re.I)
MAJOR_IMPACT = re.compile(
    r"\b(?:war|ceasefire|invasion|military|nuclear|sanctions?|missile|airstrikes?|"
    r"central bank|interest rates?|trade deal|trade war|tariffs?|recession|default|"
    r"oil supply|gas supply|production cuts?|oil prices?|shipping|strait|port closure|"
    r"pandemic|health emergency|disease outbreak|earthquake|tsunami|hurricane|typhoon|"
    r"power grid|nationwide blackout|airport closure|mass casualties)\b", re.I)


def news_priority(title, snippet="", categories=(), url=""):
    topic = " ".join(categories).lower()
    if re.search(r"/(?:sports?|opinion|program|podcasts?|videos?)/", url, re.I):
        return 0
    if re.search(r"\b(?:sports?|entertainment|opinion|analysis|culture)\b", topic):
        return 0
    if LOW_INTEREST.search(title):
        return 1
    # Keep snippets for the editor, but rank on headlines: sidebar/context words
    # in a description should not turn a local accident into a global emergency.
    return 5 if MAJOR_IMPACT.search(title) else 2


def fetch_focus_news():
    rows = []
    for source, url in FEEDS:
        try:
            response = requests.get(url, headers={"User-Agent": "InfoGrab/1.0 (personal news dashboard)"}, timeout=(4, 12))
            response.raise_for_status()
            root = ET.fromstring(response.content)
            accepted = 0
            for item in root.findall(".//item"):
                if accepted >= ITEMS_PER_FEED:
                    break
                try:
                    published = item.findtext("pubDate")
                    if not published:
                        continue
                    date = parsedate_to_datetime(published).astimezone(timezone.utc)
                    title = item.findtext("title") or ""
                    link = item.findtext("link") or ""
                    snippet = item.findtext("description") or ""
                    score = news_priority(title, snippet, [c.text or "" for c in item.findall("category")], link)
                    if score == 0:
                        continue
                    authors = item.findtext("author") or item.findtext("{http://purl.org/dc/elements/1.1/}creator") or ""
                    rows.append({"source_name": source, "source_kind": "news", "title": title,
                                 "url": link, "published_at": date.isoformat(), "score": score,
                                 "international_feed": True, "authors": authors[:150], "snippet": snippet[:500]})
                    accepted += 1
                except (ValueError, TypeError, OverflowError, AttributeError):
                    continue
        except (requests.RequestException, ET.ParseError, ValueError, TypeError, OverflowError):
            print(f"[warn] Focus news feed unavailable: {source}")
    return rows
