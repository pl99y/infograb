"""Normalize source facts separately from generated editorial comments."""
import hashlib
import re
from urllib.parse import urlparse


CHINA = re.compile(
    r"中国|中國|中华|中華|香港|澳[门門]|[台臺][湾灣]|唐朝|宋朝|元朝|明朝|清朝|"
    r"汉朝|漢朝|安史|回纥|回紇|洛[阳陽]|朱棣|刘恒|劉恆|王莽|溥[仪儀]|"
    r"[冯馮]玉祥|郭松[龄齡]|[闽閩][变變]|[张張]作霖|蒋介石|蔣介石|"
    r"北京|上海|[广廣][东東]|福建|[兰蘭]州|[辽遼][宁寧]|"
    r"\b(?:china|chinese|taiwan|hong kong|maca[ou]|tang dynasty|ming dynasty|qing dynasty|wang mang)\b",
    re.IGNORECASE,
)


def clean_text(value, limit=1200):
    return " ".join(str(value or "").split())[:limit]


def safe_wiki_url(value):
    if not isinstance(value, str):
        return ""
    parsed = urlparse(value)
    return value if parsed.scheme == "https" and parsed.hostname in {"zh.wikipedia.org", "en.wikipedia.org"} else ""


def parse_history_events(payload, day):
    events = []
    seen = set()
    for source in payload.get("sources", []):
        language = source.get("language")
        if not source.get("ok") or language not in {"zh", "en"}:
            continue
        for item in source.get("events", []):
            if not isinstance(item, dict) or not isinstance(item.get("year"), int):
                continue
            year = item["year"]
            fact = clean_text(item.get("text"), 700)
            # Ancient dates require calendar verification beyond a date feed.
            if not fact or year < 1583:
                continue
            pages = []
            for page in (item.get("pages") or [])[:20]:
                if not isinstance(page, dict):
                    continue
                title = clean_text((page.get("titles") or {}).get("normalized") or page.get("title"), 160)
                url = safe_wiki_url(((page.get("content_urls") or {}).get("desktop") or {}).get("page"))
                extract = clean_text(page.get("extract"), 1200)
                if not url or not extract or title.replace("年", "").isdigit():
                    continue
                pages.append({"title": title, "url": url, "extract": extract,
                              "qid": clean_text(page.get("wikibase_item"), 24),
                              "revision": clean_text(page.get("revision"), 24)})
                if len(pages) == 8:
                    break
            if not pages:
                continue
            # Targeted source correction supported by Debian's official project history.
            if day.strftime("%m-%d") == "08-16" and year == 1993 and "debian" in fact.lower():
                fact = "伊恩·默多克正式创立 Debian 项目。"
                pages.insert(0, {"title": "Debian 官方项目史", "url": "https://www.debian.org/doc/manuals/project-history/intro.en.html",
                                 "extract": "The Debian Project was officially founded by Ian Murdock on August 16th, 1993.",
                                 "qid": "", "revision": ""})
                language_for_fact = "zh"
            else:
                language_for_fact = language
            # Both language feeds describe Essex sinking, but disagree on distance offshore.
            if day.strftime("%m-%d") == "11-20" and year == 1820 and re.search(r"埃塞克斯|Essex", fact, re.I):
                fact = "美国捕鲸船埃塞克斯号遭抹香鲸攻击后沉没，这起事件成为小说《白鲸记》的创作素材之一。"
                language_for_fact = "zh"
            # Conflicting dates or action stages found during source validation.
            if (day.strftime("%m-%d"), year) == ("09-23", 1983) and re.search(r"港|hong|dollar", fact, re.I):
                continue
            if language == "zh" and day.strftime("%m-%d") == "11-20" and year == 1974 and "AT&T" in fact:
                continue
            key = hashlib.sha256(f"{day:%m-%d}|{year}|{fact.casefold()}".encode()).hexdigest()[:24]
            if key in seen:
                continue
            seen.add(key)
            events.append({"id": key, "year": year, "month_day": day.strftime("%m-%d"),
                           "fact": fact, "language": language_for_fact, "pages": pages,
                           "china": bool(CHINA.search(fact)), "period": "ancient" if year < 1840 else "modern"})
    return events
