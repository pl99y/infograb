from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup


BASE_URL = "https://t.me"


def clean_text(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def text_or_empty(node) -> str:
    if node is None:
        return ""
    return clean_text(node.get_text(" ", strip=True))


def extract_background_image_url(style_value: str) -> str:
    """
    Extract URL from CSS like:
    background-image:url('https://...')
    """
    if not style_value:
        return ""

    match = re.search(r"url\((['\"]?)(.*?)\1\)", style_value)
    if not match:
        return ""

    return match.group(2).strip()


def parse_channel_info(soup: BeautifulSoup) -> dict[str, str]:
    title_el = soup.select_one(".tgme_channel_info_header_title")
    username_el = soup.select_one(".tgme_channel_info_header_username")
    desc_el = soup.select_one(".tgme_channel_info_description")
    extra_el = soup.select_one(".tgme_channel_info_counters")

    return {
        "channel_name": text_or_empty(title_el),
        "channel_username": text_or_empty(username_el),
        "channel_description": text_or_empty(desc_el),
        "channel_extra": text_or_empty(extra_el),
    }


def extract_media_items(node) -> list[dict[str, str]]:
    media_items: list[dict[str, str]] = []

    # Image-style media blocks
    photo_nodes = node.select(".tgme_widget_message_photo_wrap")
    for photo in photo_nodes:
        href = photo.get("href", "") if photo.has_attr("href") else ""
        style = photo.get("style", "") if photo.has_attr("style") else ""

        image_url = extract_background_image_url(style)
        if image_url:
            media_items.append({
                "type": "image",
                "url": image_url,
            })
        elif href:
            media_items.append({
                "type": "image",
                "url": urljoin(BASE_URL, href),
            })

    # Video thumbs (background images of .tgme_widget_message_video_thumb)
    video_thumb_nodes = node.select(".tgme_widget_message_video_thumb")
    for thumb in video_thumb_nodes:
        style = thumb.get("style", "") if thumb.has_attr("style") else ""
        image_url = extract_background_image_url(style)
        if image_url:
            media_items.append({
                "type": "video_thumb",
                "url": image_url,
            })

    # Video blocks
    video_nodes = node.select("video")
    for video in video_nodes:
        src = video.get("src", "") if video.has_attr("src") else ""
        if src:
            media_items.append({
                "type": "video",
                "url": urljoin(BASE_URL, src),
            })

    # Deduplicate while preserving order
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, str]] = []
    for item in media_items:
        key = (item["type"], item["url"])
        if key not in seen and item["url"]:
            seen.add(key)
            deduped.append(item)

    return deduped


def parse_post_node(node) -> dict[str, Any]:
    text_el = node.select_one(".tgme_widget_message_text")
    date_link_el = node.select_one("a.tgme_widget_message_date")
    time_el = node.select_one("a.tgme_widget_message_date time")

    post_url = ""
    if date_link_el and date_link_el.has_attr("href"):
        post_url = urljoin(BASE_URL, date_link_el["href"])

    published_at = ""
    if time_el:
        published_at = time_el.get("datetime", "") or ""

    # Check for forwards and replies
    forward_el = node.select_one(".tgme_widget_message_forwarded_from_name")
    forward_text = ""
    if forward_el:
        forward_text = f"🔄 Forwarded from {clean_text(forward_el.get_text())}:\n"

    reply_el = node.select_one(".tgme_widget_message_reply")
    reply_text = ""
    if reply_el:
        reply_author_el = reply_el.select_one(".tgme_widget_message_author")
        reply_snippet_el = reply_el.select_one(".tgme_widget_message_text")
        author = clean_text(reply_author_el.get_text()) if reply_author_el else "Someone"
        snippet = clean_text(reply_snippet_el.get_text()) if reply_snippet_el else ""
        if snippet:
            reply_text = f"↩️ Reply to {author}: \"{snippet}\"\n"
        else:
            reply_text = f"↩️ Reply to {author}\n"

    main_text = ""
    # Find all text elements to avoid grabbing the one inside the reply block
    for el in node.select(".tgme_widget_message_text"):
        if not el.find_parent(class_="tgme_widget_message_reply") and not el.find_parent(class_="tgme_widget_message_forwarded_from"):
            main_text = clean_text(el.get_text("\n", strip=True))
            break

    text_value = clean_text(forward_text + reply_text + main_text)

    media_items = extract_media_items(node)

    return {
        "text": text_value,
        "post_url": post_url,
        "published_at": published_at,
        "media_items": media_items,
    }


def parse_posts(soup: BeautifulSoup) -> list[dict[str, Any]]:
    posts: list[dict[str, Any]] = []

    message_nodes = soup.select(".tgme_widget_message_wrap")

    for node in message_nodes:
        try:
            parsed = parse_post_node(node)
            posts.append(parsed)
        except Exception:
            continue

    return posts


def parse_telegram_page(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    return {
        "channel_info": parse_channel_info(soup),
        "posts": parse_posts(soup),
    }