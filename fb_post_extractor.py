#!/usr/bin/env python3
"""
Standalone Facebook Post Event Scraper & Extractor.

Completely self-contained local script derived from `scraper/fb_scraper.py` and
`events/extractor.py`. It does not import any other module in this repository
and can be copied anywhere on your PC and run directly.

Requirements:
    pip install playwright
    playwright install chromium
Optional (for QR-code registration link extraction from post images):
    pip install opencv-python-headless numpy

Examples:
    # Interactive mode (prompts for posts folder / text files / URLs)
    python fb_post_extractor.py

    # Extract events from sample*.txt files inside ../posts
    python fb_post_extractor.py -d ../posts -o extracted_events.json

    # Pass a folder or .txt files directly
    python fb_post_extractor.py ../posts --overwrite --include-non-events

    # Pass URLs and destination JSON via CLI
    python fb_post_extractor.py -o events.json https://www.facebook.com/.../posts/...

    # Read URLs from a text file (one URL per line)
    python fb_post_extractor.py -f urls.txt -o output/events.json
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import socket
import sys
import time
import unicodedata
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse
from zoneinfo import ZoneInfo

#from playwright.async_api import Page, Response, async_playwright

def _load_env_file() -> None:
    """Lightweight zero-dependency .env loader that checks cwd and parent dirs."""
    search_dirs = [Path.cwd(), Path(__file__).resolve().parent, Path(__file__).resolve().parent.parent]
    for d in search_dirs:
        env_path = d / ".env"
        if env_path.is_file():
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip("'\"")
                        if k and k not in os.environ:
                            os.environ[k] = v
            except Exception:
                pass
            break


_load_env_file()

# ---------------------------------------------------------------------------
# Standalone Configuration
# ---------------------------------------------------------------------------
# Set your Gemini API key(s) directly here:
# Can be a single string "AIza..." or comma-separated "key1,key2" or list ["key1", "key2"]
DEFAULT_GEMINI_API_KEY = ""
DEFAULT_GEMINI_API_KEYS: List[str] = []

# Set default Gemini model directly here (e.g. "gemini-2.0-flash-lite", "gemini-2.0-flash"):
DEFAULT_GEMINI_MODEL = "gemini-2.0-flash-lite"

# Groq API Fallback Configuration (Free tier: https://console.groq.com)
DEFAULT_GROQ_API_KEY = ""
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"
FALLBACK_GROQ_MODELS = [
    "llama-3.3-70b-versatile",
    "llama-3.1-8b-instant",
    "llama-3.2-3b-preview",
    "llama-3.2-1b-preview",
    "llama3-70b-8192",
    "llama3-8b-8192",
    "mixtral-8x7b-32768",
    "gemma2-9b-it",
    "qwen-2.5-32b",
]
GROQ_CHAT_COMPLETIONS_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODELS_URL = "https://api.groq.com/openai/v1/models"


class GeminiKeyPool:
    """Manages a pool of Gemini API keys with automatic rotation on 429 (rate limits) and 403."""

    def __init__(self, initial_keys: Optional[List[str]] = None):
        self.keys: List[str] = []
        self._index = 0
        self._cooldowns: Dict[str, float] = {}
        if initial_keys:
            self.add_keys(initial_keys)

    def add_keys(self, raw: Any) -> None:
        if isinstance(raw, list):
            for x in raw:
                self.add_keys(x)
        elif isinstance(raw, str) and raw.strip():
            for part in re.split(r"[,\s;]+", raw.strip()):
                clean = part.strip().strip("'\"")
                if clean and clean not in self.keys:
                    self.keys.append(clean)

    def has_keys(self) -> bool:
        return bool(self.keys)

    def get_active_key(self) -> Optional[str]:
        if not self.keys:
            return None
        now = time.time()
        for i in range(len(self.keys)):
            idx = (self._index + i) % len(self.keys)
            k = self.keys[idx]
            if self._cooldowns.get(k, 0) <= now:
                self._index = idx
                return k
        return min(self.keys, key=lambda k: self._cooldowns.get(k, 0))

    def mark_rate_limited(self, key: str, wait_seconds: float = 60.0) -> Optional[str]:
        self._cooldowns[key] = time.time() + max(10.0, wait_seconds)
        self._index = (self._index + 1) % len(self.keys)
        return self.get_active_key()

    def mark_invalid(self, key: str) -> Optional[str]:
        self._cooldowns[key] = time.time() + 86400.0  # 24h
        self._index = (self._index + 1) % len(self.keys)
        return self.get_active_key()


_GLOBAL_KEY_POOL = GeminiKeyPool()
try:
    SCRAPE_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
except Exception:
    SCRAPE_TIMEZONE = timezone(timedelta(hours=7))

LOCAL_TIMEZONE = datetime.now().astimezone().tzinfo or SCRAPE_TIMEZONE
DEFAULT_OUTPUT_JSON = "extracted_events.json"
PAGE_TIMEOUT_MS = 45000
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
GEMINI_LIST_MODELS_URL = "https://generativelanguage.googleapis.com/v1beta/models"
GEMINI_CACHED_CONTENTS_URL = "https://generativelanguage.googleapis.com/v1beta/cachedContents"
GEMINI_MAX_RETRIES = int(os.environ.get("GEMINI_MAX_RETRIES") or 3)
GEMINI_REQUEST_TIMEOUT_SECONDS = int(os.environ.get("GEMINI_REQUEST_TIMEOUT_SECONDS") or 90)
GEMINI_CACHE_TTL_SECONDS = int(os.environ.get("GEMINI_CACHE_TTL_SECONDS") or 3600)
GEMINI_BATCH_SIZE = max(1, int(os.environ.get("GEMINI_BATCH_SIZE") or 5))
GEMINI_ENABLE_CONTEXT_CACHE = os.environ.get("GEMINI_ENABLE_CONTEXT_CACHE", "true").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
MIN_KEYWORD_MATCHES = int(os.environ.get("MIN_KEYWORD_MATCHES") or 2)
FALLBACK_GEMINI_MODELS = [
    "gemini-2.0-flash-lite",
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-flash-8b",
]
_WORKING_GEMINI_MODEL: Optional[str] = None
_CACHED_SYSTEM_HANDLES: Dict[str, Tuple[str, float]] = {}
_CACHE_UNSUPPORTED_MODELS: Set[str] = set()

# ---------------------------------------------------------------------------
# Facebook Scraper (from scraper/fb_scraper.py)
# ---------------------------------------------------------------------------
SKIP_JSON_KEYS = {
    "attached_story",
    "feedback",
    "comments",
    "display_comments",
    "comment_rendering_instance",
    "comet_ufi_summary_and_actions_renderer",
    "related_videos",
    "suggested_pages",
    "sideFeed",
}

_POST_URL_RE = re.compile(
    r"(/posts/|/permalink\.php|/permalink/|/story\.php|/reel/|/videos/|/photos/|/photo(?:\.php|\?)|/share/|/events/|fb\.watch/)",
    flags=re.IGNORECASE,
)


def is_facebook_post_url(url: str) -> bool:
    """Return True if the URL looks like a Facebook post/reel/photo/share URL."""
    if not url:
        return False
    s = url.strip().lower()
    if "facebook.com" not in s and "fb.com" not in s and "fb.watch" not in s:
        return False
    return bool(_POST_URL_RE.search(s))


def _extract_page_slug(page_url: str) -> str:
    parsed = urlparse(page_url)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if not parts:
        return ""
    if parts[0] == "profile.php":
        qs = parse_qs(parsed.query)
        return qs.get("id", [""])[0].lower()
    return parts[-1].lower()


def _extract_post_ids_from_url(url: str) -> Set[str]:
    """Extract all candidate post identifiers (numeric ID, pfbid, fbid, story_fbid) from a URL."""
    ids: Set[str] = set()
    if not url:
        return ids

    for pattern in (
        r"/posts/(pfbid[A-Za-z0-9]+|\d+)",
        r"/permalink/(pfbid[A-Za-z0-9]+|\d+)",
        r"/(?:videos|reel|events)/(\d+)",
        r"/photos/[^/]+/(\d+)",
    ):
        for m in re.finditer(pattern, url):
            ids.add(m.group(1))

    qs = parse_qs(urlparse(url).query)
    for key in ("story_fbid", "fbid"):
        for val in qs.get(key, []):
            if val:
                ids.add(val)

    for m in re.finditer(r"(pfbid[A-Za-z0-9]{15,})", url):
        ids.add(m.group(1))

    return ids


def _extract_post_id_from_url(url: str) -> Optional[str]:
    """Extract a canonical post identifier from a Facebook post URL."""
    ids = _extract_post_ids_from_url(url)
    if not ids:
        return None
    # Prefer deterministic priority matching prior behavior
    for pattern in (
        r"/posts/(pfbid[A-Za-z0-9]+|\d+)",
        r"/permalink/(pfbid[A-Za-z0-9]+|\d+)",
        r"/(?:videos|reel|events)/(\d+)",
        r"/photos/[^/]+/(\d+)",
    ):
        m = re.search(pattern, url)
        if m:
            return m.group(1)
    qs = parse_qs(urlparse(url).query)
    for key in ("story_fbid", "fbid"):
        if qs.get(key):
            return qs[key][0]
    m = re.search(r"(pfbid[A-Za-z0-9]{15,})", url)
    return m.group(1) if m else next(iter(ids), None)


def _clean_post_url(url: str, base_page_url: str) -> str:
    """Strip tracking query params from a Facebook post URL."""
    if not url:
        return base_page_url
    if url.startswith("/"):
        url = urljoin("https://www.facebook.com", url)

    parsed = urlparse(url)
    if "permalink.php" in parsed.path or "story.php" in parsed.path or "photo" in parsed.path:
        qs = parse_qs(parsed.query)
        keep = {k: qs[k][0] for k in ("story_fbid", "id", "fbid") if k in qs and qs[k]}
        if keep:
            query_str = "&".join(f"{k}={v}" for k, v in keep.items())
            return f"https://www.facebook.com{parsed.path}?{query_str}"
    return f"https://www.facebook.com{parsed.path}".rstrip("/")


def _derive_fanpage_url_from_url(url: Optional[str]) -> Optional[str]:
    """Derive a canonical Facebook fanpage URL from a post permalink or actor URL."""
    if not url or "facebook.com" not in url.lower():
        return None

    parsed = urlparse(url)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if not parts:
        return None

    first = parts[0].lower()
    if first in ("profile.php", "permalink.php", "story.php"):
        pid = parse_qs(parsed.query).get("id", [""])[0]
        return f"https://www.facebook.com/profile.php?id={pid}" if pid else None

    if first in ("people", "pages") and len(parts) >= 3:
        return f"https://www.facebook.com/{parts[0]}/{parts[1]}/{parts[2]}"

    reserved = {
        "share", "reel", "watch", "photo", "photo.php", "photos",
        "videos", "events", "groups", "story.php", "permalink.php",
    }
    if first in reserved:
        return None

    return f"https://www.facebook.com/{parts[0]}"


def _find_first_key(obj: Any, target_key: str) -> Any:
    """Recursively find the first occurrence of target_key in nested dict/list."""
    if isinstance(obj, dict):
        if target_key in obj and obj[target_key] is not None:
            return obj[target_key]
        for k, v in obj.items():
            if k in SKIP_JSON_KEYS:
                continue
            res = _find_first_key(v, target_key)
            if res is not None:
                return res
    elif isinstance(obj, list):
        for item in obj:
            res = _find_first_key(item, target_key)
            if res is not None:
                return res
    return None


def _find_attached_story(obj: Any) -> Optional[Dict[str, Any]]:
    """Recursively find a non-null shared/attached_story dict inside a Story unit."""
    if isinstance(obj, dict):
        att = obj.get("attached_story")
        if isinstance(att, dict) and att:
            return att
        for k, v in obj.items():
            if k in ("feedback", "comments", "display_comments", "comment_rendering_instance"):
                continue
            res = _find_attached_story(v)
            if res is not None:
                return res
    elif isinstance(obj, list):
        for item in obj:
            res = _find_attached_story(item)
            if res is not None:
                return res
    return None


def _find_post_permalink(obj: Any) -> Optional[str]:
    """Find a Facebook post permalink URL inside metadata/context_layout."""
    if isinstance(obj, dict):
        for k in ("wwwURL", "url"):
            val = obj.get(k)
            if isinstance(val, str) and _extract_post_id_from_url(val):
                return val
        for k, v in obj.items():
            if k in SKIP_JSON_KEYS:
                continue
            res = _find_post_permalink(v)
            if res:
                return res
    elif isinstance(obj, list):
        for item in obj:
            res = _find_post_permalink(item)
            if res:
                return res
    return None


def _extract_story_message(node: Any) -> str:
    """Extract post message text from a Story or attached_story dict (excluding comments)."""
    if not isinstance(node, dict):
        return ""

    msg_obj = node.get("message")
    if isinstance(msg_obj, dict):
        if isinstance(msg_obj.get("text"), str) and msg_obj["text"].strip():
            return msg_obj["text"].strip()
        inner = _extract_story_message(msg_obj.get("story"))
        if inner:
            return inner
        t = _find_first_key(msg_obj, "text")
        if isinstance(t, str) and t.strip():
            return t.strip()

    comet = node.get("comet_sections")
    if isinstance(comet, dict):
        for key in ("message", "message_container", "content"):
            sub = comet.get(key)
            if isinstance(sub, dict):
                text = _extract_story_message(sub)
                if text:
                    return text
                if key in ("message", "message_container"):
                    t = _find_first_key(sub, "text")
                    if isinstance(t, str) and t.strip():
                        return t.strip()

    if isinstance(node.get("story"), dict):
        text = _extract_story_message(node["story"])
        if text:
            return text

    for k, v in node.items():
        if k in SKIP_JSON_KEYS or k in ("message", "comet_sections", "story"):
            continue
        if isinstance(v, (dict, list)):
            msg = _find_first_key(v, "message")
            if isinstance(msg, dict) and isinstance(msg.get("text"), str) and msg["text"].strip():
                return msg["text"].strip()

    return ""


def _extract_attachment_card_text(node: Any) -> str:
    """Fallback text extraction for shared event/link cards."""
    parts: List[str] = []
    for key in ("title_with_entities", "card_title", "description"):
        obj = _find_first_key(node, key)
        if isinstance(obj, dict) and isinstance(obj.get("text"), str):
            val = obj["text"].strip()
            if val and val not in parts:
                parts.append(val)
    return "\n".join(parts)


def _belongs_to_fanpage(actors: Any, permalink: Optional[str], page_slug: str) -> bool:
    """Verify that the Story node was authored/shared by the target fanpage."""
    if not page_slug:
        return True

    is_numeric_or_profile = page_slug.isdigit() or page_slug.startswith(("profile_", "people_"))

    if isinstance(actors, list) and actors and isinstance(actors[0], dict):
        actor_url = (actors[0].get("url") or "").lower()
        if actor_url and "facebook.com/" in actor_url:
            actor_slug = _extract_page_slug(actor_url)
            if actor_slug and actor_slug != page_slug and not is_numeric_or_profile:
                return False

    if isinstance(permalink, str) and "facebook.com/" in permalink:
        parts = [p for p in urlparse(permalink).path.strip("/").split("/") if p]
        if parts and parts[0].lower() == "people":
            if not is_numeric_or_profile and parts[-1].lower() != page_slug:
                return False
        if len(parts) >= 2 and parts[1].lower() in ("posts", "videos", "photos"):
            if parts[0].lower() != page_slug and not is_numeric_or_profile:
                return False

    return True


def _collect_media_urls(
    node: Any,
    found: Set[str],
    actor_id: Optional[str] = None,
    allow_shared: bool = True,
) -> None:
    """Extract photo attachment URLs from a Story's attachments."""
    if isinstance(node, dict):
        typename = node.get("__typename", "")
        if typename == "ExternalUrl" or (typename == "Story" and not allow_shared):
            return

        owner = node.get("owner")
        if (
            not allow_shared
            and isinstance(owner, dict)
            and actor_id
            and owner.get("id")
            and str(owner.get("id")) != str(actor_id)
        ):
            return

        for img_key in ("photo_image", "image"):
            sub = node.get(img_key)
            if isinstance(sub, dict):
                uri = sub.get("uri")
                if (
                    isinstance(uri, str)
                    and uri.startswith("http")
                    and "emoji" not in uri
                    and "rsrc.php" not in uri
                ):
                    found.add(uri)

        for k, v in node.items():
            if k in SKIP_JSON_KEYS and not (allow_shared and k == "attached_story"):
                continue
            _collect_media_urls(v, found, actor_id=actor_id, allow_shared=allow_shared)
    elif isinstance(node, list):
        for item in node:
            _collect_media_urls(item, found, actor_id=actor_id, allow_shared=allow_shared)


def _extract_comet_story_unit(
    data: Dict[str, Any],
    page_url: str,
    page_slug: str,
    collected: Dict[str, Dict[str, Any]],
) -> None:
    """Extract a single post from a Facebook Comet `Story` unit."""
    comet = data.get("comet_sections")
    if not isinstance(comet, dict):
        return

    content_section = comet.get("content") or {}
    content_story = content_section.get("story") if isinstance(content_section, dict) else {}
    if not isinstance(content_story, dict):
        content_story = {}

    context_layout = comet.get("context_layout") or {}
    attached_story = _find_attached_story(data)

    post_id = str(
        content_story.get("post_id")
        or data.get("post_id")
        or _find_first_key(content_story, "post_id")
        or _find_first_key(context_layout, "post_id")
        or ""
    ).strip()

    permalink = (
        content_story.get("wwwURL")
        or content_story.get("url")
        or data.get("wwwURL")
        or _find_post_permalink(context_layout)
        or _find_post_permalink(comet.get("timestamp"))
        or _find_first_key(context_layout, "url")
        or _find_first_key(comet.get("timestamp"), "url")
    )

    actors = content_story.get("actors") or data.get("actors") or _find_first_key(context_layout, "actors")
    if not _belongs_to_fanpage(actors, permalink, page_slug):
        return

    actor_id = actors[0].get("id") if isinstance(actors, list) and actors and isinstance(actors[0], dict) else None

    outer_text = _extract_story_message(content_story) or _extract_story_message(data)
    shared_text = ""
    if isinstance(attached_story, dict):
        shared_text = _extract_story_message(attached_story) or _extract_attachment_card_text(attached_story)

    if outer_text and shared_text and shared_text not in outer_text:
        message_text = f"{outer_text}\n\n{shared_text}"
    else:
        message_text = outer_text or shared_text

    if not message_text:
        message_text = _extract_attachment_card_text(content_story)

    creation_time = (
        _find_first_key(context_layout, "creation_time")
        or _find_first_key(comet.get("timestamp"), "creation_time")
        or content_story.get("creation_time")
        or data.get("creation_time")
        or (_find_first_key(attached_story, "creation_time") if isinstance(attached_story, dict) else None)
    )

    if not post_id and isinstance(permalink, str):
        post_id = _extract_post_id_from_url(permalink) or ""
    if not post_id and isinstance(attached_story, dict):
        post_id = str(
            attached_story.get("post_id")
            or _find_first_key(attached_story, "post_id")
            or _extract_post_id_from_url(str(_find_post_permalink(attached_story) or ""))
            or ""
        ).strip()

    media_set: Set[str] = set()
    attachments = content_story.get("attachments") or data.get("attachments")
    if attachments:
        _collect_media_urls(attachments, media_set, actor_id=actor_id, allow_shared=True)
    if isinstance(attached_story, dict):
        _collect_media_urls(attached_story, media_set, actor_id=actor_id, allow_shared=True)

    if not post_id or (not message_text and not media_set):
        return

    clean_url = (
        _clean_post_url(permalink, page_url)
        if isinstance(permalink, str) and "facebook.com" in permalink and _extract_post_id_from_url(permalink)
        else f"{page_url.rstrip('/')}/posts/{post_id}"
    )

    if not _belongs_to_fanpage(actors, clean_url, page_slug):
        return

    published_dt = datetime.now(LOCAL_TIMEZONE)
    if isinstance(creation_time, (int, float)) and creation_time > 1000000000:
        published_dt = datetime.fromtimestamp(creation_time, tz=LOCAL_TIMEZONE)

    actor_name: Optional[str] = None
    actor_url: Optional[str] = None
    if isinstance(actors, list) and actors and isinstance(actors[0], dict):
        raw_name = actors[0].get("name")
        if isinstance(raw_name, str) and raw_name.strip():
            actor_name = raw_name.strip()
        raw_actor_url = actors[0].get("url") or actors[0].get("wwwURL")
        if isinstance(raw_actor_url, str) and "facebook.com" in raw_actor_url:
            actor_url = raw_actor_url.strip()

    existing = collected.get(post_id)
    candidate_ids: Set[str] = {post_id}
    for raw_url in (permalink, clean_url, content_story.get("wwwURL"), content_story.get("url"), data.get("wwwURL")):
        if isinstance(raw_url, str):
            candidate_ids.update(_extract_post_ids_from_url(raw_url))
    if isinstance(attached_story, dict):
        att_pid = str(attached_story.get("post_id") or "").strip()
        if att_pid:
            candidate_ids.add(att_pid)
        att_link = _find_post_permalink(attached_story)
        if isinstance(att_link, str):
            candidate_ids.update(_extract_post_ids_from_url(att_link))
    if existing and isinstance(existing.get("_candidate_ids"), set):
        candidate_ids.update(existing["_candidate_ids"])

    if not existing or (len(message_text) > len(existing.get("content", ""))):
        collected[post_id] = {
            "post_id": post_id,
            "post_url": clean_url,
            "content": message_text.strip(),
            "media_urls": list(media_set),
            "published_at": published_dt,
            "author_name": actor_name or (existing.get("author_name") if existing else None),
            "author_url": actor_url or (existing.get("author_url") if existing else None),
            "_candidate_ids": candidate_ids,
        }
    else:
        existing["_candidate_ids"] = candidate_ids


def _extract_media_node(data: Dict[str, Any], page_url: str, collected: Dict[str, Dict[str, Any]]) -> None:
    """Extract photo/video/reel Relay nodes (`currMedia`, `creation_story`) that lack top-level `comet_sections`."""
    curr = data.get("currMedia")
    if isinstance(curr, dict):
        media_id = str(curr.get("id") or "").strip()
        msg = _extract_story_message(curr) or _extract_story_message(curr.get("creation_story"))
        media_set: Set[str] = set()
        _collect_media_urls(curr, media_set, allow_shared=True)
        if media_id and (msg or media_set):
            existing = collected.get(media_id)
            if not existing or len(msg) > len(existing.get("content", "")):
                collected[media_id] = {
                    "post_id": media_id,
                    "post_url": page_url,
                    "content": msg.strip(),
                    "media_urls": list(media_set),
                    "published_at": datetime.now(LOCAL_TIMEZONE),
                    "author_name": None,
                    "author_url": None,
                    "_candidate_ids": {media_id} | _extract_post_ids_from_url(page_url),
                }


def _walk_graphql_for_posts(
    data: Any,
    page_url: str,
    page_slug: str,
    collected: Dict[str, Dict[str, Any]],
) -> None:
    """Walk Facebook Relay / GraphQL JSON objects to find top-level Story units."""
    if isinstance(data, dict):
        if isinstance(data.get("comet_sections"), dict) and (
            data.get("__typename") == "Story" or "content" in data["comet_sections"]
        ):
            _extract_comet_story_unit(data, page_url, page_slug, collected)
            return

        if isinstance(data.get("currMedia"), dict):
            _extract_media_node(data, page_url, collected)

        for k, v in data.items():
            if k in SKIP_JSON_KEYS:
                continue
            _walk_graphql_for_posts(v, page_url, page_slug, collected)
    elif isinstance(data, list):
        for item in data:
            _walk_graphql_for_posts(item, page_url, page_slug, collected)


async def _dismiss_login_popup(page: Page) -> None:
    """Close Facebook's unauthenticated login modal/banner without touching photo/post dialogs."""
    try:
        removed_login_dialog = await page.evaluate(
            """() => {
                let removed = false;
                const dialogs = document.querySelectorAll('div[role="dialog"]');
                dialogs.forEach(d => {
                    const hasPostMsg = Boolean(d.querySelector('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]'));
                    if (hasPostMsg) return;
                    const hasLoginForm = Boolean(d.querySelector('input[name="email"], input[name="pass"], form[action*="login"], a[href*="/login"]'));
                    const text = (d.innerText || '').toLowerCase();
                    const looksLikeGate = hasLoginForm || text.includes('see more on facebook') || text.includes('log in or sign up') || text.includes('đăng nhập hoặc đăng ký');
                    if (looksLikeGate) {
                        d.remove();
                        removed = true;
                    }
                });
                if (removed && document.body) {
                    document.body.style.overflow = 'auto';
                }
                return removed;
            }"""
        )
        if removed_login_dialog:
            return
    except Exception:
        pass

    # Only click explicit close inside a login-like dialog
    for selector in (
        'div[role="dialog"]:has(input[name="email"]) [aria-label="Close"]',
        'div[role="dialog"]:has(input[name="email"]) [aria-label="Đóng"]',
        'div[role="dialog"]:has(a[href*="/login"]) [aria-label="Close"]',
    ):
        try:
            btn = page.locator(selector).first
            if await btn.is_visible(timeout=300):
                await btn.click(timeout=800)
                await page.wait_for_timeout(200)
                break
        except Exception:
            continue


_LOGIN_OR_ERROR_TEXT_RE = re.compile(
    r"(?:log\s*into\s*facebook|you\s*must\s*log\s*in|see\s*posts,\s*photos\s*and\s*more\s*on\s*facebook|"
    r"this\s*facebook\s*post\s*is\s*no\s*longer\s*available|when\s*this\s*happens,\s*it'?s\s*usually\s*because|"
    r"đăng\s*nhập\s*facebook|bạn\s*phải\s*đăng\s*nhập)",
    flags=re.IGNORECASE,
)


def _is_login_or_fanpage_redirect(url: str, had_post_input: bool) -> Tuple[bool, bool]:
    """Return (is_login_or_checkpoint, is_fanpage_root_redirect)."""
    if not url:
        return False, False
    parsed = urlparse(url)
    path = parsed.path.strip("/").lower()
    if path.startswith(("login", "checkpoint", "recover", "reg")):
        return True, False
    if had_post_input and not is_facebook_post_url(url) and not _extract_post_ids_from_url(url):
        # Redirected away from a post/share URL to a fanpage root or home page
        return False, True
    return False, False


def _is_boilerplate_fb_text(text: str) -> bool:
    if not text or not text.strip():
        return True
    return bool(_LOGIN_OR_ERROR_TEXT_RE.search(text.strip()))


_INVALID_FB_TITLES = {
    "facebook",
    "log in",
    "login",
    "log into facebook",
    "log in to facebook",
    "log in or sign up",
    "sign up",
    "sign up for facebook",
    "đăng nhập",
    "đăng nhập facebook",
    "đăng nhập hoặc đăng ký",
    "create new account",
    "watch",
    "notifications",
    "home",
    "unknown",
    "error",
    "page not found",
    "content not found",
    "security check",
    "checkpoint",
    "just a moment",
    "just a moment...",
    "something went wrong",
    "meta",
}


_INVALID_FB_TITLE_PATTERNS = re.compile(
    r"(?:"
    r"^log\s*in(?:to)?(?:\s+to)?\s+facebook\b"
    r"|^facebook\s*[-–|]"
    r"|^đăng\s*nhập\b"
    r"|tạm thời bị chặn"
    r"|temporarily blocked"
    r"|security check"
    r"|confirm your identity"
    r"|this content isn't available"
    r"|page isn't available"
    r")",
    re.IGNORECASE,
)


def _clean_fanpage_name(raw_name: Optional[str]) -> Optional[str]:
    if not raw_name or not isinstance(raw_name, str):
        return None
    text = re.sub(r"\s+", " ", raw_name).strip()
    text = re.sub(r"^\(\d+\)\s*", "", text).strip()
    text = re.sub(r"\s*[\|\-–]\s*Facebook.*$", "", text, flags=re.IGNORECASE).strip()
    if not text or not re.search(r"\w", text, flags=re.UNICODE):
        return None
    if (
        text.lower() in _INVALID_FB_TITLES
        or _INVALID_FB_TITLE_PATTERNS.search(text)
        or _LOGIN_OR_ERROR_TEXT_RE.search(text)
    ):
        return None
    return text


async def _expand_see_more(page: Page) -> None:
    """Click 'See more' / 'Xem thêm' inside post containers if present."""
    for label in ("See more", "Xem thêm"):
        try:
            btn = page.locator(f'div[role="button"]:has-text("{label}")').first
            if await btn.is_visible(timeout=400):
                await btn.click(timeout=800)
                await page.wait_for_timeout(300)
                break
        except Exception:
            continue


async def _scrape_via_embed_plugin(page: Page, post_url: str) -> Optional[Dict[str, Any]]:
    """Fallback to Facebook's public post embed endpoint when www.facebook.com redirects guests."""
    from urllib.parse import quote

    embed_url = f"https://www.facebook.com/plugins/post.php?href={quote(post_url, safe='')}&show_text=true&width=500"
    try:
        await page.goto(embed_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        await page.wait_for_timeout(1500)
        embed_data = await page.evaluate(
            """() => {
                const bodyText = (document.body ? document.body.innerText : '') || '';
                const msgEl = document.querySelector('div[data-testid="post_message"], div.userContent, div._5pbx');
                const authorEl = document.querySelector('span.fwb a, a[href*="facebook.com/"]');
                const imgs = Array.from(document.querySelectorAll('img[src*="scontent"]'))
                    .filter(img => (img.naturalWidth || 0) >= 120 || img.width >= 120)
                    .map(img => img.src);
                return {
                    body_text: bodyText.trim(),
                    msg_text: msgEl ? (msgEl.innerText || '').trim() : '',
                    author_name: authorEl ? (authorEl.innerText || '').trim() : '',
                    author_href: authorEl ? (authorEl.getAttribute('href') || '').trim() : '',
                    media_urls: Array.from(new Set(imgs)),
                };
            }"""
        )
    except Exception:
        return None

    if not embed_data or _is_boilerplate_fb_text(embed_data.get("body_text", "")):
        return None
    msg_text = (embed_data.get("msg_text") or "").strip()
    media_urls = embed_data.get("media_urls") or []
    if not msg_text and not media_urls:
        return None

    pid = _extract_post_id_from_url(post_url) or hashlib.sha256(post_url.encode("utf-8")).hexdigest()[:16]
    return {
        "post_id": pid,
        "post_url": _clean_post_url(post_url, post_url),
        "content": msg_text,
        "media_urls": media_urls,
        "published_at": datetime.now(LOCAL_TIMEZONE),
        "author_name": embed_data.get("author_name") or None,
        "author_url": embed_data.get("author_href") or None,
    }


def _match_collected_post(
    collected_posts: Dict[str, Dict[str, Any]],
    expected_ids: Set[str],
    dom_fallback: Dict[str, Any],
    allow_single_unidentified: bool,
) -> Optional[Dict[str, Any]]:
    """
    Strictly match the target post inside `collected_posts`.
    Never blindly pick an unrelated fanpage post when expected IDs do not match.
    """
    if not collected_posts:
        return None

    if expected_ids:
        matches: List[Dict[str, Any]] = []
        for pid, post in collected_posts.items():
            cand_ids = set(post.get("_candidate_ids") or set()) | {pid}
            cand_ids.update(_extract_post_ids_from_url(post.get("post_url", "")))
            if expected_ids & cand_ids:
                matches.append(post)
        if matches:
            return max(matches, key=lambda x: len(x.get("content", "")))
        return None

    # No explicit post ID in URL (e.g. /share/p/... that didn't rewrite final_url):
    # Match against rendered DOM text snippets instead of grabbing an arbitrary feed post.
    snippet_source = (dom_fallback.get("msg_text") or dom_fallback.get("og_description") or "").strip()
    if len(snippet_source) >= 20 and not _is_boilerplate_fb_text(snippet_source):
        needle = re.sub(r"\s+", " ", snippet_source[:80]).lower()
        for post in collected_posts.values():
            hay = re.sub(r"\s+", " ", (post.get("content") or "")).lower()
            if needle[:40] in hay or (len(hay) >= 25 and hay[:40] in needle):
                return post

    if allow_single_unidentified and len(collected_posts) == 1:
        return next(iter(collected_posts.values()))
    return None


async def scrape_single_post(post_url: str, headless: bool = True) -> Dict[str, Any]:
    """
    Scrape a single public Facebook post URL without logging in.
    Returns dict with post_id, post_url, content, media_urls, published_at, fanpage_url, fanpage_name,
    and optional error if Facebook redirected or blocked guest access.
    """
    target_url = post_url.strip()
    if not target_url.startswith(("http://", "https://")):
        target_url = "https://" + target_url

    collected_posts: Dict[str, Dict[str, Any]] = {}
    final_url = target_url
    page_title: Optional[str] = None
    dom_fallback: Dict[str, Any] = {}
    embed_fallback: Optional[Dict[str, Any]] = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-notifications",
                "--no-sandbox",
            ],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = await context.new_page()

        async def handle_response(response: Response) -> None:
            if "/api/graphql" in response.url and response.status == 200:
                try:
                    text = await response.text()
                    for line in text.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = json.loads(line)
                            _walk_graphql_for_posts(data, final_url, "", collected_posts)
                        except json.JSONDecodeError:
                            continue
                except Exception:
                    pass

        page.on("response", handle_response)

        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            try:
                await page.wait_for_selector(
                    'div[data-ad-preview="message"], div[data-ad-comet-preview="message"], div[data-ad-rendering-role="story_message"], div[role="article"]',
                    timeout=5000,
                )
            except Exception:
                pass
            await page.wait_for_timeout(1500)
            final_url = page.url or target_url

            raw_title = await page.title()
            page_title = _clean_fanpage_name(raw_title)

            await _dismiss_login_popup(page)
            await _expand_see_more(page)

            scripts_json = await page.evaluate(
                """() => {
                    return Array.from(document.querySelectorAll('script[type="application/json"]'))
                        .map(s => s.textContent)
                        .filter(Boolean);
                }"""
            )
            for raw_script in scripts_json:
                try:
                    data = json.loads(raw_script)
                    _walk_graphql_for_posts(data, final_url, "", collected_posts)
                except Exception:
                    continue

            expected_pre = _extract_post_ids_from_url(target_url) | _extract_post_ids_from_url(final_url)
            pre_match = _match_collected_post(collected_posts, expected_pre, {}, allow_single_unidentified=False)
            if not pre_match:
                await page.evaluate("window.scrollBy(0, 500)")
                await page.wait_for_timeout(1800)

            dom_fallback = await page.evaluate(
                """() => {
                    const meta = (prop) => {
                        const el = document.querySelector(`meta[property="${prop}"], meta[name="${prop}"]`);
                        return el ? (el.getAttribute('content') || '').trim() : '';
                    };
                    const msgEl = document.querySelector(
                        'div[data-ad-preview="message"], div[data-ad-comet-preview="message"], div[data-ad-rendering-role="story_message"]'
                    );
                    const articleEl = document.querySelector('div[role="article"]');
                    const authorLink = articleEl ? articleEl.querySelector('h2 a[href*="facebook.com"], h3 a[href*="facebook.com"], strong a[href*="facebook.com"]') : null;
                    const canonicalEl = document.querySelector('link[rel="canonical"]');
                    const root = articleEl || document;
                    const imgs = Array.from(root.querySelectorAll('img[src*="scontent"]'))
                        .filter(img => !img.src.includes('emoji') && ((img.naturalWidth || 0) >= 150 || img.width >= 150))
                        .map(img => img.src);
                    return {
                        og_title: meta('og:title'),
                        og_description: meta('og:description'),
                        meta_description: meta('description'),
                        og_image: meta('og:image'),
                        canonical: canonicalEl ? (canonicalEl.getAttribute('href') || '').trim() : '',
                        msg_text: msgEl ? (msgEl.innerText || '').trim() : '',
                        article_text: articleEl ? (articleEl.innerText || '').trim() : '',
                        author_href: authorLink ? (authorLink.getAttribute('href') || '').trim() : '',
                        author_text: authorLink ? (authorLink.innerText || '').trim() : '',
                        dom_images: Array.from(new Set(imgs)),
                    };
                }"""
            )

            expected_ids_in_browser = (
                _extract_post_ids_from_url(target_url)
                | _extract_post_ids_from_url(final_url)
                | _extract_post_ids_from_url(dom_fallback.get("canonical", ""))
            )
            login_redir, page_redir = _is_login_or_fanpage_redirect(final_url, is_facebook_post_url(target_url))
            matched_in_browser = _match_collected_post(
                collected_posts,
                expected_ids_in_browser,
                dom_fallback,
                allow_single_unidentified=(not login_redir and not page_redir),
            )
            has_usable_dom = (
                not login_redir
                and not page_redir
                and not _is_boilerplate_fb_text(
                    dom_fallback.get("msg_text") or dom_fallback.get("og_description") or dom_fallback.get("meta_description") or ""
                )
            )
            if matched_in_browser is None and not has_usable_dom:
                # Try Facebook's public post embed endpoint when direct guest visit redirected or hid the post
                embed_target = final_url if is_facebook_post_url(final_url) else target_url
                if is_facebook_post_url(embed_target):
                    embed_fallback = await _scrape_via_embed_plugin(page, embed_target)
        finally:
            await context.close()
            await browser.close()

    expected_ids = (
        _extract_post_ids_from_url(target_url)
        | _extract_post_ids_from_url(final_url)
        | _extract_post_ids_from_url(dom_fallback.get("canonical", ""))
    )
    login_redir, page_redir = _is_login_or_fanpage_redirect(final_url, is_facebook_post_url(target_url))
    canonical_url = dom_fallback.get("canonical", "")
    is_post_page = (
        bool(expected_ids)
        or is_facebook_post_url(final_url)
        or is_facebook_post_url(canonical_url)
    )

    chosen = _match_collected_post(
        collected_posts,
        expected_ids,
        dom_fallback,
        allow_single_unidentified=(not login_redir and not page_redir and is_post_page),
    )

    if chosen is None and embed_fallback is not None:
        chosen = embed_fallback

    error_reason: Optional[str] = None
    if chosen is None:
        if login_redir:
            error_reason = "Facebook redirected to a login/checkpoint wall; cannot view this post without logging in."
        elif page_redir:
            error_reason = (
                f"Facebook redirected guest visit to '{final_url}' (fanpage/home) and did not load the target post."
            )
        else:
            raw_msg = (dom_fallback.get("msg_text") or "").strip()
            raw_og = (dom_fallback.get("og_description") or dom_fallback.get("meta_description") or "").strip()
            fallback_text = ""
            for cand in (raw_msg, raw_og):
                if cand and not _is_boilerplate_fb_text(cand):
                    fallback_text = cand
                    break

            fallback_media: List[str] = []
            if dom_fallback.get("og_image") and "rsrc.php" not in dom_fallback["og_image"]:
                fallback_media.append(dom_fallback["og_image"])
            for img in dom_fallback.get("dom_images") or []:
                if img not in fallback_media:
                    fallback_media.append(img)

            if fallback_text or (fallback_media and is_post_page):
                canon_or_final = canonical_url or final_url
                pid = next(iter(expected_ids), None) or hashlib.sha256(canon_or_final.encode("utf-8")).hexdigest()[:16]
                chosen = {
                    "post_id": pid,
                    "post_url": _clean_post_url(canon_or_final, target_url),
                    "content": fallback_text,
                    "media_urls": fallback_media,
                    "published_at": datetime.now(LOCAL_TIMEZONE),
                    "author_name": dom_fallback.get("author_text") or None,
                    "author_url": dom_fallback.get("author_href") or None,
                }
            else:
                if expected_ids and collected_posts:
                    error_reason = (
                        f"Target post {next(iter(expected_ids))} was not present in the loaded page payload "
                        f"(Facebook loaded {len(collected_posts)} other fanpage post(s) instead; post may require login or be restricted)."
                    )
                else:
                    error_reason = "Could not extract any text or media from this post (may require login, be private/deleted, or be blocked)."

    if chosen is None:
        return {
            "post_id": next(iter(expected_ids), None),
            "post_url": target_url,
            "content": "",
            "media_urls": [],
            "published_at": None,
            "fanpage_url": _derive_fanpage_url_from_url(final_url) or _derive_fanpage_url_from_url(target_url),
            "fanpage_name": page_title or None,
            "error": error_reason,
        }

    chosen.pop("_candidate_ids", None)
    fanpage_url = (
        _derive_fanpage_url_from_url(chosen.get("author_url"))
        or _derive_fanpage_url_from_url(chosen.get("post_url"))
        or _derive_fanpage_url_from_url(dom_fallback.get("author_href"))
        or _derive_fanpage_url_from_url(canonical_url)
        or _derive_fanpage_url_from_url(final_url)
    )
    fanpage_name = None
    og_title_candidate = dom_fallback.get("og_title") or ""
    if "|" in og_title_candidate:
        og_title_candidate = og_title_candidate.split("|")[0].strip()
    for raw_name in (
        chosen.get("author_name"),
        dom_fallback.get("author_text"),
        og_title_candidate,
        page_title,
    ):
        cleaned = _clean_fanpage_name(raw_name)
        if cleaned:
            fanpage_name = cleaned
            break

    chosen["fanpage_url"] = fanpage_url
    chosen["fanpage_name"] = fanpage_name
    return chosen


# ---------------------------------------------------------------------------
# Event Extractor (from events/extractor.py)
# ---------------------------------------------------------------------------
EVENT_KEYWORDS = [
    # High-purity registration & call-to-action phrases (92.5%-100% Event purity)
    "dang ky tham gia", "dang ky tham du", "dang ki tham gia", "dang ky tham quan",
    "link dang ky", "form dang ky", "mo don dang ky", "mo link dang ky", "mo form dang ky",
    "thoi han dang ky", "nhanh tay dang ky", "cach thuc dang ky", "cong dang ky",
    "mo don", "dong don", "han chot dang ky", "link se dong som", "mo he thong thi",
    # High-purity schedule, venue & participant structure labels (80%-100% Event purity)
    "thoi gian to chuc", "dia diem to chuc", "doi tuong tham gia", "hinh thuc dang ky",
    "thoi gian dien ra", "dia diem dien ra", "co cau giai thuong",
    "dang ky", "dang ki", "dia diem", "thoi gian", "thoi han", "han chot",
    "tham gia", "tham du", "tham quan", "dien gia", "bao cao vien", "khach moi",
    "le phi", "mien phi", "the le", "ban to chuc", "hoi truong", "doi tuong",
    "so luong", "hinh thuc", "vao luc",
    # High-purity Vietnamese event types & activities (80%-100% Event purity)
    "hoi thao", "toa dam", "giao luu", "cuoc thi", "trien lam", "ngay hoi",
    "dem nhac", "chao tan", "tuyen quan", "tuyen thanh vien", "tuyen cong tac vien",
    "hien mau", "le khai mac", "gala chung ket", "chung ket", "so khao",
    "sinh hoat chuyen de", "chuyen de", "chien dich", "thi thu toeic", "truong he",
    # English event types, actions & online platforms (90%-100% Event purity)
    "workshop", "seminar", "webinar", "hackathon", "contest", "competition",
    "meetup", "festival", "concert", "talkshow", "bootcamp", "orientation",
    "minigame", "esports", "tournament", "career fair", "job fair", "open day",
    "livestream", "summer school", "mini-course", "register", "registration",
    "ticket", "rsvp", "save the date", "timeline", "deadline", "check in", "checkin",
    "online", "offline", "zoom", "google meet", "ms teams",
]

SYSTEM_REGULATIONS = """You analyze Facebook fanpage posts (including shared posts) and emails from university clubs, faculties, unions (Đoàn - Hội), and organizations.
Decide whether each post announces, promotes, opens registration for, or gives schedule/round details for a specific UPCOMING or ONGOING EVENT or organized student activity.

CONTENT REASONING PROTOCOL (follow these 4 steps before setting `is_event`):
Step 1 — Temporal & Intent Direction:
  Determine whether the post is FORWARD-LOOKING / ACTIVE (inviting students to register, participate, submit entries, attend an upcoming session, or reminding them of an active deadline/round) vs. RETROSPECTIVE / PASSIVE (summarizing a finished event, thanking sponsors/speakers after the fact, announcing final winners/results already sent to email, or congratulating award recipients). Retrospective posts with no new upcoming session or registration action MUST be `is_event: false`.
Step 2 — Entity & Activity Nature (Event vs. Non-Event Archetypes):
  Verify that the subject is an actual organized student event, workshop, seminar, competition, campaign, club recruitment drive, or live info session — NOT an internal union committee governance congress, a pure scholarship dossier submission notice, a full-time corporate job ad, a routine academic/dormitory/graduation administrative announcement, or a companion Q&A handbook/storytelling post.
Step 3 — Actionable Schedule or Participation Evidence:
  Confirm that the post provides actionable participation cues (e.g., event date/time/venue, registration form/link, application timeline, or competition round instructions) or is an official countdown/speaker reveal for an upcoming event.
Step 4 — Concise Reasoning Justification (`reason`):
  Write a 1-sentence explanation in `reason` stating why the post qualifies as an active/upcoming event (`is_event: true`) or which disqualification rule applies (`is_event: false`).

WHAT COUNTS AS AN EVENT (`is_event: true`):
1. Academic & skill sessions: Workshops, seminars, webinars, talkshows, academic talks (`báo cáo viên`, `chuyên đề`), summer schools (`trường hè`), mini-courses, orientations (`chào tân sinh viên`), mock exams (`thi thử TOEIC`), and company/campus tours (`tham quan doanh nghiệp`).
2. Competitions & tournaments: Contests, hackathons, academic olympiads, sports/esports tournaments, talent contests, and minigames (`cuộc thi`, `hội thi`, `giải đấu`, `mở hệ thống thi`, `vòng sơ khảo`, `gala chung kết`) that invite participation, submissions, or spectators for an active/upcoming round.
3. Cultural, community & career events: Concerts, music nights (`đêm nhạc`), festivals (`ngày hội`, `lễ hội`), career/job fairs (`ngày hội việc làm`), exhibitions (`triển lãm`), volunteer campaigns recruiting participants/volunteers (`chiến dịch tình nguyện`, `hiến máu`), and periodic student health checkup drives (`đăng ký khám sức khỏe`) with specific dates/venues.
4. Recruitment & info sessions with actionable timelines: Club/committee/volunteer recruitment drives (`tuyển quân`, `tuyển thành viên`, `tuyển cộng tác viên`) that open applications (`mở đơn`, `form đăng ký`) or specify timelines, and live scholarship/exchange information sessions (`buổi giới thiệu học bổng/chương trình`) with a scheduled time and venue.
5. Active event reminders & countdowns: Registration countdowns ("24 giờ cuối", "còn X ngày", "gia hạn đăng ký", "nhắc nhở diễn ra") or speaker/topic introductions (`giới thiệu báo cáo viên/diễn giả`) for an upcoming session.

WHAT DOES NOT COUNT (`is_event: false`) — Empirical Disqualification Rules:
1. Post-event recaps, round results, retrospective thank-you notes & semester summaries: Posts summarizing an activity that has ALREADY finished (`[RECAP]`, `tổng kết`, `đã chính thức khép lại`, `hành trình khép lại`, `nhìn lại chặng đường`, `kết quả chung cuộc`, `kết quả ngày thi đấu`, `gửi lời cảm ơn`, `tri ân Ban Chấp hành`, `trân trọng cảm ơn các Quý doanh nghiệp đã đồng hành`), or announcing that recruitment results were sent to email (`kết quả tuyển dụng đã gửi đến hộp thư`), with NO new registration or upcoming spectator/participant session.
2. Honor rolls, award announcements & congratulations: Posts congratulating or honoring individuals/teams for achievements, awards, or research publications (`chúc mừng`, `xin chúc mừng`, `vinh danh`, `đạt danh hiệu Sinh viên 5 Tốt`, `đạt giải thưởng`, `Quả Cầu Vàng`, `công bố khoa học trên hội nghị`) with no organized event to attend.
3. Internal organizational/union governance congresses: Posts announcing or celebrating internal committee congresses or administrative decision rollouts (`Hội nghị mở rộng Ban Chấp hành`, `Chào mừng Hội nghị mở rộng Đoàn/Hội`, `kiện toàn đội ngũ Ban Chấp hành`, `Hội nghị triển khai quyết định thành lập...`) that are internal delegate/governance meetings rather than events open for general student registration (note: an open academic conference like `Hội nghị Khoa học` inviting student attendees/papers IS `is_event: true`).
4. Pure scholarship / financial-aid dossier applications: Posts that only announce financial aid or scholarship application requirements/quotas (`Thông báo học bổng Quỹ Vượt khó`, `Học bổng Vallet`, `nhận hồ sơ đăng ký học bổng`) where students submit dossiers rather than attending a scheduled event (whereas a live `Buổi giới thiệu học bổng` seminar IS `is_event: true`).
5. Routine academic, administrative, dormitory & graduation notices: Classroom/room changes (`phòng máy sửa chữa`), final exam schedules (`lịch thi cuối kỳ`), exam integrity rules (`phòng chống gian lận thi cử`), course-end grading notices, dormitory stay registration (`đăng ký ở Ký túc xá`), administrative graduation ceremonies (`lễ trao bằng tốt nghiệp`), and general administrative/policy surveys (`đóng góp ý kiến - hiến kế`, `khảo sát nguyện vọng kết nạp Đảng`).
6. Corporate job postings & non-scheduled companion/promotional posts: Full-time corporate job/trainee recruitment (`Engineering Development Program`, graduate job openings without a campus talk/workshop), storytelling rubrics (`[Tin tốt chuyện đẹp]`), holiday/commemorative greetings (`20/10`, `20/11`, `10/10 Ngày Chuyển đổi số`), fundraising booth price menus (`gian hàng gây quỹ` listing drink prices without an event registration/schedule), or companion Q&A/handbook posts (`Cẩm nang... giải đáp thắc mắc trong từng bức ảnh`) that do not announce a distinct event session.

EXTRACTION RULES:
- `reason`: 1 concise sentence explaining the content reasoning decision (why `is_event` is true or which disqualification rule made it false).
- `title`: Extract the canonical name of the event/competition/program. KEEP any meaningful bracketed text intact (e.g., "[BUILD ON STELLAR] — FROM ZERO TO YOUR FIRST WEB3 APP", "[THI THỬ TOEIC – VƯƠN TẦM HỘI NHẬP]", "[TRAO YÊU THƯƠNG - ĐỔI HẠNH PHÚC]"). Read past any initial salutations (like "Thân chào các bạn") or meta tags (like "[HỖ TRỢ TRUYỀN THÔNG]", "[HTTT]", "[THÔNG BÁO]") to find the real event title in the content body. Do NOT use countdown hooks like "3 DAYS LEFT" or generic meta tags alone as the title.
- `description`: Do NOT copy the raw intro or first 1-2 sentences of the post. Write a concise 2-3 sentence summary in the same language as the post summarizing what the event is about, its main activities, and what benefits/value students will get (skills, prizes, certificates, điểm rèn luyện, networking). Only state facts explicitly in the post; never invent details.
- `event_start_datetime`: Time that the event occurs in ISO 8601 (+07:00), e.g. "2026-11-22T16:00:00+07:00". Look for specific date and time cues (including `XXh-YYh`, `XXhMM - YYhNN`, `XX:MM - YY:MM`, `từ XXh đến YYh`, `XXg - YYg`, `AM/PM`, resolving relative/DD-MM dates using `published_at`). Take `XXh` as the start time; if no time is mentioned, use `00:00:00+07:00` of the start day.
- `event_end_datetime`: Time that the event ends in ISO 8601 (+07:00), e.g. "2026-11-22T20:30:00+07:00". Look for specific date and time cues (`YYh` from `XXh-YYh`). If no end time is mentioned, use `23:59:00+07:00` of the end day (or of the event day if single-day).
- `reg_start_datetime`: Time that the event opens for registration in ISO 8601 (+07:00). Extract ONLY if explicitly stated in the post (e.g. "mở đơn từ 19/10/2026", "mở cổng đăng ký lúc 18h ngày 01/11"); otherwise return null.
- `reg_end_datetime`: Time that the event closes for registration in ISO 8601 (+07:00). Extract ONLY if explicitly stated in the post (e.g. "đóng đơn 30/10/2026", "hạn chót đăng ký 23h59 ngày 15/11", "deadline 20/11", "trước 23g59 ngày 16/11"); otherwise return null.
- `location`: Search carefully for location cues (especially lines starting with `Địa điểm`, `Dia diem`, `Địa chỉ`, `Dia chi`, `Nơi tổ chức`, `Location`, `Venue`, `Address`, `Phòng`, `Hội trường`, `Giảng đường`, `Cơ sở`, including with leading emojis like `📍` or `🏢`). Take the full venue/location line content. If no location is mentioned, use null.
- `fee`: Search carefully for lines containing `Lệ phí`, `Le phi`, `Phí tham gia`, `Phí đăng ký`, `Giá vé`, `Chi phí`, `Fee`, `Ticket`, or `miễn phí`/`free` (including after emojis like `💸`). Extract the fee value from that line (e.g., "25.000 VNĐ/sinh viên", "100.000 VND", "miễn phí"); use null if not mentioned.
- `registration_url`: ONLY a literal registration/application/form URL from the post text (e.g., forms.gle, bit.ly, tinyurl.com, luma.com). NEVER use general contact/social footer links (like fb.com/<page> or tiktok.com/...) as `registration_url`; use null if no registration link is present.

Return ONLY a JSON object with exactly these keys (use null when unknown, never invent data):
{
  "is_event": boolean,
  "reason": string,
  "title": string|null,
  "description": string|null,
  "event_start_datetime": string|null,
  "event_end_datetime": string|null,
  "reg_start_datetime": string|null,
  "reg_end_datetime": string|null,
  "location": string|null,
  "organizer": string|null,
  "registration_url": string|null,
  "fee": string|null,
  "is_online": boolean|null,
  "target_audience": string[]|null,
  "category": string[],
  "format": string[]
}"""

USER_POST_TEMPLATE = """Post published at: {published_at}
Fanpage / Source: {fanpage}
Post text:
\"\"\"
{content}
\"\"\""""

USER_BATCH_POSTS_TEMPLATE = """Analyze each of the {count} posts below independently using the JSON schema in your system instructions.
Return ONLY a JSON array with one object per post in the same order, adding `"post_id": "<post_id>"` to each object.

{posts_block}"""

GROQ_SYSTEM_PROMPT = """You extract university event details from posts into JSON.
Determine whether each post announces or promotes an UPCOMING or ONGOING EVENT or organized student activity (`is_event: true`).
DO NOT count finished event recaps (`[RECAP]`, `tổng kết`), honor rolls/awards, internal committee congresses, administrative notices, or job ads (`is_event: false`).

Extraction schema (return ONLY valid JSON):
{
  "is_event": boolean,
  "reason": "1 concise sentence explaining the decision",
  "title": "Canonical event name, keeping bracketed text (e.g. [BUILD ON STELLAR]) and stripping salutations/meta tags",
  "description": "2-3 sentence summary of main activities, dates, and student benefits/prizes",
  "event_start_datetime": "ISO 8601 (+07:00) or null",
  "event_end_datetime": "ISO 8601 (+07:00) or null",
  "reg_start_datetime": "ISO 8601 (+07:00) or null",
  "reg_end_datetime": "ISO 8601 (+07:00) or null",
  "location": "Venue/address or null",
  "organizer": "Organizing unit or null",
  "registration_url": "Literal registration form link or null",
  "fee": "Fee/ticket cost or null",
  "is_online": boolean or null,
  "target_audience": ["array of strings"] or null,
  "category": ["array of strings"],
  "format": ["array of strings"]
}"""

PROMPT = SYSTEM_REGULATIONS + "\n\n" + USER_POST_TEMPLATE

_LOCATION_LABEL_RE = re.compile(
    r"(?:^|\b)(?:dia\s*diem|dia\s*chi|noi\s*to\s*chuc|location|venue|address|room|hall|phong|hoi\s*truong|giang\s*duong|co\s*so)\b",
    flags=re.IGNORECASE,
)
_FEE_LINE_RE = re.compile(
    r"\b(?:le\s*phi|phi\s*tham\s*(?:gia|du)|phi\s*dang\s*ky|gia\s*ve|chi\s*phi|fee|ticket\s*price)\b",
    flags=re.IGNORECASE,
)
_DESC_SKIP_LINE_RE = re.compile(
    r"^(?:_+|-+|=+|\*+|#|moi thac mac|lien he|fanpage|email|tiktok|instagram|website|link dang ky|"
    r"the le|thoi gian|dia diem|doi tuong|le phi|noi dung chuyen khoan)\b",
    flags=re.IGNORECASE,
)
_DESC_LEARN_ACTIVITY_RE = re.compile(
    r"\b(?:noi dung|chu de|topic|agenda|hoat dong|trai nghiem|kham pha|tim hieu|hoc duoc|hoc hoi|"
    r"chia se|kien thuc|ky nang|co co hoi|den voi|ban se|workshop|talkshow|giao luu|thuc hanh|thi dau)\b",
    flags=re.IGNORECASE,
)
_DESC_BENEFIT_RE = re.compile(
    r"\b(?:quyen loi|loi ich|gia tri|giai thuong|co cau giai thuong|quan quan|a quan|"
    r"diem ren luyen|drl|giay chung nhan|certificate|qua tang|hoc bong|benefit|prize|reward|gain|learn)\b",
    flags=re.IGNORECASE,
)


def _strip_accents(text: str) -> str:
    text = text.replace("đ", "d").replace("Đ", "D")
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def count_event_keywords(content: str) -> int:
    """Count distinct event keywords found in text (accent- and case-insensitive)."""
    haystack = _strip_accents(content or "").lower()
    return sum(
        1 for k in set(EVENT_KEYWORDS)
        if re.search(r"\b" + re.escape(k) + r"\b", haystack)
    )


STRONG_EVENT_SIGNAL_RE = re.compile(
    r"\b(dang ky tham gia|dang ky tham du|dang ki tham gia|dang ky tham quan|"
    r"link dang ky|form dang ky|mo don dang ky|mo link dang ky|mo form dang ky|"
    r"thoi han dang ky|nhanh tay dang ky|cach thuc dang ky|cong dang ky|"
    r"mo don|dong don|han chot dang ky|link se dong som|mo he thong thi|"
    r"thoi gian to chuc|dia diem to chuc|doi tuong tham gia|hinh thuc dang ky|"
    r"thoi gian dien ra|dia diem dien ra|"
    r"workshop|seminar|webinar|hackathon|contest|competition|meetup|festival|concert|"
    r"talkshow|bootcamp|orientation|open day|livestream|career fair|job fair|"
    r"summer school|mini-course|thi thu toeic|truong he|"
    r"hoi thao|toa dam|cuoc thi|giao luu|trien lam|dem nhac|chao tan|"
    r"tuyen quan|tuyen thanh vien|tuyen cong tac vien|hien mau|le khai mac|gala chung ket)\b",
    flags=re.IGNORECASE,
)

NON_EVENT_SIGNAL_RE = re.compile(
    r"(\[\s*recap\b|\brecap\s+vong\b|\bchuc\s+mung\b|\bvinh\s+danh\b|\btri\s+an\s+ban\s+chap\s+hanh\b|"
    r"\btong\s+ket\s+(?:tieu\s+ban|ket\s+qua|mot\s+hanh\s+trinh)\b|\bket\s+qua\s+(?:chung\s+cuoc|tuyen\s+dung)\b|"
    r"\bda\s+chinh\s+thuc\s+khep\s+lai\b|\bhanh\s+trinh\s+khep\s+lai\b|\bkhep\s+lai\s+nhung\s+man\s+tranh\s+tai\b|"
    r"\btin\s+tot\s+chuyen\s+dep\b|\bdong\s+gop\s+y\s+kien\s*-\s*hien\s+ke\b|"
    r"\bkhao\s+sat\s+nguyen\s+vong\s+ket\s+nap\s+dang\b|\bdang\s+ky\s+o\s+ky\s+tuc\s+xa\b|"
    r"\ble\s+trao\s+bang\s+tot\s+nghiep\b|\blich\s+thi\s+chi\s+tiet\s+cuoi\s+ky\b|"
    r"\bphong\s+chong\s+gian\s+lan\b|\bphong\s+may\s+.*?\s+sua\s+chua\b)",
    flags=re.IGNORECASE,
)

ACTIVE_REGISTRATION_OVERRIDE_RE = re.compile(
    r"\b(mo\s+don|mo\s+link\s+dang\s+ky|mo\s+form\s+dang\s+ky|dang\s+ky\s+tham\s+gia|dang\s+ky\s+tham\s+du)\b",
    flags=re.IGNORECASE,
)

_INTERNAL_CONGRESS_RE = re.compile(
    r"\b(hoi\s+nghi\s+mo\s+rong\s+ban\s+chap\s+hanh|chao\s+mung\s+hoi\s+nghi\s+mo\s+rong|"
    r"hoi\s+nghi\s+trien\s+khai\s+cac\s+quyet\s+dinh|kien\s+toan\s+doi\s+ngu\s+ban\s+chap\s+hanh)\b",
    flags=re.IGNORECASE,
)
_SCHOLARSHIP_DOSSIER_RE = re.compile(
    r"\b(thong\s+bao\s+ve\s+hoc\s+bong|hoc\s+bong\s+quy\s+vuot\s+kho|hoc\s+bong\s+vallet|"
    r"nhan\s+ho\s+so\s+dang\s+ky\s+hoc\s+bong|thoi\s+gian\s+xet\s+hoc\s+bong|nop\s+ho\s+so\s+hoc\s+bong)\b",
    flags=re.IGNORECASE,
)
_LIVE_SESSION_CUE_RE = re.compile(
    r"\b(buoi\s+gioi\s+thieu|buoi\s+training|exclusive\s+training|workshop|seminar|webinar|talkshow|"
    r"hoi\s+thao|toa\s+dam|giao\s+luu|info\s+session|dien\s+gia|bao\s+cao\s+vien|zoom\s+meeting|google\s+meet)\b",
    flags=re.IGNORECASE,
)
_CORPORATE_JOB_PROGRAM_RE = re.compile(
    r"\b(engineering\s+development\s+program|management\s+trainee|graduate\s+trainee\s+program|"
    r"sinh\s+vien\s+nam\s+cuoi\s+hoac\s+vua\s+tot\s+nghiep[\s\S]{0,160}ung\s+tuyen)\b",
    flags=re.IGNORECASE,
)
_RETROSPECTIVE_GRATITUDE_RE = re.compile(
    r"\b(tran\s+trong\s+cam\s+on\s+cac\s+quy\s+doanh\s+nghiep|da\s+danh\s+thoi\s+gian\s+dong\s+hanh|"
    r"cac\s+dien\s+gia\s+da\s+mang\s+den|khep\s+lai\s+chang\s+duong)\b",
    flags=re.IGNORECASE,
)
_COMPANION_HANDBOOK_RE = re.compile(
    r"\b(cam\s+nang\s+[\w\s]{1,30}?tram\s+dung\s+nho|giai\s+dap\s+tron\s+ven\s+trong\s+tung\s+buc\s+anh)\b",
    flags=re.IGNORECASE,
)
_EXPLICIT_SCHEDULE_OR_REG_LABEL_RE = re.compile(
    r"\b(thoi\s+gian\s*(?:to\s+chuc|dien\s+ra)?\s*:|dia\s+diem\s*(?:to\s+chuc|dien\s+ra)?\s*:|"
    r"link\s+dang\s+ky|form\s+dang\s+ky|dang\s+ky\s+tham\s+(?:gia|du|quan)|mo\s+don)\b",
    flags=re.IGNORECASE,
)


def reason_about_post_content(content: str) -> Tuple[bool, str]:
    """Deterministic multi-step Content Reasoning engine mirrored from `events/extractor.py`."""
    if not content or not content.strip():
        return False, "disqualified:empty_post"

    stripped = _strip_accents(content).lower()
    has_active_reg_override = bool(ACTIVE_REGISTRATION_OVERRIDE_RE.search(stripped))

    m_non = NON_EVENT_SIGNAL_RE.search(stripped)
    if m_non and not has_active_reg_override:
        return False, f"disqualified:non_event_signal ({m_non.group(0).strip()})"

    m_cong = _INTERNAL_CONGRESS_RE.search(stripped)
    if m_cong and not has_active_reg_override:
        return False, f"disqualified:internal_governance_congress ({m_cong.group(0).strip()})"

    m_schol = _SCHOLARSHIP_DOSSIER_RE.search(stripped)
    if m_schol and not _LIVE_SESSION_CUE_RE.search(stripped):
        return False, f"disqualified:scholarship_dossier_notice ({m_schol.group(0).strip()})"

    m_job = _CORPORATE_JOB_PROGRAM_RE.search(stripped)
    if m_job and not _LIVE_SESSION_CUE_RE.search(stripped):
        return False, f"disqualified:corporate_job_posting ({m_job.group(0).strip()})"

    m_retro = _RETROSPECTIVE_GRATITUDE_RE.search(stripped)
    if m_retro and not has_active_reg_override and not _EXPLICIT_SCHEDULE_OR_REG_LABEL_RE.search(stripped):
        return False, f"disqualified:retrospective_summary ({m_retro.group(0).strip()})"

    m_hand = _COMPANION_HANDBOOK_RE.search(stripped)
    if m_hand:
        return False, f"disqualified:companion_qa_handbook ({m_hand.group(0).strip()})"

    m_strong = STRONG_EVENT_SIGNAL_RE.search(stripped)
    if m_strong:
        return True, f"qualified:strong_event_signal ({m_strong.group(0).strip()})"

    kw_count = count_event_keywords(content)
    if kw_count >= max(3, MIN_KEYWORD_MATCHES + 1):
        return True, f"qualified:keyword_density (kw_count={kw_count})"

    if kw_count >= MIN_KEYWORD_MATCHES:
        has_date_time_or_link = bool(
            re.search(
                r"(\b\d{1,2}[/-]\d{1,2}(?:[/-]20\d{2})?\b|\b20\d{2}-\d{2}-\d{2}\b|\b\d{1,2}[:hg]\d{2}\b|https?://)",
                content,
                flags=re.IGNORECASE,
            )
        )
        if has_date_time_or_link:
            return True, f"qualified:keywords_with_schedule_or_link (kw_count={kw_count})"

    return False, f"disqualified:insufficient_event_signals (kw_count={kw_count})"


def looks_event_related(content: str) -> bool:
    """Empirical content-reasoning pre-filter mirrored from `events/extractor.py`."""
    is_candidate, _ = reason_about_post_content(content)
    return is_candidate


_CACHED_GEMINI_V2_MODELS: Dict[str, List[str]] = {}


def _fetch_gemini_models_v2(api_key: str = "") -> List[str]:
    """Dynamically query Google Gemini API to fetch available models and select those starting with 2.

    Prevents HTTP 404 (model not found) by ensuring we only request active models enabled for this key.
    """
    global _CACHED_GEMINI_V2_MODELS
    if not api_key:
        return [m for m in FALLBACK_GEMINI_MODELS if "2" in m]

    key_hash = api_key[-8:] if len(api_key) > 8 else api_key
    if key_hash in _CACHED_GEMINI_V2_MODELS:
        return _CACHED_GEMINI_V2_MODELS[key_hash]

    discovered_v2: List[str] = []
    try:
        req = urllib.request.Request(
            GEMINI_LIST_MODELS_URL,
            headers={"x-goog-api-key": api_key, "User-Agent": "EventExtractor/2.0"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        for item in data.get("models", []):
            name = item.get("name") or ""
            model_id = name.replace("models/", "").strip()
            methods = item.get("supportedGenerationMethods") or []
            if "generateContent" not in methods:
                continue

            version = str(item.get("version") or "")
            # Match models starting with 2 (e.g. gemini-2.*, version 2.*)
            if model_id.startswith("gemini-2") or version.startswith("2") or model_id.startswith("2"):
                discovered_v2.append(model_id)
    except Exception:
        pass

    # Sort to prioritize flash-lite (lightweight/fast/reliable free tier), then flash, then others
    def _rank_v2(m_name: str) -> int:
        n = m_name.lower()
        if "flash-lite" in n:
            return 0
        if "flash" in n:
            return 1
        return 2

    discovered_v2.sort(key=_rank_v2)

    # Fallback to curated 2.x list if network discovery failed
    if not discovered_v2:
        discovered_v2 = [m for m in FALLBACK_GEMINI_MODELS if "2" in m] or FALLBACK_GEMINI_MODELS

    _CACHED_GEMINI_V2_MODELS[key_hash] = discovered_v2
    return discovered_v2


def _candidate_gemini_models(api_key: str = "") -> List[str]:
    global _WORKING_GEMINI_MODEL
    v2_models = _fetch_gemini_models_v2(api_key)

    if _WORKING_GEMINI_MODEL:
        return [_WORKING_GEMINI_MODEL] + [m for m in v2_models if m != _WORKING_GEMINI_MODEL]

    raw = (os.environ.get("GEMINI_MODEL") or DEFAULT_GEMINI_MODEL or "").strip().strip("'\"")
    if raw.startswith("models/"):
        raw = raw[len("models/"):].strip()

    retired_prefixes = ("gemini-1.0", "gemini-pro-vision")
    candidates: List[str] = []
    if raw and not raw.lower().startswith(retired_prefixes):
        candidates.append(raw)

    for m in v2_models:
        if m not in candidates:
            candidates.append(m)

    for fallback in FALLBACK_GEMINI_MODELS:
        if fallback not in candidates:
            candidates.append(fallback)

    return candidates


def _get_or_create_cached_regulations(model: str, api_key: str) -> Optional[str]:
    """Create or reuse a server-side Gemini `cachedContents` handle for `SYSTEM_REGULATIONS`."""
    if not GEMINI_ENABLE_CONTEXT_CACHE or not api_key or model in _CACHE_UNSUPPORTED_MODELS:
        return None

    now = time.time()
    cached_entry = _CACHED_SYSTEM_HANDLES.get(model)
    if cached_entry and cached_entry[1] - 60 > now:
        return cached_entry[0]

    full_model = model if model.startswith("models/") else f"models/{model}"
    ttl_s = max(300, GEMINI_CACHE_TTL_SECONDS)
    payload_bytes = json.dumps(
        {
            "model": full_model,
            "displayName": "fb_rss_event_extractor_regulations_v2",
            "systemInstruction": {
                "parts": [{"text": SYSTEM_REGULATIONS}],
            },
            "ttl": f"{ttl_s}s",
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        GEMINI_CACHED_CONTENTS_URL,
        data=payload_bytes,
        headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        cache_name = data.get("name")
        if isinstance(cache_name, str) and cache_name.strip():
            _CACHED_SYSTEM_HANDLES[model] = (cache_name.strip(), now + ttl_s)
            print(f" -> Cached regulations on Gemini ({model}) -> {cache_name.strip()} (TTL {ttl_s}s)")
            return cache_name.strip()
    except urllib.error.HTTPError:
        _CACHE_UNSUPPORTED_MODELS.add(model)
    except Exception:
        _CACHE_UNSUPPORTED_MODELS.add(model)
    return None


def _build_gemini_request_body(user_text: str, model: str, api_key: str) -> bytes:
    cached_name = _get_or_create_cached_regulations(model, api_key)
    payload: Dict[str, Any] = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": user_text}],
            }
        ],
        "generationConfig": {"responseMimeType": "application/json", "temperature": 0},
    }
    if cached_name:
        payload["cachedContent"] = cached_name
    else:
        payload["system_instruction"] = {
            "parts": [{"text": SYSTEM_REGULATIONS}],
        }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _retry_delay_seconds(attempt: int, exc: Optional[Exception] = None) -> float:
    if isinstance(exc, urllib.error.HTTPError):
        if exc.headers:
            ra = exc.headers.get("Retry-After")
            if ra:
                try:
                    return min(max(float(ra), 1.0), 60.0)
                except ValueError:
                    pass
        try:
            err_body = exc.read().decode("utf-8", errors="ignore")
            m_delay = re.search(r'"retryDelay"\s*:\s*"(\d+(?:\.\d+)?)s"', err_body)
            if m_delay:
                return min(max(float(m_delay.group(1)) + 1.0, 2.0), 65.0)
        except Exception:
            pass
    return min(4.0 * (2 ** attempt), 32.0)


def _unpack_batch_json(data: Any, expected_ids: List[str]) -> Optional[Dict[str, Dict[str, Any]]]:
    """Unpack a JSON array or dict with 'posts'/'results'/'events' returned from batch LLM calls."""
    if isinstance(data, dict):
        for key in ("results", "posts", "events", "items"):
            if isinstance(data.get(key), list):
                data = data[key]
                break
        else:
            if "post_id" in data:
                data = [data]

    if not isinstance(data, list):
        return None

    mapped: Dict[str, Dict[str, Any]] = {}
    known_ids = set(expected_ids)
    for idx, obj in enumerate(data):
        if not isinstance(obj, dict):
            continue
        pid = str(obj.get("post_id") or "").strip()
        if pid in known_ids:
            mapped[pid] = obj
        elif len(data) == len(expected_ids):
            mapped[expected_ids[idx]] = obj
    return mapped or None


_CACHED_GROQ_MODELS: List[str] = []
_CACHED_GROQ_MODELS_TIME: float = 0.0


def _get_available_groq_models(groq_key: str = "") -> List[str]:
    """Dynamically query Groq /openai/v1/models to fetch all active chat models for this API key.

    Falls back to curated list if query fails or key has no /models access.
    """
    global _CACHED_GROQ_MODELS, _CACHED_GROQ_MODELS_TIME
    key = (groq_key or os.environ.get("GROQ_API_KEY") or DEFAULT_GROQ_API_KEY or "").strip()
    if not key:
        return FALLBACK_GROQ_MODELS

    now = time.time()
    if _CACHED_GROQ_MODELS and (now - _CACHED_GROQ_MODELS_TIME) < 600:
        return _CACHED_GROQ_MODELS

    discovered: List[str] = []
    try:
        req = urllib.request.Request(
            GROQ_MODELS_URL,
            headers={
                "Authorization": f"Bearer {key}",
                "User-Agent": "EventExtractor/2.0",
            },
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        items = data.get("data") or []
        for m in items:
            m_id = m.get("id") or ""
            is_active = m.get("active", True)
            if not is_active or not m_id:
                continue
            # Filter out non-chat models (audio transcription, guardrails, embeddings, tts)
            low = m_id.lower()
            if any(skip in low for skip in ("whisper", "prompt-guard", "guard", "embed", "tts", "moderation")):
                continue
            discovered.append(m_id)
    except Exception:
        pass

    priority_order = [
        DEFAULT_GROQ_MODEL,
        "llama-3.3-70b-versatile",
        "llama-3.1-8b-instant",
        "llama-3.2-3b-preview",
        "llama-3.2-1b-preview",
        "llama3-70b-8192",
        "llama3-8b-8192",
        "mixtral-8x7b-32768",
        "gemma2-9b-it",
        "qwen-2.5-32b",
    ]

    sorted_models: List[str] = []
    for p in priority_order:
        if p and p in discovered and p not in sorted_models:
            sorted_models.append(p)
    for d in discovered:
        if d not in sorted_models:
            sorted_models.append(d)
    if not sorted_models:
        for m in priority_order:
            if m and m not in sorted_models:
                sorted_models.append(m)

    _CACHED_GROQ_MODELS = sorted_models
    _CACHED_GROQ_MODELS_TIME = now
    return sorted_models


def _call_groq_json(user_text: str, groq_key: str = "", max_output_tokens: int = 1500) -> Any:
    """Send prompt to Groq API (OpenAI-compatible) when Gemini fails or runs out of quota."""
    key = (groq_key or os.environ.get("GROQ_API_KEY") or DEFAULT_GROQ_API_KEY or "").strip()
    if not key:
        return None

    models = _get_available_groq_models(key)
    last_exc = None

    # Limit input length to prevent triggering Groq free-tier token ceilings
    clean_user_text = user_text
    if len(clean_user_text) > 4500 and "--- POST " not in clean_user_text:
        clean_user_text = clean_user_text[:4500] + '\n"""\n[Content truncated for token ceiling]'

    for model in models:
        for use_json_mode in (True, False):
            payload: Dict[str, Any] = {
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": GROQ_SYSTEM_PROMPT
                        + "\n\nCRITICAL: Respond ONLY with a valid JSON object or array according to schema.",
                    },
                    {"role": "user", "content": clean_user_text},
                ],
                "temperature": 0.0,
                "max_tokens": max_output_tokens,
            }
            if use_json_mode:
                payload["response_format"] = {"type": "json_object"}

            req = urllib.request.Request(
                GROQ_CHAT_COMPLETIONS_URL,
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {key}",
                    "User-Agent": "EventExtractor/2.0",
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    resp_data = json.loads(resp.read().decode("utf-8"))
                content_str = resp_data["choices"][0]["message"]["content"]

                # Parse JSON safely, handling markdown code fences if model wrapped them
                parsed = None
                try:
                    parsed = json.loads(content_str)
                except Exception:
                    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", content_str)
                    if m:
                        parsed = json.loads(m.group(1).strip())
                    else:
                        first_brace = content_str.find("{")
                        first_bracket = content_str.find("[")
                        start_idx = -1
                        if first_brace != -1 and first_bracket != -1:
                            start_idx = min(first_brace, first_bracket)
                        elif first_brace != -1:
                            start_idx = first_brace
                        elif first_bracket != -1:
                            start_idx = first_bracket

                        last_brace = content_str.rfind("}")
                        last_bracket = content_str.rfind("]")
                        end_idx = max(last_brace, last_bracket)
                        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                            parsed = json.loads(content_str[start_idx : end_idx + 1])

                if parsed is not None:
                    print(f" -> Successfully extracted via Groq fallback ({model})")
                    return parsed
            except urllib.error.HTTPError as exc:
                last_exc = exc
                err_msg = ""
                try:
                    err_body = exc.read().decode("utf-8", errors="replace")
                    err_json = json.loads(err_body)
                    err_msg = err_json.get("error", {}).get("message", err_body)
                except Exception:
                    err_msg = exc.reason

                # If json_object response_format not supported by this model, retry without it
                if use_json_mode and exc.code == 400 and "response_format" in err_msg.lower():
                    continue

                # If request was too long, trim input further and try next model
                if exc.code in (400, 413) and any(w in err_msg.lower() for w in ("too long", "context_length", "tokens", "rate_limit_exceeded")):
                    print(f" -> Groq model '{model}' token limit error: {err_msg}")
                    if len(clean_user_text) > 2500:
                        clean_user_text = clean_user_text[:2500] + '\n"""\n[Truncated]'
                    break

                print(f" -> Groq model '{model}' HTTP {exc.code}: {err_msg}")
                # Don't retry second pass on 404 (model not found), 401/403 (auth), or 429 (rate limit)
                break
            except Exception as exc:
                last_exc = exc
                print(f" -> Groq error for model '{model}': {exc}")
                break

    if last_exc:
        raise last_exc
    return None


def _call_gemini_json(user_text: str, api_key: str = "") -> Any:
    """Send a prompt to Gemini REST API with key rotation across pooled keys, model fallback, and retry."""
    global _WORKING_GEMINI_MODEL, _GLOBAL_KEY_POOL
    pool = _GLOBAL_KEY_POOL
    if api_key:
        pool.add_keys(api_key)
    if not pool.has_keys():
        pool.add_keys(os.environ.get("GEMINI_API_KEYS"))
        pool.add_keys(os.environ.get("GEMINI_API_KEY"))
        pool.add_keys(DEFAULT_GEMINI_API_KEYS)
        pool.add_keys(DEFAULT_GEMINI_API_KEY)

    if not pool.has_keys():
        raise ValueError("No Gemini API key configured.")

    last_exc: Optional[Exception] = None
    max_key_attempts = max(1, len(pool.keys))
    for key_attempt in range(max_key_attempts):
        active_key = pool.get_active_key()
        if not active_key:
            break

        for model in _candidate_gemini_models(active_key):
            for attempt in range(GEMINI_MAX_RETRIES + 1):
                body_bytes = _build_gemini_request_body(user_text, model, active_key)
                req = urllib.request.Request(
                    GEMINI_URL.format(model=model),
                    data=body_bytes,
                    headers={"Content-Type": "application/json", "x-goog-api-key": active_key},
                    method="POST",
                )
                try:
                    with urllib.request.urlopen(req, timeout=GEMINI_REQUEST_TIMEOUT_SECONDS) as resp:
                        payload = json.loads(resp.read().decode("utf-8"))
                    _WORKING_GEMINI_MODEL = model
                    text = payload["candidates"][0]["content"]["parts"][0]["text"]
                    return json.loads(text)
                except urllib.error.HTTPError as exc:
                    last_exc = exc
                    if exc.code in (400, 404) and model in _CACHED_SYSTEM_HANDLES:
                        _CACHED_SYSTEM_HANDLES.pop(model, None)
                        _CACHE_UNSUPPORTED_MODELS.add(model)
                        continue
                    if exc.code in (400, 404):
                        print(f" -> Gemini model '{model}' returned HTTP {exc.code} ({exc.reason}); trying next fallback model...")
                        break
                    if exc.code == 403:
                        print(f" -> Gemini key (...{active_key[-6:] if len(active_key) > 6 else active_key}) returned HTTP 403 Forbidden ({exc.reason}).")
                        if len(pool.keys) > 1:
                            pool.mark_invalid(active_key)
                            break
                        raise
                    if exc.code in (429, 500, 502, 503, 504):
                        if exc.code == 429 and len(pool.keys) > 1:
                            wait_s = _retry_delay_seconds(attempt, exc)
                            print(f" -> Gemini key (...{active_key[-6:] if len(active_key) > 6 else active_key}) hit HTTP 429 Rate Limit. Rotating to next key in pool...")
                            pool.mark_rate_limited(active_key, wait_s)
                            break
                        if attempt < GEMINI_MAX_RETRIES:
                            wait_s = _retry_delay_seconds(attempt, exc)
                            print(f" -> Gemini {model} returned HTTP {exc.code}; retrying in {wait_s:.1f}s...")
                            time.sleep(wait_s)
                            continue
                        break
                    raise
                except (TimeoutError, socket.timeout, urllib.error.URLError, OSError) as exc:
                    last_exc = exc
                    if attempt < GEMINI_MAX_RETRIES:
                        wait_s = _retry_delay_seconds(attempt, exc)
                        print(f" -> Gemini {model} network error ({exc}); retrying in {wait_s:.1f}s...")
                        time.sleep(wait_s)
                        continue
                    break

    if last_exc is not None:
        raise last_exc
    return None


def call_gemini_text(
    content: str,
    published_at_str: str,
    source_name: str,
    api_key: str = "",
) -> Tuple[Optional[Dict[str, Any]], str]:
    """Send text to Gemini (with key rotation and dynamic Gemini-2 model discovery). Returns (data, method)."""
    user_text = USER_POST_TEMPLATE.format(
        published_at=published_at_str,
        fanpage=source_name,
        content=(content or "")[:6000],
    )
    try:
        data = _call_gemini_json(user_text, api_key)
        if isinstance(data, list):
            data = data[0] if data else {}
        if isinstance(data, dict):
            return data, "gemini"
    except Exception as exc:
        print(f" -> Gemini error ({exc}); falling back to heuristic...")

    return None, "heuristic"


def call_gemini_batch_texts(
    items: List[Dict[str, Any]],
    api_key: str = "",
) -> Tuple[Optional[Dict[str, Dict[str, Any]]], str]:
    """Send a small batch of raw post texts to Gemini (with key rotation). Returns (results_map, method)."""
    if not items:
        return {}, "empty"
    if len(items) == 1:
        item = items[0]
        single, method = call_gemini_text(
            item["content"],
            item["published_at_str"],
            item["source_label"],
            api_key,
        )
        return ({item["post_id"]: single} if isinstance(single, dict) else None), method

    blocks: List[str] = []
    for idx, item in enumerate(items, start=1):
        blocks.append(
            f'--- POST {idx} (post_id: "{item["post_id"]}") ---\n'
            + USER_POST_TEMPLATE.format(
                published_at=item["published_at_str"],
                fanpage=item["source_label"],
                content=(item["content"] or "")[:4500],
            )
        )
    user_text = USER_BATCH_POSTS_TEMPLATE.format(
        count=len(items),
        posts_block="\n\n".join(blocks),
    )
    expected_ids = [it["post_id"] for it in items]

    # 1. Try Gemini batch
    try:
        data = _call_gemini_json(user_text, api_key)
        mapped = _unpack_batch_json(data, expected_ids)
        if mapped:
            return mapped, "gemini_batch"
    except Exception as exc:
        print(f" -> Gemini batch error ({exc}); retrying batch posts individually via Gemini...")
        single_mapped: Dict[str, Dict[str, Any]] = {}
        for it in items:
            try:
                single, _ = call_gemini_text(
                    it["content"],
                    it["published_at_str"],
                    it["source_label"],
                    api_key,
                )
                if isinstance(single, dict) and single:
                    single_mapped[it["post_id"]] = single
            except Exception as single_exc:
                print(f" -> Individual fallback failed for post '{it['post_id']}': {single_exc}")
        if single_mapped:
            return single_mapped, "gemini_split"

    return None, "heuristic"


def _extract_location_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        cleaned = re.sub(r"^[^\w\s]+", "", line, flags=re.UNICODE).strip() or line
        m_sep = re.match(r"^([^:\-\n]{2,40})\s*[:\-]\s*(.+)$", cleaned)
        if m_sep:
            label_norm = _strip_accents(m_sep.group(1)).lower().strip()
            value = m_sep.group(2).strip()
            if value and _LOCATION_LABEL_RE.search(label_norm):
                return value[:300]
        line_norm = _strip_accents(cleaned).lower()
        if re.match(r"^(?:dia\s*diem|dia\s*chi|noi\s*to\s*chuc|location|venue|address)\b", line_norm):
            return cleaned[:300]

    m_inline = re.search(
        r"\b(?:tại|tai|at)\s+((?:Hội\s+trường|Hoi\s+truong|Giảng\s+đường|Giang\s+duong|Phòng|Phong|"
        r"Cơ\s+sở|Co\s+so|Trường|Truong|Hall|Room)[^\n\r;]+|Zoom|Google\s+Meet|MS\s+Teams)",
        text,
        flags=re.IGNORECASE,
    )
    return m_inline.group(1).strip().rstrip(".,;")[:300] if m_inline else None


def _format_hhmm(hour_str: str, min_str: Optional[str], ampm: Optional[str]) -> Optional[str]:
    hh = int(hour_str)
    mm = int(min_str) if min_str else 0
    ap = (ampm or "").lower()
    if ap == "pm" and hh < 12:
        hh += 12
    elif ap == "am" and hh == 12:
        hh = 0
    if 0 <= hh <= 23 and 0 <= mm <= 59:
        return f"{hh:02d}:{mm:02d}"
    return None


def _extract_datetime_range_from_text(
    text: str,
    published_at: Optional[datetime] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Extract (start_date, end_date, start_time, end_time) from Vietnamese/English event text."""
    if not text:
        return None, None, None, None

    ref_year = (published_at or datetime.now(LOCAL_TIMEZONE)).year
    stripped = _strip_accents(text)

    start_date: Optional[str] = None
    end_date: Optional[str] = None

    m_drange = re.search(
        r"\b(?:tu\s+(?:ngay\s+)?)?(0?[1-9]|[12]\d|3[01])[/-](0?[1-9]|1[0-2])(?:[/-](20\d{2}))?"
        r"\s*(?:-|–|—|den|toi|to)\s*"
        r"(?:het\s+)?(?:ngay\s+)?(0?[1-9]|[12]\d|3[01])[/-](0?[1-9]|1[0-2])(?:[/-](20\d{2}))?\b",
        stripped,
        flags=re.IGNORECASE,
    )
    if m_drange:
        d1, m1, y1, d2, m2, y2 = m_drange.groups()
        yr2 = int(y2) if y2 else (int(y1) if y1 else ref_year)
        yr1 = int(y1) if y1 else yr2
        start_date = f"{yr1:04d}-{int(m1):02d}-{int(d1):02d}"
        end_date = f"{yr2:04d}-{int(m2):02d}-{int(d2):02d}"
    else:
        m_iso = re.search(r"\b(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b", stripped)
        if m_iso:
            start_date = m_iso.group(0)
        else:
            m_dmy = re.search(r"\b(0?[1-9]|[12]\d|3[01])[/-](0?[1-9]|1[0-2])(?:[/-](20\d{2}))?\b", stripped)
            if m_dmy:
                day, month, yr = int(m_dmy.group(1)), int(m_dmy.group(2)), int(m_dmy.group(3) or ref_year)
                start_date = f"{yr:04d}-{month:02d}-{day:02d}"

    start_time: Optional[str] = None
    end_time: Optional[str] = None

    m_trange = re.search(
        r"\b(?:tu\s+)?([01]?\d|2[0-3])\s*(?:[:hg]\s*([0-5]\d)?)\s*(am|pm)?\s*"
        r"(?:-|–|—|den|toi|to)\s*"
        r"([01]?\d|2[0-3])\s*(?:[:hg]\s*([0-5]\d)?)\s*(am|pm)?\b",
        stripped,
        flags=re.IGNORECASE,
    )
    if m_trange:
        h1, m1, ap1, h2, m2, ap2 = m_trange.groups()
        start_time = _format_hhmm(h1, m1, ap1 or (ap2 if int(h1) <= 12 and int(h2) <= 12 and not ap1 else None))
        end_time = _format_hhmm(h2, m2, ap2)
    else:
        m_t1 = re.search(r"\b([01]?\d|2[0-3])\s*[:hg]\s*([0-5]\d)\s*(am|pm)?\b", stripped, flags=re.IGNORECASE)
        if m_t1:
            start_time = _format_hhmm(m_t1.group(1), m_t1.group(2), m_t1.group(3))
        else:
            m_t2 = re.search(r"\b([01]?\d|2[0-3])\s*[hg]\b\s*(am|pm)?", stripped, flags=re.IGNORECASE)
            if m_t2:
                start_time = _format_hhmm(m_t2.group(1), None, m_t2.group(2))

    return start_date, end_date, start_time, end_time


def _extract_fee_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        cleaned = re.sub(r"^[^\w\s]+", "", line, flags=re.UNICODE).strip() or line
        if _FEE_LINE_RE.search(_strip_accents(cleaned).lower()):
            m_sep = re.match(r"^[^:\-\n]{2,45}\s*[:\-]\s*(.+)$", cleaned)
            value = (m_sep.group(1) if m_sep else cleaned).strip().rstrip(".")
            if value:
                if re.fullmatch(r"(?:mien\s*phi|free|0\s*(?:d|dong|vnd)?)", _strip_accents(value).lower()):
                    return "miễn phí"
                return value[:160]
    if re.search(r"\b(free|mien\s*phi)\b", _strip_accents(text).lower()):
        return "miễn phí"
    return None


def _summarize_event_description(content: str) -> Optional[str]:
    if not content or not content.strip():
        return None
    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
    if not lines:
        return None

    candidates = lines[1:] if len(lines) > 1 else lines
    picked_activities: List[str] = []
    picked_benefits: List[str] = []
    fallback_lines: List[str] = []

    for raw in candidates:
        cleaned = re.sub(r"^[^\w\s]+", "", raw, flags=re.UNICODE).strip()
        if len(cleaned) < 12 or cleaned.startswith("#"):
            continue
        norm = _strip_accents(cleaned).lower()
        if _DESC_SKIP_LINE_RE.search(norm):
            continue
        text_only = re.sub(r"https?://\S+", "", cleaned).strip(" :-")
        if len(text_only) < 12:
            continue

        if _DESC_BENEFIT_RE.search(norm) and text_only not in picked_benefits:
            picked_benefits.append(text_only)
        elif _DESC_LEARN_ACTIVITY_RE.search(norm) and text_only not in picked_activities:
            picked_activities.append(text_only)
        elif text_only not in fallback_lines:
            fallback_lines.append(text_only)

    selected = picked_activities[:2] + picked_benefits[:2]
    if not selected:
        selected = fallback_lines[:3]
    elif len(selected) == 1 and fallback_lines:
        selected.append(fallback_lines[0])

    if not selected:
        clean_body = re.sub(r"\s+", " ", content.strip())
        return clean_body[:360] + ("..." if len(clean_body) > 360 else "")

    summary = " ".join(s.rstrip(".") + "." for s in selected)
    return summary[:450] + ("..." if len(summary) > 450 else "")


GENERIC_CHANNEL_TAGS_PATTERN = re.compile(
    r"^[\[\(【\s]*(?:hỗ trợ truyền thông|ho tro truyen thong|httt|chia sẻ thông tin|chia se thong tin|thông báo|thong bao|tin tức|tin tuc|recap|góc chia sẻ|goc chia se|góc tuyển dụng|goc tuyen dung)[\]\)】\s]*",
    re.IGNORECASE,
)

DISQUALIFIED_TITLE_PREFIXES = (
    "link đăng ký", "link dang ky", "form đăng ký", "form dang ky",
    "thời gian", "thoi gian", "địa điểm", "dia diem", "đối tượng", "doi tuong",
    "lệ phí", "le phi", "hạn đăng ký", "han dang ky", "thời hạn", "thoi han",
    "lưu ý", "luu y", "quyền lợi", "quyen loi", "thông tin chi tiết", "thong tin chi tiet",
    "thông tin kỳ thi", "thong tin ky thi", "hotline", "fanpage", "liên hệ", "lien he",
)

INVITATION_PREFIX_PATTERN = re.compile(
    r"^(?:fit\.hcmus\s+|clb\s+[^\n\r,–—]+\s+)?(?:thân mời|than moi|kính mời|kinh moi|mời|moi)\s+(?:các bạn|các em|quý thầy cô|sinh viên|mọi người)?\s*(?:cùng\s+)?(?:dành thời gian\s+)?(?:tham gia|tham dự)\s+(?:buổi\s+|chương trình\s+)?",
    re.IGNORECASE,
)


def _is_greeting_line(line: str) -> bool:
    norm = line.strip().lower()
    return bool(re.match(r"^(?:thân chào|than chao|chào các|chao cac|chào bạn|chao ban|kính chào|kinh chao|kính gửi|kinh gui|dear|hi all|hello)\b", norm))


def _is_separator_line(line: str) -> bool:
    return bool(re.match(r"^[\s\-_=*~•#]+$", line))


def _is_metadata_field_line(line: str) -> bool:
    norm = re.sub(r"^[^\w\s]+", "", line).strip().lower()
    for p in DISQUALIFIED_TITLE_PREFIXES:
        if norm.startswith(p):
            return True
    if re.match(r"^https?://", norm):
        return True
    return False


def _clean_event_title(raw_title: Optional[str]) -> str:
    """Normalize whitespace in an extracted event title, stripping pure generic channel tags like [HỖ TRỢ TRUYỀN THÔNG] while preserving brackets for actual event names."""
    t = re.sub(r"\s+", " ", (raw_title or "").strip())
    t = GENERIC_CHANNEL_TAGS_PATTERN.sub("", t).strip()
    return t or "Untitled Event"


def _extract_title_from_content(content: str) -> str:
    """Scan content lines to discover the canonical event title, skipping greetings, tags, and metadata lines while preserving brackets."""
    lines = [l.strip() for l in (content or "").splitlines() if l.strip()]
    if not lines:
        return "Untitled Event"

    candidates = []
    for idx, raw_line in enumerate(lines[:15]):
        if _is_separator_line(raw_line):
            continue
        stripped = GENERIC_CHANNEL_TAGS_PATTERN.sub("", raw_line).strip()
        if not stripped:
            continue
        if _is_greeting_line(stripped):
            continue
        if _is_metadata_field_line(stripped):
            continue

        clean_inv = INVITATION_PREFIX_PATTERN.sub("", stripped).strip()
        if clean_inv and len(clean_inv) >= 5:
            stripped = clean_inv

        has_brackets = bool(re.search(r"\[.*?\]|【.*?】", stripped))
        norm = stripped.lower()
        has_keywords = bool(re.search(r"\b(workshop|seminar|hội nghị|hoi nghi|hội thảo|hoi thao|cuộc thi|cuoc thi|hội thi|hoi thi|tọa đàm|toa dam|giải đấu|giai dau|trường hè|truong he|gala|khóa học|khoa hoc|mini-course|talkshow|ngày hội|ngay hoi|đăng ký|dang ky|tuyển cộng tác viên|tuyen cong tac vien|bước chân yêu thương|nét son|green day|itogether|bản lĩnh it|stellar|thi thử|program)\b", norm))

        score = 0
        if has_brackets:
            score += 25
        if has_keywords:
            score += 10
        if stripped.isupper():
            score += 5
        if idx == 0:
            score += 8
        elif idx == 1:
            score += 6
        elif idx == 2:
            score += 4
        if len(stripped) >= 10:
            score += 2

        candidates.append((score, idx, stripped))

    if candidates:
        candidates.sort(key=lambda c: (-c[0], c[1]))
        return candidates[0][2][:180]

    return lines[0][:180]


def _normalize_iso_datetime(val: Optional[str], default_time: str = "00:00") -> Optional[str]:
    if not val or not isinstance(val, str):
        return None
    s = val.strip()
    if not s:
        return None
    m_date_only = re.match(r"^(\d{4}-\d{2}-\d{2})$", s)
    if m_date_only:
        return f"{m_date_only.group(1)}T{default_time}:00+07:00"
    m_dt = re.match(r"^(\d{4}-\d{2}-\d{2})[T\s](\d{2}:\d{2})(?::(\d{2}))?([+-]\d{2}:\d{2}|Z)?$", s)
    if m_dt:
        tz = "+07:00" if (not m_dt.group(4) or m_dt.group(4) == "Z") else m_dt.group(4)
        return f"{m_dt.group(1)}T{m_dt.group(2)}:{m_dt.group(3) or '00'}{tz}"
    return s


def _parse_date_and_time(
    start_datetime: Optional[str],
    raw_date: Optional[str] = None,
    raw_time: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    ev_date = (raw_date or "").strip() or None
    ev_time = (raw_time or "").strip() or None
    if ev_time:
        m_rt = re.match(r"^(\d{1,2})(?:[:hg](\d{2}))?", ev_time, flags=re.IGNORECASE)
        if m_rt:
            ev_time = f"{int(m_rt.group(1)):02d}:{int(m_rt.group(2) or 0):02d}"
    if start_datetime and isinstance(start_datetime, str):
        m_dt = re.match(r"^(\d{4}-\d{2}-\d{2})(?:[T\s](\d{1,2})[:hg](\d{2})?)?", start_datetime.strip(), flags=re.IGNORECASE)
        if m_dt:
            if not ev_date:
                ev_date = m_dt.group(1)
            if not ev_time and m_dt.group(2):
                hh_mm = f"{int(m_dt.group(2)):02d}:{int(m_dt.group(3) or 0):02d}"
                if hh_mm != "00:00":
                    ev_time = hh_mm
    return ev_date, ev_time


def _derive_online_mode(is_online: Optional[bool], fmt_list: Optional[List[str]]) -> Optional[str]:
    formats = [str(f).lower().strip() for f in (fmt_list or []) if f]
    has_online = "online" in formats or is_online is True
    has_offline = "offline" in formats or is_online is False
    if "online" in formats and "offline" in formats:
        return "hybrid"
    if has_online and "offline" not in formats:
        return "online"
    if has_offline:
        return "offline"
    return None


def _extract_registration_dates_from_text(
    text: str,
    published_at: Optional[datetime] = None,
) -> Tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None
    reg_start: Optional[str] = None
    reg_end: Optional[str] = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        norm = _strip_accents(line).lower()
        has_open = bool(re.search(r"\b(mo don|bat dau dang ky|mo cong dang ky|thoi gian mo don|open(?:s|ing)?)\b", norm))
        has_close = bool(re.search(r"\b(dong don|han chot|ket thuc dang ky|han dang ky|deadline|close[ds]?|het han)\b", norm))
        has_reg_range = bool(re.search(r"\b(thoi gian dang ky|dang ky)\b", norm)) and bool(
            re.search(r"(?:-|–|—|den|toi|to)", norm)
        )
        if not (has_open or has_close or has_reg_range):
            continue

        s_date, e_date, s_time, e_time = _extract_datetime_range_from_text(line, published_at=published_at)
        if (has_open and has_close or has_reg_range) and s_date and e_date:
            reg_start = reg_start or f"{s_date}T{s_time or '00:00'}:00+07:00"
            reg_end = reg_end or f"{e_date}T{e_time or '23:59'}:00+07:00"
        elif has_open and s_date:
            reg_start = reg_start or f"{s_date}T{s_time or '00:00'}:00+07:00"
        elif has_close and (e_date or s_date):
            c_date = e_date or s_date
            c_time = e_time or s_time or "23:59"
            reg_end = reg_end or f"{c_date}T{c_time}:00+07:00"

    return reg_start, reg_end


def _enrich_extracted_event_fields(
    raw_data: Dict[str, Any],
    content: str,
    published_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Post-process LLM or heuristic output to enforce content reasoning and normalize fields."""
    if not raw_data:
        return raw_data

    is_candidate, reasoning_note = reason_about_post_content(content)
    if not raw_data.get("reason"):
        raw_data["reason"] = reasoning_note

    if not raw_data.get("is_event"):
        return raw_data

    if not is_candidate and reasoning_note.startswith("disqualified:") and not reasoning_note.startswith(
        "disqualified:insufficient_event_signals"
    ):
        raw_data["is_event"] = False
        raw_data["reason"] = f"guardrail_override ({reasoning_note})"
        return raw_data

    cur_title = (raw_data.get("title") or "").strip()
    cleaned_title = _clean_event_title(cur_title)
    if (
        not cur_title
        or cleaned_title == "Untitled Event"
        or _is_greeting_line(cur_title)
        or not GENERIC_CHANNEL_TAGS_PATTERN.sub("", cur_title).strip()
    ):
        inferred_title = _extract_title_from_content(content)
        if inferred_title and inferred_title != "Untitled Event":
            raw_data["title"] = inferred_title
        else:
            raw_data["title"] = cleaned_title
    else:
        raw_data["title"] = cleaned_title

    if not (raw_data.get("description") or "").strip():
        inferred_desc = _summarize_event_description(content)
        if inferred_desc:
            raw_data["description"] = inferred_desc

    if not (raw_data.get("location") or "").strip():
        inferred_loc = _extract_location_from_text(content)
        if inferred_loc:
            raw_data["location"] = inferred_loc

    if not (raw_data.get("fee") or "").strip():
        inferred_fee = _extract_fee_from_text(content)
        if inferred_fee:
            raw_data["fee"] = inferred_fee

    s_date, e_date, s_time, e_time = _extract_datetime_range_from_text(content, published_at=published_at)
    cur_start = str(raw_data.get("event_start_datetime") or raw_data.get("start_datetime") or "").strip()
    cur_end = str(raw_data.get("event_end_datetime") or raw_data.get("end_datetime") or "").strip()

    cur_s_date, cur_s_time = _parse_date_and_time(cur_start)
    effective_s_date = cur_s_date or s_date
    final_start: Optional[str] = _normalize_iso_datetime(cur_start, default_time="00:00") if cur_start else None
    if effective_s_date:
        if not final_start:
            final_start = f"{effective_s_date}T{s_time or '00:00'}:00+07:00"
        elif not cur_s_time and s_time:
            final_start = f"{effective_s_date}T{s_time}:00+07:00"

    cur_e_date, cur_e_time = _parse_date_and_time(cur_end)
    effective_e_date = cur_e_date or e_date or effective_s_date
    final_end: Optional[str] = _normalize_iso_datetime(cur_end, default_time="23:59") if cur_end else None
    if effective_e_date:
        if not final_end:
            final_end = f"{effective_e_date}T{e_time or '23:59'}:00+07:00"
        elif not cur_e_time and e_time:
            final_end = f"{effective_e_date}T{e_time}:00+07:00"

    raw_data["event_start_datetime"] = final_start
    raw_data["event_end_datetime"] = final_end
    raw_data["start_datetime"] = final_start
    raw_data["end_datetime"] = final_end

    llm_reg_start = _normalize_iso_datetime(raw_data.get("reg_start_datetime"), default_time="00:00")
    llm_reg_end = _normalize_iso_datetime(raw_data.get("reg_end_datetime"), default_time="23:59")
    regex_reg_start, regex_reg_end = _extract_registration_dates_from_text(content, published_at=published_at)

    raw_data["reg_start_datetime"] = llm_reg_start or regex_reg_start
    raw_data["reg_end_datetime"] = llm_reg_end or regex_reg_end
    return raw_data


def heuristic_extract_event(
    content: str,
    sender: Optional[str] = None,
    published_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Rule-based fallback extractor when GEMINI_API_KEY is missing or Gemini fails."""
    combined = (content or "").strip()
    stripped = _strip_accents(combined).lower()
    is_candidate, reasoning_note = reason_about_post_content(combined)
    if not is_candidate:
        return {"is_event": False, "reason": reasoning_note}

    title = _extract_title_from_content(combined)
    title = _clean_event_title(title)

    s_date, e_date, s_time, e_time = _extract_datetime_range_from_text(combined, published_at=published_at)
    event_start_datetime = f"{s_date}T{s_time or '00:00'}:00+07:00" if s_date else None
    target_end_date = e_date or s_date
    event_end_datetime = f"{target_end_date}T{e_time or '23:59'}:00+07:00" if target_end_date else None
    reg_start_datetime, reg_end_datetime = _extract_registration_dates_from_text(combined, published_at=published_at)
    location = _extract_location_from_text(combined)

    is_online: Optional[bool] = None
    fmt_list: List[str] = []
    if re.search(r"\b(online|zoom|google meet|ms teams|microsoft teams|webinar|livestream)\b", stripped):
        is_online = True
        fmt_list.append("online")
    if re.search(r"\b(offline|in-person|truc tiep|hall|hoi truong|campus|co so)\b", stripped) or (
        location and not re.search(r"\b(zoom|meet|teams|online)\b", location.lower())
    ):
        if is_online is None:
            is_online = False
        fmt_list.append("offline")

    urls = re.findall(r"https?://[^\s<>\"')\]]+", combined)
    reg_url = next(
        (u for u in urls if not any(soc in u.lower() for soc in ("facebook.com/", "fb.com/", "tiktok.com/", "instagram.com/"))),
        None,
    )

    categories: List[str] = []
    for kw, cat in [
        ("workshop", "workshop"),
        ("seminar", "seminar"),
        ("hoi thao", "seminar"),
        ("hoi nghi", "hội nghị"),
        ("conference", "hội nghị"),
        ("cuoc thi", "cuộc thi"),
        ("contest", "cuộc thi"),
        ("competition", "cuộc thi"),
        ("hackathon", "cuộc thi"),
        ("toa dam", "tọa đàm"),
        ("talk", "tọa đàm"),
        ("ngay hoi", "ngày hội"),
        ("festival", "ngày hội"),
        ("trien lam", "triển lãm"),
    ]:
        if kw in stripped and cat not in categories:
            categories.append(cat)

    return {
        "is_event": True,
        "reason": reasoning_note,
        "title": title,
        "description": _summarize_event_description(combined) or None,
        "event_start_datetime": event_start_datetime,
        "event_end_datetime": event_end_datetime,
        "start_datetime": event_start_datetime,
        "end_datetime": event_end_datetime,
        "reg_start_datetime": reg_start_datetime,
        "reg_end_datetime": reg_end_datetime,
        "location": location,
        "organizer": sender,
        "registration_url": reg_url,
        "fee": _extract_fee_from_text(combined),
        "is_online": is_online,
        "target_audience": ["sinh viên"] if re.search(r"\b(student|sinh vien)\b", stripped) else None,
        "category": categories or ["khác"],
        "format": fmt_list,
    }


def scan_qr_link(image_url: str) -> Optional[str]:
    """Download an image and decode a QR code locally via OpenCV."""
    import cv2
    import numpy as np

    req = urllib.request.Request(image_url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read(10 * 1024 * 1024)
    img = cv2.imdecode(np.frombuffer(raw, np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        return None
    detector = cv2.QRCodeDetector()
    candidates = [img]
    h, w = img.shape[:2]
    if max(h, w) < 1600:
        candidates.append(cv2.resize(img, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC))
    candidates.append(cv2.cvtColor(img, cv2.COLOR_BGR2GRAY))
    for candidate in candidates:
        value, _, _ = detector.detectAndDecode(candidate)
        if value and value.strip().lower().startswith(("http://", "https://")):
            return value.strip()
    return None


def fill_registration_from_media(media_urls: List[str], data: Dict[str, Any]) -> None:
    """Fallback chain: link in text -> QR in post images -> 'free'."""
    if data.get("registration_url") and data.get("registration_url") != "free":
        data["registration_status"] = "link"
        return
    if media_urls:
        for img_url in media_urls[:3]:
            try:
                link = scan_qr_link(img_url)
            except ImportError:
                data["registration_status"] = "unknown"
                return
            except Exception as exc:
                print(f" -> QR scan failed ({exc}); skipping QR check.")
                data["registration_status"] = "unknown"
                return
            if link:
                data["registration_url"] = link
                data["registration_status"] = "qr"
                return
    data["registration_url"] = "free"
    data["registration_status"] = "free"


# ---------------------------------------------------------------------------
# End-to-End Pipeline & CLI
# ---------------------------------------------------------------------------
def _natural_sort_key(path: Path) -> List[Any]:
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", path.name)]


def _find_default_posts_dir() -> Optional[Path]:
    script_dir = Path(__file__).resolve().parent
    candidates = [
        Path.cwd() / "posts",
        script_dir / "posts",
        script_dir.parent / "posts",
        Path.cwd().parent / "posts",
    ]
    for cand in candidates:
        if cand.is_dir():
            return cand.resolve()
    return None


def _discover_sample_txt_files(posts_dir: Path, prefix: str = "sample") -> List[Path]:
    """Return all `.txt` files inside `posts_dir` whose filename starts with `prefix` (case-insensitive)."""
    pref = (prefix or "").lower()
    matched = [
        p
        for p in posts_dir.iterdir()
        if p.is_file()
        and p.suffix.lower() == ".txt"
        and (not pref or p.name.lower().startswith(pref))
    ]
    return sorted(matched, key=_natural_sort_key)


_HEADER_LINE_RE = re.compile(
    r"^(url|post[\s_]url|fanpage|fanpage[\s_]name|organizer|author|published[\s_]at|posted[\s_]at)\s*:\s*(.+)$",
    flags=re.IGNORECASE,
)
_BODY_FANPAGE_RE = re.compile(
    r"^[^\w\n]{0,6}\s*Fanpage(?:\s+cuộc\s+thi)?\s*:\s*(.+)$",
    flags=re.IGNORECASE,
)


def _parse_iso_or_local_dt(raw: str) -> Optional[datetime]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=LOCAL_TIMEZONE)
        return dt.astimezone(LOCAL_TIMEZONE)
    except ValueError:
        pass
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+(\d{1,2}):(\d{2}))?$", s)
    if m:
        d, mo, y, hh, mm = m.groups()
        try:
            return datetime(int(y), int(mo), int(d), int(hh or 0), int(mm or 0), tzinfo=LOCAL_TIMEZONE)
        except ValueError:
            return None
    return None


def _extract_fanpage_from_body(content: str) -> Optional[str]:
    for raw_line in reversed(content.splitlines()):
        m = _BODY_FANPAGE_RE.match(raw_line.strip())
        if not m:
            continue
        candidate = m.group(1).strip()
        if not candidate or re.search(r"https?://|fb\.com/|facebook\.com/|www\.", candidate, flags=re.IGNORECASE):
            continue
        cleaned = _clean_fanpage_name(candidate)
        if cleaned:
            return cleaned
    return None


def _parse_post_text_file(file_path: Path) -> Dict[str, Any]:
    """Read a raw post `.txt` file and extract optional metadata headers or footer fanpage hints."""
    try:
        raw_text = file_path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        raw_text = file_path.read_text(encoding="utf-8", errors="replace")

    lines = raw_text.splitlines()
    first_line = lines[0].strip() if lines else ""

    # Check if the first line is a source link or 'email'
    detected_source_link: Optional[str] = None
    detected_source_type: str = "text_file"
    body_start_idx = 0

    if first_line.lower() == "email":
        detected_source_link = "email"
        detected_source_type = "email"
        body_start_idx = 1
    elif first_line.lower().startswith(("http://", "https://")):
        detected_source_link = first_line
        detected_source_type = "facebook" if ("facebook.com" in first_line.lower() or "fb.com" in first_line.lower()) else "web"
        body_start_idx = 1

    headers: Dict[str, str] = {}
    for idx in range(body_start_idx, len(lines)):
        line = lines[idx]
        stripped = line.strip()
        if not stripped:
            if headers:
                body_start_idx = idx + 1
            break
        m = _HEADER_LINE_RE.match(stripped)
        if not m:
            break
        headers[re.sub(r"[\s_]+", "_", m.group(1).lower())] = m.group(2).strip()
        body_start_idx = idx + 1

    content = "\n".join(lines[body_start_idx:]).strip() if (headers or body_start_idx > 0) else raw_text.strip()
    header_url = headers.get("url") or headers.get("post_url") or (detected_source_link if detected_source_link and detected_source_link.startswith("http") else None)
    header_dt = _parse_iso_or_local_dt(headers.get("published_at") or headers.get("posted_at") or "")
    if header_dt is None:
        mtime = file_path.stat().st_mtime
        header_dt = datetime.fromtimestamp(mtime, tz=LOCAL_TIMEZONE)

    fanpage_raw = headers.get("fanpage_name") or headers.get("fanpage") or headers.get("organizer") or headers.get("author")
    fanpage_name = _clean_fanpage_name(fanpage_raw) or _extract_fanpage_from_body(content)
    fanpage_url = _derive_fanpage_url_from_url(header_url)
    source_label = fanpage_name or fanpage_url or ("Email" if detected_source_type == "email" else file_path.name)

    return {
        "post_id": file_path.stem,
        "post_url": header_url,
        "source_link": detected_source_link,
        "input_url": header_url if (header_url and header_url.startswith("http")) else None,
        "source_type": detected_source_type,
        "source_file": str(file_path.resolve()),
        "content": content,
        "media_urls": [],
        "published_at": header_dt,
        "published_at_str": header_dt.isoformat(),
        "fanpage_name": fanpage_name,
        "fanpage_url": fanpage_url,
        "source_label": source_label,
    }


def _finalize_event_record(
    item: Dict[str, Any],
    raw_data: Optional[Dict[str, Any]],
    extraction_method: str,
    include_non_events: bool,
) -> Optional[Dict[str, Any]]:
    """Enrich/filter raw extraction output and assemble the final JSON record."""
    content = (item.get("content") or "").strip()
    media_urls: List[str] = item.get("media_urls") or []
    pub_at = item.get("published_at")
    pub_dt = pub_at if isinstance(pub_at, datetime) else None
    pub_iso = item.get("published_at_str") or (
        pub_dt.isoformat() if pub_dt else datetime.now(LOCAL_TIMEZONE).isoformat()
    )
    fanpage_name = item.get("fanpage_name")
    fanpage_url = item.get("fanpage_url")

    if raw_data is None:
        extraction_method = "heuristic"
        raw_data = heuristic_extract_event(
            content=content,
            sender=fanpage_name,
            published_at=pub_dt,
        )
    elif extraction_method != "heuristic":
        raw_data = _enrich_extracted_event_fields(
            raw_data,
            content,
            published_at=pub_dt,
        )

    is_event = bool(raw_data.get("is_event"))
    if not is_event and not include_non_events:
        return None

    if is_event:
        fill_registration_from_media(media_urls, raw_data)

    ev_date, ev_time = _parse_date_and_time(raw_data.get("event_start_datetime"))
    is_online = raw_data.get("is_online") if isinstance(raw_data.get("is_online"), bool) else None
    fmt_list = raw_data.get("format") if isinstance(raw_data.get("format"), list) else []
    cat_list = raw_data.get("category") if isinstance(raw_data.get("category"), list) else []

    record: Dict[str, Any] = {
        "is_event": is_event,
        "reason": raw_data.get("reason"),
        "title": raw_data.get("title"),
        "online_mode": _derive_online_mode(is_online, fmt_list),
        "is_online": is_online,
        "date": ev_date,
        "time": ev_time,
        "start_datetime": raw_data.get("event_start_datetime"),
        "end_datetime": raw_data.get("event_end_datetime"),
        "reg_start_datetime": raw_data.get("reg_start_datetime"),
        "reg_end_datetime": raw_data.get("reg_end_datetime"),
        "location": raw_data.get("location"),
        "description": raw_data.get("description"),
        "organizer": raw_data.get("organizer") or fanpage_name,
        "registration_url": raw_data.get("registration_url"),
        "registration_status": raw_data.get("registration_status"),
        "fee": raw_data.get("fee"),
        "target_audience": raw_data.get("target_audience"),
        "category": cat_list,
        "format": fmt_list,
        "post_id": item.get("post_id"),
        "post_url": item.get("post_url"),
        "source_link": item.get("source_link") or item.get("post_url") or item.get("input_url"),
        "input_url": item.get("input_url"),
        "source_type": item.get("source_type") or "facebook_url",
        "source_file": item.get("source_file"),
        "posted_at": pub_iso,
        "fanpage_name": fanpage_name,
        "fanpage_url": fanpage_url,
        "media_urls": media_urls,
        "extraction_method": extraction_method,
        "scraped_at": datetime.now(LOCAL_TIMEZONE).isoformat(),
    }
    return record


def _extract_single_with_gemini_or_heuristic(
    item: Dict[str, Any],
    api_key: str = "",
) -> Tuple[Dict[str, Any], str]:
    content = (item.get("content") or "").strip()
    has_ai = bool(_GLOBAL_KEY_POOL.has_keys() or api_key)
    if has_ai and content:
        try:
            raw_data, method = call_gemini_text(
                content,
                item["published_at_str"],
                item["source_label"],
                api_key,
            )
            if isinstance(raw_data, dict):
                return raw_data, method
        except Exception as exc:
            print(f" -> AI extraction error for {item.get('post_id')} ({exc}); falling back to heuristic extractor...")
    return (
        heuristic_extract_event(
            content=content,
            sender=item.get("fanpage_name"),
            published_at=item.get("published_at") if isinstance(item.get("published_at"), datetime) else None,
        ),
        "heuristic",
    )


async def process_post_url(
    url: str,
    api_key: str,
    headless: bool = True,
    include_non_events: bool = False,
) -> Optional[Dict[str, Any]]:
    """Scrape a single Facebook post URL and extract structured event JSON."""
    if not is_facebook_post_url(url):
        print(f" -> Warning: '{url}' does not look like a standard Facebook post URL; attempting anyway...")

    print(f"\nScraping URL: {url}")
    post_data = await scrape_single_post(url, headless=headless)
    content = (post_data.get("content") or "").strip()
    media_urls: List[str] = post_data.get("media_urls") or []
    pub_at = post_data.get("published_at")
    pub_iso = pub_at.isoformat() if isinstance(pub_at, datetime) else datetime.now(LOCAL_TIMEZONE).isoformat()
    fanpage_name = post_data.get("fanpage_name")
    fanpage_url = post_data.get("fanpage_url")
    source_label = fanpage_name or fanpage_url or "Facebook Post"

    if not content and not media_urls:
        err_msg = post_data.get("error") or "Could not extract any text or media from this post (may require login or invalid URL)."
        print(f" -> Error: {err_msg}")
        return None

    item = {
        "post_id": post_data.get("post_id"),
        "post_url": post_data.get("post_url") or url,
        "source_link": post_data.get("post_url") or url,
        "input_url": url,
        "source_type": "facebook_url",
        "source_file": None,
        "content": content,
        "media_urls": media_urls,
        "published_at": pub_at if isinstance(pub_at, datetime) else None,
        "published_at_str": pub_iso,
        "fanpage_name": fanpage_name,
        "fanpage_url": fanpage_url,
        "source_label": source_label,
    }
    if api_key and content:
        print(" -> Content scraped. Calling Gemini for event extraction...")
    elif not api_key:
        print(" -> GEMINI_API_KEY not set; using local heuristic extractor...")

    raw_data, extraction_method = _extract_single_with_gemini_or_heuristic(item, api_key)
    record = _finalize_event_record(item, raw_data, extraction_method, include_non_events=include_non_events)
    if record is None:
        print(" -> Analyzed: Post does not appear to be an upcoming/ongoing event (skipped).")
    return record


def process_text_files(
    text_files: List[Path],
    api_key: str,
    include_non_events: bool = False,
    batch_size: int = GEMINI_BATCH_SIZE,
    delay_seconds: float = 0.5,
) -> List[Dict[str, Any]]:
    """Extract events from raw post `.txt` files (using batched Gemini calls with fallback)."""
    parsed_items: List[Dict[str, Any]] = []
    for path in text_files:
        try:
            item = _parse_post_text_file(path)
        except Exception as exc:
            print(f" -> Failed to read {path}: {exc}")
            continue
        if not item["content"]:
            print(f" -> Skipping empty file: {path.name}")
            continue
        parsed_items.append(item)

    if not parsed_items:
        return []

    results_map: Dict[str, Tuple[Dict[str, Any], str]] = {}
    has_ai = bool(_GLOBAL_KEY_POOL.has_keys() or api_key)
    if has_ai:
        effective_batch = max(1, batch_size)
        for start in range(0, len(parsed_items), effective_batch):
            if start > 0 and delay_seconds > 0:
                time.sleep(delay_seconds)
            chunk = parsed_items[start : start + effective_batch]
            chunk_names = ", ".join(it["post_id"] for it in chunk)
            batch_out: Optional[Dict[str, Dict[str, Any]]] = None
            batch_method = "gemini_batch"
            if len(chunk) > 1:
                print(f"\nCalling AI batch ({len(chunk)} posts): {chunk_names}")
                try:
                    batch_out, batch_method = call_gemini_batch_texts(chunk, api_key)
                except Exception as exc:
                    print(f" -> AI batch error ({exc}); falling back to per-post extraction...")
                    batch_out = None

            for item in chunk:
                pid = item["post_id"]
                if batch_out and isinstance(batch_out.get(pid), dict):
                    results_map[pid] = (batch_out[pid], batch_method)
                else:
                    if len(chunk) > 1 and batch_out is not None:
                        print(f" -> '{pid}' missing from batch response; retrying single-post call...")
                    if delay_seconds > 0 and len(chunk) > 1:
                        time.sleep(delay_seconds)
                    results_map[pid] = _extract_single_with_gemini_or_heuristic(item, api_key)
    else:
        for item in parsed_items:
            results_map[item["post_id"]] = _extract_single_with_gemini_or_heuristic(item, "")

    output_records: List[Dict[str, Any]] = []
    for item in parsed_items:
        pid = item["post_id"]
        raw_data, method = results_map[pid]
        rec = _finalize_event_record(item, raw_data, method, include_non_events=include_non_events)
        if rec is None:
            print(f" -> SKIPPED non-event: {pid} [{method}]")
            continue
        output_records.append(rec)
    return output_records


def _load_existing_json(output_path: Path, overwrite: bool) -> Tuple[Any, List[Dict[str, Any]]]:
    """Load existing JSON file (supports either a JSON list or {'events': [...]})."""
    if overwrite or not output_path.exists():
        return None, []
    try:
        with output_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, list):
            return None, [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("events"), list):
            return payload, [item for item in payload["events"] if isinstance(item, dict)]
    except Exception as exc:
        print(f"Warning: Could not parse existing JSON at {output_path} ({exc}); starting fresh list.")
    return None, []


def _upsert_record(records: List[Dict[str, Any]], new_rec: Dict[str, Any]) -> str:
    """Insert or update an event record in-place by source_file, post_id, post_url, input_url, or source_link."""
    new_pid = str(new_rec.get("post_id") or "").strip()
    new_url = str(new_rec.get("post_url") or "").strip()
    new_input = str(new_rec.get("input_url") or "").strip()
    new_file = str(new_rec.get("source_file") or "").strip()
    new_link = str(new_rec.get("source_link") or "").strip()

    for idx, existing in enumerate(records):
        ex_pid = str(existing.get("post_id") or "").strip()
        ex_url = str(existing.get("post_url") or "").strip()
        ex_input = str(existing.get("input_url") or "").strip()
        ex_file = str(existing.get("source_file") or "").strip()
        ex_link = str(existing.get("source_link") or "").strip()
        if (
            (new_file and ex_file == new_file)
            or (new_pid and ex_pid == new_pid)
            or (new_url and ex_url == new_url)
            or (new_input and ex_input == new_input)
            or (new_link and new_link != "email" and ex_link == new_link)
        ):
            records[idx] = new_rec
            return "updated"

    records.append(new_rec)
    return "added"


def _save_json(output_path: Path, wrapper: Optional[Dict[str, Any]], records: List[Dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if wrapper is not None:
        wrapper["events"] = records
        wrapper["count"] = len(records)
        wrapper["updated_at"] = datetime.now(LOCAL_TIMEZONE).isoformat()
        data_to_write: Any = wrapper
    else:
        data_to_write = records

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data_to_write, f, ensure_ascii=False, indent=2)


def _add_dir_files(dir_path: Path, prefix: str, text_files: List[Path], seen_files: Set[Path]) -> None:
    if not dir_path.is_dir():
        print(f"Error: Posts folder not found: {dir_path}")
        sys.exit(1)
    found = _discover_sample_txt_files(dir_path, prefix=prefix)
    if not found:
        print(f"Warning: No '{prefix}*.txt' files found in {dir_path}")
    for p in found:
        rp = p.resolve()
        if rp not in seen_files:
            seen_files.add(rp)
            text_files.append(rp)


def _classify_input_token(
    token: str,
    prefix: str,
    urls: List[str],
    seen_urls: Set[str],
    text_files: List[Path],
    seen_files: Set[Path],
) -> None:
    clean = token.strip()
    if not clean:
        return
    if clean.lower().startswith(("http://", "https://")):
        if clean not in seen_urls:
            seen_urls.add(clean)
            urls.append(clean)
        return
    cand = Path(clean).expanduser()
    if cand.is_dir():
        _add_dir_files(cand, prefix, text_files, seen_files)
        return
    if cand.is_file() and cand.suffix.lower() == ".txt":
        rp = cand.resolve()
        if rp not in seen_files:
            seen_files.add(rp)
            text_files.append(rp)
        return
    if clean not in seen_urls:
        seen_urls.add(clean)
        urls.append(clean)


def _collect_inputs_from_args_and_prompt(args: argparse.Namespace) -> Tuple[List[str], List[Path], Path]:
    urls: List[str] = []
    seen_urls: Set[str] = set()
    text_files: List[Path] = []
    seen_files: Set[Path] = set()

    if args.file:
        file_path = Path(args.file).expanduser()
        if not file_path.exists():
            print(f"Error: URLs file not found: {file_path}")
            sys.exit(1)
        for line in file_path.read_text(encoding="utf-8").splitlines():
            clean = line.strip()
            if clean and not clean.startswith("#") and clean not in seen_urls:
                seen_urls.add(clean)
                urls.append(clean)

    if args.posts_dir is not None:
        if args.posts_dir == "auto":
            detected = _find_default_posts_dir()
            if detected is None:
                print("Error: Could not locate a 'posts' folder automatically. Pass `-d <path/to/posts>`.")
                sys.exit(1)
            target_dir = detected
        else:
            target_dir = Path(args.posts_dir).expanduser()
        _add_dir_files(target_dir, args.prefix, text_files, seen_files)

    for raw in args.urls:
        for part in raw.split(","):
            _classify_input_token(part, args.prefix, urls, seen_urls, text_files, seen_files)

    output_arg = args.output
    if not urls and not text_files:
        if not output_arg:
            entered_out = input(f"Destination JSON file [{DEFAULT_OUTPUT_JSON}]: ").strip()
            output_arg = entered_out or DEFAULT_OUTPUT_JSON
        default_dir = _find_default_posts_dir()
        if default_dir:
            print(f"Detected posts folder: {default_dir}")
            print(
                f"Enter a folder path, .txt file paths, or Facebook post URLs (one per line). "
                f"Type 'posts' to load '{args.prefix}*.txt' from detected folder, then blank line to start:"
            )
        else:
            print("Enter a folder path, .txt file paths, or Facebook post URLs (one per line). Submit a blank line to start:")
        while True:
            try:
                line = input("> ").strip()
            except EOFError:
                break
            if not line:
                break
            if line.lower() == "posts" and default_dir:
                _add_dir_files(default_dir, args.prefix, text_files, seen_files)
                continue
            for part in line.split(","):
                _classify_input_token(part, args.prefix, urls, seen_urls, text_files, seen_files)
    elif not output_arg:
        output_arg = DEFAULT_OUTPUT_JSON

    if args.limit is not None and args.limit > 0:
        urls = urls[: args.limit]
        remaining = max(0, args.limit - len(urls))
        text_files = text_files[:remaining]

    return urls, text_files, Path(output_arg).expanduser().resolve()


async def async_main() -> int:
    parser = argparse.ArgumentParser(
        description="Standalone local Facebook post scraper & raw-text event extractor to JSON."
    )
    parser.add_argument(
        "urls",
        nargs="*",
        help="Facebook post URLs, a posts folder path, or raw post .txt file paths.",
    )
    parser.add_argument(
        "-d", "--dir", "--posts-dir",
        dest="posts_dir",
        nargs="?",
        const="auto",
        default=None,
        help="Folder containing post .txt files (auto-detects ./posts or ../posts if omitted).",
    )
    parser.add_argument(
        "--prefix",
        default="sample",
        help="Filename prefix filter when scanning --posts-dir (default: 'sample'; pass '' for all .txt).",
    )
    parser.add_argument(
        "-o", "--output",
        help=f"Destination JSON file path (default: {DEFAULT_OUTPUT_JSON}).",
    )
    parser.add_argument(
        "-f", "--file",
        help="Path to a text file containing Facebook post URLs (one per line).",
    )
    parser.add_argument(
        "--api-key",
        default="",
        help="Google Gemini API key(s) (comma-separated for key rotation). Defaults to GEMINI_API_KEYS / GEMINI_API_KEY env vars or DEFAULT_GEMINI_API_KEY.",
    )
    parser.add_argument(
        "--groq-key",
        default="",
        help="Groq API key for ultra-fast secondary fallback. Defaults to GROQ_API_KEY env var or DEFAULT_GROQ_API_KEY.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=GEMINI_BATCH_SIZE,
        help=f"Number of text files per Gemini batch call (default: {GEMINI_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay in seconds between Gemini requests when processing text files (default: 0.5).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of inputs to process.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite destination JSON instead of appending/updating existing records.",
    )
    parser.add_argument(
        "--include-non-events",
        action="store_true",
        help="Also save posts that were classified as non-events (is_event=false).",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Run Chromium with a visible window instead of headless mode.",
    )
    args = parser.parse_args()

    urls, text_files, output_path = _collect_inputs_from_args_and_prompt(args)
    if not urls and not text_files:
        print("No URLs or text files provided. Exiting.")
        return 1

    if args.api_key:
        _GLOBAL_KEY_POOL.add_keys(args.api_key)
    if args.groq_key:
        os.environ["GROQ_API_KEY"] = args.groq_key.strip()

    # Load defaults & env into pool
    _GLOBAL_KEY_POOL.add_keys(os.environ.get("GEMINI_API_KEYS"))
    _GLOBAL_KEY_POOL.add_keys(os.environ.get("GEMINI_API_KEY"))
    _GLOBAL_KEY_POOL.add_keys(DEFAULT_GEMINI_API_KEYS)
    _GLOBAL_KEY_POOL.add_keys(DEFAULT_GEMINI_API_KEY)

    has_gemini = _GLOBAL_KEY_POOL.has_keys()
    if has_gemini:
        print(f"Gemini API configured with {len(_GLOBAL_KEY_POOL.keys)} key(s) in rotation pool (auto-discovering 2.x models).")
    else:
        print("Note: Gemini API key not set; local heuristic extraction will be used.")

    api_key = _GLOBAL_KEY_POOL.get_active_key() or ""

    wrapper, records = _load_existing_json(output_path, overwrite=args.overwrite)
    added_or_updated = 0

    if text_files:
        print(f"Found {len(text_files)} post text file(s) to process.")
        extracted_text_records = process_text_files(
            text_files=text_files,
            api_key=api_key,
            include_non_events=args.include_non_events,
            batch_size=args.batch_size,
            delay_seconds=args.delay,
        )
        for rec in extracted_text_records:
            action = _upsert_record(records, rec)
            added_or_updated += 1
            print(
                f" -> {action.upper()} [{rec.get('post_id')}]: '{rec.get('title') or 'Non-event'}' "
                f"[is_event={rec.get('is_event')}, {rec.get('extraction_method')}]"
            )

    for url in urls:
        try:
            rec = await process_post_url(
                url=url,
                api_key=api_key,
                headless=not args.headful,
                include_non_events=args.include_non_events,
            )
            if rec:
                action = _upsert_record(records, rec)
                added_or_updated += 1
                print(
                    f" -> {action.upper()}: '{rec.get('title') or 'Non-event'}' "
                    f"[{rec.get('extraction_method')}]"
                )
        except Exception as exc:
            print(f" -> Failed to process {url}: {exc}")

    if added_or_updated > 0 or args.overwrite:
        _save_json(output_path, wrapper, records)
        print(f"\nDone! Saved {added_or_updated} post(s) (total {len(records)} in file) -> {output_path}")
    else:
        print("\nNo events were extracted or saved.")
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()
