import asyncio
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

from playwright.async_api import async_playwright, Page, Response

from config import (
    LOCAL_TIMEZONE,
    PAGE_TIMEOUT_MS,
    SCROLL_COUNT,
    SCROLL_DELAY_MS,
    USER_AGENT,
)

# Keys in Facebook Relay/GraphQL JSON that represent comments, shares, or unrelated widgets
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


def _extract_page_slug(page_url: str) -> str:
    parsed = urlparse(page_url)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if not parts:
        return ""
    if parts[0] == "profile.php":
        qs = parse_qs(parsed.query)
        return qs.get("id", [""])[0].lower()
    return parts[-1].lower()


def _extract_post_id_from_url(url: str) -> Optional[str]:
    """Extract a canonical post identifier from a Facebook post URL."""
    if not url:
        return None

    m = re.search(r"/posts/(pfbid[A-Za-z0-9]+|\d+)", url)
    if m:
        return m.group(1)

    m = re.search(r"/(?:videos|reel)/(\d+)", url)
    if m:
        return m.group(1)

    m = re.search(r"/photos/[^/]+/(\d+)", url)
    if m:
        return m.group(1)

    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    for key in ("story_fbid", "fbid"):
        if key in qs and qs[key]:
            return qs[key][0]

    m = re.search(r"(pfbid[A-Za-z0-9]{15,})", url)
    if m:
        return m.group(1)

    return None


def _clean_post_url(url: str, base_page_url: str) -> str:
    """Strip tracking query params from a Facebook post URL."""
    if not url:
        return base_page_url
    if url.startswith("/"):
        url = urljoin("https://www.facebook.com", url)

    parsed = urlparse(url)
    if "permalink.php" in parsed.path or "story.php" in parsed.path or "photo" in parsed.path:
        qs = parse_qs(parsed.query)
        keep = {}
        for k in ("story_fbid", "id", "fbid"):
            if k in qs:
                keep[k] = qs[k][0]
        if keep:
            query_str = "&".join(f"{k}={v}" for k, v in keep.items())
            return f"https://www.facebook.com{parsed.path}?{query_str}"
    return f"https://www.facebook.com{parsed.path}".rstrip("/")


def _find_first_key(obj: Any, target_key: str) -> Any:
    """Recursively find the first occurrence of target_key in a nested dict/list, ignoring SKIP_JSON_KEYS."""
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


def _has_non_null_attached_story(obj: Any) -> bool:
    """Check whether a Story node contains a shared/attached story from another post or page."""
    if isinstance(obj, dict):
        if obj.get("attached_story") is not None:
            return True
        for k, v in obj.items():
            if k in ("feedback", "comments", "display_comments"):
                continue
            if _has_non_null_attached_story(v):
                return True
    elif isinstance(obj, list):
        for item in obj:
            if _has_non_null_attached_story(item):
                return True
    return False


def _belongs_to_fanpage(actors: Any, permalink: Optional[str], page_slug: str) -> bool:
    """Verify that the Story node was authored by the target fanpage and not another page or visitor."""
    if not page_slug:
        return True

    if isinstance(actors, list) and actors:
        first_actor = actors[0]
        if isinstance(first_actor, dict):
            actor_url = (first_actor.get("url") or "").lower()
            if actor_url and "facebook.com/" in actor_url:
                actor_slug = _extract_page_slug(actor_url)
                if actor_slug and actor_slug != page_slug and not page_slug.startswith("profile_"):
                    return False

    if isinstance(permalink, str) and "facebook.com/" in permalink:
        parsed = urlparse(permalink)
        parts = [p for p in parsed.path.strip("/").split("/") if p]
        if parts and parts[0].lower() == "people":
            # If the target page is not a /people/ URL, reject visitor posts from /people/
            if not page_slug.startswith("people_"):
                return False
        if len(parts) >= 2 and parts[1].lower() in ("posts", "videos", "photos"):
            owner_slug = parts[0].lower()
            if owner_slug != page_slug and not page_slug.startswith("profile_"):
                return False

    return True


def _collect_media_urls(node: Any, found: Set[str], actor_id: Optional[str] = None) -> None:
    """
    Extract direct photo attachment URLs from a Story's attachments
    (excluding shared media from other pages, videos from other owners, and comment images).
    """
    if isinstance(node, dict):
        typename = node.get("__typename", "")
        if typename in ("ExternalUrl", "Story"):
            return

        # If this node is a Media object (Photo/Video), verify its owner matches the fanpage actor_id
        owner = node.get("owner")
        if isinstance(owner, dict) and actor_id and owner.get("id") and str(owner.get("id")) != str(actor_id):
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
            if k in SKIP_JSON_KEYS:
                continue
            _collect_media_urls(v, found, actor_id=actor_id)
    elif isinstance(node, list):
        for item in node:
            _collect_media_urls(item, found, actor_id=actor_id)


def _extract_comet_story_unit(
    data: Dict[str, Any],
    page_url: str,
    page_slug: str,
    collected: Dict[str, Dict],
) -> None:
    """
    Extract a single timeline post from a top-level Facebook Comet `Story` unit (containing `comet_sections`).
    Strictly filters out shared posts (`attached_story`), posts by other pages, and comments.
    """
    comet = data.get("comet_sections")
    if not isinstance(comet, dict):
        return

    # 1. Reject if this timeline unit is a shared post (contains attached_story)
    if _has_non_null_attached_story(data):
        return

    content_section = comet.get("content") or {}
    content_story = content_section.get("story") if isinstance(content_section, dict) else {}
    if not isinstance(content_story, dict):
        content_story = {}

    context_layout = comet.get("context_layout") or {}

    # 2. Extract canonical numeric post_id
    post_id = str(
        content_story.get("post_id")
        or data.get("post_id")
        or _find_first_key(content_story, "post_id")
        or ""
    ).strip()

    # 3. Extract post permalink URL (strictly from content_story.wwwURL or context_layout metadata, NEVER from attachments)
    permalink = (
        content_story.get("wwwURL")
        or content_story.get("url")
        or data.get("wwwURL")
        or _find_first_key(context_layout, "url")
        or _find_first_key(comet.get("timestamp"), "url")
    )

    # 4. Extract author/actor info and verify it belongs to this fanpage
    actors = content_story.get("actors") or data.get("actors") or _find_first_key(context_layout, "actors")
    if not _belongs_to_fanpage(actors, permalink, page_slug):
        return

    actor_id = None
    if isinstance(actors, list) and actors and isinstance(actors[0], dict):
        actor_id = actors[0].get("id")

    # 5. Extract only the fanpage's own post message text (never comments or shared text)
    message_text = ""
    msg_obj = content_story.get("message") or data.get("message")
    if isinstance(msg_obj, dict) and isinstance(msg_obj.get("text"), str):
        message_text = msg_obj["text"]
    elif isinstance(content_section, dict):
        # Sometimes message is nested inside content_section.story.comet_sections.message
        msg_container = _find_first_key(content_story.get("comet_sections"), "message")
        if isinstance(msg_container, dict) and isinstance(msg_container.get("text"), str):
            message_text = msg_container["text"]
        else:
            text_val = _find_first_key(content_story.get("comet_sections"), "text")
            if isinstance(text_val, str):
                message_text = text_val

    # 6. Extract creation_time from context_layout / metadata and convert to LOCAL_TIMEZONE (GMT+7)
    creation_time = (
        _find_first_key(context_layout, "creation_time")
        or _find_first_key(comet.get("timestamp"), "creation_time")
        or content_story.get("creation_time")
        or data.get("creation_time")
    )

    if not post_id and isinstance(permalink, str):
        post_id = _extract_post_id_from_url(permalink) or ""

    # 7. Extract direct attachments belonging to this fanpage
    media_set: Set[str] = set()
    attachments = content_story.get("attachments") or data.get("attachments")
    if attachments:
        _collect_media_urls(attachments, media_set, actor_id=actor_id)

    if not post_id or (not message_text and not media_set):
        return

    clean_url = (
        _clean_post_url(permalink, page_url)
        if isinstance(permalink, str) and "facebook.com" in permalink
        else f"{page_url.rstrip('/')}/posts/{post_id}"
    )

    # Final check that the resolved permalink is not pointing to another user/page
    if not _belongs_to_fanpage(actors, clean_url, page_slug):
        return

    published_dt = datetime.now(LOCAL_TIMEZONE)
    if isinstance(creation_time, (int, float)) and creation_time > 1000000000:
        published_dt = datetime.fromtimestamp(creation_time, tz=LOCAL_TIMEZONE)

    existing = collected.get(post_id)
    if not existing or (len(message_text) > len(existing.get("content", ""))):
        collected[post_id] = {
            "post_id": post_id,
            "post_url": clean_url,
            "content": message_text.strip(),
            "media_urls": list(media_set),
            "published_at": published_dt,
        }


def _walk_graphql_for_posts(
    data: Any,
    page_url: str,
    page_slug: str,
    collected: Dict[str, Dict],
) -> None:
    """
    Walk Facebook Relay / GraphQL JSON objects to find top-level timeline Story units.
    Once a top-level Story with `comet_sections` is processed, we do NOT recurse into it,
    preventing duplicate extractions, comment extractions, and shared inner story extractions.
    """
    if isinstance(data, dict):
        if isinstance(data.get("comet_sections"), dict) and (
            data.get("__typename") == "Story" or "content" in data["comet_sections"]
        ):
            _extract_comet_story_unit(data, page_url, page_slug, collected)
            return

        for k, v in data.items():
            if k in SKIP_JSON_KEYS:
                continue
            _walk_graphql_for_posts(v, page_url, page_slug, collected)

    elif isinstance(data, list):
        for item in data:
            _walk_graphql_for_posts(item, page_url, page_slug, collected)


async def _dismiss_login_popup(page: Page) -> None:
    """Close Facebook's unauthenticated login modal/banner if it appears."""
    try:
        await page.keyboard.press("Escape")
    except Exception:
        pass

    close_selectors = [
        'div[role="dialog"] div[aria-label="Close"]',
        'div[role="dialog"] div[aria-label="Đóng"]',
        'div[role="dialog"] [aria-label="Close"]',
        '[aria-label="Close"][role="button"]',
    ]
    for selector in close_selectors:
        try:
            btn = page.locator(selector).first
            if await btn.is_visible(timeout=500):
                await btn.click(timeout=1000)
                await page.wait_for_timeout(300)
                break
        except Exception:
            continue

    try:
        await page.evaluate(
            """() => {
                const dialogs = document.querySelectorAll('div[role="dialog"]');
                dialogs.forEach(d => d.remove());
                if (document.body) {
                    document.body.style.overflow = 'auto';
                }
            }"""
        )
    except Exception:
        pass


async def scrape_fanpage(page_url: str, scroll_count: int = SCROLL_COUNT) -> Tuple[Optional[str], List[Dict]]:
    """
    Scrape a public Facebook fanpage without logging in.
    Extracts only the fanpage's own posts (excluding comments and shared media from other pages).
    Returns (page_title, list_of_post_dicts).
    """
    collected_posts: Dict[str, Dict] = {}
    page_title: Optional[str] = None
    page_slug = _extract_page_slug(page_url)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
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
                            _walk_graphql_for_posts(data, page_url, page_slug, collected_posts)
                        except json.JSONDecodeError:
                            continue
                except Exception:
                    pass

        page.on("response", handle_response)

        try:
            await page.goto(page_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            await page.wait_for_timeout(2500)

            raw_title = await page.title()
            if raw_title:
                page_title = re.sub(r"\s*\|\s*Facebook.*$", "", raw_title, flags=re.IGNORECASE).strip()
                page_title = re.sub(r"^\(\d+\)\s*", "", page_title).strip()

            await _dismiss_login_popup(page)

            # Parse embedded <script type="application/json"> Relay payloads from initial HTML
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
                    _walk_graphql_for_posts(data, page_url, page_slug, collected_posts)
                except Exception:
                    continue

            # Scroll down to trigger lazy loading of timeline posts
            for _ in range(scroll_count):
                await _dismiss_login_popup(page)
                await page.evaluate("window.scrollBy(0, window.innerHeight * 1.6)")
                await page.wait_for_timeout(SCROLL_DELAY_MS)

        finally:
            await context.close()
            await browser.close()

    posts_list = sorted(
        collected_posts.values(),
        key=lambda x: x.get("published_at", datetime.now(LOCAL_TIMEZONE)),
        reverse=True,
    )
    return page_title, posts_list


async def scrape_multiple_fanpages(
    fanpages: List[Any], delay_between_pages: float = 4.0
) -> Dict[int, Tuple[Optional[str], List[Dict], Optional[str]]]:
    """
    Scrape a list of Fanpage ORM objects sequentially with a polite delay.
    Returns dict mapping fanpage.id -> (page_title, posts_list, error_message).
    """
    results: Dict[int, Tuple[Optional[str], List[Dict], Optional[str]]] = {}
    for idx, fp in enumerate(fanpages):
        try:
            title, posts = await scrape_fanpage(fp.url)
            results[fp.id] = (title, posts, None)
        except Exception as exc:
            results[fp.id] = (None, [], str(exc))

        if idx < len(fanpages) - 1:
            await asyncio.sleep(delay_between_pages)

    return results
