import asyncio
import hashlib
import json
import re
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Response

from config import (
    LOCAL_TIMEZONE,
    PAGE_TIMEOUT_MS,
    SCRAPE_TIMEZONE,
    SCROLL_COUNT,
    SCROLL_DELAY_MS,
    USER_AGENT,
)
from database import clean_fanpage_name, is_valid_fanpage_name

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
    """Find a Facebook post permalink URL inside metadata/context_layout (preferring URLs with a post ID)."""
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
    """Fallback text extraction for shared event/link cards that store title/description in attachments."""
    parts: List[str] = []
    for key in ("title_with_entities", "card_title", "description"):
        obj = _find_first_key(node, key)
        if isinstance(obj, dict) and isinstance(obj.get("text"), str):
            val = obj["text"].strip()
            if val and val not in parts:
                parts.append(val)
    return "\n".join(parts)


def _belongs_to_fanpage(actors: Any, permalink: Optional[str], page_slug: str) -> bool:
    """Verify that the Story node was authored/shared by the target fanpage and not a visitor."""
    if not page_slug:
        return True

    is_numeric_or_profile = page_slug.isdigit() or page_slug.startswith(("profile_", "people_"))

    if isinstance(actors, list) and actors:
        first_actor = actors[0]
        if isinstance(first_actor, dict):
            actor_url = (first_actor.get("url") or "").lower()
            if actor_url and "facebook.com/" in actor_url:
                actor_slug = _extract_page_slug(actor_url)
                if actor_slug and actor_slug != page_slug and not is_numeric_or_profile:
                    return False

    if isinstance(permalink, str) and "facebook.com/" in permalink:
        parsed = urlparse(permalink)
        parts = [p for p in parsed.path.strip("/").split("/") if p]
        if parts and parts[0].lower() == "people":
            if not is_numeric_or_profile and parts[-1].lower() != page_slug:
                return False
        if len(parts) >= 2 and parts[1].lower() in ("posts", "videos", "photos"):
            owner_slug = parts[0].lower()
            if owner_slug != page_slug and not is_numeric_or_profile:
                return False

    return True


def _collect_media_urls(
    node: Any,
    found: Set[str],
    actor_id: Optional[str] = None,
    allow_shared: bool = True,
) -> None:
    """
    Extract photo attachment URLs from a Story's attachments (including shared post attachments
    when `allow_shared=True`, while excluding comment images and unrelated sidebars).
    """
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
    collected: Dict[str, Dict],
) -> None:
    """
    Extract a single timeline post from a top-level Facebook Comet `Story` unit (containing `comet_sections`).
    Includes both original fanpage posts and posts shared by the fanpage (`attached_story`),
    while filtering out visitor posts and comments.
    """
    comet = data.get("comet_sections")
    if not isinstance(comet, dict):
        return

    content_section = comet.get("content") or {}
    content_story = content_section.get("story") if isinstance(content_section, dict) else {}
    if not isinstance(content_story, dict):
        content_story = {}

    context_layout = comet.get("context_layout") or {}
    attached_story = _find_attached_story(data)

    # 1. Extract canonical post_id (prefer outer fanpage story ID)
    post_id = str(
        content_story.get("post_id")
        or data.get("post_id")
        or _find_first_key(content_story, "post_id")
        or _find_first_key(context_layout, "post_id")
        or ""
    ).strip()

    # 2. Extract post permalink URL (preferring URLs that contain a canonical post ID)
    permalink = (
        content_story.get("wwwURL")
        or content_story.get("url")
        or data.get("wwwURL")
        or _find_post_permalink(context_layout)
        or _find_post_permalink(comet.get("timestamp"))
        or _find_first_key(context_layout, "url")
        or _find_first_key(comet.get("timestamp"), "url")
    )

    # 3. Extract author/actor info of the outer timeline unit and verify it belongs to this fanpage
    actors = content_story.get("actors") or data.get("actors") or _find_first_key(context_layout, "actors")
    if not _belongs_to_fanpage(actors, permalink, page_slug):
        return

    actor_id = None
    if isinstance(actors, list) and actors and isinstance(actors[0], dict):
        actor_id = actors[0].get("id")

    # 4. Extract outer fanpage message + shared post (`attached_story`) message if present
    outer_text = _extract_story_message(content_story) or _extract_story_message(data)
    shared_text = ""
    if isinstance(attached_story, dict):
        shared_text = _extract_story_message(attached_story)
        if not shared_text:
            shared_text = _extract_attachment_card_text(attached_story)

    if outer_text and shared_text and shared_text not in outer_text:
        message_text = f"{outer_text}\n\n{shared_text}"
    else:
        message_text = outer_text or shared_text

    if not message_text:
        message_text = _extract_attachment_card_text(content_story)

    # 5. Extract creation_time from context_layout / metadata (or fallback to attached_story)
    creation_time = (
        _find_first_key(context_layout, "creation_time")
        or _find_first_key(comet.get("timestamp"), "creation_time")
        or content_story.get("creation_time")
        or data.get("creation_time")
        or ( _find_first_key(attached_story, "creation_time") if isinstance(attached_story, dict) else None )
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

    # 6. Extract media URLs from both the outer post and any shared/attached post
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

    # Final check that the resolved permalink is not pointing to another user/page
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
    if not existing or (len(message_text) > len(existing.get("content", ""))):
        collected[post_id] = {
            "post_id": post_id,
            "post_url": clean_url,
            "content": message_text.strip(),
            "media_urls": list(media_set),
            "published_at": published_dt,
            "author_name": actor_name or (existing.get("author_name") if existing else None),
            "author_url": actor_url or (existing.get("author_url") if existing else None),
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


_STEALTH_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
"""


async def _launch_browser(p: Any) -> Browser:
    return await p.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-notifications",
            "--no-sandbox",
        ],
    )


async def _new_stealth_context(browser: Browser) -> BrowserContext:
    tz_id = getattr(SCRAPE_TIMEZONE, "key", None) or (
        SCRAPE_TIMEZONE if isinstance(SCRAPE_TIMEZONE, str) else "Asia/Ho_Chi_Minh"
    )
    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1280, "height": 900},
        locale="en-US",
        timezone_id=tz_id,
        extra_http_headers={"Accept-Language": "en-US,en;q=0.9,vi;q=0.8"},
    )
    await context.add_init_script(_STEALTH_INIT_SCRIPT)
    return context


def _attach_graphql_listener(
    page: Page,
    get_page_url: Any,
    page_slug: str,
    collected_posts: Dict[str, Dict],
) -> None:
    async def handle_response(response: Response) -> None:
        if "/api/graphql" in response.url and response.status == 200:
            try:
                text = await response.text()
                base_url = get_page_url()
                for line in text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        _walk_graphql_for_posts(data, base_url, page_slug, collected_posts)
                    except json.JSONDecodeError:
                        continue
            except Exception:
                pass

    page.on("response", handle_response)


async def _parse_embedded_scripts(
    page: Page,
    page_url: str,
    page_slug: str,
    collected_posts: Dict[str, Dict],
) -> None:
    """Parse embedded <script type="application/json"> Relay payloads from initial HTML."""
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


async def _dismiss_login_popup(page: Page) -> None:
    """Close Facebook's unauthenticated login modal/banner and restore page scrollability."""
    try:
        has_dialog = await page.evaluate(
            "() => document.querySelectorAll('div[role=\"dialog\"]').length > 0"
        )
    except Exception:
        has_dialog = False

    if has_dialog:
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass

        close_selectors = [
            'div[role="dialog"] [aria-label="Close"]',
            'div[role="dialog"] [aria-label="Đóng"]',
            '[aria-label="Close"][role="button"]',
        ]
        for selector in close_selectors:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=250):
                    await btn.click(timeout=800)
                    await page.wait_for_timeout(200)
                    break
            except Exception:
                continue

    try:
        await page.evaluate(
            """() => {
                document.querySelectorAll('div[role="dialog"]').forEach(d => d.remove());
                document.documentElement.style.overflow = 'auto';
                if (document.body) {
                    document.body.style.overflow = 'auto';
                }
            }"""
        )
    except Exception:
        pass


async def _extract_page_metadata(page: Page) -> Dict[str, Any]:
    """Capture document title, URL, og:title, heading candidates, and block-wall signals."""
    try:
        return await page.evaluate(
            """() => {
                const ogTitleEl = document.querySelector('meta[property="og:title"], meta[name="og:title"]');
                const h1Texts = Array.from(document.querySelectorAll('h1'))
                    .map(el => (el.innerText || '').trim())
                    .filter(Boolean);
                return {
                    title: (document.title || '').trim(),
                    url: window.location.href || '',
                    og_title: ogTitleEl ? (ogTitleEl.getAttribute('content') || '').trim() : '',
                    h1_texts: h1Texts,
                    has_login_form: Boolean(
                        document.querySelector('form[action*="login"], input[name="pass"][type="password"]')
                    ),
                    article_count: document.querySelectorAll('div[role="article"]').length,
                };
            }"""
        )
    except Exception:
        return {
            "title": "",
            "url": page.url or "",
            "og_title": "",
            "h1_texts": [],
            "has_login_form": False,
            "article_count": 0,
        }


def _resolve_fanpage_title(
    collected_posts: Dict[str, Dict],
    meta: Dict[str, Any],
) -> Optional[str]:
    """Select the cleanest genuine fanpage name, rejecting Facebook/login/error titles."""
    author_counts = Counter(
        cleaned
        for post in collected_posts.values()
        if (cleaned := clean_fanpage_name(post.get("author_name"))) is not None
    )
    if author_counts:
        return author_counts.most_common(1)[0][0]

    candidates = [
        *(meta.get("h1_texts") or []),
        meta.get("og_title"),
        meta.get("title"),
    ]
    for raw in candidates:
        cleaned = clean_fanpage_name(raw)
        if cleaned:
            return cleaned
    return None


def _is_blocked_shell(meta: Dict[str, Any], collected_posts: Dict[str, Dict]) -> bool:
    """Return True when Facebook returned a login wall, checkpoint, or unhydrated shell with 0 posts."""
    if collected_posts:
        return False
    url_lower = (meta.get("url") or "").lower()
    if any(token in url_lower for token in ("/login", "/checkpoint", "/recover/")):
        return True
    if meta.get("article_count", 0) == 0:
        if meta.get("has_login_form") or clean_fanpage_name(meta.get("title")) is None:
            return True
    return False


async def _extract_dom_fallback_posts(
    page: Page,
    page_url: str,
    collected_posts: Dict[str, Dict],
) -> None:
    """Extract posts directly from top-level DOM role='article' nodes when GraphQL missed them."""
    try:
        dom_articles = await page.evaluate(
            """() => {
                const topArticles = Array.from(document.querySelectorAll('div[role="article"]'))
                    .filter(el => !el.parentElement || !el.parentElement.closest('div[role="article"]'));
                return topArticles.slice(0, 10).map(article => {
                    const msgEls = Array.from(
                        article.querySelectorAll('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]')
                    );
                    const msgText = msgEls.length
                        ? msgEls.map(m => (m.innerText || '').trim()).filter(Boolean).join('\\n')
                        : Array.from(article.querySelectorAll('div[dir="auto"]'))
                              .map(d => (d.innerText || '').trim())
                              .filter(t => t.length > 20)
                              .slice(0, 4)
                              .join('\\n');
                    const linkEl = article.querySelector(
                        'a[href*="/posts/"], a[href*="permalink.php"], a[href*="story.php"], a[href*="/reel/"], a[href*="/videos/"], a[href*="/photo"]'
                    );
                    const authorEl = article.querySelector('h2 a, h3 a, h4 a, strong a');
                    const imgs = Array.from(article.querySelectorAll('img[src]'))
                        .filter(img => {
                            const src = img.getAttribute('src') || '';
                            if (!src.includes('scontent') && !src.includes('fbcdn')) return false;
                            if (src.includes('emoji.php')) return false;
                            return (img.naturalWidth || img.width || 200) >= 120;
                        })
                        .map(img => img.getAttribute('src'));
                    return {
                        href: linkEl ? (linkEl.getAttribute('href') || '').trim() : '',
                        content: (msgText || '').trim(),
                        author_name: authorEl ? (authorEl.innerText || '').trim() : '',
                        author_url: authorEl ? (authorEl.getAttribute('href') || '').trim() : '',
                        media_urls: Array.from(new Set(imgs)).slice(0, 6),
                    };
                });
            }"""
        )
    except Exception:
        return

    now_local = datetime.now(LOCAL_TIMEZONE)
    for item in dom_articles or []:
        href = item.get("href") or ""
        post_id = _extract_post_id_from_url(href)
        content = (item.get("content") or "").strip()
        media_urls = item.get("media_urls") or []
        if not post_id or (len(content) < 10 and not media_urls):
            continue
        if post_id in collected_posts:
            continue
        collected_posts[post_id] = {
            "post_id": post_id,
            "post_url": _clean_post_url(href, page_url),
            "content": content,
            "media_urls": media_urls,
            "published_at": now_local,
            "author_name": clean_fanpage_name(item.get("author_name")),
            "author_url": item.get("author_url") or None,
        }


def _build_fanpage_candidate_urls(page_url: str) -> List[str]:
    """Return primary fanpage URL followed by /posts fallback URL when applicable."""
    candidates = [page_url]
    parsed = urlparse(page_url)
    path = parsed.path.strip("/")
    if path.lower() == "profile.php":
        if "sk=posts" not in parsed.query.lower():
            sep = "&" if parsed.query else "?"
            candidates.append(f"{page_url}{sep}sk=posts")
    elif not path.lower().endswith("/posts"):
        candidates.append(f"{page_url.rstrip('/')}/posts")
    return candidates


async def scrape_fanpage(page_url: str, scroll_count: int = SCROLL_COUNT) -> Tuple[Optional[str], List[Dict]]:
    """
    Scrape a public Facebook fanpage without logging in.
    Extracts only the fanpage's own posts (excluding comments and shared media from other pages).
    Returns (cleaned_page_title, list_of_post_dicts).
    Raises RuntimeError when Facebook blocks access with a login/checkpoint wall and 0 posts.
    """
    collected_posts: Dict[str, Dict] = {}
    page_slug = _extract_page_slug(page_url)
    last_meta: Dict[str, Any] = {}
    resolved_title: Optional[str] = None
    candidate_urls = _build_fanpage_candidate_urls(page_url)

    async with async_playwright() as p:
        browser = await _launch_browser(p)
        try:
            for attempt_idx, target_url in enumerate(candidate_urls):
                context = await _new_stealth_context(browser)
                page = await context.new_page()
                _attach_graphql_listener(page, lambda u=target_url: u, page_slug, collected_posts)

                try:
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
                    try:
                        await page.wait_for_selector('div[role="article"]', timeout=4500)
                        await page.wait_for_timeout(1000)
                    except Exception:
                        await page.wait_for_timeout(2500)

                    await _dismiss_login_popup(page)
                    await _parse_embedded_scripts(page, target_url, page_slug, collected_posts)

                    # Scroll down to trigger lazy loading of timeline posts
                    for _ in range(scroll_count):
                        await _dismiss_login_popup(page)
                        prev_y = await page.evaluate("() => window.scrollY")
                        await page.evaluate("window.scrollBy(0, window.innerHeight * 1.6)")
                        await page.wait_for_timeout(SCROLL_DELAY_MS)
                        new_y = await page.evaluate("() => window.scrollY")
                        if new_y == prev_y:
                            await _dismiss_login_popup(page)
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await page.wait_for_timeout(1200)

                    last_meta = await _extract_page_metadata(page)
                    if not collected_posts and last_meta.get("article_count", 0) > 0:
                        await _extract_dom_fallback_posts(page, target_url, collected_posts)

                    candidate_title = _resolve_fanpage_title(collected_posts, last_meta)
                    if candidate_title:
                        resolved_title = candidate_title

                    if collected_posts:
                        break
                    if attempt_idx == len(candidate_urls) - 1:
                        break
                finally:
                    await context.close()
        finally:
            await browser.close()

    if not collected_posts and _is_blocked_shell(last_meta, collected_posts):
        raise RuntimeError(
            f"Blocked by Facebook login/checkpoint wall (title={last_meta.get('title')!r}, url={last_meta.get('url')!r})"
        )

    posts_list = sorted(
        collected_posts.values(),
        key=lambda x: x.get("published_at", datetime.now(LOCAL_TIMEZONE)),
        reverse=True,
    )
    return resolved_title, posts_list


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


_POST_URL_RE = re.compile(
    r"(/posts/|/permalink\.php|/story\.php|/reel/|/videos/|/photos/|/photo(?:\.php|\?)|/share/)",
    flags=re.IGNORECASE,
)


def is_facebook_post_url(url: str) -> bool:
    """Return True if the given URL looks like a Facebook post/reel/photo/share URL rather than a fanpage root."""
    if not url:
        return False
    s = url.strip()
    if "facebook.com" not in s.lower() and "fb.com" not in s.lower():
        return False
    return bool(_POST_URL_RE.search(s))


def _derive_fanpage_url_from_url(url: Optional[str]) -> Optional[str]:
    """Derive a canonical Facebook fanpage URL from a post permalink or actor URL."""
    if not url or "facebook.com" not in url.lower():
        return None

    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if not path:
        return None

    parts = [p for p in path.split("/") if p]
    if not parts:
        return None

    first = parts[0].lower()
    if first == "profile.php":
        qs = parse_qs(parsed.query)
        pid = qs.get("id", [""])[0]
        return f"https://www.facebook.com/profile.php?id={pid}" if pid else None

    if first in ("permalink.php", "story.php"):
        qs = parse_qs(parsed.query)
        pid = qs.get("id", [""])[0]
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


async def scrape_single_post(post_url: str) -> Dict[str, Any]:
    """
    Scrape a single public Facebook post URL without logging in.
    Returns dict with post_id, post_url, content, media_urls, published_at, fanpage_url, fanpage_name.
    """
    import hashlib

    target_url = post_url.strip()
    if not target_url.startswith(("http://", "https://")):
        target_url = "https://" + target_url

    collected_posts: Dict[str, Dict] = {}
    final_url = target_url
    page_title: Optional[str] = None
    dom_fallback: Dict[str, Any] = {}

    async with async_playwright() as p:
        browser = await _launch_browser(p)
        context = await _new_stealth_context(browser)
        page = await context.new_page()
        _attach_graphql_listener(page, lambda: final_url, "", collected_posts)

        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            await page.wait_for_timeout(2500)
            final_url = page.url or target_url

            raw_title = await page.title()
            page_title = clean_fanpage_name(raw_title)

            await _dismiss_login_popup(page)
            await _parse_embedded_scripts(page, final_url, "", collected_posts)

            if not collected_posts:
                await page.evaluate("window.scrollBy(0, 600)")
                await page.wait_for_timeout(2000)

            dom_fallback = await page.evaluate(
                """() => {
                    const meta = (prop) => {
                        const el = document.querySelector(`meta[property="${prop}"], meta[name="${prop}"]`);
                        return el ? (el.getAttribute('content') || '').trim() : '';
                    };
                    const msgEl = document.querySelector('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]');
                    const articleEl = document.querySelector('div[role="article"]');
                    const authorLink = articleEl ? articleEl.querySelector('h2 a[href*="facebook.com"], h3 a[href*="facebook.com"], strong a[href*="facebook.com"]') : null;
                    const canonicalEl = document.querySelector('link[rel="canonical"]');
                    const h1Texts = Array.from(document.querySelectorAll('h1'))
                        .map(el => (el.innerText || '').trim())
                        .filter(Boolean);
                    return {
                        og_title: meta('og:title'),
                        og_description: meta('og:description'),
                        og_image: meta('og:image'),
                        canonical: canonicalEl ? (canonicalEl.getAttribute('href') || '').trim() : '',
                        msg_text: msgEl ? (msgEl.innerText || '').trim() : '',
                        article_text: articleEl ? (articleEl.innerText || '').trim() : '',
                        author_href: authorLink ? (authorLink.getAttribute('href') || '').trim() : '',
                        author_text: authorLink ? (authorLink.innerText || '').trim() : '',
                        h1_texts: h1Texts,
                    };
                }"""
            )
        finally:
            await context.close()
            await browser.close()

    expected_ids = {
        pid for pid in (
            _extract_post_id_from_url(target_url),
            _extract_post_id_from_url(final_url),
            _extract_post_id_from_url(dom_fallback.get("canonical", "")),
        ) if pid
    }

    chosen: Optional[Dict[str, Any]] = None
    if collected_posts:
        for pid in expected_ids:
            if pid in collected_posts:
                chosen = collected_posts[pid]
                break
        if chosen is None:
            chosen = max(collected_posts.values(), key=lambda x: len(x.get("content", "")))

    if chosen is None:
        fallback_text = (
            dom_fallback.get("msg_text")
            or dom_fallback.get("og_description")
            or dom_fallback.get("article_text")
            or ""
        ).strip()
        fallback_media = [dom_fallback["og_image"]] if dom_fallback.get("og_image") else []
        canon_or_final = dom_fallback.get("canonical") or final_url
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

    fanpage_url = (
        _derive_fanpage_url_from_url(chosen.get("author_url"))
        or _derive_fanpage_url_from_url(chosen.get("post_url"))
        or _derive_fanpage_url_from_url(dom_fallback.get("author_href"))
        or _derive_fanpage_url_from_url(dom_fallback.get("canonical"))
        or _derive_fanpage_url_from_url(final_url)
    )
    fanpage_name: Optional[str] = None
    og_title_candidate = dom_fallback.get("og_title") or ""
    if "|" in og_title_candidate:
        og_title_candidate = og_title_candidate.split("|")[0].strip()
    for raw_name in (
        chosen.get("author_name"),
        dom_fallback.get("author_text"),
        *(dom_fallback.get("h1_texts") or []),
        og_title_candidate,
        page_title,
    ):
        cleaned = clean_fanpage_name(raw_name)
        if cleaned:
            fanpage_name = cleaned
            break

    chosen["fanpage_url"] = fanpage_url
    chosen["fanpage_name"] = fanpage_name
    return chosen
