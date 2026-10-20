from datetime import datetime, timezone
from email.utils import format_datetime
import html
from pathlib import Path
from typing import List
import xml.etree.ElementTree as ET
from xml.dom import minidom

from config import FEEDS_DIR, LOCAL_TIMEZONE, MAX_POSTS_IN_RSS
from database import (
    Fanpage,
    Post,
    get_all_recent_posts,
    get_posts_for_fanpage,
    get_posts_for_user,
    list_fanpages,
)
from database.models import ensure_local_tz


def _make_post_title(post: Post, prefix_fanpage: bool = False) -> str:
    """Create a concise RSS item title from post content."""
    text = (post.content or "").strip()
    first_line = text.splitlines()[0].strip() if text else "New Facebook Post"
    if len(first_line) > 90:
        first_line = first_line[:87] + "..."
    if prefix_fanpage and post.fanpage:
        page_name = post.fanpage.name or post.fanpage.slug
        return f"[{page_name}] {first_line}"
    return first_line


def _make_post_description(post: Post) -> str:
    """Build HTML description including post text and media images."""
    escaped_text = html.escape(post.content or "").replace("\n", "<br/>")
    parts = []
    if escaped_text:
        parts.append(f"<div>{escaped_text}</div>")

    media_urls = post.media_urls
    if media_urls:
        imgs_html = "".join(
            f'<p><img src="{html.escape(url)}" alt="Post media" style="max-width:100%;"/></p>'
            for url in media_urls
        )
        parts.append(imgs_html)

    parts.append(f'<p><a href="{html.escape(post.post_url)}">View original post on Facebook</a></p>')
    return "\n".join(parts)


def _build_rss_xml(
    channel_title: str,
    channel_link: str,
    channel_description: str,
    posts: List[Post],
    prefix_fanpage: bool = False,
) -> str:
    rss = ET.Element("rss", {"version": "2.0", "xmlns:atom": "http://www.w3.org/2005/Atom"})
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = channel_title
    ET.SubElement(channel, "link").text = channel_link
    ET.SubElement(channel, "description").text = channel_description
    ET.SubElement(channel, "lastBuildDate").text = format_datetime(datetime.now(LOCAL_TIMEZONE))
    ET.SubElement(channel, "generator").text = "Facebook Fanpage RSS Scraper Bot (Python)"

    for post in posts:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = _make_post_title(post, prefix_fanpage=prefix_fanpage)
        ET.SubElement(item, "link").text = post.post_url
        guid = ET.SubElement(item, "guid", {"isPermaLink": "false"})
        guid.text = f"fb-post-{post.post_id}"

        pub_dt = ensure_local_tz(post.published_at or post.scraped_at) or datetime.now(LOCAL_TIMEZONE)
        ET.SubElement(item, "pubDate").text = format_datetime(pub_dt)

        ET.SubElement(item, "description").text = _make_post_description(post)

        media_urls = post.media_urls
        if media_urls:
            ET.SubElement(
                item,
                "enclosure",
                {
                    "url": media_urls[0],
                    "type": "image/jpeg",
                    "length": "0",
                },
            )

    raw_bytes = ET.tostring(rss, encoding="utf-8")
    parsed = minidom.parseString(raw_bytes)
    return parsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")


def generate_fanpage_rss(fanpage: Fanpage, limit: int = MAX_POSTS_IN_RSS) -> Path:
    """Generate `<slug>.xml` RSS feed for a single fanpage."""
    FEEDS_DIR.mkdir(parents=True, exist_ok=True)
    posts = get_posts_for_fanpage(fanpage.id, limit=limit)
    page_name = fanpage.name or fanpage.slug
    xml_content = _build_rss_xml(
        channel_title=f"{page_name} - Facebook Fanpage RSS",
        channel_link=fanpage.url,
        channel_description=f"Latest public posts scraped from {fanpage.url}",
        posts=posts,
        prefix_fanpage=False,
    )
    out_path = FEEDS_DIR / f"{fanpage.slug}.xml"
    out_path.write_text(xml_content, encoding="utf-8")
    return out_path


def generate_all_rss(limit: int = MAX_POSTS_IN_RSS * 2) -> Path:
    """Generate `all.xml` combined RSS feed across all tracked fanpages."""
    FEEDS_DIR.mkdir(parents=True, exist_ok=True)
    posts = get_all_recent_posts(limit=limit)
    xml_content = _build_rss_xml(
        channel_title="All Tracked Facebook Fanpages - Combined RSS",
        channel_link="https://www.facebook.com",
        channel_description="Combined RSS feed of newly uploaded posts across all tracked Facebook fanpages.",
        posts=posts,
        prefix_fanpage=True,
    )
    out_path = FEEDS_DIR / "all.xml"
    out_path.write_text(xml_content, encoding="utf-8")
    return out_path


def generate_user_rss_xml(client_id: str, limit: int = MAX_POSTS_IN_RSS) -> str:
    """Build RSS 2.0 XML string tailored to a specific user's subscribed fanpages."""
    posts = get_posts_for_user(client_id, limit=limit)
    xml_content = _build_rss_xml(
        channel_title=f"Personalized Facebook Fanpage Feed ({client_id})",
        channel_link="https://www.facebook.com",
        channel_description=f"Custom RSS feed of selected Facebook fanpages for user '{client_id}'.",
        posts=posts,
        prefix_fanpage=True,
    )
    FEEDS_DIR.mkdir(parents=True, exist_ok=True)
    user_feed_path = FEEDS_DIR / f"user_{client_id}.xml"
    user_feed_path.write_text(xml_content, encoding="utf-8")
    return xml_content


def regenerate_all_feeds() -> List[Path]:
    """Regenerate XML feeds for every tracked fanpage plus the combined `all.xml`."""
    generated: List[Path] = []
    fanpages_with_counts = list_fanpages(active_only=False)
    for fanpage, _ in fanpages_with_counts:
        generated.append(generate_fanpage_rss(fanpage))
    generated.append(generate_all_rss())
    return generated
