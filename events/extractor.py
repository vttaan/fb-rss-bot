"""LLM-based event detection & extraction (Google Gemini REST API).

Flow per fanpage (see `analyze_fanpage_posts`):
  1. Take posts that have no cached analysis yet (cache table: `post_analyses`).
  2. Keyword pre-filter: posts without any event-like keyword are marked "not event" with NO API call.
  3. One Gemini call per remaining post -> JSON {is_event, title, start, end, ...}.
  4. Results are merged into `feeds/<fanpage_slug>/events.json` (deduplicated by post_id).

Env vars:
  GEMINI_API_KEY   required (if missing, LLM step is skipped and nothing is cached)
  GEMINI_MODEL     optional, default "gemini-2.5-flash"
"""
import json
import os
import re
import unicodedata
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from config import FEEDS_DIR, LOCAL_TIMEZONE
from database import Fanpage, Post, get_session
from database.models import PostAnalysis

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
MIN_KEYWORD_MATCHES = int(os.environ.get("MIN_KEYWORD_MATCHES") or 5)  # distinct keywords needed to call the LLM
MAX_LLM_CALLS_PER_FANPAGE = 15  # safety cap per cycle to protect free-tier quota

# Compared against lowercase, diacritics-stripped text.
EVENT_KEYWORDS = [
    "event", "workshop", "seminar", "webinar", "conference", "meetup", "festival", "concert",
    "hackathon", "contest", "competition", "launch", "livestream", "open day", "register",
    "registration", "ticket", "rsvp", "join us", "save the date", "exhibition", "talk",
    "su kien", "hoi thao", "hoi nghi", "toa dam", "giao luu", "khai mac", "le hoi", "cuoc thi",
    "dang ky", "tham gia", "sinh hoat", "buoi", "chuong trinh", "trien lam", "ngay hoi",
    "tuyen sinh", "lich", "vao luc", "dia diem", "thoi gian",
]

PROMPT = """You analyze Facebook fanpage posts. Decide whether the post announces or promotes a
specific upcoming or ongoing EVENT (workshop, seminar, concert, contest, launch, meetup, festival,
livestream, etc.). General news, memes, or reminders with no event do NOT count.

Post published at: {published_at}
Fanpage: {fanpage}
Post text:
\"\"\"
{content}
\"\"\"

Return ONLY a JSON object with exactly these keys (use null when unknown, never invent data):
{{
  "is_event": boolean,
  "title": string|null,
  "description": string|null,        // 1-2 sentence summary, same language as the post
  "start_datetime": string|null,     // ISO 8601, resolve relative dates using the publish date
  "end_datetime": string|null,
  "location": string|null,
  "organizer": string|null,
  "registration_url": string|null,   // only a URL that literally appears in the post text; null otherwise (never invent one)
  "fee": string|null,                  // e.g., "miễn phí", "100.000 VND"
  "is_online": boolean|null,
  "target_audience": string[]|null,  // 1-4 values from: "sinh viên","giáo viên","nghiên cứu sinh","giảng viên","nhân viên","công chức","doanh nghiệp","cộng đồng","tổ chức phi lợi nhuận","tổ chức chính phủ","khác"; null if unknown; if the post specifies student in any major or year, list them specifically (e.g., "sinh viên CNTT", "sinh viên năm 2"); if the post specifies a specific school, list it (e.g., "sinh viên Đại học Bách Khoa")
  "category": string[],               // 1-4 values from: "workshop","seminar","hội nghị","ca nhạc","học thuật","cộng đồng","ngày hội","pháp luật","khoa học","cuộc thi","tọa đàm","triển lãm","thể thao","hướng nghiệp","thiện nguyện", "quyên góp","khác"; [] if unknown
  "format": string[]                  // 1-4 values from: "offline","online","cá nhân","đồng đội","chạy trạm","thi","khảo sát"; [] if unknown
}}"""


def _strip_accents(text: str) -> str:
    text = text.replace("đ", "d").replace("Đ", "D")
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def count_event_keywords(content: str) -> int:
    """Number of DISTINCT event keywords found in the post (accent- and case-insensitive)."""
    haystack = _strip_accents(content or "").lower()
    return sum(
        1 for k in set(EVENT_KEYWORDS)
        if re.search(r"\b" + re.escape(k) + r"\b", haystack)
    )


def looks_event_related(content: str) -> bool:
    return count_event_keywords(content) >= MIN_KEYWORD_MATCHES


def call_gemini(post: Post, fanpage_name: str, api_key: str) -> Optional[Dict[str, Any]]:
    model = (os.environ.get("GEMINI_MODEL") or "gemini-2.5-flash").strip()
    pub = post.published_at.isoformat() if post.published_at else "unknown"
    body = {
        "contents": [{"parts": [{"text": PROMPT.format(
            published_at=pub, fanpage=fanpage_name, content=(post.content or "")[:6000]
        )}]}],
        "generationConfig": {"responseMimeType": "application/json", "temperature": 0},
    }
    req = urllib.request.Request(
        GEMINI_URL.format(model=model),
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    text = payload["candidates"][0]["content"]["parts"][0]["text"]
    data = json.loads(text)
    if isinstance(data, list):
        data = data[0] if data else {}
    return data if isinstance(data, dict) else None


def scan_qr_link(image_url: str) -> Optional[str]:
    """Download an image and decode a QR code locally (OpenCV). Returns an http(s) URL or None.

    Raises on download/decoder-unavailable errors so the caller can tell "no QR" from "could not check".
    """
    import cv2  # opencv-python-headless
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


def fill_registration(post: Post, data: Dict[str, Any]) -> None:
    """Fallback chain: link in text -> QR in first image -> "free" (registration_status tells which)."""
    if data.get("registration_url"):
        data["registration_status"] = "link"
        return
    media = post.media_urls
    if media:
        try:
            link = scan_qr_link(media[0])
        except Exception as exc:  # download/decoder problem: don't claim "free"
            print(f"[events] QR scan failed for post {post.post_id}: {exc}")
            data["registration_status"] = "unknown"
            return
        if link:
            data["registration_url"] = link
            data["registration_status"] = "qr"
            return
    data["registration_url"] = "free"
    data["registration_status"] = "free"

def _events_path(slug: str):
    return FEEDS_DIR / slug / "events.json"


def _write_events_json(fanpage: Fanpage, session) -> None:
    rows = session.scalars(
        select(PostAnalysis)
        .where(PostAnalysis.fanpage_id == fanpage.id, PostAnalysis.is_event.is_(True))
        .order_by(PostAnalysis.analyzed_at.desc())
    ).all()
    post_map = {
        p.post_id: p
        for p in session.scalars(select(Post).where(Post.fanpage_id == fanpage.id)).all()
    }
    events: List[Dict[str, Any]] = []
    for row in rows:
        info = json.loads(row.event_json or "{}")
        post = post_map.get(row.post_id)
        events.append({
            "post_id": row.post_id,
            "post_url": post.post_url if post else None,
            "posted_at": post.published_at.isoformat() if post and post.published_at else None,
            **info,
        })
    events.sort(key=lambda e: e.get("start_datetime") or e.get("posted_at") or "", reverse=True)
    path = _events_path(fanpage.slug)
    path.parent.mkdir(parents=True, exist_ok=True)
    doc = {
        "fanpage": {"name": fanpage.name or fanpage.slug, "url": fanpage.url, "slug": fanpage.slug},
        "updated_at": datetime.now(LOCAL_TIMEZONE).isoformat(),
        "count": len(events),
        "events": events,
    }
    path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")


def analyze_fanpage_posts(fanpage_id: int) -> Dict[str, int]:
    """Analyze not-yet-analyzed posts of a fanpage and refresh its events.json. Blocking (run in a thread)."""
    stats = {"analyzed": 0, "llm_calls": 0, "events": 0, "errors": 0}
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    with get_session() as session:
        fanpage = session.get(Fanpage, fanpage_id)
        if not fanpage:
            return stats
        done_ids = set(session.scalars(
            select(PostAnalysis.post_id).where(PostAnalysis.fanpage_id == fanpage_id)
        ).all())
        pending = [
            p for p in session.scalars(
                select(Post).where(Post.fanpage_id == fanpage_id).order_by(Post.published_at.desc())
            ).all()
            if p.post_id not in done_ids
        ]
        name = fanpage.name or fanpage.slug

        for post in pending:
            if not looks_event_related(post.content):
                session.add(PostAnalysis(post_id=post.post_id, fanpage_id=fanpage_id, is_event=False))
                stats["analyzed"] += 1
                continue
            if not api_key or stats["llm_calls"] >= MAX_LLM_CALLS_PER_FANPAGE:
                continue  # leave uncached; retried next cycle
            try:
                stats["llm_calls"] += 1
                data = call_gemini(post, name, api_key)
            except (urllib.error.URLError, OSError, KeyError, IndexError, ValueError, TypeError) as exc:
                stats["errors"] += 1
                print(f"[events] LLM error for post {post.post_id}: {exc}")
                continue
            if not data:
                stats["errors"] += 1
                continue
            is_event = bool(data.pop("is_event", False))
            if is_event:
                fill_registration(post, data)
            session.add(PostAnalysis(
                post_id=post.post_id,
                fanpage_id=fanpage_id,
                is_event=is_event,
                event_json=json.dumps(data, ensure_ascii=False) if is_event else "{}",
            ))
            stats["analyzed"] += 1
            stats["events"] += int(is_event)

        session.commit()
        _write_events_json(fanpage, session)
    return stats


def purge_fanpage_events(fanpage_id: int, slug: str) -> None:
    """Delete cached analyses and the events folder of a removed fanpage."""
    import shutil

    with get_session() as session:
        for row in session.scalars(select(PostAnalysis).where(PostAnalysis.fanpage_id == fanpage_id)).all():
            session.delete(row)
        session.commit()
    shutil.rmtree(FEEDS_DIR / slug, ignore_errors=True)
