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
import difflib
import hashlib
import json
import os
import re
import socket
import time
import unicodedata
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select

from config import FEEDS_DIR, LOCAL_TIMEZONE
from database import EmailMessage, EventRecord, Fanpage, Post, PostAnalysis, ensure_local_tz, get_session, init_db

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
MIN_KEYWORD_MATCHES = int(os.environ.get("MIN_KEYWORD_MATCHES") or 2)  # distinct keywords needed to call the LLM
MAX_LLM_CALLS_PER_FANPAGE = 15  # safety cap per cycle to protect free-tier quota
GEMINI_CALL_DELAY_SECONDS = float(os.environ.get("GEMINI_CALL_DELAY_SECONDS") or 3.0)
GEMINI_MAX_RETRIES = int(os.environ.get("GEMINI_MAX_RETRIES") or 3)
GEMINI_REQUEST_TIMEOUT_SECONDS = int(os.environ.get("GEMINI_REQUEST_TIMEOUT_SECONDS") or 90)

# Compared against lowercase, diacritics-stripped text.
EVENT_KEYWORDS = [
    # English event types & actions
    "event", "workshop", "seminar", "webinar", "conference", "meetup", "festival", "concert",
    "hackathon", "contest", "competition", "launch", "livestream", "open day", "register",
    "registration", "ticket", "rsvp", "join us", "save the date", "exhibition", "talk",
    "talkshow", "bootcamp", "orientation", "minigame", "esports", "tournament", "training",
    "career fair", "job fair", "timeline", "deadline", "check in", "checkin", "online", "offline",
    "zoom", "google meet", "ms teams",
    # Vietnamese event types & activities
    "su kien", "hoi thao", "hoi nghi", "toa dam", "giao luu", "khai mac", "be mac", "le hoi",
    "cuoc thi", "hoi thi", "giai dau", "san choi", "sinh hoat", "buoi", "chuong trinh",
    "trien lam", "ngay hoi", "dem nhac", "chao tan", "tap huan", "tuyen quan",
    "tuyen thanh vien", "tuyen cong tac vien", "dai hoi", "chien dich", "tinh nguyen",
    "mua he xanh", "xuan tinh nguyen", "hien mau", "hoc bong",
    # Vietnamese logistics, stages & registration signals
    "dang ky", "link dang ky", "form dang ky", "mo don", "dong don", "han chot", "tham gia",
    "tham du", "tuyen sinh", "lich", "vao luc", "dia diem", "thoi gian", "hoi truong",
    "giang duong", "co so", "so khao", "ban ket", "chung ket", "vong thi", "the le",
    "giai thuong", "co cau giai thuong", "doi tuong", "quyen loi", "le phi", "mien phi",
    "dien gia", "khach moi", "ban to chuc",
]

PROMPT = """You analyze Facebook fanpage posts (including shared posts) and emails from university clubs, faculties, unions (Đoàn - Hội), and organizations.
Decide whether the post announces, promotes, opens registration for, or gives schedule/round details for a specific UPCOMING or ONGOING EVENT or organized activity.

WHAT COUNTS AS AN EVENT (`is_event: true`):
- Workshops, seminars, webinars, talkshows, academic talks, training programs (tập huấn, sinh hoạt chuyên đề), orientations (chào tân sinh viên).
- Contests, competitions, hackathons, academic olympiads, sports/esports tournaments, singing/talent contests, minigames (cuộc thi, hội thi, giải đấu, vòng sơ khảo, đêm chung kết).
- Concerts, music nights (đêm nhạc), festivals, fairs, career/job fairs (ngày hội việc làm), exhibitions (triển lãm), open days.
- Conferences, congresses, ceremonies, campaign launches (hội nghị, đại hội, lễ khai mạc, lễ phát động, ngày cao điểm).
- Club/committee/project recruitment or audition drives with specific timeline/interview dates (tuyển quân, tuyển thành viên, tuyển CTV), volunteer campaigns (chiến dịch tình nguyện, hiến máu), or scholarship info sessions.
- Registration countdowns ("3 days left", "còn X ngày", "gia hạn đăng ký") or round/mentor/sponsor announcements that still promote an ongoing/upcoming competition or event.

WHAT DOES NOT COUNT (`is_event: false`):
- Pure holiday/celebration greetings (e.g., 20/10, 20/11, New Year wishes) with no organized activity to attend or register for.
- Pure post-event recaps, thank-you notes, or final winner/honor roll announcements (recap, bảng vàng vinh danh, tuyên dương gương Sinh viên 5 Tốt, đóng đơn/kết thúc hoàn toàn) where the event has ALREADY completely finished and no upcoming round or activity is mentioned.
- General news, scam warnings, memes, or administrative notices with no event.

Post published at: {published_at}
Fanpage / Source: {fanpage}
Post text:
\"\"\"
{content}
\"\"\"

EXTRACTION RULES:
- `description`: Don't take the raw text from the post, try to summarize what the event is about, in the same language as the post. If the post is in Vietnamese, summarize in Vietnamese; if in English, summarize in English. Do NOT invent details; only summarize what is explicitly stated.
- `title`: Extract the clean, canonical name of the event/competition/program (e.g., "Cuộc thi âm nhạc Let IT Shine 2026 - Winter Aria", "Tuyển quân SAB Fresh 2026", "Hội nghị mở rộng Đoàn - Hội Khoa CNTT 2026-2027"). Do NOT use countdown hooks like "3 DAYS LEFT" alone as the title if the actual event name is present.
- `start_datetime` & `end_datetime`: ISO 8601 in Asia/Ho_Chi_Minh (+07:00), e.g. "2026-11-22T16:00:00+07:00" or "2026-11-22". Resolve relative dates and DD/MM dates using `published_at`. If a post lists multiple timeline milestones (e.g., registration period, preliminary round, final night), prefer the main upcoming event/round date-time (or registration start/end for recruitment/application posts).
- `location`: Specific venue, lecture hall (giảng đường), auditorium (hội trường), room, campus (cơ sở), or online platform mentioned in the post. If no specific room or hall is mentioned, use the campus or city (the address). If no location is mentioned, use null.
- `registration_url`: ONLY a literal registration/application/form URL from the post text (e.g., forms.gle, bit.ly, go.sabies.vn/...). NEVER use general contact/social footer links (like fb.com/<page> or tiktok.com/...) as `registration_url`; use null if no registration link is present.

Return ONLY a JSON object with exactly these keys (use null when unknown, never invent data):
{{
  "is_event": boolean,
  "title": string|null,
  "description": string|null,        // 1-2 sentence summary in the same language as the post summarizing what the event is about
  "start_datetime": string|null,     // ISO 8601, resolve relative dates using the publish date
  "end_datetime": string|null,
  "location": string|null,
  "organizer": string|null,
  "registration_url": string|null,   // only a registration URL that literally appears in the post text; null otherwise
  "fee": string|null,                // e.g., "miễn phí", "100.000 VND"
  "is_online": boolean|null,
  "target_audience": string[]|null,  // 1-4 values from: "sinh viên","giáo viên","nghiên cứu sinh","giảng viên","nhân viên","công chức","doanh nghiệp","cộng đồng","tổ chức phi lợi nhuận","tổ chức chính phủ","khác"; null if unknown; if the post specifies students in a major/year/school, list specifically (e.g., "sinh viên CNTT", "sinh viên K25, K26")
  "category": string[],              // 1-4 values from: "workshop","seminar","hội nghị","ca nhạc","học thuật","cộng đồng","ngày hội","pháp luật","khoa học","cuộc thi","tọa đàm","triển lãm","thể thao","hướng nghiệp","thiện nguyện","quyên góp","khác"; [] if unknown
  "format": string[]                 // 1-4 values from: "offline","online","cá nhân","đồng đội","chạy trạm","thi","khảo sát"; [] if unknown
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


STRONG_EVENT_SIGNAL_RE = re.compile(
    r"\b(workshop|seminar|webinar|conference|hackathon|contest|competition|meetup|festival|concert|"
    r"talkshow|bootcamp|orientation|open day|livestream|career fair|job fair|"
    r"hoi thao|hoi nghi|toa dam|cuoc thi|hoi thi|ngay hoi|trien lam|le hoi|khai mac|"
    r"tap huan|tuyen quan|tuyen thanh vien|tuyen cong tac vien|dem nhac|giai dau|chao tan)\b",
    flags=re.IGNORECASE,
)


def looks_event_related(content: str) -> bool:
    """
    Fast pre-filter before calling Gemini:
    Passes if:
      - the post contains at least `MIN_KEYWORD_MATCHES` (default 2) distinct event keywords, OR
      - the post contains at least 1 strong event-type keyword (e.g., workshop, seminar, hội nghị, cuộc thi), OR
      - the post contains at least 1 event keyword accompanied by an explicit date, time, or URL.
    """
    if not content or not content.strip():
        return False
    stripped = _strip_accents(content).lower()
    if STRONG_EVENT_SIGNAL_RE.search(stripped):
        return True
    kw_count = count_event_keywords(content)
    if kw_count >= MIN_KEYWORD_MATCHES:
        return True
    if kw_count >= 1:
        has_date_time_or_link = bool(
            re.search(
                r"(\b\d{1,2}[/-]\d{1,2}(?:[/-]20\d{2})?\b|\b20\d{2}-\d{2}-\d{2}\b|\b\d{1,2}[:h]\d{2}\b|https?://)",
                content,
                flags=re.IGNORECASE,
            )
        )
        if has_date_time_or_link:
            return True
    return False


FALLBACK_GEMINI_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.0-flash",
    "gemini-flash-latest",
]
_WORKING_GEMINI_MODEL: Optional[str] = None


def _candidate_gemini_models() -> List[str]:
    """Return ordered list of Gemini model IDs to try, normalizing any `models/` prefix or retired 1.5 aliases."""
    global _WORKING_GEMINI_MODEL
    if _WORKING_GEMINI_MODEL:
        return [_WORKING_GEMINI_MODEL] + [m for m in FALLBACK_GEMINI_MODELS if m != _WORKING_GEMINI_MODEL]

    raw = (os.environ.get("GEMINI_MODEL") or "").strip().strip("'\"")
    if raw.startswith("models/"):
        raw = raw[len("models/"):].strip()

    # Map retired Gemini 1.0/1.5 model names that now return HTTP 404 on v1beta
    retired_prefixes = ("gemini-1.5", "gemini-1.0", "gemini-pro")
    candidates: List[str] = []
    if raw and not raw.lower().startswith(retired_prefixes):
        candidates.append(raw)
    for fallback in FALLBACK_GEMINI_MODELS:
        if fallback not in candidates:
            candidates.append(fallback)
    return candidates


def _retry_delay_seconds(attempt: int, exc: Optional[Exception] = None) -> float:
    """Compute backoff delay in seconds, honoring Retry-After if provided."""
    if isinstance(exc, urllib.error.HTTPError) and exc.headers:
        ra = exc.headers.get("Retry-After")
        if ra:
            try:
                return min(max(float(ra), 1.0), 30.0)
            except ValueError:
                pass
    return min(3.0 * (2 ** attempt), 24.0)


def call_gemini_text(
    content: str,
    published_at_str: str,
    source_name: str,
    api_key: str,
) -> Optional[Dict[str, Any]]:
    """Send raw text (Facebook post or client Email content) to Gemini with model fallback and retry backoff."""
    global _WORKING_GEMINI_MODEL
    body_bytes = json.dumps(
        {
            "contents": [{
                "parts": [{
                    "text": PROMPT.format(
                        published_at=published_at_str,
                        fanpage=source_name,
                        content=(content or "")[:6000],
                    )
                }]
            }],
            "generationConfig": {"responseMimeType": "application/json", "temperature": 0},
        }
    ).encode("utf-8")

    last_exc: Optional[Exception] = None
    for model in _candidate_gemini_models():
        for attempt in range(GEMINI_MAX_RETRIES + 1):
            req = urllib.request.Request(
                GEMINI_URL.format(model=model),
                data=body_bytes,
                headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=GEMINI_REQUEST_TIMEOUT_SECONDS) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                _WORKING_GEMINI_MODEL = model
                text = payload["candidates"][0]["content"]["parts"][0]["text"]
                data = json.loads(text)
                if isinstance(data, list):
                    data = data[0] if data else {}
                return data if isinstance(data, dict) else None
            except urllib.error.HTTPError as exc:
                last_exc = exc
                if exc.code in (400, 404):
                    break  # try next model
                if exc.code in (429, 500, 502, 503, 504):
                    if attempt < GEMINI_MAX_RETRIES:
                        wait_s = _retry_delay_seconds(attempt, exc)
                        print(f"[events] Gemini {model} returned HTTP {exc.code}; retrying in {wait_s:.1f}s...")
                        time.sleep(wait_s)
                        continue
                    # Exhausted retries on this model (e.g. per-model quota/overload) -> try next fallback model
                    break
                raise
            except (TimeoutError, socket.timeout, urllib.error.URLError, OSError) as exc:
                last_exc = exc
                if attempt < GEMINI_MAX_RETRIES:
                    wait_s = _retry_delay_seconds(attempt, exc)
                    print(f"[events] Gemini {model} network/timeout error ({exc}); retrying in {wait_s:.1f}s...")
                    time.sleep(wait_s)
                    continue
                break

    if last_exc is not None:
        raise last_exc
    return None


def call_gemini(post: Post, fanpage_name: str, api_key: str) -> Optional[Dict[str, Any]]:
    pub = post.published_at.isoformat() if post.published_at else "unknown"
    return call_gemini_text(post.content or "", pub, fanpage_name, api_key)


def heuristic_extract_event(
    content: str,
    subject: str = "",
    sender: Optional[str] = None,
    published_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Rule-based fallback extractor when GEMINI_API_KEY is not configured in local dev."""
    combined = f"{subject}\n{content}".strip()
    stripped = _strip_accents(combined).lower()
    kw_count = count_event_keywords(combined)
    has_strong_signal = bool(
        re.search(
            r"\b(workshop|seminar|webinar|conference|hackathon|contest|competition|meetup|festival|concert|"
            r"hoi thao|hoi nghi|toa dam|cuoc thi|ngay hoi|trien lam|open day|livestream)\b",
            stripped,
        )
    )
    is_event = kw_count >= 2 or (has_strong_signal and kw_count >= 1)
    if not is_event:
        return {"is_event": False}

    # Title: prefer subject, else first non-empty line
    title = subject.strip()
    if not title:
        for line in content.splitlines():
            clean_line = line.strip().strip("#*-• ")
            if len(clean_line) >= 5:
                title = clean_line[:180]
                break
    title = title or "Untitled Event"

    # Extract date (YYYY-MM-DD or DD/MM/YYYY or DD-MM-YYYY)
    ref_year = (published_at or datetime.now(LOCAL_TIMEZONE)).year
    date_str: Optional[str] = None
    m_iso = re.search(r"\b(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b", combined)
    if m_iso:
        date_str = m_iso.group(0)
    else:
        m_dmy = re.search(r"\b(0?[1-9]|[12]\d|3[01])[/-](0?[1-9]|1[0-2])(?:[/-](20\d{2}))?\b", combined)
        if m_dmy:
            day = int(m_dmy.group(1))
            month = int(m_dmy.group(2))
            year = int(m_dmy.group(3)) if m_dmy.group(3) else ref_year
            date_str = f"{year:04d}-{month:02d}-{day:02d}"

    # Extract time (HH:MM or HhMM or H:MM AM/PM)
    time_str: Optional[str] = None
    m_time = re.search(r"\b([01]?\d|2[0-3])[:h]([0-5]\d)\s*(am|pm)?\b", combined, flags=re.IGNORECASE)
    if m_time:
        hh = int(m_time.group(1))
        mm = int(m_time.group(2))
        ampm = (m_time.group(3) or "").lower()
        if ampm == "pm" and hh < 12:
            hh += 12
        elif ampm == "am" and hh == 12:
            hh = 0
        time_str = f"{hh:02d}:{mm:02d}"

    start_datetime = None
    if date_str:
        start_datetime = f"{date_str}T{time_str or '00:00'}:00+07:00"

    # Location extraction
    location: Optional[str] = None
    m_loc = re.search(
        r"(?:location|venue|address|room|hall|dia diem|phong|hoi truong)\s*[:\-]\s*([^\n\r.;]+)",
        combined,
        flags=re.IGNORECASE,
    )
    if m_loc:
        location = m_loc.group(1).strip()[:200]
    else:
        m_loc2 = re.search(
            r"\b(?:at|tai)\s+(Hall\s+[A-Z0-9]+|Room\s+[A-Z0-9.\-]+|Hoi truong\s+[^\n,.]+|Phong\s+[A-Z0-9.\-]+|Zoom|Google Meet|MS Teams)",
            combined,
            flags=re.IGNORECASE,
        )
        if m_loc2:
            location = m_loc2.group(1).strip()

    # Format / Online vs Offline
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

    # Registration URL
    urls = re.findall(r"https?://[^\s<>\"')\]]+", combined)
    reg_url = urls[0] if urls else None

    # Categories
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

    # Description summary (first 2 sentences of content)
    clean_body = re.sub(r"\s+", " ", content.strip())
    description = clean_body[:240] + ("..." if len(clean_body) > 240 else "")

    return {
        "is_event": True,
        "title": title,
        "description": description or None,
        "start_datetime": start_datetime,
        "end_datetime": None,
        "location": location,
        "organizer": sender,
        "registration_url": reg_url,
        "fee": "miễn phí" if re.search(r"\b(free|mien phi)\b", stripped) else None,
        "is_online": is_online,
        "target_audience": ["sinh viên"] if re.search(r"\b(student|sinh vien)\b", stripped) else None,
        "category": categories or ["khác"],
        "format": fmt_list,
    }


def _normalize_title_key(title: str) -> str:
    """Normalize event title for anchor-field matching across fanpages and emails."""
    s = _strip_accents(title or "").lower()
    # Strip leading bracket tags like [workshop], [seminar], [thong bao]
    s = re.sub(r"^\s*(\[[^\]]+\]|\([^\)]+\))\s*", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _normalize_location_key(location: Optional[str]) -> str:
    if not location:
        return ""
    s = _strip_accents(location).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _parse_date_and_time(start_datetime: Optional[str], raw_date: Optional[str] = None, raw_time: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """Extract canonical YYYY-MM-DD and HH:MM strings from start_datetime or explicit date/time fields."""
    ev_date = (raw_date or "").strip() or None
    ev_time = (raw_time or "").strip() or None
    if start_datetime and isinstance(start_datetime, str):
        s = start_datetime.strip()
        m_dt = re.match(r"^(\d{4}-\d{2}-\d{2})(?:[T\s](\d{2}:\d{2}))?", s)
        if m_dt:
            if not ev_date:
                ev_date = m_dt.group(1)
            if not ev_time and m_dt.group(2) and m_dt.group(2) != "00:00":
                ev_time = m_dt.group(2)
    return ev_date, ev_time


def _derive_online_mode(is_online: Optional[bool], fmt_list: Optional[List[str]]) -> Optional[str]:
    """Derive canonical format mode ('online', 'offline', 'hybrid') from is_online and format list."""
    formats = [str(f).lower().strip() for f in (fmt_list or []) if f]
    has_online = "online" in formats or is_online is True
    has_offline = "offline" in formats or is_online is False
    if "online" in formats and "offline" in formats:
        return "hybrid"
    if has_online and not ("offline" in formats):
        return "online"
    if has_offline:
        return "offline"
    return None


def extract_anchor_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Step 1 of Unified Extraction:
    Extract the 5 anchor fields (title, format online/offline, date, time, location)
    used to determine deduplication or update of an existing event.
    """
    title = str(data.get("title") or "").strip() or "Untitled Event"
    norm_title = _normalize_title_key(title)
    is_online = data.get("is_online")
    if not isinstance(is_online, bool):
        is_online = None
    fmt_list = data.get("format") if isinstance(data.get("format"), list) else []
    online_mode = _derive_online_mode(is_online, fmt_list)

    ev_date, ev_time = _parse_date_and_time(
        data.get("start_datetime"),
        raw_date=data.get("date"),
        raw_time=data.get("time"),
    )
    location = str(data.get("location") or "").strip() or None
    norm_location = _normalize_location_key(location)

    return {
        "title": title,
        "normalized_title": norm_title,
        "online_mode": online_mode,
        "is_online": is_online,
        "date": ev_date,
        "time": ev_time,
        "start_datetime": data.get("start_datetime"),
        "end_datetime": data.get("end_datetime"),
        "location": location,
        "normalized_location": norm_location,
    }


def compare_anchor_fields(
    session,
    anchor: Dict[str, Any],
) -> Tuple[str, Optional[EventRecord], Dict[str, Any]]:
    """
    Step 2 of Unified Extraction:
    Compare anchor fields (title, format online/offline, date, time, location) against
    existing events in the database to determine:
      - "new": No matching event exists.
      - "duplicate": Matching event exists with identical anchor fields.
      - "updated": Matching event exists, and date, time, location, or format (online/offline) changed.
    """
    norm_title = anchor["normalized_title"]
    if not norm_title:
        return "new", None, {}

    # 1. Exact normalized title match first
    matched: Optional[EventRecord] = session.scalar(
        select(EventRecord).where(EventRecord.normalized_title == norm_title).order_by(EventRecord.id.desc())
    )

    # 2. Fuzzy title similarity fallback (>= 0.85 ratio)
    if not matched:
        candidates = session.scalars(select(EventRecord).order_by(EventRecord.id.desc()).limit(300)).all()
        best_ratio = 0.0
        for cand in candidates:
            if not cand.normalized_title:
                continue
            ratio = difflib.SequenceMatcher(None, norm_title, cand.normalized_title).ratio()
            if ratio >= 0.85 and ratio > best_ratio:
                best_ratio = ratio
                matched = cand

    if not matched:
        return "new", None, {}

    # Compare the anchor fields: format (online/offline), date, time, location
    changes: Dict[str, Dict[str, Any]] = {}

    if anchor["online_mode"] and anchor["online_mode"] != matched.online_mode:
        changes["format"] = {"old": matched.online_mode, "new": anchor["online_mode"]}

    if anchor["date"] and anchor["date"] != matched.event_date:
        changes["date"] = {"old": matched.event_date, "new": anchor["date"]}

    if anchor["time"] and anchor["time"] != matched.event_time:
        changes["time"] = {"old": matched.event_time, "new": anchor["time"]}

    if anchor["normalized_location"] and anchor["normalized_location"] != matched.normalized_location:
        changes["location"] = {"old": matched.location, "new": anchor["location"]}

    if changes:
        return "updated", matched, changes

    return "duplicate", matched, {}


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


def fill_registration_from_media(media_urls: List[str], data: Dict[str, Any], source_id: str = "") -> None:
    """Fallback chain: link in text -> QR in first image -> "free" (registration_status tells which)."""
    if data.get("registration_url") and data.get("registration_url") != "free":
        data["registration_status"] = "link"
        return
    if media_urls:
        try:
            link = scan_qr_link(media_urls[0])
        except Exception as exc:  # download/decoder problem: don't claim "free"
            print(f"[events] QR scan failed for {source_id}: {exc}")
            data["registration_status"] = "unknown"
            return
        if link:
            data["registration_url"] = link
            data["registration_status"] = "qr"
            return
    data["registration_url"] = "free"
    data["registration_status"] = "free"


def fill_registration(post: Post, data: Dict[str, Any]) -> None:
    """Fallback chain: link in text -> QR in first image -> "free" (registration_status tells which)."""
    fill_registration_from_media(post.media_urls, data, source_id=f"post {post.post_id}")


def gather_additional_event_info(
    raw_data: Dict[str, Any],
    media_urls: List[str],
    source_id: str = "",
) -> Dict[str, Any]:
    """
    Step 3 of Unified Extraction:
    After anchor-field comparison determines the event is NEW or UPDATED,
    run extractor functions to gather remaining details (description, organizer,
    registration link / QR scan, fee, target_audience, category, format).
    """
    enriched = dict(raw_data)
    fill_registration_from_media(media_urls, enriched, source_id=source_id)
    return {
        "description": enriched.get("description"),
        "organizer": enriched.get("organizer"),
        "registration_url": enriched.get("registration_url"),
        "registration_status": enriched.get("registration_status"),
        "fee": enriched.get("fee"),
        "target_audience": enriched.get("target_audience"),
        "category": enriched.get("category") if isinstance(enriched.get("category"), list) else [],
        "format": enriched.get("format") if isinstance(enriched.get("format"), list) else [],
    }


def upsert_unified_event(
    session,
    raw_data: Dict[str, Any],
    source_type: str,
    source_id: str,
    media_urls: Optional[List[str]] = None,
    fanpage_id: Optional[int] = None,
    client_id: Optional[str] = None,
    post_url: Optional[str] = None,
    posted_at: Optional[str] = None,
) -> Tuple[str, EventRecord, Dict[str, Any]]:
    """
    Execute the complete unified extraction & deduplication/update workflow:
      1. Extract anchor fields (title, format online/offline, date, time, location).
      2. Compare against existing EventRecord rows -> 'new', 'duplicate', or 'updated'.
      3. If 'new' or 'updated', gather additional event info (QR code, description, audience, etc.).
      4. Persist or update EventRecord in SQLite.
    Returns (status, event_record, changes_dict).
    """
    media = media_urls or []
    anchor = extract_anchor_fields(raw_data)
    status, existing, changes = compare_anchor_fields(session, anchor)
    now_dt = datetime.now(LOCAL_TIMEZONE)

    source_entry = {
        "source_type": source_type,
        "source_id": source_id,
        "fanpage_id": fanpage_id,
        "client_id": client_id,
        "post_url": post_url,
        "posted_at": posted_at,
        "seen_at": now_dt.isoformat(),
    }

    if status == "duplicate" and existing is not None:
        # Link source if not already listed, skip heavy QR / re-extraction
        try:
            stored_info = json.loads(existing.event_json or "{}")
        except Exception:
            stored_info = {}
        sources = stored_info.get("sources") or []
        if not any(s.get("source_id") == source_id for s in sources):
            sources.append(source_entry)
            stored_info["sources"] = sources
            existing.event_json = json.dumps(stored_info, ensure_ascii=False)
        return "duplicate", existing, {}

    # Gather additional information (runs QR scanner and metadata normalization)
    extra_info = gather_additional_event_info(raw_data, media, source_id=source_id)
    # Sync raw_data registration fields back so caller has them too
    raw_data["registration_url"] = extra_info.get("registration_url")
    raw_data["registration_status"] = extra_info.get("registration_status")

    if status == "updated" and existing is not None:
        try:
            stored_info = json.loads(existing.event_json or "{}")
        except Exception:
            stored_info = {}

        # Update anchor fields that changed
        if anchor["online_mode"]:
            existing.online_mode = anchor["online_mode"]
        if anchor["is_online"] is not None:
            existing.is_online = anchor["is_online"]
        if anchor["date"]:
            existing.event_date = anchor["date"]
        if anchor["time"]:
            existing.event_time = anchor["time"]
        if anchor["start_datetime"]:
            existing.start_datetime = anchor["start_datetime"]
        if anchor["end_datetime"]:
            existing.end_datetime = anchor["end_datetime"]
        if anchor["location"]:
            existing.location = anchor["location"]
            existing.normalized_location = anchor["normalized_location"]

        # Merge newly gathered non-empty additional fields
        for k, v in extra_info.items():
            if v not in (None, [], "") and (k != "registration_url" or v != "free" or not stored_info.get(k)):
                stored_info[k] = v

        sources = stored_info.get("sources") or []
        if not any(s.get("source_id") == source_id for s in sources):
            sources.append(source_entry)
        stored_info["sources"] = sources

        history = stored_info.get("update_history") or []
        history.append({
            "updated_at": now_dt.isoformat(),
            "source_type": source_type,
            "source_id": source_id,
            "client_id": client_id,
            "changes": changes,
        })
        stored_info["update_history"] = history

        existing.version = (existing.version or 1) + 1
        existing.updated_at = now_dt
        existing.event_json = json.dumps(stored_info, ensure_ascii=False)
        session.flush()
        return "updated", existing, changes

    # Brand new event
    full_info = {
        **extra_info,
        "post_url": post_url,
        "posted_at": posted_at,
        "sources": [source_entry],
        "update_history": [],
    }
    new_record = EventRecord(
        title=anchor["title"],
        normalized_title=anchor["normalized_title"],
        online_mode=anchor["online_mode"],
        is_online=anchor["is_online"],
        event_date=anchor["date"],
        event_time=anchor["time"],
        start_datetime=anchor["start_datetime"],
        end_datetime=anchor["end_datetime"],
        location=anchor["location"],
        normalized_location=anchor["normalized_location"],
        source_type=source_type,
        source_id=source_id,
        fanpage_id=fanpage_id,
        client_id=client_id,
        event_json=json.dumps(full_info, ensure_ascii=False),
        version=1,
        created_at=now_dt,
        updated_at=now_dt,
    )
    session.add(new_record)
    session.flush()
    return "created", new_record, {}


def _events_path(slug: str) -> Path:
    return FEEDS_DIR / slug / "events.json"


def _fanpages_folder_events_path(slug: str) -> Path:
    return FEEDS_DIR / "fanpages" / slug / "events.json"


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
    doc = {
        "fanpage": {"name": fanpage.name or fanpage.slug, "url": fanpage.url, "slug": fanpage.slug},
        "updated_at": datetime.now(LOCAL_TIMEZONE).isoformat(),
        "count": len(events),
        "events": events,
    }
    serialized = json.dumps(doc, ensure_ascii=False, indent=2)

    # Write both legacy `feeds/<slug>/events.json` and distributed `feeds/fanpages/<slug>/events.json`
    for path in (_events_path(fanpage.slug), _fanpages_folder_events_path(fanpage.slug)):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(serialized, encoding="utf-8")


def sync_all_event_folders(session=None) -> Dict[str, int]:
    """
    Distribute all extracted event JSON files into a clean folder structure under `feeds/`:
      - `feeds/all/events.json`               (All unified deduplicated events)
      - `feeds/fanpages/<slug>/events.json`   (Per-fanpage events)
      - `feeds/<slug>/events.json`            (Legacy per-fanpage events)
      - `feeds/emails/events.json`            (All client email events)
      - `feeds/emails/<client_id>/events.json`(Per-client email events)
      - `feeds/inbox/messages.json`           (Inbox received email entries & extraction status)
    """
    init_db()
    own_session = False
    if session is None:
        session = get_session()
        own_session = True

    try:
        now_iso = datetime.now(LOCAL_TIMEZONE).isoformat()

        # 1. Sync per-fanpage events.json files
        fanpages = session.scalars(select(Fanpage)).all()
        for fp in fanpages:
            _write_events_json(fp, session)

        # 2. All unified events (`feeds/all/events.json`)
        all_records = session.scalars(
            select(EventRecord).order_by(EventRecord.updated_at.desc(), EventRecord.id.desc())
        ).all()
        all_events_list = [r.to_dict() for r in all_records]
        all_events_list.sort(
            key=lambda e: e.get("start_datetime") or e.get("date") or e.get("updated_at") or "",
            reverse=True,
        )
        all_path = FEEDS_DIR / "all" / "events.json"
        all_path.parent.mkdir(parents=True, exist_ok=True)
        all_path.write_text(
            json.dumps(
                {
                    "folder": "all",
                    "description": "Unified deduplicated & updated events from Facebook fanpages and client emails",
                    "updated_at": now_iso,
                    "count": len(all_events_list),
                    "events": all_events_list,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        # 3. Email-sourced events (`feeds/emails/events.json` and `feeds/emails/<client_id>/events.json`)
        email_events_list: List[Dict[str, Any]] = []
        by_client: Dict[str, List[Dict[str, Any]]] = {}
        for ev_dict in all_events_list:
            sources = ev_dict.get("sources") or []
            is_email_ev = ev_dict.get("source_type") == "email" or any(
                s.get("source_type") == "email" for s in sources
            )
            if is_email_ev:
                email_events_list.append(ev_dict)
                client_ids = set()
                if ev_dict.get("client_id"):
                    client_ids.add(str(ev_dict["client_id"]))
                for s in sources:
                    if s.get("client_id"):
                        client_ids.add(str(s["client_id"]))
                for cid in client_ids:
                    safe_cid = re.sub(r"[^a-zA-Z0-9._-]", "_", cid.strip()) or "anonymous"
                    by_client.setdefault(safe_cid, []).append(ev_dict)

        emails_root_path = FEEDS_DIR / "emails" / "events.json"
        emails_root_path.parent.mkdir(parents=True, exist_ok=True)
        emails_root_path.write_text(
            json.dumps(
                {
                    "folder": "emails",
                    "description": "Events extracted from client email submissions",
                    "updated_at": now_iso,
                    "count": len(email_events_list),
                    "events": email_events_list,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        for safe_cid, cid_events in by_client.items():
            cid_path = FEEDS_DIR / "emails" / safe_cid / "events.json"
            cid_path.parent.mkdir(parents=True, exist_ok=True)
            cid_path.write_text(
                json.dumps(
                    {
                        "folder": f"emails/{safe_cid}",
                        "client_id": safe_cid,
                        "updated_at": now_iso,
                        "count": len(cid_events),
                        "events": cid_events,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

        # 4. Inbox received email messages (`feeds/inbox/messages.json`)
        inbox_rows = session.scalars(
            select(EmailMessage).order_by(EmailMessage.ingested_at.desc(), EmailMessage.id.desc()).limit(200)
        ).all()
        inbox_list = [m.to_dict() for m in inbox_rows]
        inbox_path = FEEDS_DIR / "inbox" / "messages.json"
        inbox_path.parent.mkdir(parents=True, exist_ok=True)
        inbox_path.write_text(
            json.dumps(
                {
                    "folder": "inbox",
                    "description": "Client email inbox entries and extraction outcomes",
                    "updated_at": now_iso,
                    "count": len(inbox_list),
                    "messages": inbox_list,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        return {
            "all_events": len(all_events_list),
            "email_events": len(email_events_list),
            "inbox_messages": len(inbox_list),
        }
    finally:
        if own_session:
            session.close()


def ingest_email_message(
    content: str,
    subject: str = "",
    client_id: str = "anonymous",
    message_id: Optional[str] = None,
    sender: Optional[str] = None,
    received_at: Optional[str] = None,
    media_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Receive an email entry from a client into the server Inbox, move it to the Unified Extractor,
    compare anchor fields (title, format online/offline, date, time, location) to determine
    deduplication or update of an existing event, gather remaining information, and refresh
    all distributed JSON folders.
    """
    init_db()
    clean_client = (client_id or "anonymous").strip() or "anonymous"
    clean_subject = (subject or "").strip()
    clean_content = (content or "").strip()
    media = [u.strip() for u in (media_urls or []) if u and str(u).strip()]

    # Parse received_at timestamp
    rec_dt = datetime.now(LOCAL_TIMEZONE)
    if received_at:
        try:
            parsed = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
            rec_dt = ensure_local_tz(parsed) or rec_dt
        except Exception:
            pass

    # Deterministic message_id if client didn't supply one
    if not message_id or not str(message_id).strip():
        digest = hashlib.sha256(
            f"{clean_client}|{clean_subject}|{sender or ''}|{clean_content}".encode("utf-8")
        ).hexdigest()[:20]
        clean_msg_id = f"email_{digest}"
    else:
        clean_msg_id = str(message_id).strip()

    with get_session() as session:
        # 1. Message-ID level deduplication check
        existing_msg = session.scalar(
            select(EmailMessage).where(EmailMessage.message_id == clean_msg_id)
        )
        if existing_msg:
            ev_record = (
                session.get(EventRecord, existing_msg.event_id)
                if existing_msg.event_id
                else None
            )
            return {
                "status": "duplicate",
                "reason": "message_id_already_processed",
                "is_event": existing_msg.is_event,
                "is_duplicate": True,
                "is_updated": False,
                "changes": json.loads(existing_msg.changes_json or "{}"),
                "event_id": existing_msg.event_id,
                "event": ev_record.to_dict() if ev_record else None,
                "inbox_message": existing_msg.to_dict(),
            }

        # 2. Extract event candidate via Gemini LLM (or heuristic fallback if GEMINI_API_KEY unset)
        api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        full_email_text = (
            f"Subject: {clean_subject}\n"
            f"From: {sender or 'unknown'}\n"
            f"Content:\n{clean_content}"
        ).strip()
        source_label = f"Email ({sender or clean_client})"

        raw_data: Optional[Dict[str, Any]] = None
        if api_key:
            try:
                raw_data = call_gemini_text(
                    content=full_email_text,
                    published_at_str=rec_dt.isoformat(),
                    source_name=source_label,
                    api_key=api_key,
                )
            except Exception as exc:
                print(f"[events] Gemini error for email {clean_msg_id}, falling back to heuristic: {exc}")

        if raw_data is None:
            raw_data = heuristic_extract_event(
                content=clean_content,
                subject=clean_subject,
                sender=sender,
                published_at=rec_dt,
            )

        is_event = bool(raw_data.pop("is_event", False))
        if not is_event:
            msg_row = EmailMessage(
                message_id=clean_msg_id,
                client_id=clean_client,
                subject=clean_subject,
                sender=sender,
                content=clean_content,
                media_urls_json=json.dumps(media, ensure_ascii=False),
                received_at=rec_dt,
                ingested_at=datetime.now(LOCAL_TIMEZONE),
                status="ignored",
                is_event=False,
                event_id=None,
                changes_json="{}",
                analysis_json="{}",
            )
            session.add(msg_row)
            session.commit()
            sync_all_event_folders(session)
            return {
                "status": "ignored",
                "is_event": False,
                "is_duplicate": False,
                "is_updated": False,
                "changes": {},
                "event_id": None,
                "event": None,
                "inbox_message": msg_row.to_dict(),
            }

        # 3. Unified Extraction: compare anchor fields (title, format, date, time, location)
        #    -> determine 'created', 'duplicate', or 'updated', then gather remaining fields
        upsert_status, ev_record, changes = upsert_unified_event(
            session=session,
            raw_data=raw_data,
            source_type="email",
            source_id=clean_msg_id,
            media_urls=media,
            fanpage_id=None,
            client_id=clean_client,
            post_url=None,
            posted_at=rec_dt.isoformat(),
        )

        ev_dict = ev_record.to_dict()
        msg_row = EmailMessage(
            message_id=clean_msg_id,
            client_id=clean_client,
            subject=clean_subject,
            sender=sender,
            content=clean_content,
            media_urls_json=json.dumps(media, ensure_ascii=False),
            received_at=rec_dt,
            ingested_at=datetime.now(LOCAL_TIMEZONE),
            status=upsert_status,
            is_event=True,
            event_id=ev_record.id,
            changes_json=json.dumps(changes, ensure_ascii=False),
            analysis_json=json.dumps(ev_dict, ensure_ascii=False),
        )
        session.add(msg_row)
        session.commit()
        sync_all_event_folders(session)

        return {
            "status": upsert_status,
            "is_event": True,
            "is_duplicate": upsert_status == "duplicate",
            "is_updated": upsert_status == "updated",
            "changes": changes,
            "event_id": ev_record.id,
            "event": ev_dict,
            "inbox_message": msg_row.to_dict(),
        }


def analyze_fanpage_posts(
    fanpage_id: int,
    limit: Optional[int] = None,
    force_recheck: bool = False,
) -> Dict[str, int]:
    """
    Analyze remaining/unanalyzed posts of a fanpage (optionally scoped to the `limit` latest posts)
    and refresh its events.json. Blocking (run in a thread).
    """
    init_db()
    stats = {"analyzed": 0, "llm_calls": 0, "events": 0, "updated": 0, "duplicates": 0, "errors": 0}
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    max_llm_calls = max(MAX_LLM_CALLS_PER_FANPAGE, limit) if (limit and limit > 0) else MAX_LLM_CALLS_PER_FANPAGE
    with get_session() as session:
        fanpage = session.get(Fanpage, fanpage_id)
        if not fanpage:
            return stats
        existing_analyses = {
            pa.post_id: pa
            for pa in session.scalars(
                select(PostAnalysis).where(PostAnalysis.fanpage_id == fanpage_id)
            ).all()
        }
        stmt = select(Post).where(Post.fanpage_id == fanpage_id).order_by(Post.published_at.desc(), Post.id.desc())
        if limit is not None and limit > 0:
            stmt = stmt.limit(limit)
        all_posts = session.scalars(stmt).all()

        pending: List[Post] = []
        for p in all_posts:
            pa = existing_analyses.get(p.post_id)
            if pa is None:
                pending.append(p)
            elif not pa.is_event and (
                force_recheck or (pa.event_json or "{}") == "{}"
            ) and looks_event_related(p.content):
                # Re-queue legacy pre-filtered posts (or force_recheck non-events) that pass the event filter
                session.delete(pa)
                session.flush()
                pending.append(p)

        name = fanpage.name or fanpage.slug

        for post in pending:
            if not looks_event_related(post.content):
                session.add(
                    PostAnalysis(
                        post_id=post.post_id,
                        fanpage_id=fanpage_id,
                        is_event=False,
                        event_json='{"reason":"prefilter"}',
                    )
                )
                stats["analyzed"] += 1
                continue
            if not api_key or stats["llm_calls"] >= max_llm_calls:
                continue  # leave uncached; retried next cycle
            if stats["llm_calls"] > 0 and GEMINI_CALL_DELAY_SECONDS > 0:
                time.sleep(GEMINI_CALL_DELAY_SECONDS)
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
                # Run through unified anchor-field deduplication/update & additional info gatherer
                upsert_status, ev_record, changes = upsert_unified_event(
                    session=session,
                    raw_data=data,
                    source_type="facebook",
                    source_id=post.post_id,
                    media_urls=post.media_urls,
                    fanpage_id=fanpage_id,
                    client_id=None,
                    post_url=post.post_url,
                    posted_at=post.published_at.isoformat() if post.published_at else None,
                )
                data["dedup_status"] = upsert_status
                if changes:
                    data["updated_changes"] = changes
                if upsert_status == "updated":
                    stats["updated"] += 1
                elif upsert_status == "duplicate":
                    stats["duplicates"] += 1

            session.add(PostAnalysis(
                post_id=post.post_id,
                fanpage_id=fanpage_id,
                is_event=is_event,
                event_json=json.dumps(data, ensure_ascii=False) if is_event else '{"reason":"llm_not_event"}',
            ))
            stats["analyzed"] += 1
            stats["events"] += int(is_event)

        session.commit()
        _write_events_json(fanpage, session)
        sync_all_event_folders(session)
    return stats


def purge_fanpage_events(fanpage_id: int, slug: str) -> None:
    """Delete cached analyses and the events folder of a removed fanpage."""
    import shutil

    with get_session() as session:
        for row in session.scalars(select(PostAnalysis).where(PostAnalysis.fanpage_id == fanpage_id)).all():
            session.delete(row)
        for ev in session.scalars(select(EventRecord).where(EventRecord.fanpage_id == fanpage_id)).all():
            session.delete(ev)
        session.commit()
        sync_all_event_folders(session)
    shutil.rmtree(FEEDS_DIR / slug, ignore_errors=True)
    shutil.rmtree(FEEDS_DIR / "fanpages" / slug, ignore_errors=True)
