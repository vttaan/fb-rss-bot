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
from database import (
    EmailMessage,
    EventRecord,
    Fanpage,
    Post,
    PostAnalysis,
    ensure_local_tz,
    get_session,
    init_db,
)

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

# Set your Gemini API key directly here (or leave empty to use GEMINI_API_KEY env var):
DEFAULT_GEMINI_API_KEY = ""
DEFAULT_GEMINI_API_KEYS: List[str] = []
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
_GLOBAL_KEY_POOL.add_keys(os.environ.get("GEMINI_API_KEYS"))
_GLOBAL_KEY_POOL.add_keys(os.environ.get("GEMINI_API_KEY"))
_GLOBAL_KEY_POOL.add_keys(DEFAULT_GEMINI_API_KEYS)
_GLOBAL_KEY_POOL.add_keys(DEFAULT_GEMINI_API_KEY)
if _GLOBAL_KEY_POOL.has_keys() and not os.environ.get("GEMINI_API_KEY"):
    os.environ["GEMINI_API_KEY"] = _GLOBAL_KEY_POOL.get_active_key() or ""

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
GEMINI_LIST_MODELS_URL = "https://generativelanguage.googleapis.com/v1beta/models"
GEMINI_CACHED_CONTENTS_URL = "https://generativelanguage.googleapis.com/v1beta/cachedContents"
MIN_KEYWORD_MATCHES = int(os.environ.get("MIN_KEYWORD_MATCHES") or 2)  # distinct keywords needed to call the LLM
MAX_LLM_CALLS_PER_FANPAGE = 15  # safety cap per cycle to protect free-tier quota
GEMINI_CALL_DELAY_SECONDS = float(os.environ.get("GEMINI_CALL_DELAY_SECONDS") or 4.0)
GEMINI_MAX_RETRIES = int(os.environ.get("GEMINI_MAX_RETRIES") or 3)
GEMINI_REQUEST_TIMEOUT_SECONDS = int(os.environ.get("GEMINI_REQUEST_TIMEOUT_SECONDS") or 90)
GEMINI_BATCH_SIZE = max(1, int(os.environ.get("GEMINI_BATCH_SIZE") or 8))
GEMINI_CACHE_TTL_SECONDS = int(os.environ.get("GEMINI_CACHE_TTL_SECONDS") or 3600)
GEMINI_ENABLE_CONTEXT_CACHE = os.environ.get("GEMINI_ENABLE_CONTEXT_CACHE", "true").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)

# Compared against lowercase, diacritics-stripped text.
# Empirically selected from a 100-post labeled university corpus (60 Event, 40 Non-Event;
# 24,416 n-grams in post_word_frequency_v2.csv). Generic high-leakage words such as
# "chuong trinh" (41.3% non-event), "co so" (20.8% non-event), "hoi nghi" (88.9% non-event),
# "buoi" (38.5% non-event), "giai thuong" (57.1% non-event), "hoc bong" (50.0% non-event),
# "quyen loi" (42.9% non-event), and "lich" (matches "lich su"/"lich thi"/"lich hoc")
# are intentionally excluded from the pre-filter keyword list.
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

OUTPUT JSON SCHEMA PER POST (use null when unknown, never invent data):
{
  "post_id": string|null,             // echo back input post_id when provided in batch mode
  "is_event": boolean,
  "reason": string,                   // 1-sentence content reasoning justification for why is_event is true or false
  "title": string|null,               // event title
  "description": string|null,         // summary of what the event is about and what benefits students will get (NOT raw intro text)
  "event_start_datetime": string|null,// ISO 8601 (+07:00), time event starts (XXh from XXh-YYh, or 00:00:00+07:00 if no time mentioned)
  "event_end_datetime": string|null,  // ISO 8601 (+07:00), time event ends (YYh from XXh-YYh, or 23:59:00+07:00 of end day if no time mentioned)
  "reg_start_datetime": string|null,  // ISO 8601 (+07:00) ONLY if registration open time is explicitly mentioned, else null
  "reg_end_datetime": string|null,    // ISO 8601 (+07:00) ONLY if registration close/deadline time is explicitly mentioned, else null
  "location": string|null,            // If the post contains a line starting with "Địa điểm", "Dia diem", "Địa chỉ", "Location", "Venue", etc. (even after emojis like 📍), take the full location line content
  "organizer": string|null,
  "registration_url": string|null,    // only a registration URL that literally appears in the post text; null otherwise
  "fee": string|null,                 // search lines containing "Lệ phí", "le phi", "Phí tham gia", "Giá vé", "Fee", etc.; e.g., "miễn phí", "25.000 VNĐ/sinh viên", "100.000 VND"
  "is_online": boolean|null,
  "target_audience": string[]|null,   // 1-4 values from: "sinh viên","giáo viên","nghiên cứu sinh","giảng viên","nhân viên","công chức","doanh nghiệp","cộng đồng","tổ chức phi lợi nhuận","tổ chức chính phủ","khác"; null if unknown; if the post specifies students in a major/year/school, list specifically (e.g., "sinh viên CNTT", "sinh viên K25, K26")
  "category": string[],               // 1-4 values from: "workshop","seminar","hội nghị","ca nhạc","học thuật","cộng đồng","ngày hội","pháp luật","khoa học","cuộc thi","tọa đàm","triển lãm","thể thao","hướng nghiệp","thiện nguyện","quyên góp","khác"; [] if unknown
  "format": string[]                  // 1-4 values from: "offline","online","cá nhân","đồng đội","chạy trạm","thi","khảo sát"; [] if unknown
}
If given a single post, return a single JSON object. If given a JSON array of posts under `posts`, return a JSON array of objects in the exact same order, each including its `post_id`."""

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

# Legacy combined prompt alias for backward compatibility
PROMPT = SYSTEM_REGULATIONS + "\n\n" + USER_POST_TEMPLATE


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


# Tier-1 phrases with 90%-100% empirical Event purity in the 100-post study.
# Note: "hoi nghi" (88.9% non-event) and "tap huan" (100% non-event in recaps) are excluded here.
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

# High-purity Non-Event / Recap / Honor / Routine-Admin signals (75%-100% Non-Event in corpus).
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

# Contextual non-event patterns requiring multi-signal content reasoning
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
    """
    Deterministic multi-step Content Reasoning engine (validated on the 100-post corpus:
    58 Event posts and 42 Non-Event posts):
      - Step 1 (Disqualification & Contextual Override Reasoning):
        Evaluates explicit recap/congratulation/admin signals (`NON_EVENT_SIGNAL_RE`) and the 5
        borderline structural non-event archetypes (internal union congresses, pure scholarship
        dossier submissions, corporate graduate job postings, retrospective sponsor thank-you
        summaries without a schedule, and companion Q&A handbook posts).
      - Step 2 (Positive Event Signal & Structure Reasoning):
        Checks Tier-1 `STRONG_EVENT_SIGNAL_RE` phrases (90%-100% Event purity), keyword density
        (>= 3 distinct `EVENT_KEYWORDS`), or >= `MIN_KEYWORD_MATCHES` with a concrete date/time/URL.
    Returns `(is_event_candidate, reasoning_explanation)`.
    """
    if not content or not content.strip():
        return False, "disqualified:empty_post"

    stripped = _strip_accents(content).lower()
    has_active_reg_override = bool(ACTIVE_REGISTRATION_OVERRIDE_RE.search(stripped))

    # 1. Explicit recap / honor / routine admin signal (unless overridden by active registration CTA)
    m_non = NON_EVENT_SIGNAL_RE.search(stripped)
    if m_non and not has_active_reg_override:
        return False, f"disqualified:non_event_signal ({m_non.group(0).strip()})"

    # 2. Internal union/committee governance congress (even if time/venue for delegates is listed)
    m_cong = _INTERNAL_CONGRESS_RE.search(stripped)
    if m_cong and not has_active_reg_override:
        return False, f"disqualified:internal_governance_congress ({m_cong.group(0).strip()})"

    # 3. Pure scholarship / financial-aid dossier submission without a live seminar/info-session
    m_schol = _SCHOLARSHIP_DOSSIER_RE.search(stripped)
    if m_schol and not _LIVE_SESSION_CUE_RE.search(stripped):
        return False, f"disqualified:scholarship_dossier_notice ({m_schol.group(0).strip()})"

    # 4. Full-time corporate graduate job/trainee posting without a campus talk/workshop/fair
    m_job = _CORPORATE_JOB_PROGRAM_RE.search(stripped)
    if m_job and not _LIVE_SESSION_CUE_RE.search(stripped):
        return False, f"disqualified:corporate_job_posting ({m_job.group(0).strip()})"

    # 5. Retrospective sponsor/speaker gratitude summary lacking upcoming schedule or registration CTA
    m_retro = _RETROSPECTIVE_GRATITUDE_RE.search(stripped)
    if m_retro and not has_active_reg_override and not _EXPLICIT_SCHEDULE_OR_REG_LABEL_RE.search(stripped):
        return False, f"disqualified:retrospective_summary ({m_retro.group(0).strip()})"

    # 6. Companion Q&A handbook post that answers questions in images rather than announcing a new event
    m_hand = _COMPANION_HANDBOOK_RE.search(stripped)
    if m_hand:
        return False, f"disqualified:companion_qa_handbook ({m_hand.group(0).strip()})"

    # Positive Step A: Tier-1 strong event phrase (90%-100% Event purity)
    m_strong = STRONG_EVENT_SIGNAL_RE.search(stripped)
    if m_strong:
        return True, f"qualified:strong_event_signal ({m_strong.group(0).strip()})"

    # Positive Step B: Keyword density (>= 3 distinct event keywords)
    kw_count = count_event_keywords(content)
    if kw_count >= max(3, MIN_KEYWORD_MATCHES + 1):
        return True, f"qualified:keyword_density (kw_count={kw_count})"

    # Positive Step C: Moderate keyword count (>= MIN_KEYWORD_MATCHES) + concrete date, time, or link
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
    """
    Empirical content-reasoning pre-filter before calling Gemini (validated on 100 labeled posts).
    Delegates to `reason_about_post_content(content)`.
    """
    is_candidate, _ = reason_about_post_content(content)
    return is_candidate


FALLBACK_GEMINI_MODELS = [
    "gemini-2.0-flash-lite",
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-flash-8b",
]
_WORKING_GEMINI_MODEL: Optional[str] = None
# In-memory registry of active Gemini cachedContents handles per model:
#   { model_name: (cached_content_name, expires_epoch_seconds) }
_CACHED_SYSTEM_HANDLES: Dict[str, Tuple[str, float]] = {}
_CACHE_UNSUPPORTED_MODELS: set[str] = set()


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
            # Filter for models starting with 2 (e.g. gemini-2.*, version 2.*)
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
    """
    Create or reuse a server-side Gemini `cachedContents` resource holding `SYSTEM_REGULATIONS`
    for `model`. If the model/tier rejects explicit context caching (e.g., < minimum token threshold
    or free-tier restriction), marks `model` in `_CACHE_UNSUPPORTED_MODELS` and falls back to
    `system_instruction` without extra round-trips.
    """
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
            print(f"[events] Cached regulations on Gemini ({model}) -> {cache_name.strip()} (TTL {ttl_s}s)")
            return cache_name.strip()
    except urllib.error.HTTPError as exc:
        # 400/403/404 usually means prompt < min token threshold (1024-4096) or free-tier caching disabled
        _CACHE_UNSUPPORTED_MODELS.add(model)
        if exc.code not in (400, 403, 404):
            print(f"[events] Gemini cachedContents unavailable on {model} (HTTP {exc.code}); using system_instruction.")
    except Exception:
        _CACHE_UNSUPPORTED_MODELS.add(model)
    return None


def _build_gemini_request_body(user_text: str, model: str, api_key: str) -> bytes:
    """
    Build the `generateContent` request body:
      - Uses server-side `cachedContent` handle if available for `model` (sends ONLY raw post text).
      - Otherwise uses `system_instruction` for `SYSTEM_REGULATIONS` (eligible for Gemini implicit
        prefix caching) and sends ONLY the raw post text in `contents`.
    """
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
    """Compute backoff delay in seconds, honoring Retry-After or Gemini error retryDelay if provided."""
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
                    print(f"[events] Successfully extracted via Groq fallback ({model})")
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
                    print(f"[events] Groq model '{model}' token limit error: {err_msg}")
                    if len(clean_user_text) > 2500:
                        clean_user_text = clean_user_text[:2500] + '\n"""\n[Truncated]'
                    break

                print(f"[events] Groq model '{model}' HTTP {exc.code}: {err_msg}")
                # Don't retry second pass on 404 (model not found), 401/403 (auth), or 429 (rate limit)
                break
            except Exception as exc:
                last_exc = exc
                print(f"[events] Groq error for model '{model}': {exc}")
                break

    if last_exc:
        raise last_exc
    return None


def _execute_gemini_generate(user_text: str, api_key: str = "") -> Any:
    """Execute a Gemini `generateContent` request with key rotation across pooled keys, model fallback, and retry."""
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
                        print(f"[events] Gemini model '{model}' returned HTTP {exc.code} ({exc.reason}); trying next fallback model...")
                        break
                    if exc.code == 403:
                        print(f"[events] Gemini key (...{active_key[-6:] if len(active_key) > 6 else active_key}) returned HTTP 403 Forbidden ({exc.reason}).")
                        if len(pool.keys) > 1:
                            pool.mark_invalid(active_key)
                            break
                        raise
                    if exc.code in (429, 500, 502, 503, 504):
                        if exc.code == 429 and len(pool.keys) > 1:
                            wait_s = _retry_delay_seconds(attempt, exc)
                            print(f"[events] Gemini key (...{active_key[-6:] if len(active_key) > 6 else active_key}) hit HTTP 429 Rate Limit. Rotating to next key in pool...")
                            pool.mark_rate_limited(active_key, wait_s)
                            break
                        if attempt < GEMINI_MAX_RETRIES:
                            wait_s = _retry_delay_seconds(attempt, exc)
                            print(f"[events] Gemini {model} returned HTTP {exc.code}; retrying in {wait_s:.1f}s...")
                            time.sleep(wait_s)
                            continue
                        break
                    raise
                except (TimeoutError, socket.timeout, urllib.error.URLError, OSError) as exc:
                    last_exc = exc
                    if attempt < GEMINI_MAX_RETRIES:
                        wait_s = _retry_delay_seconds(attempt, exc)
                        print(f"[events] Gemini {model} network error ({exc}); retrying in {wait_s:.1f}s...")
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
) -> Optional[Dict[str, Any]]:
    """Send text to Gemini (with key rotation and dynamic Gemini-2 model discovery)."""
    user_text = USER_POST_TEMPLATE.format(
        published_at=published_at_str,
        fanpage=source_name,
        content=(content or "")[:6000],
    )
    try:
        data = _execute_gemini_generate(user_text, api_key)
        if isinstance(data, list):
            data = data[0] if data else {}
        if isinstance(data, dict):
            return data
    except Exception as exc:
        print(f"[events] Gemini error ({exc}); falling back to heuristic...")

    return None


def call_gemini_batch(
    posts: List[Post],
    fanpage_name: str,
    api_key: str = "",
) -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Send a batch of raw posts in a SINGLE Gemini API call (with key rotation).
    Returns a dict mapping `post_id -> extracted_dict`.
    """
    if not posts:
        return {}
    if len(posts) == 1:
        single = call_gemini(posts[0], fanpage_name, api_key)
        return {posts[0].post_id: single} if isinstance(single, dict) else None

    blocks: List[str] = []
    for idx, p in enumerate(posts, start=1):
        pub_str = p.published_at.isoformat() if p.published_at else "unknown"
        blocks.append(
            f'--- POST {idx} (post_id: "{p.post_id}") ---\n'
            + USER_POST_TEMPLATE.format(
                published_at=pub_str,
                fanpage=fanpage_name,
                content=(p.content or "")[:4500],
            )
        )
    user_text = USER_BATCH_POSTS_TEMPLATE.format(
        count=len(posts),
        posts_block="\n\n".join(blocks),
    )

    def _unpack_batch(data: Any) -> Optional[Dict[str, Dict[str, Any]]]:
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
        result_map: Dict[str, Dict[str, Any]] = {}
        known_ids = {p.post_id for p in posts}
        for idx, item in enumerate(data):
            if not isinstance(item, dict):
                continue
            pid = str(item.get("post_id") or "").strip()
            if pid in known_ids:
                result_map[pid] = item
            elif len(data) == len(posts):
                result_map[posts[idx].post_id] = item
        return result_map or None

    # 1. Try Gemini
    try:
        data = _execute_gemini_generate(user_text, api_key)
        mapped = _unpack_batch(data)
        if mapped:
            return mapped
    except Exception as exc:
        print(f"[events] Gemini batch error ({exc}); retrying batch posts individually via Gemini...")
        single_mapped: Dict[str, Dict[str, Any]] = {}
        for p in posts:
            try:
                single = call_gemini(p, fanpage_name, api_key)
                if isinstance(single, dict) and single:
                    single_mapped[p.post_id] = single
            except Exception as single_exc:
                print(f"[events] Individual fallback failed for post '{p.post_id}': {single_exc}")
        if single_mapped:
            return single_mapped

    return None


def call_gemini(post: Post, fanpage_name: str, api_key: str) -> Optional[Dict[str, Any]]:
    pub = post.published_at.isoformat() if post.published_at else "unknown"
    return call_gemini_text(post.content or "", pub, fanpage_name, api_key)


_LOCATION_LABEL_RE = re.compile(
    r"(?:^|\b)(?:dia\s*diem|dia\s*chi|noi\s*to\s*chuc|location|venue|address|room|hall|phong|hoi\s*truong|giang\s*duong|co\s*so)\b",
    flags=re.IGNORECASE,
)


def _extract_location_from_text(text: str) -> Optional[str]:
    """
    Search text line-by-line (accent-insensitively) for location cues like
    'Địa điểm', 'Dia diem', 'Địa chỉ', 'Hội trường', 'Giảng đường', 'Cơ sở', 'Location', 'Venue'.
    Preserves full UTF-8 location string (including periods inside 'TP. HCM' or 'ĐH. KHTN').
    """
    if not text:
        return None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # Strip leading emojis, symbols, and list bullets before checking the label
        cleaned = re.sub(r"^[^\w\s]+", "", line, flags=re.UNICODE).strip()
        if not cleaned:
            cleaned = line

        m_sep = re.match(r"^([^:\-\n]{2,40})\s*[:\-]\s*(.+)$", cleaned)
        if m_sep:
            label_norm = _strip_accents(m_sep.group(1)).lower().strip()
            value = m_sep.group(2).strip()
            if value and _LOCATION_LABEL_RE.search(label_norm):
                return value[:300]

        line_norm = _strip_accents(cleaned).lower()
        if re.match(r"^(?:dia\s*diem|dia\s*chi|noi\s*to\s*chuc|location|venue|address)\b", line_norm):
            return cleaned[:300]

    # Inline fallback ("tại Hội trường...", "at Hall...", "tại cơ sở...", "Zoom", etc.)
    m_inline = re.search(
        r"\b(?:tại|tai|at)\s+((?:Hội\s+trường|Hoi\s+truong|Giảng\s+đường|Giang\s+duong|Phòng|Phong|Cơ\s+sở|Co\s+so|Trường|Truong|Hall|Room)[^\n\r;]+|Zoom|Google\s+Meet|MS\s+Teams)",
        text,
        flags=re.IGNORECASE,
    )
    if m_inline:
        return m_inline.group(1).strip().rstrip(".,;")[:300]
    return None


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
    """
    Extract (start_date, end_date, start_time, end_time) from Vietnamese/English event text.
    Handles time ranges like '8h-11h', '08h30 - 11h30', '16h00 đến 20h30', '14:00 - 17:00',
    single times like '14h', '14h30', '19:00', and date ranges like '05/11/2026 - 10/11/2026'.
    """
    if not text:
        return None, None, None, None

    ref_year = (published_at or datetime.now(LOCAL_TIMEZONE)).year
    stripped = _strip_accents(text)

    # 1. Extract dates (prefer date range if present, otherwise single date)
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

    # 2. Extract time range (e.g., XXh-YYh, XXhMM - YYhNN, XX:MM - YY:MM, tu XXh den YYh)
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
        # Single time with explicit minutes (14:30, 14h30, 8g30)
        m_t1 = re.search(r"\b([01]?\d|2[0-3])\s*[:hg]\s*([0-5]\d)\s*(am|pm)?\b", stripped, flags=re.IGNORECASE)
        if m_t1:
            start_time = _format_hhmm(m_t1.group(1), m_t1.group(2), m_t1.group(3))
        else:
            # Bare hour with 'h' or 'g' (e.g., "14h", "luc 8h", "vao luc 19h")
            m_t2 = re.search(r"\b([01]?\d|2[0-3])\s*[hg]\b\s*(am|pm)?", stripped, flags=re.IGNORECASE)
            if m_t2:
                start_time = _format_hhmm(m_t2.group(1), None, m_t2.group(2))

    return start_date, end_date, start_time, end_time


_FEE_LINE_RE = re.compile(
    r"\b(?:le\s*phi|phi\s*tham\s*(?:gia|du)|phi\s*dang\s*ky|gia\s*ve|chi\s*phi|fee|ticket\s*price)\b",
    flags=re.IGNORECASE,
)


def _extract_fee_from_text(text: str) -> Optional[str]:
    """
    Search text line-by-line (accent-insensitively) for lines containing 'Lệ phí', 'le phi',
    'Phí tham gia', 'Giá vé', 'Fee', etc., or free-entry cues ('miễn phí', 'free').
    """
    if not text:
        return None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        cleaned = re.sub(r"^[^\w\s]+", "", line, flags=re.UNICODE).strip() or line
        line_norm = _strip_accents(cleaned).lower()
        if _FEE_LINE_RE.search(line_norm):
            m_sep = re.match(r"^[^:\-\n]{2,45}\s*[:\-]\s*(.+)$", cleaned)
            value = (m_sep.group(1) if m_sep else cleaned).strip().rstrip(".")
            if value:
                if re.fullmatch(r"(?:mien\s*phi|free|0\s*(?:d|dong|vnd)?)", _strip_accents(value).lower()):
                    return "miễn phí"
                return value[:160]

    stripped = _strip_accents(text).lower()
    if re.search(r"\b(free|mien\s*phi)\b", stripped):
        return "miễn phí"
    return None


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


def _summarize_event_description(content: str) -> Optional[str]:
    """
    Build a fallback event summary focusing on core activities, what participants will learn/experience,
    and what benefits/prizes they get, rather than blindly slicing the first 2 sentences.
    """
    if not content or not content.strip():
        return None

    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
    if not lines:
        return None

    # Skip the first line if it looks like an all-caps/emoji title banner
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
        # Strip inline URLs when summarizing
        text_only = re.sub(r"https?://\S+", "", cleaned).strip(" :-")
        if len(text_only) < 12:
            continue

        if _DESC_BENEFIT_RE.search(norm) and text_only not in picked_benefits:
            picked_benefits.append(text_only)
        elif _DESC_LEARN_ACTIVITY_RE.search(norm) and text_only not in picked_activities:
            picked_activities.append(text_only)
        elif text_only not in fallback_lines:
            fallback_lines.append(text_only)

    selected = (picked_activities[:2] + picked_benefits[:2])
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
    """Normalize date/datetime string into ISO 8601 (+07:00), applying `default_time` if only a date is given."""
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


def _extract_registration_dates_from_text(
    text: str,
    published_at: Optional[datetime] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract explicit registration open (`reg_start_datetime`) and close (`reg_end_datetime`)
    from Vietnamese/English registration timeline cues. Returns (None, None) when not explicitly stated.
    """
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
        if has_open and has_close and s_date and e_date:
            reg_start = reg_start or f"{s_date}T{s_time or '00:00'}:00+07:00"
            reg_end = reg_end or f"{e_date}T{e_time or '23:59'}:00+07:00"
        elif has_reg_range and s_date and e_date:
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
    """
    Post-process LLM / heuristic output to enforce content reasoning & field regulations:
      - apply deterministic content-reasoning guardrail (`reason_about_post_content`) to override
        false-positive LLM classifications on recaps, internal congresses, scholarship dossiers,
        or corporate job ads
      - normalize `title` whitespace
      - fill `description`, `location`, `fee`
      - normalize `event_start_datetime` (00:00 default) & `event_end_datetime` (23:59 default)
      - preserve explicit `reg_start_datetime` / `reg_end_datetime` flags
    """
    if not raw_data:
        return raw_data

    is_candidate, reasoning_note = reason_about_post_content(content)
    if not raw_data.get("reason"):
        raw_data["reason"] = reasoning_note

    if not raw_data.get("is_event"):
        return raw_data

    # Guardrail: if deterministic content reasoning disqualifies the post via a hard non-event rule
    # (recap, internal congress, pure scholarship dossier, corporate job ad, retrospective thank-you,
    # or Q&A handbook), override false-positive LLM classification.
    if not is_candidate and reasoning_note.startswith("disqualified:") and not reasoning_note.startswith(
        "disqualified:insufficient_event_signals"
    ):
        raw_data["is_event"] = False
        raw_data["reason"] = f"guardrail_override ({reasoning_note})"
        return raw_data

    # 0. Clean title & fill missing description
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

    # 1. Fill missing location from explicit 'Địa điểm' / 'Dia diem' / venue cues
    if not (raw_data.get("location") or "").strip():
        inferred_loc = _extract_location_from_text(content)
        if inferred_loc:
            raw_data["location"] = inferred_loc

    # 2. Fill missing fee from lines containing 'Lệ phí' / 'le phi' / fee cues
    if not (raw_data.get("fee") or "").strip():
        inferred_fee = _extract_fee_from_text(content)
        if inferred_fee:
            raw_data["fee"] = inferred_fee

    # 3. Normalize event_start_datetime (default 00:00) & event_end_datetime (default 23:59)
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

    # 4. Registration open/close dates (track explicit mention vs default)
    llm_reg_start = _normalize_iso_datetime(raw_data.get("reg_start_datetime"), default_time="00:00")
    llm_reg_end = _normalize_iso_datetime(raw_data.get("reg_end_datetime"), default_time="23:59")
    regex_reg_start, regex_reg_end = _extract_registration_dates_from_text(content, published_at=published_at)

    explicit_reg_start = llm_reg_start or regex_reg_start
    explicit_reg_end = llm_reg_end or regex_reg_end

    raw_data["reg_start_datetime"] = explicit_reg_start
    raw_data["reg_end_datetime"] = explicit_reg_end
    raw_data["_reg_start_explicit"] = bool(explicit_reg_start)
    raw_data["_reg_end_explicit"] = bool(explicit_reg_end)

    return raw_data


def heuristic_extract_event(
    content: str,
    subject: str = "",
    sender: Optional[str] = None,
    published_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Rule-based fallback extractor when GEMINI_API_KEY is not configured in local dev."""
    combined = f"{subject}\n{content}".strip()
    stripped = _strip_accents(combined).lower()
    is_candidate, reasoning_note = reason_about_post_content(combined)
    if not is_candidate:
        return {"is_event": False, "reason": reasoning_note}

    # Title: prefer subject if non-generic, else extract from content preserving brackets
    title = ""
    if subject and subject.strip():
        subj_clean = _clean_event_title(subject)
        if subj_clean != "Untitled Event" and not _is_greeting_line(subj_clean) and GENERIC_CHANNEL_TAGS_PATTERN.sub("", subj_clean).strip():
            title = subj_clean
    if not title:
        title = _extract_title_from_content(combined)
    title = _clean_event_title(title)

    # Extract event start/end date & time (00:00 start default, 23:59 end default)
    s_date, e_date, s_time, e_time = _extract_datetime_range_from_text(combined, published_at=published_at)
    event_start_datetime = f"{s_date}T{s_time or '00:00'}:00+07:00" if s_date else None
    target_end_date = e_date or s_date
    event_end_datetime = f"{target_end_date}T{e_time or '23:59'}:00+07:00" if target_end_date else None

    reg_start_datetime, reg_end_datetime = _extract_registration_dates_from_text(combined, published_at=published_at)

    # Location extraction (accent-insensitive across 'Địa điểm', 'Dia diem', 'Hội trường', etc.)
    location = _extract_location_from_text(combined)

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

    # Description summary (activities, what participants learn/experience, and benefits/prizes)
    description = _summarize_event_description(content)

    return {
        "is_event": True,
        "reason": reasoning_note,
        "title": title,
        "description": description or None,
        "event_start_datetime": event_start_datetime,
        "event_end_datetime": event_end_datetime,
        "start_datetime": event_start_datetime,
        "end_datetime": event_end_datetime,
        "reg_start_datetime": reg_start_datetime,
        "reg_end_datetime": reg_end_datetime,
        "_reg_start_explicit": bool(reg_start_datetime),
        "_reg_end_explicit": bool(reg_end_datetime),
        "location": location,
        "organizer": sender,
        "registration_url": reg_url,
        "fee": _extract_fee_from_text(combined),
        "is_online": is_online,
        "target_audience": ["sinh viên"] if re.search(r"\b(student|sinh vien)\b", stripped) else None,
        "category": categories or ["khác"],
        "format": fmt_list,
    }


def _normalize_title_key(title: str) -> str:
    """Normalize event title for anchor-field matching across fanpages and emails."""
    cleaned = _clean_event_title(title)
    s = _strip_accents(cleaned).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _normalize_location_key(location: Optional[str]) -> str:
    if not location:
        return ""
    s = _strip_accents(location).lower()
    # Strip common Vietnamese/English location prefixes so "Địa điểm: Hội trường A" matches "Hội trường A"
    s = re.sub(r"^\s*(?:dia\s*diem|dia\s*chi|noi\s*to\s*chuc|location|venue|address)\s*[:\-]?\s*", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _parse_date_and_time(
    start_datetime: Optional[str],
    raw_date: Optional[str] = None,
    raw_time: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """Extract canonical YYYY-MM-DD and HH:MM strings from start_datetime or explicit date/time fields."""
    ev_date = (raw_date or "").strip() or None
    ev_time = (raw_time or "").strip() or None
    if ev_time:
        m_rt = re.match(r"^(\d{1,2})(?:[:hg](\d{2}))?", ev_time, flags=re.IGNORECASE)
        if m_rt:
            ev_time = f"{int(m_rt.group(1)):02d}:{int(m_rt.group(2) or 0):02d}"
    if start_datetime and isinstance(start_datetime, str):
        s = start_datetime.strip()
        m_dt = re.match(r"^(\d{4}-\d{2}-\d{2})(?:[T\s](\d{1,2})[:hg](\d{2})?)?", s, flags=re.IGNORECASE)
        if m_dt:
            if not ev_date:
                ev_date = m_dt.group(1)
            if not ev_time and m_dt.group(2):
                hh_mm = f"{int(m_dt.group(2)):02d}:{int(m_dt.group(3) or 0):02d}"
                if hh_mm != "00:00":
                    ev_time = hh_mm
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

    raw_start = data.get("event_start_datetime") or data.get("start_datetime")
    raw_end = data.get("event_end_datetime") or data.get("end_datetime")
    ev_date, ev_time = _parse_date_and_time(
        raw_start,
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
        "start_datetime": raw_start,
        "end_datetime": raw_end,
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
    ev_start = enriched.get("event_start_datetime") or enriched.get("start_datetime")
    ev_end = enriched.get("event_end_datetime") or enriched.get("end_datetime")
    return {
        "description": enriched.get("description"),
        "event_start_datetime": ev_start,
        "event_end_datetime": ev_end,
        "reg_start_datetime": enriched.get("reg_start_datetime"),
        "reg_end_datetime": enriched.get("reg_end_datetime"),
        "organizer": enriched.get("organizer"),
        "registration_url": enriched.get("registration_url"),
        "registration_status": enriched.get("registration_status"),
        "fee": enriched.get("fee"),
        "target_audience": enriched.get("target_audience"),
        "category": enriched.get("category") if isinstance(enriched.get("category"), list) else [],
        "format": enriched.get("format") if isinstance(enriched.get("format"), list) else [],
    }


def _merge_registration_and_null_fields(
    stored_info: Dict[str, Any],
    extra_info: Dict[str, Any],
    reg_start_explicit: bool,
    reg_end_explicit: bool,
) -> Dict[str, Dict[str, Any]]:
    """
    Apply registration open/close update rules and backfill any previously null fields:
      - If a later post explicitly mentions `reg_start_datetime` or `reg_end_datetime`, update it.
      - If NOT explicitly mentioned, skip updating that registration field.
      - Backfill any non-empty fields where `stored_info` previously held None/empty/'free'.
    Returns dict of field-level changes applied.
    """
    extra_changes: Dict[str, Dict[str, Any]] = {}

    if reg_start_explicit and extra_info.get("reg_start_datetime"):
        new_rs = extra_info["reg_start_datetime"]
        if new_rs != stored_info.get("reg_start_datetime"):
            extra_changes["reg_start_datetime"] = {"old": stored_info.get("reg_start_datetime"), "new": new_rs}
            stored_info["reg_start_datetime"] = new_rs

    if reg_end_explicit and extra_info.get("reg_end_datetime"):
        new_re = extra_info["reg_end_datetime"]
        if new_re != stored_info.get("reg_end_datetime"):
            extra_changes["reg_end_datetime"] = {"old": stored_info.get("reg_end_datetime"), "new": new_re}
            stored_info["reg_end_datetime"] = new_re

    for k, v in extra_info.items():
        if k in ("reg_start_datetime", "reg_end_datetime"):
            continue
        if v in (None, [], ""):
            continue
        existing_val = stored_info.get(k)
        is_missing = existing_val in (None, [], "") or (k == "registration_url" and existing_val == "free" and v != "free")
        if is_missing:
            extra_changes[k] = {"old": existing_val, "new": v}
            stored_info[k] = v

    return extra_changes


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
      3. Enforce registration open/close defaults on new events & explicit-only updates on later posts,
         plus backfill any previously null fields.
      4. Persist or update EventRecord in SQLite.
    Returns (status, event_record, changes_dict).
    """
    media = media_urls or []
    reg_start_explicit = bool(raw_data.pop("_reg_start_explicit", False) or raw_data.get("reg_start_datetime"))
    reg_end_explicit = bool(raw_data.pop("_reg_end_explicit", False) or raw_data.get("reg_end_datetime"))

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
        try:
            stored_info = json.loads(existing.event_json or "{}")
        except Exception:
            stored_info = {}

        # Check explicit registration updates or newly filled non-anchor fields (e.g. fee, end_datetime)
        candidate_extra = {
            "description": raw_data.get("description"),
            "event_start_datetime": raw_data.get("event_start_datetime") or raw_data.get("start_datetime"),
            "event_end_datetime": raw_data.get("event_end_datetime") or raw_data.get("end_datetime"),
            "reg_start_datetime": raw_data.get("reg_start_datetime"),
            "reg_end_datetime": raw_data.get("reg_end_datetime"),
            "organizer": raw_data.get("organizer"),
            "registration_url": raw_data.get("registration_url"),
            "fee": raw_data.get("fee"),
            "target_audience": raw_data.get("target_audience"),
            "category": raw_data.get("category") if isinstance(raw_data.get("category"), list) else [],
            "format": raw_data.get("format") if isinstance(raw_data.get("format"), list) else [],
        }
        extra_changes = _merge_registration_and_null_fields(
            stored_info, candidate_extra, reg_start_explicit, reg_end_explicit
        )
        if candidate_extra.get("event_end_datetime") and not existing.end_datetime:
            existing.end_datetime = candidate_extra["event_end_datetime"]

        sources = stored_info.get("sources") or []
        if not any(s.get("source_id") == source_id for s in sources):
            sources.append(source_entry)
            stored_info["sources"] = sources

        if extra_changes:
            history = stored_info.get("update_history") or []
            history.append({
                "updated_at": now_dt.isoformat(),
                "source_type": source_type,
                "source_id": source_id,
                "client_id": client_id,
                "changes": extra_changes,
            })
            stored_info["update_history"] = history
            existing.version = (existing.version or 1) + 1
            existing.updated_at = now_dt
            existing.event_json = json.dumps(stored_info, ensure_ascii=False)
            session.flush()
            return "updated", existing, extra_changes

        existing.event_json = json.dumps(stored_info, ensure_ascii=False)
        return "duplicate", existing, {}

    # Gather additional information (runs QR scanner and metadata normalization)
    extra_info = gather_additional_event_info(raw_data, media, source_id=source_id)
    raw_data["registration_url"] = extra_info.get("registration_url")
    raw_data["registration_status"] = extra_info.get("registration_status")

    if status == "updated" and existing is not None:
        try:
            stored_info = json.loads(existing.event_json or "{}")
        except Exception:
            stored_info = {}

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

        # Update explicit registration dates only if mentioned; skip otherwise; merge non-empty fields
        extra_changes = _merge_registration_and_null_fields(
            stored_info, extra_info, reg_start_explicit, reg_end_explicit
        )
        for k, v in extra_info.items():
            if k in ("reg_start_datetime", "reg_end_datetime"):
                continue
            if v not in (None, [], "") and (k != "registration_url" or v != "free" or not stored_info.get(k)):
                stored_info[k] = v

        all_changes = {**changes, **extra_changes}
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
            "changes": all_changes,
        })
        stored_info["update_history"] = history

        existing.version = (existing.version or 1) + 1
        existing.updated_at = now_dt
        existing.event_json = json.dumps(stored_info, ensure_ascii=False)
        session.flush()
        return "updated", existing, all_changes

    # Brand new event: apply registration defaults when not explicitly mentioned
    if not extra_info.get("reg_start_datetime"):
        extra_info["reg_start_datetime"] = posted_at or now_dt.isoformat()
    if not extra_info.get("reg_end_datetime"):
        if anchor["date"]:
            extra_info["reg_end_datetime"] = f"{anchor['date']}T00:00:00+07:00"
        elif anchor["start_datetime"]:
            extra_info["reg_end_datetime"] = anchor["start_datetime"]

    raw_data["reg_start_datetime"] = extra_info.get("reg_start_datetime")
    raw_data["reg_end_datetime"] = extra_info.get("reg_end_datetime")

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
    events.sort(key=lambda e: e.get("event_start_datetime") or e.get("start_datetime") or e.get("posted_at") or "", reverse=True)
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
            key=lambda e: e.get("event_start_datetime") or e.get("start_datetime") or e.get("date") or e.get("updated_at") or "",
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
        else:
            raw_data = _enrich_extracted_event_fields(raw_data, full_email_text, published_at=rec_dt)

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
        llm_candidates: List[Post] = []

        for post in pending:
            is_candidate, prefilter_reason = reason_about_post_content(post.content or "")
            if not is_candidate:
                session.add(
                    PostAnalysis(
                        post_id=post.post_id,
                        fanpage_id=fanpage_id,
                        is_event=False,
                        event_json=json.dumps({"reason": f"prefilter:{prefilter_reason}"}, ensure_ascii=False),
                    )
                )
                stats["analyzed"] += 1
            else:
                llm_candidates.append(post)

        def _persist_post_analysis(post: Post, raw_item: Dict[str, Any]) -> None:
            data = dict(raw_item)
            data.pop("post_id", None)
            data = _enrich_extracted_event_fields(data, post.content or "", published_at=post.published_at)
            is_event = bool(data.pop("is_event", False))
            non_event_reason = str(data.get("reason") or "llm_not_event").strip()
            if is_event:
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

            session.add(
                PostAnalysis(
                    post_id=post.post_id,
                    fanpage_id=fanpage_id,
                    is_event=is_event,
                    event_json=json.dumps(data, ensure_ascii=False)
                    if is_event
                    else json.dumps({"reason": non_event_reason}, ensure_ascii=False),
                )
            )
            stats["analyzed"] += 1
            stats["events"] += int(is_event)

        if api_key and llm_candidates:
            for start_idx in range(0, len(llm_candidates), GEMINI_BATCH_SIZE):
                if stats["llm_calls"] >= max_llm_calls:
                    break
                chunk = llm_candidates[start_idx : start_idx + GEMINI_BATCH_SIZE]
                if stats["llm_calls"] > 0 and GEMINI_CALL_DELAY_SECONDS > 0:
                    time.sleep(GEMINI_CALL_DELAY_SECONDS)

                batch_results: Optional[Dict[str, Dict[str, Any]]] = None
                try:
                    stats["llm_calls"] += 1
                    batch_results = call_gemini_batch(chunk, name, api_key)
                except (urllib.error.URLError, OSError, KeyError, IndexError, ValueError, TypeError) as exc:
                    print(f"[events] LLM batch error for fanpage {fanpage.slug} ({len(chunk)} post(s)): {exc}")
                    batch_results = None

                missing_posts: List[Post] = []
                if batch_results:
                    for post in chunk:
                        item_data = batch_results.get(post.post_id)
                        if isinstance(item_data, dict):
                            _persist_post_analysis(post, item_data)
                        else:
                            missing_posts.append(post)
                else:
                    missing_posts = list(chunk)

                if missing_posts and len(chunk) > 1:
                    # Fallback to single-post calls if a multi-post batch failed or returned partial items
                    for post in missing_posts:
                        if stats["llm_calls"] >= max_llm_calls:
                            stats["errors"] += 1
                            continue
                        if GEMINI_CALL_DELAY_SECONDS > 0:
                            time.sleep(GEMINI_CALL_DELAY_SECONDS)
                        try:
                            stats["llm_calls"] += 1
                            single_data = call_gemini(post, name, api_key)
                        except (urllib.error.URLError, OSError, KeyError, IndexError, ValueError, TypeError) as exc:
                            stats["errors"] += 1
                            print(f"[events] LLM error for post {post.post_id}: {exc}")
                            continue
                        if not single_data:
                            stats["errors"] += 1
                            continue
                        _persist_post_analysis(post, single_data)
                elif missing_posts:
                    stats["errors"] += len(missing_posts)

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


def _process_single_post_extraction(post_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Synchronous extraction & persistence for a scraped single Facebook post.
    If `is_event` is True and `fanpage_url` is resolved, adds the fanpage if missing,
    saves the post, generates RSS/JSON feeds, and upserts the unified EventRecord.
    """
    from database import add_fanpage, save_posts, update_fanpage_metadata
    from rss import generate_all_rss, generate_fanpage_rss

    init_db()
    content = (post_data.get("content") or "").strip()
    media_urls = post_data.get("media_urls") or []
    post_id = str(post_data.get("post_id") or "").strip()
    post_url = str(post_data.get("post_url") or "").strip()
    published_at = post_data.get("published_at")
    pub_iso = published_at.isoformat() if isinstance(published_at, datetime) else datetime.now(LOCAL_TIMEZONE).isoformat()
    fanpage_url = post_data.get("fanpage_url")
    fanpage_name = post_data.get("fanpage_name")

    if not content and not media_urls:
        return {
            "status": "error",
            "is_event": False,
            "fanpage_added": False,
            "fanpage": None,
            "fanpage_url": fanpage_url,
            "event": None,
            "detail": "Could not extract any text or media from that Facebook post URL.",
        }

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    source_label = fanpage_name or fanpage_url or "Facebook Post"
    raw_data: Optional[Dict[str, Any]] = None
    if api_key and content:
        try:
            raw_data = call_gemini_text(
                content=content,
                published_at_str=pub_iso,
                source_name=source_label,
                api_key=api_key,
            )
        except Exception as exc:
            print(f"[events] Gemini error for single post {post_id}, falling back to heuristic: {exc}")

    if raw_data is None:
        raw_data = heuristic_extract_event(
            content=content,
            sender=fanpage_name,
            published_at=published_at if isinstance(published_at, datetime) else None,
        )
    else:
        raw_data = _enrich_extracted_event_fields(
            raw_data,
            content,
            published_at=published_at if isinstance(published_at, datetime) else None,
        )

    is_event = bool(raw_data.pop("is_event", False))
    post_summary = {
        "post_id": post_id,
        "post_url": post_url,
        "content": content,
        "media_urls": media_urls,
        "published_at": pub_iso,
    }

    if not is_event:
        return {
            "status": "not_event",
            "is_event": False,
            "fanpage_added": False,
            "fanpage": None,
            "fanpage_url": fanpage_url,
            "event": None,
            "post": post_summary,
            "detail": "Post fetched and analyzed, but it does not announce an upcoming/ongoing event.",
        }

    fp: Optional[Fanpage] = None
    fp_created = False
    if fanpage_url:
        try:
            fp, fp_created = add_fanpage(fanpage_url, name=fanpage_name)
            resolved_name = update_fanpage_metadata(fp.id, name=fanpage_name)
            if resolved_name:
                fp.name = resolved_name
            save_posts(fp.id, [post_data])
        except ValueError as exc:
            print(f"[events] Could not add derived fanpage '{fanpage_url}': {exc}")
            fp = None
            fp_created = False

    with get_session() as session:
        upsert_status, ev_record, changes = upsert_unified_event(
            session=session,
            raw_data=raw_data,
            source_type="facebook",
            source_id=post_id,
            media_urls=media_urls,
            fanpage_id=fp.id if fp else None,
            client_id=None,
            post_url=post_url,
            posted_at=pub_iso,
        )
        raw_data["dedup_status"] = upsert_status
        if changes:
            raw_data["updated_changes"] = changes

        if fp is not None:
            existing_pa = session.scalar(select(PostAnalysis).where(PostAnalysis.post_id == post_id))
            if existing_pa:
                existing_pa.is_event = True
                existing_pa.event_json = json.dumps(raw_data, ensure_ascii=False)
            else:
                session.add(
                    PostAnalysis(
                        post_id=post_id,
                        fanpage_id=fp.id,
                        is_event=True,
                        event_json=json.dumps(raw_data, ensure_ascii=False),
                    )
                )

        session.commit()
        if fp is not None:
            _write_events_json(fp, session)
        sync_all_event_folders(session)
        ev_dict = ev_record.to_dict()

    if fp is not None:
        generate_fanpage_rss(fp)
    generate_all_rss()

    return {
        "status": upsert_status,
        "is_event": True,
        "fanpage_added": fp_created,
        "fanpage": fp.to_dict() if fp else None,
        "fanpage_url": fp.url if fp else fanpage_url,
        "changes": changes,
        "event": ev_dict,
        "post": post_summary,
    }


async def extract_single_post_url(post_url: str) -> Dict[str, Any]:
    """
    Scrape a single Facebook post URL, feed it to the Event Extractor, and if it is an event,
    extract the fanpage link (if able) and add it to the tracked fanpages list if not already tracked.
    """
    import asyncio
    from scraper import scrape_single_post

    post_data = await scrape_single_post(post_url)
    return await asyncio.to_thread(_process_single_post_extraction, post_data)
