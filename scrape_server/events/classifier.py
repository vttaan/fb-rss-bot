"""Event classification module.

Normalizes and enriches extracted event records with structured classification tags:
  - category (string list)
  - format (string list)
  - target_audience (string list)
  - award (string)
  - is_online (bool)

Combines LLM-extracted tags with deterministic Vietnamese/English rule-based matching
and can reclassify existing EventRecord rows in SQLite on demand.
"""
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select

from database import EventRecord, get_session, init_db, local_now_iso

ALLOWED_CATEGORIES = [
    "workshop",
    "seminar",
    "hội nghị",
    "ca nhạc",
    "học thuật",
    "cộng đồng",
    "ngày hội",
    "pháp luật",
    "khoa học",
    "cuộc thi",
    "tọa đàm",
    "triển lãm",
    "thể thao",
    "hướng nghiệp",
    "thiện nguyện",
    "quyên góp",
    "khác",
]

ALLOWED_FORMATS = [
    "offline",
    "online",
    "cá nhân",
    "đồng đội",
    "chạy trạm",
    "thi",
    "khảo sát",
]

CATEGORY_RULES: List[Tuple[str, re.Pattern]] = [
    ("workshop", re.compile(r"\b(workshop|hands-on|thực hành|bootcamp)\b", re.IGNORECASE)),
    ("seminar", re.compile(r"\b(seminar|webinar|chuyên đề)\b", re.IGNORECASE)),
    ("hội nghị", re.compile(r"\b(hội nghị|conference|symposium)\b", re.IGNORECASE)),
    ("tọa đàm", re.compile(r"\b(tọa đàm|toạ đàm|talkshow|talk show|giao lưu|chia sẻ|panel)\b", re.IGNORECASE)),
    ("cuộc thi", re.compile(r"\b(cuộc thi|hackathon|olympic|olympiad|giải đấu|tranh tài|thử thách|challenge|contest|minigame|quiz)\b", re.IGNORECASE)),
    ("học thuật", re.compile(r"\b(học thuật|academic|lập trình|algorithm|ai|machine learning|data|toán|tin học|vật lý|hóa học|sinh học|nghiên cứu)\b", re.IGNORECASE)),
    ("khoa học", re.compile(r"\b(khoa học|science|stem|nghiên cứu khoa học|nckh|sáng tạo)\b", re.IGNORECASE)),
    ("hướng nghiệp", re.compile(r"\b(hướng nghiệp|career|tuyển dụng|việc làm|cv|phỏng vấn|internship|thực tập|job fair|ngày hội việc làm)\b", re.IGNORECASE)),
    ("ngày hội", re.compile(r"\b(ngày hội|festival|fest|fair|hội trại|trại)\b", re.IGNORECASE)),
    ("ca nhạc", re.compile(r"\b(ca nhạc|âm nhạc|concert|văn nghệ|acoustic|music|hội diễn)\b", re.IGNORECASE)),
    ("thể thao", re.compile(r"\b(thể thao|bóng đá|bóng chuyền|cầu lông|chạy bộ|marathon|esports|cờ vua|kéo co|hội thao)\b", re.IGNORECASE)),
    ("thiện nguyện", re.compile(r"\b(thiện nguyện|tình nguyện|mùa hè xanh|xuân tình nguyện|chủ nhật xanh|hiến máu)\b", re.IGNORECASE)),
    ("quyên góp", re.compile(r"\b(quyên góp|gây quỹ|ủ?ng hộ|fundraising|donate)\b", re.IGNORECASE)),
    ("pháp luật", re.compile(r"\b(pháp luật|hiến pháp|luật|an toàn giao thông|tuyên truyền pháp luật)\b", re.IGNORECASE)),
    ("triển lãm", re.compile(r"\b(triển lãm|exhibition|showcase|trưng bày)\b", re.IGNORECASE)),
    ("cộng đồng", re.compile(r"\b(cộng đồng|sinh viên|đoàn|hội|clb|câu lạc bộ|kỹ năng|tuyển thành viên|tuyển ctv)\b", re.IGNORECASE)),
]

FORMAT_RULES: List[Tuple[str, re.Pattern]] = [
    ("online", re.compile(r"\b(online|trực tuyến|zoom|google meet|ms teams|microsoft teams|livestream|myaloha|webinar)\b", re.IGNORECASE)),
    ("offline", re.compile(r"\b(offline|trực tiếp|hội trường|giảng đường|cơ sở|phòng|sân|khuôn viên|campus|nhà văn hóa)\b", re.IGNORECASE)),
    ("đồng đội", re.compile(r"\b(đồng đội|nhóm|đội thi|team|theo đội|3\s*-\s*5\s*thành viên)\b", re.IGNORECASE)),
    ("cá nhân", re.compile(r"\b(cá nhân|individual|tự do|mỗi thí sinh)\b", re.IGNORECASE)),
    ("chạy trạm", re.compile(r"\b(chạy trạm|vượt trạm|teambuilding)\b", re.IGNORECASE)),
    ("thi", re.compile(r"\b(trắc nghiệm|vòng thi|bài thi|thi đấu|cuộc thi|làm bài|quiz|hackathon)\b", re.IGNORECASE)),
    ("khảo sát", re.compile(r"\b(khảo sát|survey|lấy ý kiến)\b", re.IGNORECASE)),
]

AUDIENCE_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("sinh viên CNTT", re.compile(r"\b(sinh viên\s+(?:khoa\s+)?(?:cntt|công nghệ thông tin|fit)|sv\s+cntt)\b", re.IGNORECASE)),
    ("sinh viên", re.compile(r"\b(sinh viên|sv|học sinh\s*[-–/]\s*sinh viên|hssv|tân sinh viên)\b", re.IGNORECASE)),
    ("giảng viên", re.compile(r"\b(giảng viên|thầy cô|cán bộ giảng dạy)\b", re.IGNORECASE)),
    ("nghiên cứu sinh", re.compile(r"\b(nghiên cứu sinh|học viên cao học|thạc sĩ)\b", re.IGNORECASE)),
    ("doanh nghiệp", re.compile(r"\b(doanh nghiệp|công ty|nhà tuyển dụng)\b", re.IGNORECASE)),
    ("cộng đồng", re.compile(r"\b(cộng đồng|tất cả mọi người|công chúng)\b", re.IGNORECASE)),
]

AWARD_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("Điểm rèn luyện", re.compile(r"\b(điểm rèn luyện|đrl|drl|cộng điểm)\b", re.IGNORECASE)),
    ("Giấy chứng nhận", re.compile(r"\b(giấy chứng nhận|chứng nhận|certificate|e-?certificate)\b", re.IGNORECASE)),
    ("Danh hiệu SV5T", re.compile(r"\b(sinh viên 5 tốt|sv5t)\b", re.IGNORECASE)),
    ("Giải thưởng / Học bổng", re.compile(r"\b(giải thưởng|học bổng|tiền thưởng|hiện kim|quà tặng|phần quà|giải nhất|vinh danh)\b", re.IGNORECASE)),
]


def _dedupe_preserve(items: List[str], limit: int = 4) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        clean = str(item or "").strip()
        if not clean:
            continue
        key = clean.lower()
        if key not in seen:
            seen.add(key)
            out.append(clean)
            if len(out) >= limit:
                break
    return out


def _normalize_list(raw: Any) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    if isinstance(raw, str):
        return [s.strip() for s in re.split(r"[,;]+", raw) if s.strip()]
    return []


def classify_event(event_data: Dict[str, Any], raw_text: str = "") -> Dict[str, Any]:
    """
    Classify an event dictionary into normalized `category`, `format`, `target_audience`,
    `award`, and `is_online` fields.
    """
    title = str(event_data.get("title") or "")
    description = str(event_data.get("description") or "")
    location = str(event_data.get("location") or "")
    combined_text = "\n".join(p for p in [title, description, location, raw_text] if p)

    # 1. Category
    existing_cats = [c.lower() for c in _normalize_list(event_data.get("category"))]
    cats = [c for c in existing_cats if c in ALLOWED_CATEGORIES]
    for label, pattern in CATEGORY_RULES:
        if pattern.search(combined_text) and label not in cats:
            cats.append(label)
    if not cats:
        cats = ["khác"]
    cats = _dedupe_preserve(cats, limit=4)

    # 2. Format & is_online
    existing_fmts = [f.lower() for f in _normalize_list(event_data.get("format"))]
    fmts = [f for f in existing_fmts if f in ALLOWED_FORMATS]
    for label, pattern in FORMAT_RULES:
        if pattern.search(combined_text) and label not in fmts:
            fmts.append(label)

    raw_is_online = event_data.get("is_online")
    online_mode = str(event_data.get("online_mode") or "").lower()
    if isinstance(raw_is_online, bool):
        is_online = raw_is_online
    elif online_mode == "online":
        is_online = True
    elif online_mode == "offline":
        is_online = False
    else:
        is_online = ("online" in fmts) and ("offline" not in fmts)

    if is_online and "online" not in fmts:
        fmts.insert(0, "online")
    elif not is_online and "offline" not in fmts and online_mode != "hybrid":
        fmts.insert(0, "offline")
    fmts = _dedupe_preserve(fmts, limit=4)

    # 3. Target audience
    aud = _normalize_list(event_data.get("target_audience"))
    for label, pattern in AUDIENCE_PATTERNS:
        if pattern.search(combined_text):
            if not any(label.lower() == a.lower() for a in aud):
                aud.append(label)
    # Detect specific cohort tags like K23, K24, K25, K26
    for m in re.findall(r"\bK2[0-9]\b", combined_text, flags=re.IGNORECASE):
        tag = f"sinh viên {m.upper()}"
        if not any(tag.lower() == a.lower() for a in aud):
            aud.append(tag)
    if not aud:
        aud = ["sinh viên"]
    aud = _dedupe_preserve(aud, limit=5)

    # 4. Award
    raw_award = str(event_data.get("award") or "").strip()
    award_tags: List[str] = [raw_award] if raw_award else []
    for label, pattern in AWARD_PATTERNS:
        if pattern.search(combined_text):
            if not any(label.lower() in t.lower() for t in award_tags):
                award_tags.append(label)
    award = " · ".join(_dedupe_preserve(award_tags, limit=4))

    return {
        "category": cats,
        "format": fmts,
        "target_audience": aud,
        "award": award,
        "is_online": bool(is_online),
    }


def reclassify_all_events(event_ids: Optional[List[int]] = None) -> Dict[str, Any]:
    """Run the classification module across EventRecord rows in SQLite."""
    init_db()
    updated_count = 0
    total_scanned = 0
    now_iso = local_now_iso()

    with get_session() as session:
        stmt = select(EventRecord).order_by(EventRecord.event_id.asc())
        if event_ids:
            stmt = stmt.where(EventRecord.event_id.in_(event_ids))
        rows = list(session.scalars(stmt).all())
        for ev in rows:
            total_scanned += 1
            data = ev.to_dict()
            classified = classify_event(data)
            changed = (
                ev.category != classified["category"]
                or ev.format != classified["format"]
                or ev.target_audience != classified["target_audience"]
                or ev.award != classified["award"]
                or bool(ev.is_online) != classified["is_online"]
            )
            ev.category = classified["category"]
            ev.format = classified["format"]
            ev.target_audience = classified["target_audience"]
            ev.award = classified["award"]
            ev.is_online = classified["is_online"]

            try:
                extra = json.loads(ev.event_json or "{}")
            except Exception:
                extra = {}
            extra.update(classified)
            ev.event_json = json.dumps(extra, ensure_ascii=False)
            if changed:
                ev.updated_at = now_iso
                updated_count += 1
        session.commit()

    return {
        "scanned": total_scanned,
        "updated": updated_count,
    }
