import os
import re
import json
import hashlib
import unicodedata
from html import unescape
from datetime import datetime
from email.utils import parseaddr, parsedate_to_datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "filter_config.json")


def load_filter_config(config_path=CONFIG_PATH):
    """Load allowed senders, positive DRL/event keywords, and privacy/non-event exclusions."""
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "allowed_senders": ["hcmus.edu.vn", "fit.hcmus.edu.vn", "clc.hcmus.edu.vn"],
        "keywords": [
            "địa điểm", "đăng ký tham gia", "link đăng ký", "cuộc thi",
            "giao lưu", "điểm rèn luyện", "ĐRL", "hạn đăng ký",
            "workshop", "ngày hội", "diễn giả", "chính thức mở",
            "hội thảo", "seminar", "talkshow", "tình nguyện", "chiến dịch",
        ],
        "excluded_keywords": list(BASELINE_EXCLUDED_KEYWORDS),
        "non_event_keywords": list(BASELINE_NON_EVENT_KEYWORDS),
        "active_registration_cues": list(BASELINE_ACTIVE_REGISTRATION_CUES),
        "newer_than_days": 14,
        "max_results": 20,
        "ingest_endpoint": "http://localhost:8000/api/events/ingest-email",
    }


def _replace_anchor(match):
    url = unescape(match.group(2)).strip()
    label = unescape(re.sub(r"<[^>]+>", " ", match.group(3) or "")).strip()
    label = re.sub(r"\s+", " ", label)
    if not label:
        return url
    if url.lower().rstrip("/") in label.lower().rstrip("/"):
        return label
    return f"{label} ({url})"


def strip_html(html_text):
    """Convert HTML email body into clean plain text, preserving hyperlinks (<a href>)."""
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html_text or "", flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(
        r'<a\b[^>]*?\bhref\s*=\s*(["\'])(https?://[^"\'>\s]+)\1[^>]*>(.*?)</a>',
        _replace_anchor,
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>|</div>|</li>|</tr>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def extract_html_media_urls(html_text):
    """Extract candidate poster/attachment image URLs (<img src="https://...">) from HTML email body."""
    if not html_text:
        return []
    cleaned = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html_text, flags=re.DOTALL | re.IGNORECASE)
    urls = []
    seen = set()
    for match in re.finditer(
        r'<img\b[^>]*?\bsrc\s*=\s*(["\'])(https?://[^"\'>\s]+)\1[^>]*>',
        cleaned,
        flags=re.DOTALL | re.IGNORECASE,
    ):
        tag = match.group(0)
        url = unescape(match.group(2)).strip()
        if not url or url in seen:
            continue
        # Skip 0px/1px tracking pixels
        if re.search(r'\b(?:width|height)\s*=\s*["\']?[01](?:px)?["\']?(?:\s|>|/)', tag, flags=re.IGNORECASE):
            continue
        lower_url = url.lower()
        if any(
            noise in lower_url
            for noise in ("pixel", "beacon", "spacer", "1x1", "emoji", "rsrc.php", ".gif", ".svg", ".ico")
        ):
            continue
        seen.add(url)
        urls.append(url)
    return urls


def extract_email_address(from_header_or_addr):
    """Extract clean lowercase email address from 'Name <user@domain>' or 'user@domain'."""
    _, addr = parseaddr(from_header_or_addr or "")
    return (addr or from_header_or_addr or "").lower().strip()


def is_sender_allowed(from_header_or_addr, allowed_senders):
    """
    Gate 1 (Sender Allowlist):
    Only allow emails from official HCMUS domains or explicitly whitelisted addresses.
    """
    if not allowed_senders:
        return True
    email_addr = extract_email_address(from_header_or_addr)
    if not email_addr or "@" not in email_addr:
        return False

    for allowed in allowed_senders:
        allowed = allowed.lower().strip()
        if not allowed:
            continue
        if "@" in allowed:
            if email_addr == allowed:
                return True
        else:
            domain = email_addr.split("@", 1)[1]
            if domain == allowed or domain.endswith("." + allowed):
                return True
    return False


def _normalize_nfc(text):
    """Normalize text to Unicode NFC so decomposed (NFD) and precomposed (NFC) Vietnamese match."""
    return unicodedata.normalize("NFC", text or "")


def find_matched_keywords(text, keywords):
    """Return list of keywords found in text (case-insensitive, NFC-normalized, word-boundary matched)."""
    if not keywords or not text:
        return []
    norm_text = _normalize_nfc(text).lower()
    matched = []
    for kw in keywords:
        if not kw or not str(kw).strip():
            continue
        kw_norm = _normalize_nfc(str(kw)).strip().lower()
        if not kw_norm:
            continue
        pattern = r"(?<!\w)" + re.escape(kw_norm) + r"(?!\w)"
        if re.search(pattern, norm_text, flags=re.UNICODE):
            matched.append(kw)
    return matched


def passes_privacy_and_keyword_gate(from_addr, subject, body_text, config=None):
    """
    4-Step Local Privacy & Empirical Relevance Gate before sending an email to the server:
      1. Sender must match `allowed_senders`.
      2a. Hard Privacy Exclusion (NEVER overridden): Subject + Body must NOT contain `excluded_keywords`
          (OTP, password, tuition bills, personal grades, academic warnings).
      2b. Empirical Non-Event Disqualifier (with Active-Registration Override): Rejects recaps,
          congratulations, honor rolls, and internal admin/dorm notices UNLESS an active
          registration CTA (`active_registration_cues`) is also present.
      3. Positive Relevance: Subject + Body MUST match at least one `keywords` term.

    Returns: (passed: bool, reason: str, matched_keywords: list[str])
    """
    if config is None:
        config = load_filter_config()

    allowed_senders = config.get("allowed_senders", [])
    keywords = config.get("keywords", [])
    excluded_keywords = config.get("excluded_keywords", [])
    non_event_keywords = config.get("non_event_keywords", BASELINE_NON_EVENT_KEYWORDS)
    active_cues = config.get("active_registration_cues", BASELINE_ACTIVE_REGISTRATION_CUES)

    # Stage 1: Sender check
    if not is_sender_allowed(from_addr, allowed_senders):
        return False, "sender_not_allowed", []

    combined_text = f"{subject or ''}\n{body_text or ''}"

    # Stage 2a: Hard privacy exclusion check (never overridden)
    excluded_hits = find_matched_keywords(combined_text, excluded_keywords)
    if excluded_hits:
        return False, f"excluded_private_keyword:{excluded_hits[0]}", []

    # Stage 2b: Empirical non-event check (overridden only if active registration CTA is present)
    if non_event_keywords:
        non_event_hits = find_matched_keywords(combined_text, non_event_keywords)
        if non_event_hits:
            override_hits = find_matched_keywords(combined_text, active_cues) if active_cues else []
            if not override_hits:
                return False, f"excluded_non_event_signal:{non_event_hits[0]}", []

    # Stage 3: Positive DRL / Event keyword check
    matched_kw = find_matched_keywords(combined_text, keywords)
    if not matched_kw:
        return False, "no_drl_keyword_match", []

    return True, "passed", matched_kw


def compute_content_hash(from_addr, subject, body_text, word_limit=None):
    """
    Deterministic Hash Design: sender + normalized subject + cleaned body.
    Hashes the full cleaned body by default (so follow-up/reschedule emails with the
    same opening paragraph do not collide), while supporting an optional word_limit slice.
    """
    email_addr = extract_email_address(from_addr)
    norm_subj = " ".join(re.findall(r"\S+", _normalize_nfc(subject or "").lower()))
    body_words = re.findall(r"\S+", _normalize_nfc(body_text or "").lower())
    if word_limit is not None:
        body_words = body_words[:word_limit]
    norm_body = " ".join(body_words)
    raw_key = f"{email_addr}|{norm_subj}|{norm_body}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest(), raw_key


def normalize_date_to_iso(date_str):
    """Normalize RFC 2822 (Gmail) or ISO 8601 (Outlook) date strings into ISO 8601."""
    if not date_str:
        return ""
    raw = str(date_str).strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.isoformat()
    except Exception:
        pass
    try:
        dt = parsedate_to_datetime(raw)
        return dt.isoformat()
    except Exception:
        return raw


def format_for_server(source, msg_id, from_str, subject, date_str, body_text, matched_keywords, media_urls=None):
    """Package a verified email into the standard payload for POST /api/inbox/batch."""
    content_hash, hash_source = compute_content_hash(from_str, subject, body_text)
    return {
        "source": source,
        "message_id": msg_id,
        "content_hash": content_hash,
        "hash_source": hash_source,
        "from": from_str,
        "subject": subject or "",
        "date": normalize_date_to_iso(date_str),
        "matched_keywords": matched_keywords,
        "text": body_text or "",
        "media_urls": list(media_urls or []),
    }


# Backwards compatibility alias
format_for_llm_server = format_for_server


import requests

BASELINE_EXCLUDED_KEYWORDS = [
    "mật khẩu",
    "password",
    "otp",
    "mã xác nhận",
    "đóng học phí",
    "nợ học phí",
    "thanh toán học phí",
    "tiền học phí",
    "thông báo học phí",
    "học phí học kỳ",
    "miễn giảm học phí",
    "biên lai",
    "bảng điểm cá nhân",
    "kết quả học tập",
    "cảnh báo học vụ",
    "kỷ luật",
]

BASELINE_NON_EVENT_KEYWORDS = [
    "xin chúc mừng",
    "chúc mừng",
    "recap",
    "tổng kết",
    "vinh danh",
    "tri ân ban chấp hành",
    "đã chính thức khép lại",
    "hành trình khép lại",
    "gửi lời cảm ơn",
    "hội nghị mở rộng",
    "đăng ký ở ký túc xá",
    "lễ trao bằng tốt nghiệp",
    "kết quả tuyển dụng",
    "diễn ra thành công",
    "các diễn giả đã mang đến",
]

BASELINE_ACTIVE_REGISTRATION_CUES = [
    "mở đơn",
    "link đăng ký",
    "form đăng ký",
    "đăng ký tham gia",
    "đăng ký tham dự",
    "mở cổng đăng ký",
    "forms.gle",
    "chính thức mở",
]


def _merge_ordered_unique(*lists):
    """Deduplicate strings while preserving priority order (critical for Outlook's top-12 slice)."""
    seen = set()
    merged = []
    for seq in lists:
        for item in seq or []:
            val = str(item).strip()
            if not val:
                continue
            key = _normalize_nfc(val).lower()
            if key not in seen:
                seen.add(key)
                merged.append(val)
    return merged


def sync_filter_config_from_server(config_path=CONFIG_PATH, timeout=5):
    """
    Pull latest filter_config.json from scrape_server before running email queries.
    Enforces privacy guardrails:
      - Preserves local credentials, client_id, ingest_api_key, and ingest_endpoint.
      - Preserves user_added_senders, user_added_keywords, and user_excluded_keywords.
      - Enforces a baseline floor for excluded_keywords.
      - Drops wildcard or public webmail domains from allowed_senders.
    """
    local_cfg = load_filter_config(config_path)
    if not local_cfg.get("auto_sync_config", True):
        return local_cfg

    endpoint = local_cfg.get("ingest_endpoint", "")
    if not endpoint:
        return local_cfg

    # Derive GET /api/inbox/filter-config from endpoint
    base_url = None
    for suffix in ("/api/inbox/batch", "/api/inbox", "/api/events/ingest-email"):
        if endpoint.endswith(suffix):
            base_url = endpoint[:-len(suffix)]
            break
    if not base_url:
        return local_cfg

    config_url = f"{base_url}/api/inbox/filter-config"
    current_ver = local_cfg.get("version", 0)
    params = {"client_version": current_ver}
    headers = {}
    token = local_cfg.get("ingest_api_key", "")
    if token:
        headers["X-Ingest-Token"] = token

    try:
        res = requests.get(config_url, params=params, headers=headers, timeout=timeout)
        if res.status_code == 304:
            return local_cfg
        if not res.ok:
            return local_cfg

        data = res.json()
        if data.get("up_to_date"):
            return local_cfg

        # Privacy guardrails: merge server updates safely
        remote_senders = data.get("allowed_senders", [])
        valid_senders = []
        for s in remote_senders:
            clean = s.strip().lower()
            if not clean or clean in ("*", ".", "gmail.com", "yahoo.com", "outlook.com", "hotmail.com"):
                continue
            valid_senders.append(clean)

        # Merge with user-added custom senders
        user_senders = [s.strip().lower() for s in local_cfg.get("user_added_senders", []) if s.strip()]
        final_senders = sorted(set(valid_senders + user_senders))

        # Merge keywords preserving priority order (Outlook $search uses keywords[:12])
        remote_kws = data.get("keywords", [])
        user_kws = local_cfg.get("user_added_keywords", [])
        final_kws = _merge_ordered_unique(remote_kws, user_kws)

        # Enforce baseline floor for hard privacy excluded_keywords
        remote_excl = data.get("excluded_keywords", [])
        user_excl = local_cfg.get("user_excluded_keywords", [])
        final_excl = _merge_ordered_unique(BASELINE_EXCLUDED_KEYWORDS, remote_excl, user_excl)

        # Merge empirical non-event disqualifiers and active-registration override cues.
        # Unlike hard privacy excluded_keywords, baselines are fallback-only when neither
        # server nor local config defines them, so admins can remove/tune terms via config.
        remote_non_event = data.get("non_event_keywords")
        if remote_non_event is not None:
            base_non_event = remote_non_event
        elif "non_event_keywords" in local_cfg:
            base_non_event = local_cfg["non_event_keywords"]
        else:
            base_non_event = BASELINE_NON_EVENT_KEYWORDS
        user_non_event = local_cfg.get("user_excluded_non_event_keywords", [])
        final_non_event = _merge_ordered_unique(base_non_event, user_non_event)

        remote_cues = data.get("active_registration_cues")
        if remote_cues is not None:
            base_cues = remote_cues
        elif "active_registration_cues" in local_cfg:
            base_cues = local_cfg["active_registration_cues"]
        else:
            base_cues = BASELINE_ACTIVE_REGISTRATION_CUES
        final_cues = _merge_ordered_unique(base_cues)

        # Update local config structure
        local_cfg["version"] = data.get("version", current_ver + 1)
        local_cfg["updated_at"] = data.get("updated_at")
        local_cfg["allowed_senders"] = final_senders
        local_cfg["keywords"] = final_kws
        local_cfg["excluded_keywords"] = final_excl
        local_cfg["non_event_keywords"] = final_non_event
        local_cfg["active_registration_cues"] = final_cues
        if "newer_than_days" in data:
            local_cfg["newer_than_days"] = data["newer_than_days"]
        if "max_results" in data:
            local_cfg["max_results"] = data["max_results"]

        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(local_cfg, f, ensure_ascii=False, indent=2)

        print(f"[Config Sync] Updated filter_config.json to version {local_cfg['version']} from server.")
        return local_cfg
    except Exception as exc:
        print(f"[Config Sync] Server config sync skipped ({exc}); using local filter_config.json.")
        return local_cfg
