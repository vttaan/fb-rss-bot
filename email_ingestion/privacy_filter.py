import os
import re
import json
import hashlib
from html import unescape
from datetime import datetime
from email.utils import parseaddr, parsedate_to_datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "filter_config.json")


def load_filter_config(config_path=CONFIG_PATH):
    """Load allowed senders, positive DRL keywords, and privacy exclusion keywords."""
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "allowed_senders": ["hcmus.edu.vn", "fit.hcmus.edu.vn", "clc.hcmus.edu.vn"],
        "keywords": ["điểm rèn luyện", "ĐRL", "workshop", "hội thảo", "tình nguyện"],
        "excluded_keywords": ["mật khẩu", "password", "otp", "học phí", "bảng điểm cá nhân"],
        "newer_than_days": 14,
        "max_results": 20,
        "ingest_endpoint": "http://localhost:8000/api/events/ingest-email",
    }


def strip_html(html_text):
    """Convert HTML email body into clean plain text for keyword inspection and transmission."""
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html_text or "", flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>|</div>|</li>|</tr>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


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


def find_matched_keywords(text, keywords):
    """Return list of keywords found in text (case-insensitive)."""
    if not keywords:
        return []
    lowered = (text or "").lower()
    return [kw for kw in keywords if kw.lower() in lowered]


def passes_privacy_and_keyword_gate(from_addr, subject, body_text, config=None):
    """
    3-Stage Local Privacy Gate before sending an email to the server:
      1. Sender must match `allowed_senders`.
      2. Subject + Body must NOT contain any `excluded_keywords` (e.g. OTP, password, tuition, personal grades).
      3. Subject + Body MUST contain at least one positive event/ĐRL keyword from `keywords`.

    Returns: (passed: bool, reason: str, matched_keywords: list[str])
    """
    if config is None:
        config = load_filter_config()

    allowed_senders = config.get("allowed_senders", [])
    keywords = config.get("keywords", [])
    excluded_keywords = config.get("excluded_keywords", [])

    # Stage 1: Sender check
    if not is_sender_allowed(from_addr, allowed_senders):
        return False, "sender_not_allowed", []

    combined_text = f"{subject or ''}\n{body_text or ''}"

    # Stage 2: Sensitive personal email exclusion check
    excluded_hits = find_matched_keywords(combined_text, excluded_keywords)
    if excluded_hits:
        return False, f"excluded_private_keyword:{excluded_hits[0]}", []

    # Stage 3: Positive DRL / Event keyword check
    matched_kw = find_matched_keywords(combined_text, keywords)
    if not matched_kw:
        return False, "no_drl_keyword_match", []

    return True, "passed", matched_kw


def compute_content_hash(from_addr, subject, body_text, word_limit=30):
    """
    Deterministic Hash Design: sender + first 30 words.
    Extracts the normalized sender email and combines it with the first 30 words of
    (subject + body). This allows the server's deduplication engine to quickly check
    if the event has already been recorded BEFORE invoking the LLM / QR decoder.
    """
    email_addr = extract_email_address(from_addr)
    combined = f"{subject or ''} {body_text or ''}"
    words = re.findall(r"\S+", combined.lower())
    first_words = " ".join(words[:word_limit])
    raw_key = f"{email_addr}|{first_words}"
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


def format_for_server(source, msg_id, from_str, subject, date_str, body_text, matched_keywords):
    """Package a verified email into the standard payload for POST /api/inbox/batch."""
    content_hash, hash_source = compute_content_hash(from_str, subject, body_text, word_limit=30)
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
    }


# Backwards compatibility alias
format_for_llm_server = format_for_server


import requests

BASELINE_EXCLUDED_KEYWORDS = [
    "mật khẩu",
    "password",
    "otp",
    "mã xác nhận",
    "học phí",
    "biên lai",
    "bảng điểm cá nhân",
    "kết quả học tập",
    "cảnh báo học vụ",
    "kỷ luật",
]


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

        # Merge keywords
        remote_kws = data.get("keywords", [])
        user_kws = local_cfg.get("user_added_keywords", [])
        final_kws = sorted(set(remote_kws + user_kws))

        # Enforce baseline floor for excluded_keywords
        remote_excl = data.get("excluded_keywords", [])
        user_excl = local_cfg.get("user_excluded_keywords", [])
        final_excl = sorted(set(BASELINE_EXCLUDED_KEYWORDS + remote_excl + user_excl))

        # Update local config structure
        local_cfg["version"] = data.get("version", current_ver + 1)
        local_cfg["updated_at"] = data.get("updated_at")
        local_cfg["allowed_senders"] = final_senders
        local_cfg["keywords"] = final_kws
        local_cfg["excluded_keywords"] = final_excl
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
