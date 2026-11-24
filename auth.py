"""Session authentication & password hashing utilities for client users and server admin."""
import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import Header, HTTPException, Request, Response

from config import BASE_DIR
from database import User, get_user_by_id

USER_SESSION_COOKIE = "rss_user_session"
ADMIN_SESSION_COOKIE = "rss_admin_session"
DEFAULT_SESSION_TTL_SECONDS = 7 * 24 * 3600
_SECRET_FILE = BASE_DIR / ".session_secret"
_CACHED_SECRET: Optional[bytes] = None


def _get_session_secret() -> bytes:
    global _CACHED_SECRET
    env_secret = os.environ.get("SESSION_SECRET", "").strip() or os.environ.get("ADMIN_TOKEN", "").strip()
    if env_secret:
        return hashlib.sha256(env_secret.encode("utf-8")).digest()
    if _CACHED_SECRET is not None:
        return _CACHED_SECRET
    try:
        if _SECRET_FILE.is_file():
            raw = _SECRET_FILE.read_text(encoding="utf-8").strip()
            if raw:
                _CACHED_SECRET = raw.encode("utf-8")
                return _CACHED_SECRET
        generated = secrets.token_hex(32)
        _SECRET_FILE.write_text(generated, encoding="utf-8")
        _CACHED_SECRET = generated.encode("utf-8")
        return _CACHED_SECRET
    except Exception:
        _CACHED_SECRET = secrets.token_bytes(32)
        return _CACHED_SECRET


def hash_password(password: str) -> str:
    """Hash a plaintext password using PBKDF2-HMAC-SHA256 with a random 16-byte salt."""
    if not password:
        raise ValueError("Password cannot be empty.")
    salt = secrets.token_hex(16)
    iterations = 200_000
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations)
    return f"pbkdf2_sha256${iterations}${salt}${dk.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify a plaintext password against a stored PBKDF2-HMAC-SHA256 hash."""
    if not password or not stored_hash:
        return False
    try:
        algo, iter_str, salt, hex_hash = stored_hash.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), int(iter_str))
        return hmac.compare_digest(dk.hex(), hex_hash)
    except Exception:
        return False


def create_session_token(payload: Dict[str, Any], ttl_seconds: int = DEFAULT_SESSION_TTL_SECONDS) -> str:
    data = {**payload, "exp": int(time.time()) + ttl_seconds}
    raw_json = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
    b64 = base64.urlsafe_b64encode(raw_json).decode("ascii").rstrip("=")
    sig = hmac.new(_get_session_secret(), b64.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"


def verify_session_token(token: Optional[str]) -> Optional[Dict[str, Any]]:
    if not token or "." not in token:
        return None
    try:
        b64, sig = token.rsplit(".", 1)
        expected_sig = hmac.new(_get_session_secret(), b64.encode("ascii"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return None
        padded = b64 + "=" * (-len(b64) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
        if int(data.get("exp", 0)) < int(time.time()):
            return None
        return data
    except Exception:
        return None


def set_session_cookie(response: Response, cookie_name: str, token: str, ttl_seconds: int = DEFAULT_SESSION_TTL_SECONDS) -> None:
    response.set_cookie(
        key=cookie_name,
        value=token,
        max_age=ttl_seconds,
        httponly=True,
        samesite="lax",
        path="/",
    )


def clear_session_cookie(response: Response, cookie_name: str) -> None:
    response.delete_cookie(key=cookie_name, path="/")


def get_current_user_optional(request: Request) -> Optional[User]:
    token = request.cookies.get(USER_SESSION_COOKIE)
    data = verify_session_token(token)
    if not data or data.get("role") != "user":
        return None
    user_id = data.get("user_id")
    if not user_id:
        return None
    return get_user_by_id(str(user_id))


def require_current_user(request: Request) -> User:
    user = get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required. Please log in.")
    return user


def is_admin_authenticated(request: Request, x_admin_token: str = "") -> bool:
    expected = os.environ.get("ADMIN_TOKEN", "")
    if not expected:
        return False
    if x_admin_token and hmac.compare_digest(x_admin_token.encode("utf-8"), expected.encode("utf-8")):
        return True
    token = request.cookies.get(ADMIN_SESSION_COOKIE)
    data = verify_session_token(token)
    return bool(data and data.get("role") == "admin")


def require_admin_session(request: Request, x_admin_token: str = Header(default="")) -> None:
    expected = os.environ.get("ADMIN_TOKEN", "")
    if not expected:
        raise HTTPException(status_code=503, detail="Admin is disabled: ADMIN_TOKEN is not configured on the server.")
    if not is_admin_authenticated(request, x_admin_token=x_admin_token):
        raise HTTPException(status_code=401, detail="Invalid admin session or token.")
