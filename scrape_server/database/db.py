import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from sqlalchemy import create_engine, select, func, text
from sqlalchemy.orm import Session, sessionmaker, joinedload

from config import DATABASE_URL, LOCAL_TIMEZONE
from .models import (
    Base,
    EmailMessage,
    EventRecord,
    EventSubscription,
    Fanpage,
    Post,
    PostAnalysis,
    User,
    UserSubscription,
    ensure_local_tz,
    local_now_iso,
)

engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
_DB_INITIALIZED = False

_INVALID_FANPAGE_EXACT_NAMES = {
    "facebook",
    "log in",
    "login",
    "log into facebook",
    "log in to facebook",
    "log in or sign up",
    "sign up",
    "sign up for facebook",
    "đăng nhập",
    "đăng nhập facebook",
    "đăng nhập hoặc đăng ký",
    "đăng ký",
    "create new account",
    "watch",
    "notifications",
    "home",
    "unknown",
    "error",
    "page not found",
    "content not found",
    "security check",
    "security check required",
    "checkpoint",
    "just a moment",
    "just a moment...",
    "something went wrong",
    "you're temporarily blocked",
    "bạn tạm thời bị chặn",
    "this content isn't available right now",
    "confirm your identity",
    "meta",
}

_INVALID_FANPAGE_NAME_PATTERNS = re.compile(
    r"(?:"
    r"^log\s*in(?:to)?(?:\s+to)?\s+facebook\b"
    r"|^facebook\s*[-–|]"
    r"|^đăng\s*nhập\b"
    r"|tạm thời bị chặn"
    r"|temporarily blocked"
    r"|security check"
    r"|confirm your identity"
    r"|this content isn't available"
    r"|page isn't available"
    r")",
    re.IGNORECASE,
)


def clean_fanpage_name(raw_name: Optional[str]) -> Optional[str]:
    """Normalize a scraped fanpage title and reject generic Facebook/login/error placeholders."""
    if not raw_name or not isinstance(raw_name, str):
        return None
    text = re.sub(r"\s+", " ", raw_name).strip()
    text = re.sub(r"^\(\d+\)\s*", "", text).strip()
    text = re.sub(r"\s*[\|\-–]\s*Facebook.*$", "", text, flags=re.IGNORECASE).strip()
    if not text or not re.search(r"\w", text, flags=re.UNICODE):
        return None
    lowered = text.lower()
    if lowered in _INVALID_FANPAGE_EXACT_NAMES or _INVALID_FANPAGE_NAME_PATTERNS.search(text):
        return None
    return text


def is_valid_fanpage_name(name: Optional[str]) -> bool:
    """Return True if `name` is a genuine fanpage title rather than a Facebook placeholder."""
    return clean_fanpage_name(name) is not None


def _table_columns(conn, table_name: str) -> List[str]:
    rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
    return [r[1] for r in rows]


def _migrate_sqlite_schema() -> None:
    """Safely migrate legacy SQLite tables (`users`, `user_subscriptions`, `events`) in place."""
    with engine.begin() as conn:
        existing_tables = {
            r[0]
            for r in conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table'")
            ).fetchall()
        }

        # 1. Migrate legacy `users` & `user_subscriptions` if `user_id` is not yet the PK
        if "users" in existing_tables:
            u_cols = _table_columns(conn, "users")
            if "user_id" not in u_cols or "username" not in u_cols or "password" not in u_cols:
                old_users = conn.execute(
                    text("SELECT id, client_id, created_at FROM users")
                ).fetchall()
                old_subs = []
                if "user_subscriptions" in existing_tables:
                    old_subs = conn.execute(
                        text("SELECT user_id, fanpage_id, created_at FROM user_subscriptions")
                    ).fetchall()
                    conn.execute(text("DROP TABLE user_subscriptions"))
                conn.execute(text("DROP TABLE users"))
                conn.execute(
                    text(
                        """
                        CREATE TABLE users (
                            user_id VARCHAR(128) NOT NULL PRIMARY KEY,
                            username VARCHAR(128) NOT NULL UNIQUE,
                            password VARCHAR(255) NOT NULL DEFAULT '',
                            start_year INTEGER NOT NULL DEFAULT 2024,
                            created_at DATETIME
                        )
                        """
                    )
                )
                id_map: Dict[int, str] = {}
                for old_id, client_id, created_at in old_users:
                    new_uid = uuid.uuid4().hex
                    id_map[old_id] = new_uid
                    conn.execute(
                        text(
                            "INSERT INTO users (user_id, username, password, start_year, created_at) "
                            "VALUES (:uid, :uname, '', 2024, :cat)"
                        ),
                        {"uid": new_uid, "uname": client_id, "cat": created_at},
                    )
                conn.execute(
                    text(
                        """
                        CREATE TABLE user_subscriptions (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id VARCHAR(128) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                            fanpage_id INTEGER NOT NULL REFERENCES fanpages(id) ON DELETE CASCADE,
                            created_at DATETIME,
                            CONSTRAINT uq_user_fanpage UNIQUE (user_id, fanpage_id)
                        )
                        """
                    )
                )
                for old_uid, fp_id, created_at in old_subs:
                    new_uid = id_map.get(old_uid)
                    if new_uid:
                        conn.execute(
                            text(
                                "INSERT OR IGNORE INTO user_subscriptions (user_id, fanpage_id, created_at) "
                                "VALUES (:uid, :fpid, :cat)"
                            ),
                            {"uid": new_uid, "fpid": fp_id, "cat": created_at},
                        )

        # 2. Migrate legacy `events` table (id -> event_id + new structured columns)
        if "events" in existing_tables:
            e_cols = _table_columns(conn, "events")
            if "event_id" not in e_cols:
                fanpage_names: Dict[int, str] = {}
                if "fanpages" in existing_tables:
                    for fid, fname, fslug in conn.execute(
                        text("SELECT id, name, slug FROM fanpages")
                    ).fetchall():
                        fanpage_names[fid] = fname or fslug or ""

                old_events = conn.execute(text("SELECT * FROM events")).mappings().all()
                conn.execute(text("ALTER TABLE events RENAME TO events_legacy_backup"))
                conn.execute(
                    text(
                        """
                        CREATE TABLE events (
                            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            title VARCHAR(512) NOT NULL,
                            description TEXT NOT NULL DEFAULT '',
                            organizer VARCHAR(512) NOT NULL DEFAULT '',
                            is_online BOOLEAN NOT NULL DEFAULT 0,
                            event_start_datetime VARCHAR(64),
                            reg_start_datetime VARCHAR(64),
                            reg_end_datetime VARCHAR(64),
                            event_end_datetime VARCHAR(64),
                            location VARCHAR(512) NOT NULL DEFAULT '',
                            source_type VARCHAR(255) NOT NULL DEFAULT 'facebook',
                            created_at VARCHAR(64) NOT NULL,
                            updated_at VARCHAR(64) NOT NULL,
                            fee VARCHAR(255) NOT NULL DEFAULT '',
                            target_audience TEXT NOT NULL DEFAULT '[]',
                            award TEXT NOT NULL DEFAULT '',
                            registration VARCHAR(1024),
                            category TEXT NOT NULL DEFAULT '[]',
                            format TEXT NOT NULL DEFAULT '[]',
                            post_url VARCHAR(1024),
                            normalized_title VARCHAR(512) NOT NULL DEFAULT '',
                            online_mode VARCHAR(32),
                            event_date VARCHAR(32),
                            event_time VARCHAR(32),
                            start_datetime VARCHAR(64),
                            end_datetime VARCHAR(64),
                            normalized_location VARCHAR(512),
                            source_id VARCHAR(255) NOT NULL DEFAULT '',
                            fanpage_id INTEGER REFERENCES fanpages(id) ON DELETE SET NULL,
                            client_id VARCHAR(128),
                            event_json TEXT NOT NULL DEFAULT '{}',
                            version INTEGER NOT NULL DEFAULT 1
                        )
                        """
                    )
                )
                for row in old_events:
                    try:
                        extra = json.loads(row.get("event_json") or "{}")
                    except Exception:
                        extra = {}
                    fp_id = row.get("fanpage_id")
                    raw_st = (row.get("source_type") or "facebook").strip()
                    if raw_st == "facebook" and fp_id and fanpage_names.get(fp_id):
                        src_type = f"facebook: {fanpage_names[fp_id]}"
                    else:
                        src_type = raw_st

                    ev_start = extra.get("event_start_datetime") or row.get("start_datetime")
                    ev_end = extra.get("event_end_datetime") or row.get("end_datetime")
                    reg_start = extra.get("reg_start_datetime")
                    reg_end = extra.get("reg_end_datetime")
                    reg_url = extra.get("registration_url") or extra.get("registration")
                    p_url = extra.get("post_url")
                    cat = extra.get("category") or []
                    fmt = extra.get("format") or []
                    aud = extra.get("target_audience") or []
                    if isinstance(aud, str):
                        aud = [aud] if aud.strip() else []

                    conn.execute(
                        text(
                            """
                            INSERT INTO events (
                                event_id, title, description, organizer, is_online,
                                event_start_datetime, reg_start_datetime, reg_end_datetime, event_end_datetime,
                                location, source_type, created_at, updated_at,
                                fee, target_audience, award, registration, category, format, post_url,
                                normalized_title, online_mode, event_date, event_time,
                                start_datetime, end_datetime, normalized_location,
                                source_id, fanpage_id, client_id, event_json, version
                            ) VALUES (
                                :event_id, :title, :description, :organizer, :is_online,
                                :event_start_datetime, :reg_start_datetime, :reg_end_datetime, :event_end_datetime,
                                :location, :source_type, :created_at, :updated_at,
                                :fee, :target_audience, :award, :registration, :category, :format, :post_url,
                                :normalized_title, :online_mode, :event_date, :event_time,
                                :start_datetime, :end_datetime, :normalized_location,
                                :source_id, :fanpage_id, :client_id, :event_json, :version
                            )
                            """
                        ),
                        {
                            "event_id": row["id"],
                            "title": row["title"],
                            "description": extra.get("description") or "",
                            "organizer": extra.get("organizer") or "",
                            "is_online": 1 if row.get("is_online") else 0,
                            "event_start_datetime": ev_start,
                            "reg_start_datetime": reg_start,
                            "reg_end_datetime": reg_end,
                            "event_end_datetime": ev_end,
                            "location": row.get("location") or "",
                            "source_type": src_type,
                            "created_at": str(row.get("created_at") or local_now_iso()),
                            "updated_at": str(row.get("updated_at") or local_now_iso()),
                            "fee": extra.get("fee") or "",
                            "target_audience": json.dumps(aud, ensure_ascii=False),
                            "award": extra.get("award") or "",
                            "registration": reg_url,
                            "category": json.dumps(cat if isinstance(cat, list) else [], ensure_ascii=False),
                            "format": json.dumps(fmt if isinstance(fmt, list) else [], ensure_ascii=False),
                            "post_url": p_url,
                            "normalized_title": row.get("normalized_title") or "",
                            "online_mode": row.get("online_mode"),
                            "event_date": row.get("event_date"),
                            "event_time": row.get("event_time"),
                            "start_datetime": row.get("start_datetime"),
                            "end_datetime": row.get("end_datetime"),
                            "normalized_location": row.get("normalized_location"),
                            "source_id": row.get("source_id") or "",
                            "fanpage_id": fp_id,
                            "client_id": row.get("client_id"),
                            "event_json": row.get("event_json") or "{}",
                            "version": row.get("version") or 1,
                        },
                    )
                conn.execute(text("DROP TABLE events_legacy_backup"))

        # 3. Ensure `email_messages.content_hash` exists on legacy DBs
        if "email_messages" in existing_tables:
            em_cols = _table_columns(conn, "email_messages")
            if "content_hash" not in em_cols:
                conn.execute(
                    text("ALTER TABLE email_messages ADD COLUMN content_hash VARCHAR(128)")
                )
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_email_messages_content_hash "
                    "ON email_messages (content_hash)"
                )
            )


def init_db() -> None:
    """Create database tables if they do not exist and run safe schema migrations."""
    global _DB_INITIALIZED
    if not _DB_INITIALIZED:
        _migrate_sqlite_schema()
        _DB_INITIALIZED = True
    Base.metadata.create_all(bind=engine)


def get_session() -> Session:
    return SessionLocal()


def normalize_fb_url(raw_url: str) -> Tuple[str, str]:
    """
    Normalize a Facebook fanpage URL or username into a canonical URL and file-safe slug.
    """
    raw_url = raw_url.strip()
    if not raw_url.startswith(("http://", "https://")):
        if "facebook.com" in raw_url or "fb.com" in raw_url:
            raw_url = "https://" + raw_url
        else:
            slug = re.sub(r"[^a-zA-Z0-9._-]", "", raw_url.strip("/"))
            if not slug:
                raise ValueError(f"Invalid Facebook Fanpage identifier: {raw_url}")
            return f"https://www.facebook.com/{slug}", slug

    parsed = urlparse(raw_url)
    path = parsed.path.strip("/")

    if path == "profile.php":
        qs = parse_qs(parsed.query)
        page_id = qs.get("id", ["unknown"])[0]
        canonical = f"https://www.facebook.com/profile.php?id={page_id}"
        slug = f"profile_{page_id}"
        return canonical, slug

    parts = [p for p in path.split("/") if p]
    if not parts:
        raise ValueError(f"Invalid Facebook Fanpage URL: {raw_url}")

    if parts[0] == "pages" and len(parts) >= 3:
        slug = f"{parts[1]}_{parts[2]}"
        canonical = f"https://www.facebook.com/pages/{parts[1]}/{parts[2]}"
    elif parts[0] == "people" and len(parts) >= 3:
        slug = f"{parts[1]}_{parts[2]}"
        canonical = f"https://www.facebook.com/people/{parts[1]}/{parts[2]}"
    else:
        slug = parts[0]
        canonical = f"https://www.facebook.com/{slug}"

    safe_slug = re.sub(r"[^a-zA-Z0-9._-]", "_", slug)
    return canonical, safe_slug


def add_fanpage(raw_url: str, name: Optional[str] = None) -> Tuple[Fanpage, bool]:
    """
    Add a fanpage to the database.
    Returns (Fanpage, created_bool).
    """
    init_db()
    canonical_url, slug = normalize_fb_url(raw_url)
    cleaned_name = clean_fanpage_name(name)
    with get_session() as session:
        existing = session.scalar(select(Fanpage).where(Fanpage.url == canonical_url))
        if existing:
            changed = False
            if not existing.is_active:
                existing.is_active = True
                changed = True
            if cleaned_name and (not is_valid_fanpage_name(existing.name) or existing.name == existing.slug):
                existing.name = cleaned_name
                changed = True
            elif not is_valid_fanpage_name(existing.name):
                existing.name = existing.slug
                changed = True
            if changed:
                session.commit()
            return existing, False

        fanpage = Fanpage(
            url=canonical_url,
            slug=slug,
            name=cleaned_name or slug,
            is_active=True,
        )
        session.add(fanpage)
        session.commit()
        session.refresh(fanpage)
        return fanpage, True


def list_fanpages(active_only: bool = False) -> List[Tuple[Fanpage, int]]:
    """Return list of (Fanpage, post_count)."""
    init_db()
    with get_session() as session:
        stmt = (
            select(Fanpage, func.count(Post.id))
            .outerjoin(Post, Post.fanpage_id == Fanpage.id)
            .group_by(Fanpage.id)
            .order_by(Fanpage.id.asc())
        )
        if active_only:
            stmt = stmt.where(Fanpage.is_active.is_(True))
        rows = session.execute(stmt).all()
        return [(row[0], row[1]) for row in rows]


def remove_fanpage(identifier: str) -> Optional[Fanpage]:
    """Remove a fanpage by ID, slug, or URL."""
    init_db()
    with get_session() as session:
        fanpage = None
        if identifier.isdigit():
            fanpage = session.get(Fanpage, int(identifier))
        if not fanpage:
            fanpage = session.scalar(select(Fanpage).where(Fanpage.slug == identifier))
        if not fanpage:
            try:
                canonical_url, _ = normalize_fb_url(identifier)
                fanpage = session.scalar(select(Fanpage).where(Fanpage.url == canonical_url))
            except ValueError:
                pass

        if fanpage:
            session.delete(fanpage)
            session.commit()
            return fanpage
        return None


def update_fanpage_metadata(fanpage_id: int, name: Optional[str] = None) -> Optional[str]:
    """Update last_checked_at and optionally page name when valid (healing bad names to slug).
    Returns the resulting stored name, or None if the fanpage does not exist.
    """
    cleaned_name = clean_fanpage_name(name)
    with get_session() as session:
        fanpage = session.get(Fanpage, fanpage_id)
        if not fanpage:
            return None
        fanpage.last_checked_at = datetime.now(LOCAL_TIMEZONE)
        if cleaned_name:
            fanpage.name = cleaned_name
        elif not is_valid_fanpage_name(fanpage.name):
            fanpage.name = fanpage.slug
        resolved = fanpage.name
        session.commit()
        return resolved


def save_posts(fanpage_id: int, scraped_items: List[Dict]) -> List[Post]:
    """
    Save newly scraped posts for a fanpage, skipping any whose post_id already exists.
    Returns list of newly inserted Post records (with fanpage relationship loaded).
    """
    if not scraped_items:
        return []

    new_post_ids: List[int] = []
    with get_session() as session:
        for item in scraped_items:
            post_id = str(item.get("post_id", "")).strip()
            if not post_id:
                continue

            exists = session.scalar(select(Post.id).where(Post.post_id == post_id))
            if exists:
                continue

            published_at = item.get("published_at")
            if not isinstance(published_at, datetime):
                published_at = datetime.now(LOCAL_TIMEZONE)
            else:
                published_at = ensure_local_tz(published_at)

            media_urls = item.get("media_urls") or []
            post = Post(
                post_id=post_id,
                fanpage_id=fanpage_id,
                post_url=item.get("post_url", ""),
                content=item.get("content", "").strip(),
                media_urls_json=json.dumps(media_urls, ensure_ascii=False),
                published_at=published_at,
                scraped_at=datetime.now(LOCAL_TIMEZONE),
            )
            session.add(post)
            try:
                session.flush()
                new_post_ids.append(post.id)
            except Exception:
                session.rollback()

        session.commit()

        if not new_post_ids:
            return []

        stmt = (
            select(Post)
            .options(joinedload(Post.fanpage))
            .where(Post.id.in_(new_post_ids))
            .order_by(Post.published_at.desc(), Post.id.desc())
        )
        return list(session.scalars(stmt).all())


def get_posts_for_fanpage(fanpage_id: int, limit: int = 50) -> List[Post]:
    """Retrieve recent posts for a specific fanpage ordered by published_at desc."""
    init_db()
    with get_session() as session:
        stmt = (
            select(Post)
            .options(joinedload(Post.fanpage))
            .where(Post.fanpage_id == fanpage_id)
            .order_by(Post.published_at.desc(), Post.id.desc())
            .limit(limit)
        )
        return list(session.scalars(stmt).all())


def get_all_recent_posts(limit: int = 100) -> List[Post]:
    """Retrieve recent posts across all fanpages."""
    init_db()
    with get_session() as session:
        stmt = (
            select(Post)
            .options(joinedload(Post.fanpage))
            .order_by(Post.published_at.desc(), Post.id.desc())
            .limit(limit)
        )
        return list(session.scalars(stmt).all())


# --- User & Subscription Management ---


def get_or_create_user(client_id: str) -> User:
    init_db()
    clean_id = client_id.strip()
    with get_session() as session:
        user = session.scalar(
            select(User).where((User.username == clean_id) | (User.user_id == clean_id))
        )
        if not user:
            user = User(
                user_id=uuid.uuid4().hex,
                username=clean_id,
                password="",
                start_year=2024,
            )
            session.add(user)
            session.commit()
            session.refresh(user)
        return user


def create_user(username: str, hashed_password: str, start_year: int) -> User:
    """Create a registered client user account."""
    init_db()
    clean_username = username.strip()
    with get_session() as session:
        existing = session.scalar(select(User).where(User.username == clean_username))
        if existing:
            if not existing.password:
                # Upgrade legacy RSS-only placeholder user with password & start_year
                existing.password = hashed_password
                existing.start_year = int(start_year)
                session.commit()
                session.refresh(existing)
                return existing
            raise ValueError(f"Username '{clean_username}' is already taken.")
        user = User(
            user_id=uuid.uuid4().hex,
            username=clean_username,
            password=hashed_password,
            start_year=int(start_year),
        )
        session.add(user)
        session.commit()
        session.refresh(user)
        return user


def get_user_by_username(username: str) -> Optional[User]:
    init_db()
    with get_session() as session:
        return session.scalar(select(User).where(User.username == username.strip()))


def get_user_by_id(user_id: str) -> Optional[User]:
    init_db()
    with get_session() as session:
        return session.get(User, user_id.strip())


def list_users() -> List[Dict[str, Any]]:
    """Return all users with their event subscription counts and total credits."""
    init_db()
    with get_session() as session:
        users = list(session.scalars(select(User).order_by(User.created_at.desc())).all())
        subs = list(session.scalars(select(EventSubscription)).all())
        stats: Dict[str, Dict[str, int]] = {}
        for s in subs:
            entry = stats.setdefault(s.user_id, {"event_count": 0, "total_credit": 0})
            entry["event_count"] += 1
            entry["total_credit"] += int(s.credit or 0)
        result = []
        for u in users:
            st = stats.get(u.user_id, {"event_count": 0, "total_credit": 0})
            result.append(
                {
                    **u.to_dict(),
                    "has_password": bool(u.password),
                    "subscribed_events_count": st["event_count"],
                    "total_credit": st["total_credit"],
                }
            )
        return result


def subscribe_user_to_fanpages(
    client_id: str, raw_urls: List[str]
) -> Tuple[List[Fanpage], List[Fanpage]]:
    """
    Subscribe a user (`client_id`) to one or more fanpage URLs.
    Automatically creates the user and any fanpage not yet in DB.
    Returns (user_subscribed_fanpages, newly_created_or_unscraped_fanpages).
    """
    init_db()
    user = get_or_create_user(client_id)
    needs_scrape: List[Fanpage] = []

    for raw_url in raw_urls:
        fp, created = add_fanpage(raw_url)
        if created or fp.last_checked_at is None:
            needs_scrape.append(fp)

        with get_session() as session:
            existing_sub = session.scalar(
                select(UserSubscription).where(
                    UserSubscription.user_id == user.id,
                    UserSubscription.fanpage_id == fp.id,
                )
            )
            if not existing_sub:
                sub = UserSubscription(user_id=user.id, fanpage_id=fp.id)
                session.add(sub)
                session.commit()

    return get_user_subscriptions(client_id), needs_scrape


def unsubscribe_user_from_fanpage(client_id: str, identifier: str) -> Optional[Fanpage]:
    """Unsubscribe `client_id` from a fanpage by ID, slug, or URL."""
    init_db()
    with get_session() as session:
        user = session.scalar(select(User).where(User.client_id == client_id.strip()))
        if not user:
            return None

        fanpage = None
        if identifier.isdigit():
            fanpage = session.get(Fanpage, int(identifier))
        if not fanpage:
            fanpage = session.scalar(select(Fanpage).where(Fanpage.slug == identifier))
        if not fanpage:
            try:
                canonical_url, _ = normalize_fb_url(identifier)
                fanpage = session.scalar(select(Fanpage).where(Fanpage.url == canonical_url))
            except ValueError:
                pass

        if not fanpage:
            return None

        sub = session.scalar(
            select(UserSubscription).where(
                UserSubscription.user_id == user.id,
                UserSubscription.fanpage_id == fanpage.id,
            )
        )
        if sub:
            session.delete(sub)
            session.commit()
            return fanpage
        return None


def get_user_subscriptions(client_id: str) -> List[Fanpage]:
    """Return all Fanpages that `client_id` is currently subscribed to."""
    init_db()
    with get_session() as session:
        stmt = (
            select(Fanpage)
            .join(UserSubscription, UserSubscription.fanpage_id == Fanpage.id)
            .join(User, User.id == UserSubscription.user_id)
            .where(User.client_id == client_id.strip())
            .order_by(Fanpage.id.asc())
        )
        return list(session.scalars(stmt).all())


def get_posts_for_user(client_id: str, limit: int = 50) -> List[Post]:
    """Return recent posts across all fanpages that `client_id` is subscribed to."""
    init_db()
    with get_session() as session:
        sub_fanpage_ids = list(
            session.scalars(
                select(UserSubscription.fanpage_id)
                .join(User, User.id == UserSubscription.user_id)
                .where(User.client_id == client_id.strip())
            ).all()
        )
        if not sub_fanpage_ids:
            return []

        stmt = (
            select(Post)
            .options(joinedload(Post.fanpage))
            .where(Post.fanpage_id.in_(sub_fanpage_ids))
            .order_by(Post.published_at.desc(), Post.id.desc())
            .limit(limit)
        )
        return list(session.scalars(stmt).all())


def get_subscribers_for_fanpage(fanpage_id: int) -> List[str]:
    """Return list of `client_id`s subscribed to a given `fanpage_id`."""
    init_db()
    with get_session() as session:
        stmt = (
            select(User.client_id)
            .join(UserSubscription, UserSubscription.user_id == User.id)
            .where(UserSubscription.fanpage_id == fanpage_id)
        )
        return list(session.scalars(stmt).all())


def get_inbox_messages(client_id: Optional[str] = None, limit: int = 100) -> List[EmailMessage]:
    """Retrieve recent email entries received in the server inbox."""
    init_db()
    with get_session() as session:
        stmt = select(EmailMessage).order_by(EmailMessage.ingested_at.desc(), EmailMessage.id.desc())
        if client_id:
            stmt = stmt.where(EmailMessage.client_id == client_id.strip())
        stmt = stmt.limit(limit)
        return list(session.scalars(stmt).all())


def get_known_content_hashes(hashes: List[str]) -> List[str]:
    """Return subset of client-supplied SHA-256 content_hash strings already recorded on the server."""
    clean = [str(h).strip().lower() for h in (hashes or []) if h and str(h).strip()]
    if not clean:
        return []
    init_db()
    known: List[str] = []
    with get_session() as session:
        for i in range(0, len(clean), 500):
            chunk = clean[i : i + 500]
            rows = session.scalars(
                select(EmailMessage.content_hash).where(EmailMessage.content_hash.in_(chunk))
            ).all()
            known.extend([r for r in rows if r])
    return known


def get_all_events(
    source_type: Optional[str] = None,
    client_id: Optional[str] = None,
    fanpage_id: Optional[int] = None,
    limit: int = 200,
) -> List[EventRecord]:
    """Retrieve deduplicated events from the unified EventRecord table."""
    init_db()
    with get_session() as session:
        stmt = select(EventRecord).order_by(EventRecord.updated_at.desc(), EventRecord.event_id.desc())
        if source_type:
            st_clean = source_type.strip()
            if st_clean.lower() == "facebook":
                stmt = stmt.where(EventRecord.source_type.ilike("facebook%"))
            elif st_clean.lower() == "email":
                stmt = stmt.where(EventRecord.source_type.ilike("email%"))
            else:
                stmt = stmt.where(EventRecord.source_type == st_clean)
        if client_id:
            stmt = stmt.where(EventRecord.client_id == client_id.strip())
        if fanpage_id is not None:
            stmt = stmt.where(EventRecord.fanpage_id == fanpage_id)
        stmt = stmt.limit(limit)
        return list(session.scalars(stmt).all())


def get_event_by_id(event_id: int) -> Optional[EventRecord]:
    init_db()
    with get_session() as session:
        return session.get(EventRecord, event_id)


def subscribe_user_to_event(
    user_id: str,
    event_id: int,
    audience: str = "",
    progress: str = "interested",
    evidence: str = "",
    credit: int = 0,
) -> EventSubscription:
    """Create or update a user's subscription to an event."""
    init_db()
    now_iso = local_now_iso()
    with get_session() as session:
        ev = session.get(EventRecord, event_id)
        if not ev:
            raise ValueError(f"Event {event_id} not found.")
        sub = session.get(EventSubscription, (user_id, event_id))
        if sub:
            if audience:
                sub.audience = audience.strip()
            if progress:
                sub.progress = progress.strip()
            if evidence:
                sub.evidence = evidence.strip()
            if credit is not None:
                sub.credit = int(credit)
            sub.updated_at = now_iso
        else:
            sub = EventSubscription(
                user_id=user_id,
                event_id=event_id,
                audience=(audience or "").strip(),
                progress=(progress or "interested").strip(),
                evidence=(evidence or "").strip(),
                credit=int(credit or 0),
                created_at=now_iso,
                updated_at=now_iso,
            )
            session.add(sub)
        session.commit()
        session.refresh(sub)
        return sub


def update_event_subscription(
    user_id: str,
    event_id: int,
    audience: Optional[str] = None,
    progress: Optional[str] = None,
    evidence: Optional[str] = None,
    credit: Optional[int] = None,
) -> Optional[EventSubscription]:
    """Update fields on an existing (user_id, event_id) subscription."""
    init_db()
    with get_session() as session:
        sub = session.get(EventSubscription, (user_id, event_id))
        if not sub:
            return None
        if audience is not None:
            sub.audience = audience.strip()
        if progress is not None:
            sub.progress = progress.strip()
        if evidence is not None:
            sub.evidence = evidence.strip()
        if credit is not None:
            sub.credit = int(credit)
        sub.updated_at = local_now_iso()
        session.commit()
        session.refresh(sub)
        return sub


def unsubscribe_user_from_event(user_id: str, event_id: int) -> bool:
    """Remove a user's subscription to an event."""
    init_db()
    with get_session() as session:
        sub = session.get(EventSubscription, (user_id, event_id))
        if not sub:
            return False
        session.delete(sub)
        session.commit()
        return True


def get_user_event_subscriptions(user_id: str) -> List[Dict[str, Any]]:
    """Return all event subscriptions (with nested event details) for a given user_id."""
    init_db()
    with get_session() as session:
        stmt = (
            select(EventSubscription)
            .options(joinedload(EventSubscription.event))
            .where(EventSubscription.user_id == user_id)
            .order_by(EventSubscription.updated_at.desc())
        )
        rows = list(session.scalars(stmt).all())
        return [r.to_dict(include_event=True) for r in rows]


def list_all_event_subscriptions(limit: int = 200) -> List[Dict[str, Any]]:
    """Return recent event subscriptions across all users (for admin view)."""
    init_db()
    with get_session() as session:
        stmt = (
            select(EventSubscription)
            .options(joinedload(EventSubscription.event), joinedload(EventSubscription.user))
            .order_by(EventSubscription.updated_at.desc())
            .limit(limit)
        )
        rows = list(session.scalars(stmt).all())
        out = []
        for r in rows:
            item = r.to_dict(include_event=True)
            item["username"] = r.user.username if r.user else r.user_id
            out.append(item)
        return out
