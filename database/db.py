import json
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session, sessionmaker, joinedload

from config import DATABASE_URL, LOCAL_TIMEZONE
from .models import Base, Fanpage, Post, User, UserSubscription, ensure_local_tz

engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def init_db() -> None:
    """Create database tables if they do not exist."""
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
    with get_session() as session:
        existing = session.scalar(select(Fanpage).where(Fanpage.url == canonical_url))
        if existing:
            if not existing.is_active:
                existing.is_active = True
                session.commit()
            return existing, False

        fanpage = Fanpage(
            url=canonical_url,
            slug=slug,
            name=name or slug,
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


def update_fanpage_metadata(fanpage_id: int, name: Optional[str] = None) -> None:
    """Update last_checked_at and optionally page name."""
    with get_session() as session:
        fanpage = session.get(Fanpage, fanpage_id)
        if fanpage:
            fanpage.last_checked_at = datetime.now(LOCAL_TIMEZONE)
            if name and name.strip():
                fanpage.name = name.strip()
            session.commit()


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
        user = session.scalar(select(User).where(User.client_id == clean_id))
        if not user:
            user = User(client_id=clean_id)
            session.add(user)
            session.commit()
            session.refresh(user)
        return user


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
