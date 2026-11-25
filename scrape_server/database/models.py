from datetime import datetime
import json
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, synonym, validates
from sqlalchemy.types import TypeDecorator

from config import LOCAL_TIMEZONE


def local_now() -> datetime:
    return datetime.now(LOCAL_TIMEZONE)


def local_now_iso() -> str:
    return local_now().isoformat()


def ensure_local_tz(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=LOCAL_TIMEZONE)
    return dt.astimezone(LOCAL_TIMEZONE)


def _to_iso_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = ensure_local_tz(val)
        return dt.isoformat() if dt else None
    text = str(val).strip()
    return text or None


class StringListType(TypeDecorator):
    """SQLite Text column that transparently serializes/deserializes a list of strings."""

    impl = Text
    cache_ok = True

    def process_bind_param(self, value: Optional[List[str]], dialect: Any) -> str:
        if not value:
            return "[]"
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return json.dumps([str(x).strip() for x in parsed if str(x).strip()], ensure_ascii=False)
            except Exception:
                return json.dumps([value.strip()] if value.strip() else [], ensure_ascii=False)
        return json.dumps([str(x).strip() for x in value if str(x).strip()], ensure_ascii=False)

    def process_result_value(self, value: Optional[str], dialect: Any) -> List[str]:
        if not value:
            return []
        if isinstance(value, list):
            return [str(x) for x in value]
        try:
            parsed = json.loads(value)
            return [str(x) for x in parsed] if isinstance(parsed, list) else []
        except Exception:
            return []


class Base(DeclarativeBase):
    pass


class User(Base):
    """Registered client user account (also used for RSS/fanpage subscriptions)."""

    __tablename__ = "users"

    user_id: Mapped[str] = mapped_column(
        String(128), primary_key=True, default=lambda: uuid.uuid4().hex
    )
    username: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    password: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    start_year: Mapped[int] = mapped_column(Integer, default=2024, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=local_now)

    # Compatibility aliases for existing RSS helpers
    id = synonym("user_id")
    client_id = synonym("username")

    subscriptions: Mapped[List["UserSubscription"]] = relationship(
        "UserSubscription", back_populates="user", cascade="all, delete-orphan"
    )
    event_subscriptions: Mapped[List["EventSubscription"]] = relationship(
        "EventSubscription", back_populates="user", cascade="all, delete-orphan"
    )

    def to_dict(self) -> Dict[str, Any]:
        dt = ensure_local_tz(self.created_at)
        return {
            "user_id": self.user_id,
            "username": self.username,
            "start_year": self.start_year,
            "created_at": dt.isoformat() if dt else None,
        }


class UserSubscription(Base):
    """Fanpage RSS feed subscription per user."""

    __tablename__ = "user_subscriptions"
    __table_args__ = (
        UniqueConstraint("user_id", "fanpage_id", name="uq_user_fanpage"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False, index=True
    )
    fanpage_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("fanpages.id", ondelete="CASCADE"), nullable=False, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=local_now)

    user: Mapped["User"] = relationship("User", back_populates="subscriptions")
    fanpage: Mapped["Fanpage"] = relationship("Fanpage", back_populates="subscribers")


class Fanpage(Base):
    __tablename__ = "fanpages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(String(512), unique=True, nullable=False, index=True)
    slug: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=local_now)
    last_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    posts: Mapped[List["Post"]] = relationship(
        "Post", back_populates="fanpage", cascade="all, delete-orphan"
    )
    subscribers: Mapped[List["UserSubscription"]] = relationship(
        "UserSubscription", back_populates="fanpage", cascade="all, delete-orphan"
    )

    def to_dict(self) -> Dict[str, Any]:
        last_checked = ensure_local_tz(self.last_checked_at)
        return {
            "id": self.id,
            "url": self.url,
            "slug": self.slug,
            "name": self.name or self.slug,
            "is_active": self.is_active,
            "last_checked_at": last_checked.isoformat() if last_checked else None,
            "rss_file": f"feeds/{self.slug}.xml",
        }


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    post_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    fanpage_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("fanpages.id", ondelete="CASCADE"), nullable=False, index=True
    )
    post_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    content: Mapped[str] = mapped_column(Text, default="", nullable=False)
    media_urls_json: Mapped[str] = mapped_column(Text, default="[]", nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=local_now, index=True)
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=local_now)

    fanpage: Mapped["Fanpage"] = relationship("Fanpage", back_populates="posts")

    @property
    def media_urls(self) -> List[str]:
        try:
            return json.loads(self.media_urls_json)
        except Exception:
            return []

    @media_urls.setter
    def media_urls(self, urls: List[str]) -> None:
        self.media_urls_json = json.dumps(urls, ensure_ascii=False)

    def to_dict(self) -> Dict[str, Any]:
        pub_dt = ensure_local_tz(self.published_at)
        scr_dt = ensure_local_tz(self.scraped_at)
        return {
            "id": self.id,
            "post_id": self.post_id,
            "fanpage_id": self.fanpage_id,
            "fanpage_name": (self.fanpage.name or self.fanpage.slug) if self.fanpage else None,
            "fanpage_url": self.fanpage.url if self.fanpage else None,
            "post_url": self.post_url,
            "content": self.content,
            "media_urls": self.media_urls,
            "published_at": pub_dt.isoformat() if pub_dt else None,
            "scraped_at": scr_dt.isoformat() if scr_dt else None,
        }


class PostAnalysis(Base):
    """LLM event-analysis result cache so each post is analyzed only once."""

    __tablename__ = "post_analyses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    post_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    fanpage_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("fanpages.id", ondelete="CASCADE"), nullable=False, index=True
    )
    is_event: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    event_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    analyzed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=local_now)


class EventRecord(Base):
    """Unified deduplicated & classified event store across Facebook fanpage posts and email entries."""

    __tablename__ = "events"

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    description: Mapped[str] = mapped_column(Text, default="", nullable=False)
    organizer: Mapped[str] = mapped_column(String(512), default="", nullable=False)
    is_online: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    event_start_datetime: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    reg_start_datetime: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    reg_end_datetime: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    event_end_datetime: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    location: Mapped[str] = mapped_column(String(512), default="", nullable=False)
    source_type: Mapped[str] = mapped_column(String(255), default="facebook", nullable=False, index=True)
    created_at: Mapped[str] = mapped_column(String(64), default=local_now_iso, nullable=False)
    updated_at: Mapped[str] = mapped_column(String(64), default=local_now_iso, nullable=False)
    fee: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    target_audience: Mapped[List[str]] = mapped_column(StringListType, default=list, nullable=False)
    award: Mapped[str] = mapped_column(Text, default="", nullable=False)
    registration: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    category: Mapped[List[str]] = mapped_column(StringListType, default=list, nullable=False)
    format: Mapped[List[str]] = mapped_column(StringListType, default=list, nullable=False)
    post_url: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    # Internal deduplication & ingestion metadata columns
    normalized_title: Mapped[str] = mapped_column(String(512), default="", nullable=False, index=True)
    online_mode: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    event_date: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    event_time: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    start_datetime: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    end_datetime: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    normalized_location: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    source_id: Mapped[str] = mapped_column(String(255), default="", nullable=False, index=True)
    fanpage_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("fanpages.id", ondelete="SET NULL"), nullable=True, index=True
    )
    client_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    event_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    # Compatibility alias so existing queries using EventRecord.id keep working
    id = synonym("event_id")

    subscriptions: Mapped[List["EventSubscription"]] = relationship(
        "EventSubscription", back_populates="event", cascade="all, delete-orphan"
    )

    @validates("created_at", "updated_at")
    def _coerce_iso_datetime(self, key: str, value: Any) -> str:
        return _to_iso_str(value) or local_now_iso()

    def to_dict(self) -> Dict[str, Any]:
        try:
            extra = json.loads(self.event_json or "{}")
        except Exception:
            extra = {}

        ev_start = self.event_start_datetime or self.start_datetime or extra.get("event_start_datetime")
        ev_end = self.event_end_datetime or self.end_datetime or extra.get("event_end_datetime")
        reg_start = self.reg_start_datetime or extra.get("reg_start_datetime")
        reg_end = self.reg_end_datetime or extra.get("reg_end_datetime")
        reg_url = self.registration if self.registration is not None else extra.get("registration_url")
        p_url = self.post_url if self.post_url is not None else extra.get("post_url")

        merged = {
            **extra,
            "event_id": self.event_id,
            "title": self.title,
            "description": self.description or extra.get("description") or "",
            "organizer": self.organizer or extra.get("organizer") or "",
            "online_mode": self.online_mode,
            "is_online": bool(self.is_online),
            "date": self.event_date,
            "time": self.event_time,
            "start_datetime": ev_start,
            "end_datetime": ev_end,
            "event_start_datetime": ev_start,
            "reg_start_datetime": reg_start,
            "reg_end_datetime": reg_end,
            "event_end_datetime": ev_end,
            "location": self.location or "",
            "source_type": self.source_type,
            "source_id": self.source_id,
            "fanpage_id": self.fanpage_id,
            "client_id": self.client_id,
            "version": self.version,
            "created_at": _to_iso_str(self.created_at),
            "updated_at": _to_iso_str(self.updated_at),
            "fee": self.fee or extra.get("fee") or "",
            "target_audience": self.target_audience if self.target_audience else (extra.get("target_audience") or []),
            "award": self.award or extra.get("award") or "",
            "registration": reg_url,
            "registration_url": reg_url,
            "category": self.category if self.category else (extra.get("category") or []),
            "format": self.format if self.format else (extra.get("format") or []),
            "post_url": p_url,
        }
        return merged


class EventSubscription(Base):
    """Tracks a client user's subscription, participation progress, evidence, and credit for an event."""

    __tablename__ = "event_subscriptions"

    user_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.user_id", ondelete="CASCADE"), primary_key=True
    )
    event_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("events.event_id", ondelete="CASCADE"), primary_key=True
    )
    audience: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    progress: Mapped[str] = mapped_column(String(64), default="interested", nullable=False)
    evidence: Mapped[str] = mapped_column(Text, default="", nullable=False)
    credit: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[str] = mapped_column(String(64), default=local_now_iso, nullable=False)
    updated_at: Mapped[str] = mapped_column(String(64), default=local_now_iso, nullable=False)

    user: Mapped["User"] = relationship("User", back_populates="event_subscriptions")
    event: Mapped["EventRecord"] = relationship("EventRecord", back_populates="subscriptions")

    def to_dict(self, include_event: bool = False) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "user_id": self.user_id,
            "event_id": self.event_id,
            "audience": self.audience,
            "progress": self.progress,
            "evidence": self.evidence,
            "credit": self.credit,
            "created_at": _to_iso_str(self.created_at),
            "updated_at": _to_iso_str(self.updated_at),
        }
        if include_event and self.event is not None:
            data["event"] = self.event.to_dict()
        return data


class EmailMessage(Base):
    """Client-submitted email entries sent to the server inbox for event extraction."""

    __tablename__ = "email_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    content_hash: Mapped[Optional[str]] = mapped_column(String(128), unique=True, nullable=True, index=True)
    client_id: Mapped[str] = mapped_column(String(128), default="anonymous", nullable=False, index=True)
    subject: Mapped[str] = mapped_column(String(512), default="", nullable=False)
    sender: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    content: Mapped[str] = mapped_column(Text, default="", nullable=False)
    media_urls_json: Mapped[str] = mapped_column(Text, default="[]", nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=local_now, index=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=local_now)
    status: Mapped[str] = mapped_column(String(64), default="pending", nullable=False)
    is_event: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    event_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("events.event_id", ondelete="SET NULL"), nullable=True, index=True
    )
    changes_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    analysis_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)

    @property
    def media_urls(self) -> List[str]:
        try:
            return json.loads(self.media_urls_json)
        except Exception:
            return []

    def to_dict(self, include_content: bool = False) -> Dict[str, Any]:
        rec_dt = ensure_local_tz(self.received_at)
        ing_dt = ensure_local_tz(self.ingested_at)
        try:
            changes = json.loads(self.changes_json or "{}")
        except Exception:
            changes = {}
        try:
            analysis = json.loads(self.analysis_json or "{}")
        except Exception:
            analysis = {}
        content_part = {"content": self.content} if include_content else {}
        return {
            "id": self.id,
            "message_id": self.message_id,
            "content_hash": self.content_hash,
            "client_id": self.client_id,
            "subject": self.subject,
            "sender": self.sender,
            **content_part,
            "media_urls": self.media_urls,
            "received_at": rec_dt.isoformat() if rec_dt else None,
            "ingested_at": ing_dt.isoformat() if ing_dt else None,
            "status": self.status,
            "is_event": self.is_event,
            "event_id": self.event_id,
            "changes": changes,
            "event": analysis if self.is_event else None,
        }
