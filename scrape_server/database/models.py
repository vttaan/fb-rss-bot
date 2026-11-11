from datetime import datetime
from typing import Any, Dict, List, Optional
import json

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from config import LOCAL_TIMEZONE


def local_now() -> datetime:
    return datetime.now(LOCAL_TIMEZONE)


def ensure_local_tz(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=LOCAL_TIMEZONE)
    return dt.astimezone(LOCAL_TIMEZONE)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    client_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=local_now)

    subscriptions: Mapped[List["UserSubscription"]] = relationship(
        "UserSubscription", back_populates="user", cascade="all, delete-orphan"
    )


class UserSubscription(Base):
    __tablename__ = "user_subscriptions"
    __table_args__ = (
        UniqueConstraint("user_id", "fanpage_id", name="uq_user_fanpage"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
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
    """Unified deduplicated event store across Facebook fanpage posts and client email entries."""

    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    normalized_title: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    online_mode: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    is_online: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    event_date: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    event_time: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    start_datetime: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    end_datetime: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    location: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    normalized_location: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    source_type: Mapped[str] = mapped_column(String(32), default="facebook", nullable=False, index=True)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    fanpage_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("fanpages.id", ondelete="SET NULL"), nullable=True, index=True
    )
    client_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    event_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=local_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=local_now)

    def to_dict(self) -> Dict[str, Any]:
        try:
            extra = json.loads(self.event_json or "{}")
        except Exception:
            extra = {}
        created_dt = ensure_local_tz(self.created_at)
        updated_dt = ensure_local_tz(self.updated_at)
        return {
            "event_id": self.id,
            "title": self.title,
            "online_mode": self.online_mode,
            "is_online": self.is_online,
            "date": self.event_date,
            "time": self.event_time,
            "start_datetime": self.start_datetime,
            "end_datetime": self.end_datetime,
            "event_start_datetime": extra.get("event_start_datetime") or self.start_datetime,
            "event_end_datetime": extra.get("event_end_datetime") or self.end_datetime,
            "reg_start_datetime": extra.get("reg_start_datetime"),
            "reg_end_datetime": extra.get("reg_end_datetime"),
            "location": self.location,
            "source_type": self.source_type,
            "source_id": self.source_id,
            "fanpage_id": self.fanpage_id,
            "client_id": self.client_id,
            "version": self.version,
            "created_at": created_dt.isoformat() if created_dt else None,
            "updated_at": updated_dt.isoformat() if updated_dt else None,
            **extra,
        }


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
        Integer, ForeignKey("events.id", ondelete="SET NULL"), nullable=True, index=True
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
