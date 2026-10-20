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
