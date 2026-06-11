"""
Pydantic v2 data schemas for the TikTok crawler pipeline.

Defines:
  - TikTokSearchRequest:    cURL payload for the search endpoint
  - TikTokPost:             parsed post data returned by the API
  - TikTokSearchResponse:   top-level API response envelope
  - KafkaEvent:             event envelope published to Kafka
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ---------------------------------------------------------------------------
# Request schema
# ---------------------------------------------------------------------------


class TikTokSearchRequest(BaseModel):
    """Search request payload for the TikTok feed search API."""

    keywords: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Search keywords / hashtag query",
    )
    count: int = Field(default=12, ge=1, le=50)
    cursor: int = Field(default=0, ge=0)
    web: int = Field(default=1, ge=0, le=1)
    hd: int = Field(default=1, ge=0, le=1)

    def to_form_data(self) -> dict[str, str]:
        """Encode the request as ``application/x-www-form-urlencoded`` fields."""
        return {
            "keywords": self.keywords,
            "count": str(self.count),
            "cursor": str(self.cursor),
            "web": str(self.web),
            "hd": str(self.hd),
        }


class TikTokUserPostsRequest(BaseModel):
    """Request payload for fetching posts by user ``unique_id``.

    Corresponds to::

        unique_id=%40persib&count=12&cursor=0&web=1&hd=1
    """

    unique_id: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="TikTok user unique_id (e.g. @persib — the @ is optional)",
    )
    count: int = Field(default=12, ge=1, le=50)
    cursor: int = Field(default=0, ge=0)
    web: int = Field(default=1, ge=0, le=1)
    hd: int = Field(default=1, ge=0, le=1)

    @field_validator("unique_id", mode="before")
    @classmethod
    def normalize_unique_id(cls, v: str) -> str:
        """Strip whitespace; ensure leading @ is present (API expects @username)."""
        v = v.strip()
        if not v.startswith("@"):
            v = f"@{v}"
        return v

    def to_form_data(self) -> dict[str, str]:
        """Encode the request as ``application/x-www-form-urlencoded`` fields."""
        return {
            "unique_id": self.unique_id,
            "count": str(self.count),
            "cursor": str(self.cursor),
            "web": str(self.web),
            "hd": str(self.hd),
        }


# ---------------------------------------------------------------------------
# Sub-models (author, music, video, stats)
# ---------------------------------------------------------------------------


class TikTokAuthor(BaseModel):
    """Author / creator metadata."""

    id: str = Field(default="", alias="author_id")
    unique_id: str = Field(default="")
    nickname: str = Field(default="")
    avatar: Optional[str] = None

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TikTokMusic(BaseModel):
    """Music / audio metadata."""

    id: str = Field(default="", alias="music_id")
    title: str = Field(default="")
    author: str = Field(default="")
    duration: int = Field(default=0, ge=0)

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TikTokVideo(BaseModel):
    """Video resource metadata."""

    id: str = Field(default="", alias="video_id")
    duration: int = Field(default=0, ge=0)
    cover: Optional[str] = None
    play_url: Optional[str] = Field(default=None, alias="play")
    wm_play_url: Optional[str] = Field(default=None, alias="wmplay")
    size: Optional[int] = Field(default=None, ge=0)

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TikTokStats(BaseModel):
    """Engagement statistics for a post."""

    digg_count: int = Field(default=0, ge=0, alias="digg_count")
    share_count: int = Field(default=0, ge=0, alias="share_count")
    comment_count: int = Field(default=0, ge=0, alias="comment_count")
    play_count: int = Field(default=0, ge=0, alias="play_count")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


# ---------------------------------------------------------------------------
# Post & response schemas
# ---------------------------------------------------------------------------


class TikTokPost(BaseModel):
    """A single TikTok post as returned by the API.

    Handles API inconsistencies where some fields may be returned as
    plain strings instead of full objects.
    """

    id: str = Field(..., alias="video_id", description="Unique video/post ID")
    title: str = Field(default="")
    description: Optional[str] = Field(default=None, alias="desc")
    create_time: Optional[int] = Field(default=None, ge=0)
    author: TikTokAuthor = Field(default_factory=TikTokAuthor)
    music: Any = Field(default_factory=TikTokMusic)
    video: TikTokVideo = Field(default_factory=TikTokVideo)
    stats: TikTokStats = Field(default_factory=TikTokStats)
    hashtags: list[str] = Field(default_factory=list)
    is_ad: bool = Field(default=False)

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @field_validator("create_time", mode="before")
    @classmethod
    def coerce_create_time(cls, v: Any) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    @field_validator("music", mode="before")
    @classmethod
    def coerce_music(cls, v: Any) -> TikTokMusic:
        if isinstance(v, str):
            return TikTokMusic(id=v, title="")
        if isinstance(v, dict):
            return TikTokMusic.model_validate(v)
        return TikTokMusic()

    @field_validator("author", mode="before")
    @classmethod
    def coerce_author(cls, v: Any) -> TikTokAuthor:
        if isinstance(v, str):
            return TikTokAuthor(id=v)
        if isinstance(v, dict):
            return TikTokAuthor.model_validate(v)
        return TikTokAuthor()

    @field_validator("video", mode="before")
    @classmethod
    def coerce_video(cls, v: Any) -> TikTokVideo:
        if isinstance(v, str):
            return TikTokVideo(id=v)
        if isinstance(v, dict):
            return TikTokVideo.model_validate(v)
        return TikTokVideo()

    @field_validator("stats", mode="before")
    @classmethod
    def coerce_stats(cls, v: Any) -> TikTokStats:
        if isinstance(v, dict):
            return TikTokStats.model_validate(v)
        return TikTokStats()

    @classmethod
    def from_api_response(cls, payload: dict[str, Any]) -> TikTokPost:
        return cls.model_validate(payload)


class TikTokSearchData(BaseModel):
    """Inner data object inside a TikTokSearchResponse."""

    videos: list[TikTokPost] = Field(default_factory=list)
    cursor: Optional[int] = Field(default=None)
    has_more: bool = Field(default=False, alias="hasMore")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TikTokSearchResponse(BaseModel):
    """Top-level response envelope from the search API."""

    code: int = Field(default=0)
    msg: str = Field(default="success")
    data: TikTokSearchData = Field(default_factory=TikTokSearchData)

    model_config = ConfigDict(extra="allow", populate_by_name=True)


# ---------------------------------------------------------------------------
# Kafka event envelope
# ---------------------------------------------------------------------------


class KafkaEvent(BaseModel):
    """Standardised event envelope for Kafka messages."""

    event_id: str = Field(default_factory=lambda: uuid4().hex)
    event_type: str = Field(default="tiktok.post.scraped")
    source: str = Field(default="tiktok-crawler")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    payload: TikTokPost = Field(...)
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
