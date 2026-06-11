"""Shared pytest fixtures for the TikTok crawler test suite."""

from __future__ import annotations

import pytest

from library.config import (
    ElasticsearchSettings,
    KafkaSettings,
    TikTokCrawlerSettings,
)
from library.schemas import (
    KafkaEvent,
    TikTokPost,
    TikTokSearchRequest,
)


# ---------------------------------------------------------------------------
# Sample data builders
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_search_request() -> TikTokSearchRequest:
    return TikTokSearchRequest(keywords="persib", count=12, cursor=0, web=1, hd=1)


@pytest.fixture
def sample_post_dict() -> dict:
    return {
        "video_id": "7123456789012345678",
        "title": "Amazing TikTok video",
        "desc": "This is a test post",
        "create_time": 1690000000,
        "author_id": "user_001",
        "author": {
            "author_id": "user_001",
            "unique_id": "testuser",
            "nickname": "Test User",
            "avatar": "https://example.com/avatar.jpg",
        },
        "music": {
            "music_id": "music_001",
            "title": "Original Sound",
            "author": "Test User",
            "duration": 30,
        },
        "video": {
            "video_id": "7123456789012345678",
            "duration": 15,
            "cover": "https://example.com/cover.jpg",
            "play": "https://example.com/video.mp4",
            "wmplay": "https://example.com/video_wm.mp4",
            "size": 1048576,
        },
        "stats": {
            "digg_count": 1000,
            "share_count": 50,
            "comment_count": 200,
            "play_count": 50000,
        },
        "hashtags": ["fyp", "viral"],
        "is_ad": False,
    }


@pytest.fixture
def sample_post(sample_post_dict: dict) -> TikTokPost:
    return TikTokPost.model_validate(sample_post_dict)


@pytest.fixture
def sample_kafka_event(sample_post: TikTokPost) -> KafkaEvent:
    return KafkaEvent(
        event_type="tiktok.post.scraped",
        payload=sample_post,
        metadata={"query": "persib"},
    )


@pytest.fixture
def sample_search_response_dict(sample_post_dict: dict) -> dict:
    return {
        "code": 0,
        "msg": "success",
        "data": {
            "videos": [sample_post_dict],
            "cursor": 12,
            "hasMore": True,
        },
    }


# ---------------------------------------------------------------------------
# Settings fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def kafka_settings() -> KafkaSettings:
    return KafkaSettings(
        bootstrap_servers="test-kafka:9092",
        topic="test.topic",
    )


@pytest.fixture
def es_settings() -> ElasticsearchSettings:
    return ElasticsearchSettings(
        hosts=["http://test-es:9200"],
        index_name="test_index",
    )


@pytest.fixture
def crawler_settings() -> TikTokCrawlerSettings:
    return TikTokCrawlerSettings(
        base_url="https://www.tikwm.com",
        rate_limit_rps=100.0,
        request_timeout=5.0,
        max_retries=2,
    )
