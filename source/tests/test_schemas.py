"""Tests for library/schemas.py — Pydantic v2 models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from library.schemas import (
    KafkaEvent,
    TikTokPost,
    TikTokSearchRequest,
    TikTokSearchResponse,
)


class TestTikTokSearchRequest:
    def test_valid_request(self, sample_search_request: TikTokSearchRequest) -> None:
        assert sample_search_request.keywords == "persib"
        assert sample_search_request.count == 12

    def test_defaults(self) -> None:
        req = TikTokSearchRequest(keywords="test")
        assert req.count == 12
        assert req.cursor == 0

    def test_missing_keywords_raises(self) -> None:
        with pytest.raises(ValidationError):
            TikTokSearchRequest()

    def test_blank_keywords_raises(self) -> None:
        with pytest.raises(ValidationError):
            TikTokSearchRequest(keywords="")

    def test_count_out_of_bounds(self) -> None:
        with pytest.raises(ValidationError):
            TikTokSearchRequest(keywords="test", count=0)
        with pytest.raises(ValidationError):
            TikTokSearchRequest(keywords="test", count=100)

    def test_to_form_data(self) -> None:
        req = TikTokSearchRequest(keywords="persib", count=12)
        data = req.to_form_data()
        assert data == {
            "keywords": "persib",
            "count": "12",
            "cursor": "0",
            "web": "1",
            "hd": "1",
        }


class TestTikTokPost:
    def test_parse_from_dict(self, sample_post_dict: dict) -> None:
        post = TikTokPost.model_validate(sample_post_dict)
        assert post.id == "7123456789012345678"
        assert post.title == "Amazing TikTok video"
        assert post.hashtags == ["fyp", "viral"]

    def test_nested_objects(self, sample_post: TikTokPost) -> None:
        assert sample_post.author.id == "user_001"
        assert sample_post.music.id == "music_001"
        assert sample_post.video.id == "7123456789012345678"
        assert sample_post.stats.digg_count == 1000

    def test_minimal_post(self) -> None:
        post = TikTokPost.model_validate({"video_id": "123"})
        assert post.id == "123"

    def test_music_string_coercion(self) -> None:
        post = TikTokPost.model_validate({
            "video_id": "123",
            "music": "/video/music/test.mp3",
        })
        assert post.music.id == "/video/music/test.mp3"

    def test_create_time_coercion(self) -> None:
        post = TikTokPost.model_validate({
            "video_id": "123",
            "create_time": "1690000000",
        })
        assert post.create_time == 1690000000

    def test_from_api_response(self) -> None:
        post = TikTokPost.from_api_response({"video_id": "abc", "title": "Hello"})
        assert post.id == "abc"


class TestTikTokSearchResponse:
    def test_parse(self, sample_search_response_dict: dict) -> None:
        resp = TikTokSearchResponse.model_validate(sample_search_response_dict)
        assert resp.code == 0
        assert len(resp.data.videos) == 1
        assert resp.data.cursor == 12
        assert resp.data.has_more is True

    def test_empty_data(self) -> None:
        resp = TikTokSearchResponse(code=0, msg="success")
        assert resp.data.videos == []


class TestKafkaEvent:
    def test_create_event(self, sample_post: TikTokPost) -> None:
        event = KafkaEvent(payload=sample_post, metadata={"query": "persib"})
        assert event.event_type == "tiktok.post.scraped"
        assert len(event.event_id) == 32
        assert event.payload == sample_post

    def test_extra_fields_forbidden(self, sample_post: TikTokPost) -> None:
        with pytest.raises(ValidationError):
            KafkaEvent(payload=sample_post, unknown_field="oops")
