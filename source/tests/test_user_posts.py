"""Tests for controllers/tiktok/user_posts.py — TikTokUserPosts controller."""

from __future__ import annotations

import pytest
from pytest_mock import MockerFixture

from controllers.tiktok.user_posts import TikTokUserPosts
from library.schemas import TikTokUserPostsRequest


class TestTikTokUserPostsRequest:
    """Tests for TikTokUserPostsRequest schema."""

    def test_valid_request(self) -> None:
        req = TikTokUserPostsRequest(unique_id="persib", count=20, cursor=0)
        assert req.unique_id == "@persib"   # @ auto-added
        assert req.count == 20

    def test_preserves_at_sign(self) -> None:
        req = TikTokUserPostsRequest(unique_id="@persib")
        assert req.unique_id == "@persib"   # @ preserved

    def test_strips_whitespace_adds_at(self) -> None:
        req = TikTokUserPostsRequest(unique_id="  persib  ")
        assert req.unique_id == "@persib"   # trimmed + @ added

    def test_defaults(self) -> None:
        req = TikTokUserPostsRequest(unique_id="test")
        assert req.count == 12
        assert req.cursor == 0
        assert req.web == 1
        assert req.hd == 1

    def test_missing_unique_id_raises(self) -> None:
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            TikTokUserPostsRequest()

    def test_to_form_data(self) -> None:
        req = TikTokUserPostsRequest(unique_id="@persib", count=20)
        data = req.to_form_data()
        assert data == {
            "unique_id": "@persib",
            "count": "20",
            "cursor": "0",
            "web": "1",
            "hd": "1",
        }


class TestTikTokUserPostsController:
    """Tests for TikTokUserPosts controller."""

    @pytest.mark.asyncio
    async def test_scrape_to_json(
        self, mocker: MockerFixture
    ) -> None:
        """scrape_to_json() should return a list of raw post dicts."""
        mock_event = mocker.MagicMock()
        mock_event.payload.model_dump.return_value = {
            "video_id": "test123",
            "title": "User Post",
        }

        async def mock_get_user_posts(*args, **kwargs):
            yield mock_event

        mocker.patch(
            "controllers.tiktok.TikTokAPI.get_user_posts",
            side_effect=mock_get_user_posts,
        )
        mocker.patch("controllers.tiktok.TikTokAPI.start", new_callable=mocker.AsyncMock)
        mocker.patch("controllers.tiktok.TikTokAPI.stop", new_callable=mocker.AsyncMock)

        kwargs = {"unique_id": "@persib", "count": 10, "max_pages": 1}
        ctl = TikTokUserPosts(**kwargs)

        posts = await ctl.scrape_to_json({"unique_id": "persib"})
        assert len(posts) == 1
        assert posts[0]["video_id"] == "test123"

    @pytest.mark.asyncio
    async def test_handler_sends_output(
        self, mocker: MockerFixture
    ) -> None:
        """handler() should call send_output for each post."""
        mock_event = mocker.MagicMock()
        mock_event.payload.model_dump.return_value = {"video_id": "x"}

        async def mock_get_user_posts(*args, **kwargs):
            yield mock_event

        mocker.patch(
            "controllers.tiktok.TikTokAPI.get_user_posts",
            side_effect=mock_get_user_posts,
        )
        mocker.patch("controllers.tiktok.TikTokAPI.start", new_callable=mocker.AsyncMock)
        mocker.patch("controllers.tiktok.TikTokAPI.stop", new_callable=mocker.AsyncMock)

        send_spy = mocker.patch.object(TikTokUserPosts, "send_output")

        kwargs = {"unique_id": "persib", "destination": "std", "count": 5, "max_pages": 1}
        ctl = TikTokUserPosts(**kwargs)
        await ctl.handler({"unique_id": "persib"})

        assert send_spy.call_count >= 1
