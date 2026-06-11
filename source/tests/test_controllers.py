"""Tests for controllers/tiktok/search_post.py — TikTokSearchPost controller."""

from __future__ import annotations

import pytest
from pytest_mock import MockerFixture

from controllers.tiktok.search_post import TikTokSearchPost


class TestTikTokSearchPostController:
    """Tests for TikTokSearchPost controller."""

    @pytest.mark.asyncio
    async def test_scrape_to_json(
        self, mocker: MockerFixture, sample_search_response_dict: dict
    ) -> None:
        """scrape_to_json() should return a list of raw post dicts."""
        # Mock the TikTokAPI.search_posts method
        mock_event = mocker.MagicMock()
        mock_event.payload.model_dump.return_value = {
            "video_id": "test123",
            "title": "Test Post",
        }

        async def mock_search(*args, **kwargs):
            yield mock_event

        mocker.patch(
            "controllers.tiktok.TikTokAPI.search_posts",
            side_effect=mock_search,
        )
        mocker.patch(
            "controllers.tiktok.TikTokAPI.start",
            new_callable=mocker.AsyncMock,
        )
        mocker.patch(
            "controllers.tiktok.TikTokAPI.stop",
            new_callable=mocker.AsyncMock,
        )

        kwargs = {"keyword": "test", "count": 10, "max_pages": 1}
        ctl = TikTokSearchPost(**kwargs)

        posts = await ctl.scrape_to_json({"keyword": "test"})
        assert len(posts) == 1
        assert posts[0]["video_id"] == "test123"

    @pytest.mark.asyncio
    async def test_handler_sends_output(
        self, mocker: MockerFixture, sample_search_response_dict: dict
    ) -> None:
        """handler() should call send_output for each post."""
        # This test verifies the handler loop sends data to output
        mock_event = mocker.MagicMock()
        mock_event.payload.model_dump.return_value = {"video_id": "x"}

        async def mock_search(*args, **kwargs):
            yield mock_event

        mocker.patch(
            "controllers.tiktok.TikTokAPI.search_posts",
            side_effect=mock_search,
        )
        mocker.patch(
            "controllers.tiktok.TikTokAPI.start",
            new_callable=mocker.AsyncMock,
        )
        mocker.patch(
            "controllers.tiktok.TikTokAPI.stop",
            new_callable=mocker.AsyncMock,
        )

        send_spy = mocker.patch.object(TikTokSearchPost, "send_output")

        kwargs = {"keyword": "test", "destination": "std", "count": 5, "max_pages": 1}
        ctl = TikTokSearchPost(**kwargs)
        await ctl.handler({"keyword": "test"})

        assert send_spy.call_count >= 1
