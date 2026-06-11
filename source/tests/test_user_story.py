"""Tests for controllers/tiktok/user_story.py — TikTokUserStory controller."""

from __future__ import annotations

import pytest
from pytest_mock import MockerFixture

from controllers.tiktok.user_story import TikTokUserStory


class TestTikTokUserStoryController:
    """Tests for TikTokUserStory controller."""

    @pytest.mark.asyncio
    async def test_scrape_to_json(
        self, mocker: MockerFixture
    ) -> None:
        """scrape_to_json() should return a list of raw story dicts."""
        mock_event = mocker.MagicMock()
        mock_event.payload.model_dump.return_value = {
            "video_id": "story123",
            "title": "My Story",
        }

        async def mock_get_user_stories(*args, **kwargs):
            yield mock_event

        mocker.patch(
            "controllers.tiktok.TikTokAPI.get_user_stories",
            side_effect=mock_get_user_stories,
        )
        mocker.patch("controllers.tiktok.TikTokAPI.start", new_callable=mocker.AsyncMock)
        mocker.patch("controllers.tiktok.TikTokAPI.stop", new_callable=mocker.AsyncMock)

        kwargs = {"unique_id": "@zavann_d", "count": 10, "max_pages": 1}
        ctl = TikTokUserStory(**kwargs)

        posts = await ctl.scrape_to_json({"unique_id": "zavann_d"})
        assert len(posts) == 1
        assert posts[0]["video_id"] == "story123"

    @pytest.mark.asyncio
    async def test_handler_sends_output(
        self, mocker: MockerFixture
    ) -> None:
        """handler() should call send_output for each story."""
        mock_event = mocker.MagicMock()
        mock_event.payload.model_dump.return_value = {"video_id": "x"}
        mock_event.payload.model_dump_json.return_value = '{"video_id":"x"}'

        async def mock_get_user_stories(*args, **kwargs):
            yield mock_event

        mocker.patch(
            "controllers.tiktok.TikTokAPI.get_user_stories",
            side_effect=mock_get_user_stories,
        )
        mocker.patch("controllers.tiktok.TikTokAPI.start", new_callable=mocker.AsyncMock)
        mocker.patch("controllers.tiktok.TikTokAPI.stop", new_callable=mocker.AsyncMock)

        send_spy = mocker.patch.object(TikTokUserStory, "send_output")

        kwargs = {"unique_id": "zavann_d", "destination": "std", "count": 5, "max_pages": 1}
        ctl = TikTokUserStory(**kwargs)
        await ctl.handler({"unique_id": "zavann_d"})

        assert send_spy.call_count >= 1

    def test_parse_unique_id(self) -> None:
        """_parse_unique_id should normalize the username."""
        ctl = TikTokUserStory(unique_id="zavann_d")
        assert ctl._parse_unique_id({"unique_id": "zavann_d"}) == "@zavann_d"
        assert ctl._parse_unique_id({"unique_id": "@zavann_d"}) == "@zavann_d"

    def test_parse_unique_id_empty_raises(self) -> None:
        """Empty unique_id should raise ValueError."""
        ctl = TikTokUserStory(unique_id="")
        with pytest.raises(ValueError, match="unique_id is required"):
            ctl._parse_unique_id({"unique_id": ""})