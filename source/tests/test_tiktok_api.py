"""Tests for library/tiktok_api.py — TikTokAPI client."""

from __future__ import annotations

import copy

import pytest
from pytest_mock import MockerFixture

from library.tiktok_api import TikTokAPI
from library.schemas import KafkaEvent


class TestTikTokAPI:
    @pytest.mark.asyncio
    async def test_start_creates_client(self, crawler_settings) -> None:
        api = TikTokAPI(crawler_settings)
        await api.start()
        assert api._client is not None
        await api.stop()

    @pytest.mark.asyncio
    async def test_stop_closes_client(self, crawler_settings) -> None:
        api = TikTokAPI(crawler_settings)
        await api.start()
        await api.stop()
        assert api._client is None

    @pytest.mark.asyncio
    async def test_async_context_manager(self, crawler_settings) -> None:
        async with TikTokAPI(crawler_settings) as api:
            assert api._client is not None
        assert api._client is None

    @pytest.mark.asyncio
    async def test_search_yields_kafka_events(
        self, mocker: MockerFixture, crawler_settings, sample_search_response_dict: dict
    ) -> None:
        api = TikTokAPI(crawler_settings)

        mock_response = mocker.MagicMock()
        mock_response.json.return_value = sample_search_response_dict
        mock_response.raise_for_status.return_value = None

        mock_client = mocker.AsyncMock()
        mock_client.post.return_value = mock_response
        api._client = mock_client

        events = [e async for e in api.search_posts(query="persib", max_pages=1)]
        assert len(events) == 1
        assert isinstance(events[0], KafkaEvent)

    @pytest.mark.asyncio
    async def test_search_paginates(
        self, mocker: MockerFixture, crawler_settings, sample_search_response_dict: dict
    ) -> None:
        api = TikTokAPI(crawler_settings)
        call_count = 0

        async def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            resp = mocker.MagicMock()
            resp.raise_for_status.return_value = None
            data = copy.deepcopy(sample_search_response_dict)
            if call_count == 1:
                data["data"]["cursor"] = 12
            else:
                data["data"]["cursor"] = 24
            resp.json.return_value = data
            return resp

        mock_client = mocker.AsyncMock()
        mock_client.post = mocker.AsyncMock(side_effect=side_effect)
        api._client = mock_client

        events = [e async for e in api.search_posts(query="persib", max_pages=2)]
        assert len(events) == 2

    @pytest.mark.asyncio
    async def test_search_stops_when_cursor_unchanged(
        self, mocker: MockerFixture, crawler_settings, sample_search_response_dict: dict
    ) -> None:
        api = TikTokAPI(crawler_settings)
        data = copy.deepcopy(sample_search_response_dict)
        data["data"]["cursor"] = None

        mock_response = mocker.MagicMock()
        mock_response.json.return_value = data
        mock_response.raise_for_status.return_value = None
        mock_client = mocker.AsyncMock()
        mock_client.post.return_value = mock_response
        api._client = mock_client

        events = [e async for e in api.search_posts(query="persib", max_pages=5)]
        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_fetch_post_by_id(
        self, mocker: MockerFixture, crawler_settings, sample_search_response_dict: dict
    ) -> None:
        api = TikTokAPI(crawler_settings)

        mock_response = mocker.MagicMock()
        mock_response.json.return_value = sample_search_response_dict
        mock_response.raise_for_status.return_value = None
        mock_client = mocker.AsyncMock()
        mock_client.post.return_value = mock_response
        api._client = mock_client

        event = await api.fetch_post("7123456789012345678")
        assert event is not None
        assert isinstance(event, KafkaEvent)

    def test_default_headers(self) -> None:
        headers = TikTokAPI._default_headers()
        assert headers["Content-Type"] == "application/x-www-form-urlencoded; charset=UTF-8"
        assert headers["Origin"] == "https://www.tikwm.com"
        assert headers["X-Requested-With"] == "XMLHttpRequest"
