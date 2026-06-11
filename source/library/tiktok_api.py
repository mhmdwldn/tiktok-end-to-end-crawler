"""
TikTok API client for the tikwm.com proxy.

Provides async methods to search posts and fetch individual posts.
Used by controllers as the HTTP data-access layer.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncIterator, Optional

import httpx

from library.config import TikTokCrawlerSettings
from library.schemas import (
    KafkaEvent,
    TikTokPost,
    TikTokSearchRequest,
    TikTokSearchResponse,
    TikTokUserPostsRequest,
)

logger = logging.getLogger(__name__)


class TikTokAPI:
    """Async HTTP client for the tikwm.com TikTok proxy API.

    Handles rate limiting, retries, and response parsing.

    Example::

        api = TikTokAPI(settings.crawler)
        await api.start()
        async for event in api.search_posts("persib"):
            print(event.payload.title)
        await api.stop()
    """

    def __init__(self, settings: TikTokCrawlerSettings, cookies: str | None = None) -> None:
        self._settings = settings
        self._cookies_override = cookies
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_delay: float = (
            1.0 / settings.rate_limit_rps if settings.rate_limit_rps > 0 else 0.0
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Create the async HTTP client."""
        # Prefer explicit override; fall back to config
        cookie_source = self._cookies_override or self._settings.cookies
        cookies = None
        if cookie_source:
            cookies = dict(
                pair.split("=", 1)
                for pair in cookie_source.split("; ")
                if "=" in pair
            )

        self._client = httpx.AsyncClient(
            base_url=self._settings.base_url,
            timeout=httpx.Timeout(self._settings.request_timeout),
            headers=self._default_headers(),
            cookies=cookies,
            follow_redirects=True,
            proxy=self._settings.proxy_url,
        )
        logger.info("TikTokAPI client created (base_url=%s, cookies=%s)",
                     self._settings.base_url, "yes" if cookies else "no")

    async def stop(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.info("TikTokAPI client stopped")

    async def __aenter__(self) -> TikTokAPI:
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def search_posts(
        self,
        query: str,
        max_pages: int = 1,
        count: int = 12,
        hd: int = 1,
        **kwargs: Any,
    ) -> AsyncIterator[KafkaEvent]:
        """Paginate through search results for *query*.

        Yields:
            :class:`KafkaEvent` per post found.
        """
        cursor = kwargs.get("cursor", 0)
        pages_fetched = 0

        while pages_fetched < max_pages:
            request = TikTokSearchRequest(
                keywords=query,
                count=count,
                cursor=cursor,
                hd=hd,
            )
            response = await self._do_search(request)

            if response is None:
                logger.warning("Empty response for query=%r cursor=%d", query, cursor)
                break

            for post in response.data.videos:
                yield self._post_to_event(post, metadata={"query": query, "cursor": cursor})

            pages_fetched += 1

            if response.data.cursor is not None and response.data.cursor != cursor:
                cursor = response.data.cursor
            else:
                break

    async def fetch_post(self, identifier: str, **kwargs: Any) -> Optional[KafkaEvent]:
        """Fetch a single post by video ID (best-effort via search)."""
        async for event in self.search_posts(query=identifier, max_pages=1, count=1, **kwargs):
            return event
        return None

    async def get_user_posts(
        self,
        unique_id: str,
        max_pages: int = 1,
        count: int = 12,
        hd: int = 1,
        **kwargs: Any,
    ) -> AsyncIterator[KafkaEvent]:
        """Fetch all posts from a specific TikTok user by ``unique_id``.

        Example::

            async for event in api.get_user_posts("@persib", max_pages=3):
                print(event.payload.title)

        Args:
            unique_id: TikTok username (e.g. ``@persib`` or ``persib``).
            max_pages: Maximum pages to fetch.
            count: Results per page (1–50).
            hd: HD quality flag.

        Yields:
            :class:`KafkaEvent` per post found.
        """
        cursor = kwargs.get("cursor", 0)
        pages_fetched = 0

        while pages_fetched < max_pages:
            request = TikTokUserPostsRequest(
                unique_id=unique_id,
                count=count,
                cursor=cursor,
                hd=hd,
            )
            response = await self._do_user_posts_request(request)

            if response is None:
                logger.warning("Empty response for unique_id=%r cursor=%d", unique_id, cursor)
                break

            for post in response.data.videos:
                yield self._post_to_event(post, metadata={"unique_id": unique_id, "cursor": cursor})

            pages_fetched += 1

            if response.data.cursor is not None and response.data.cursor != cursor:
                cursor = response.data.cursor
            else:
                break

    async def get_user_stories(
        self,
        unique_id: str,
        max_pages: int = 1,
        count: int = 12,
        hd: int = 1,
        **kwargs: Any,
    ) -> AsyncIterator[KafkaEvent]:
        """Fetch stories from a specific TikTok user by ``unique_id``.

        Uses ``POST /api/user/story``.

        Example::

            async for event in api.get_user_stories("@zavann_d", max_pages=2):
                print(event.payload.title)

        Args:
            unique_id: TikTok username (e.g. ``@zavann_d`` or ``zavann_d``).
            max_pages: Maximum pages to fetch.
            count: Results per page (1–50).
            hd: HD quality flag.

        Yields:
            :class:`KafkaEvent` per story found.
        """
        cursor = kwargs.get("cursor", 0)
        pages_fetched = 0

        while pages_fetched < max_pages:
            request = TikTokUserPostsRequest(
                unique_id=unique_id,
                count=count,
                cursor=cursor,
                hd=hd,
            )
            response = await self._do_user_story_request(request)

            if response is None:
                logger.warning("Empty response for unique_id=%r cursor=%d", unique_id, cursor)
                break

            for post in response.data.videos:
                yield self._post_to_event(post, metadata={"unique_id": unique_id, "cursor": cursor, "type": "story"})

            pages_fetched += 1

            if response.data.cursor is not None and response.data.cursor != cursor:
                cursor = response.data.cursor
            else:
                break

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _do_search(self, request: TikTokSearchRequest) -> Optional[TikTokSearchResponse]:
        """Execute a single HTTP POST to the search endpoint with retries."""
        return await self._do_request(self._settings.search_endpoint, request.to_form_data())

    async def _do_user_posts_request(self, request: TikTokUserPostsRequest) -> Optional[TikTokSearchResponse]:
        """Execute a single HTTP POST to the user/posts endpoint with retries."""
        endpoint = self._settings.user_posts_endpoint
        return await self._do_request(endpoint, request.to_form_data())

    async def _do_user_story_request(self, request: TikTokUserPostsRequest) -> Optional[TikTokSearchResponse]:
        """Execute a single HTTP POST to the user/story endpoint with retries."""
        endpoint = self._settings.user_story_endpoint
        return await self._do_request(endpoint, request.to_form_data())

    async def _do_request(self, endpoint: str, data: dict[str, str]) -> Optional[TikTokSearchResponse]:
        """Execute a single HTTP POST with retries and parse the response."""
        assert self._client is not None, "HTTP client not initialised"

        max_retries = max(self._settings.max_retries, 1)  # at least 1 attempt
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                await self._throttle()
                resp = await self._client.post(endpoint, data=data)
                resp.raise_for_status()
                payload = resp.json()
                return TikTokSearchResponse.model_validate(payload)
            except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError) as exc:
                last_exc = exc
                wait = self._settings.retry_backoff ** attempt
                logger.warning(
                    "Request attempt %d/%d failed (%s): %s. Retrying in %.1fs ...",
                    attempt, self._settings.max_retries, endpoint, exc, wait,
                )
                await asyncio.sleep(wait)

        logger.error("All %d request attempts failed for %s.", self._settings.max_retries, endpoint)
        raise last_exc  # type: ignore[misc]

    async def _throttle(self) -> None:
        """Enforce rate limit."""
        if self._rate_delay > 0:
            await asyncio.sleep(self._rate_delay)

    @staticmethod
    def _default_headers() -> dict[str, str]:
        """Return default HTTP headers matching the cURL payload."""
        return {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://www.tikwm.com",
            "Referer": "https://www.tikwm.com/",
            "Sec-Ch-Ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
            "Sec-Ch-Ua-Arch": '"x86"',
            "Sec-Ch-Ua-Bitness": '"64"',
            "Sec-Ch-Ua-Full-Version": '"149.0.7827.54"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Ch-Ua-Platform-Version": '"19.0.0"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Requested-With": "XMLHttpRequest",
        }

    @staticmethod
    def _post_to_event(
        post: TikTokPost, metadata: Optional[dict[str, Any]] = None
    ) -> KafkaEvent:
        """Wrap a parsed TikTokPost in a KafkaEvent envelope."""
        return KafkaEvent(
            event_type="tiktok.post.scraped",
            payload=post,
            metadata=metadata or {},
        )
