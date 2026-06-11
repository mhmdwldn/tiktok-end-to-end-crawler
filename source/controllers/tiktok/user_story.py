"""TikTok user-story controller — fetch stories from a user account."""

import logging

from controllers.tiktok import TikTokControllers

logger = logging.getLogger(__name__)


class TikTokUserStory(TikTokControllers):
    """Handler that fetches stories from a TikTok user by ``unique_id``.

    Uses ``POST /api/user/story``.  Requires Cloudflare cookies.

    Job dict fields (also overridable via CLI):
        - ``unique_id``: TikTok username (e.g. ``@zavann_d`` or ``zavann_d``)
        - ``count``: results per page (default 12)
        - ``max_pages``: number of pages to crawl (default 1)
        - ``hd``: HD quality flag (default 1)
        - ``output_file``: optional path to save raw JSON results
    """

    log = logging.getLogger("tiktok.user_story")

    async def handler(self, job: dict):
        """Execute the user-story fetch and pipe results to the output driver."""
        unique_id = self._parse_unique_id(job)
        cnt = self.parse_job_count(job)
        max_pages = self.parse_job_max_pages(job)
        hd = int(self.args.get("hd", job.get("hd", 1)))
        output_file = self.args.get("output_file") or job.get("output_file")

        self.log.info(
            "Fetching user stories: unique_id=%r  count=%d  max_pages=%d  hd=%d",
            unique_id, cnt, max_pages, hd,
        )

        await self._ensure_api()

        try:
            posts_data: list[dict] | None = [] if output_file else None
            story_count = 0
            async for event in self.api.get_user_stories(
                unique_id=unique_id,
                max_pages=max_pages,
                count=cnt,
                hd=hd,
            ):
                post_dict = event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
                post_json = event.payload.model_dump_json(by_alias=True, exclude_none=True)

                if posts_data is not None:
                    posts_data.append(post_dict)
                story_count += 1

                post_id = post_dict.get("video_id", "?")
                title = str(post_dict.get("title", ""))[:80]
                self.log.info("  [story_id=%s] %s", post_id, title)

                self.send_output(post_json)

            if output_file and posts_data:
                self.save_to_file(posts_data, output_file)

            self.log.info("User-story complete — %d stories for %r", story_count, unique_id)

        finally:
            await self._close_api()

    # ------------------------------------------------------------------
    # Convenience: synchronous scrape (no output driver)
    # ------------------------------------------------------------------

    async def scrape_to_json(self, job: dict) -> list[dict]:
        """Scrape user stories and return raw dicts — no output driver involved."""
        unique_id = self._parse_unique_id(job)
        cnt = self.parse_job_count(job)
        max_pages = self.parse_job_max_pages(job)
        hd = int(self.args.get("hd", job.get("hd", 1)))

        posts: list[dict] = []
        await self._ensure_api()
        try:
            async for event in self.api.get_user_stories(
                unique_id=unique_id,
                max_pages=max_pages,
                count=cnt,
                hd=hd,
            ):
                posts.append(
                    event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
                )
        finally:
            await self._close_api()

        return posts

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _parse_unique_id(self, job: dict) -> str:
        """Extract unique_id from job or CLI args — @ prefix is preserved."""
        unique_id = self.args.get("unique_id") or job.get("unique_id", "")
        unique_id = str(unique_id).strip().lstrip("@")
        if not unique_id:
            raise ValueError(
                "unique_id is required for --type user-story. "
                "Provide --unique-id @<username>"
            )
        return f"@{unique_id}"