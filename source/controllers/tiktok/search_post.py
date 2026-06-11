"""TikTok search-post controller — the main crawling handler."""

import logging

from controllers.tiktok import TikTokControllers

logger = logging.getLogger(__name__)


class TikTokSearchPost(TikTokControllers):
    """Handler that executes a TikTok post search and sends results to output.

    Job dict fields (also overridable via CLI):
        - ``keyword``: search query
        - ``count``: results per page (default 12)
        - ``max_pages``: number of pages to crawl (default 1)
        - ``hd``: HD quality flag (default 1)
        - ``output_file``: optional path to save raw JSON results
    """

    log = logging.getLogger("tiktok.search_post")

    async def handler(self, job: dict):
        """Execute the search and pipe results to the output driver."""
        keyword = self.parse_job_keyword(job)
        cnt = self.parse_job_count(job)
        max_pages = self.parse_job_max_pages(job)
        hd = int(self.args.get("hd", job.get("hd", 1)))
        output_file = self.args.get("output_file") or job.get("output_file")

        self.log.info(
            "Searching TikTok: keyword=%r  count=%d  max_pages=%d  hd=%d",
            keyword, cnt, max_pages, hd,
        )

        await self._ensure_api()

        try:
            # Only accumulate the full list when we'll need to save to file
            posts_data: list[dict] | None = [] if output_file else None
            post_count = 0
            async for event in self.api.search_posts(
                query=keyword,
                max_pages=max_pages,
                count=cnt,
                hd=hd,
            ):
                # model_dump for logging + optional file save
                post_dict = event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
                # Serialize once to JSON string for the output driver
                post_json = event.payload.model_dump_json(by_alias=True, exclude_none=True)

                if posts_data is not None:
                    posts_data.append(post_dict)
                post_count += 1

                post_id = post_dict.get("video_id", "?")
                title = str(post_dict.get("title", ""))[:80]
                self.log.info("  [post_id=%s] %s", post_id, title)

                # Send pre-serialized JSON string to output driver
                self.send_output(post_json)

            if output_file and posts_data:
                self.save_to_file(posts_data, output_file)

            self.log.info("Search complete — %d posts scraped for %r", post_count, keyword)

        finally:
            await self._close_api()

    # ------------------------------------------------------------------
    # Convenience: synchronous scrape (no output driver)
    # ------------------------------------------------------------------

    async def scrape_to_json(self, job: dict) -> list[dict]:
        """Scrape and return raw dicts — no output driver involved.

        Useful when called programmatically.
        """
        keyword = self.parse_job_keyword(job)
        cnt = self.parse_job_count(job)
        max_pages = self.parse_job_max_pages(job)
        hd = int(self.args.get("hd", job.get("hd", 1)))

        posts: list[dict] = []
        await self._ensure_api()
        try:
            async for event in self.api.search_posts(
                query=keyword,
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