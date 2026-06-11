"""TikTok controllers — shared base for all TikTok-related handlers."""

import json
import logging
import os

from controllers import Controllers
from library.tiktok_api import TikTokAPI


class TikTokControllers(Controllers):
    """Shared base for TikTok crawler controllers.

    Sets up the TikTokAPI client and provides helper methods
    for loading config, parsing jobs, and saving intermediate results.
    """

    log = logging.getLogger("tiktok.controller")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Load settings from library
        from library.config import settings

        self.settings = settings
        self.api: TikTokAPI | None = None

    async def _ensure_api(self):
        """Lazily initialise the TikTokAPI client."""
        if self.api is None:
            cookies = self.args.get("cookies") or self.settings.crawler.cookies or None
            self.api = TikTokAPI(self.settings.crawler, cookies=cookies)
            await self.api.start()

    async def _close_api(self):
        """Tear down the TikTokAPI client."""
        if self.api is not None:
            await self.api.stop()
            self.api = None

    # ------------------------------------------------------------------
    # Job helpers
    # ------------------------------------------------------------------

    def parse_job_keyword(self, job: dict, default: str = "") -> str:
        """Extract keyword from a job dict, with optional CLI override."""
        keyword = job.get("keyword", default)
        if self.args.get("keyword"):
            keyword = self.args["keyword"]
        return str(keyword).strip('"').strip("'")

    def parse_job_count(self, job: dict, default: int = 12) -> int:
        """Extract count from job dict or CLI args."""
        if self.args.get("count"):
            return int(self.args["count"])
        return int(job.get("count", default))

    def parse_job_max_pages(self, job: dict, default: int = 1) -> int:
        """Extract max_pages from job dict or CLI args."""
        if self.args.get("max_pages"):
            return int(self.args["max_pages"])
        return int(job.get("max_pages", default))

    def save_to_file(self, data, path: str):
        """Save JSON-serialisable *data* to a local file."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        self.log.info("Saved to %s", path)
