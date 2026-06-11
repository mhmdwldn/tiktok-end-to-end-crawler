"""Standard-input driver — reads jobs from a keyword or file."""

import json
import logging
import os

from helpers.input.driver import InputDriver

logger = logging.getLogger(__name__)


class StdInputDriver(InputDriver):
    """Read jobs from a JSON file or fall back to a single empty job."""

    name = "std"

    def __init__(self, source: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._source = source
        self._jobs: list[dict] = []
        self._load()

    def _load(self):
        """Load jobs from the source (JSON file path) or use empty job."""
        if self._source and os.path.isfile(self._source):
            with open(self._source, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    self._jobs = data
                else:
                    self._jobs = [data]
            logger.info("Loaded %d jobs from %s", len(self._jobs), self._source)
        else:
            # Single empty job — controller will use CLI args
            self._jobs = [{}]

    def exception_handler(self, e, **kwargs):
        action = kwargs.get("action", "delete")
        logger.warning("STD input exception (action=%s): %s", action, e)

    def __iter__(self):
        yield from self._jobs
