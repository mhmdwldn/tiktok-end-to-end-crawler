"""Base Controllers — abstract class providing input, output, config, logging."""

import json
import logging
import re
from abc import ABC, abstractmethod
from typing import Any, Optional

from helpers.input import Input
from helpers.output import Output


class Controllers(ABC):
    """Abstract base controller for all crawlers.

    Provides:
        - Input job iteration (via ``self.input``)
        - Output publishing (via ``self.output``)
        - Structured logging
        - Exception handling dispatch

    Subclasses must override :meth:`handler`.

    Lifecycle::

        controller = MyController(**kwargs)
        await controller.main()
    """

    job: dict = {}
    log: logging.Logger = logging.getLogger(__name__)

    def __init__(self, *args, **kwargs):
        self.args = kwargs

        # Input driver
        if kwargs.get("source"):
            self.input_name = kwargs.get("input")
            self.source_name = kwargs.get("source")
            self.input: Input | list[dict] = Input(*args, **kwargs)
        else:
            self.input = [{}]

        # Output driver
        if kwargs.get("destination"):
            self.output_name = kwargs.get("output")
            self.destination_name = kwargs.get("destination")
            self.output: Output = Output(*args, **kwargs)
        else:
            self.output = None

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def main(self):
        """Iterate over input jobs and invoke :meth:`handler` for each."""
        jobs = self.input or [{}]
        for job in jobs:
            if not job:
                self.log.info("No jobs available, running with defaults")
            self.job = job
            try:
                await self.handler(job)
            except Exception as e:
                self.exceptions_handler(e)

    # ------------------------------------------------------------------
    # Subclass contract
    # ------------------------------------------------------------------

    @abstractmethod
    async def handler(self, job: dict):
        """Process a single job dict. Must be implemented by subclasses."""
        ...

    # ------------------------------------------------------------------
    # Exception handling
    # ------------------------------------------------------------------

    def exceptions_handler(self, e: Exception):
        """Default exception handler — logs and delegates to input driver.

        Critical errors (KeyboardInterrupt, SystemExit) are re-raised.
        """
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
            raise

        self.log.error("%s: %s", type(e).__name__, e)
        msg = str(e)

        if re.search("Too Many Requests", msg):
            if isinstance(self.input, Input):
                self.input.exception_handler(e, action="bury")
            return

        if isinstance(self.input, Input):
            self.input.exception_handler(e, action="delete")
        # When self.input is a plain list [{}], there is no driver to notify.
        # The error is still logged for visibility.

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    def send_output(self, data: Any):
        """Send *data* to the configured output driver.

        Accepts dict/list (serialised to JSON) or pre-serialised str/bytes.
        """
        if self.output is None:
            self.log.debug("No output driver configured — skipping")
            return

        if isinstance(data, str):
            payload = data
        elif isinstance(data, (dict, list)):
            payload = json.dumps(data, ensure_ascii=False, default=str)
        else:
            payload = str(data)

        self.output.put(payload)
