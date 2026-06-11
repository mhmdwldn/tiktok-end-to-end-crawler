"""Input helper — wraps an InputDriver."""

import logging

from helpers.input.driver.factory import InputDriverFactory

logger = logging.getLogger(__name__)


class Input:
    """Input facade that delegates to an InputDriver."""

    def __init__(self, *args, **kwargs):
        self.driver = InputDriverFactory.create_input_driver(*args, **kwargs)
        logger.debug("using %s input driver", self.driver.name)

    def exception_handler(self, e, **kwargs):
        self.driver.exception_handler(e, **kwargs)

    def __iter__(self):
        yield from self.driver
