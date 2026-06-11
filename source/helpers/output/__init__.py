"""Output helper — wraps an OutputDriver."""

import logging

from helpers.output.driver.factory import OutputDriverFactory

logger = logging.getLogger(__name__)


class Output:
    """Output facade that delegates to a driver created by the factory."""

    def __init__(self, *args, **kwargs):
        self.driver = OutputDriverFactory.create_output_driver(*args, **kwargs)
        logger.debug("using %s output driver", self.driver.name)

    def put(self, output: str, **kwargs):
        self.driver.put(output, **kwargs)
