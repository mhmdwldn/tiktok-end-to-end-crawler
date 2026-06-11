"""Output driver — abstract base class."""

from abc import ABC, abstractmethod


class OutputDriver(ABC):
    """Abstract output driver.

    Subclass this to add new output destinations (Kafka, ES, file, etc.).
    """

    name: str | None = None

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def put(self, output: str, **kwargs):
        """Write/emit *output* to the destination."""

    @abstractmethod
    def close(self):
        """Release any resources held by the driver."""
