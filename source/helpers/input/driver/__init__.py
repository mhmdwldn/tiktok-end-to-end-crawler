"""Input driver — abstract base class."""

from abc import ABC, abstractmethod


class InputDriver(ABC):
    """Abstract input driver — yields job dicts."""

    name: str | None = None

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def exception_handler(self, e, **kwargs):
        """Handle exceptions on the input (e.g. bury, delete)."""

    @abstractmethod
    def __iter__(self):
        """Yield job dicts."""
