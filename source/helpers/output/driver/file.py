"""File output driver."""

import logging
import os

from helpers.output.driver import OutputDriver

logger = logging.getLogger(__name__)


class FileOutputDriver(OutputDriver):
    """Append output lines to a file on disk."""

    name = "file"

    def __init__(self, path: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path: str | None = path
        self.file = None
        self._extra_files: dict[str, object] = {}
        if path:
            parent = os.path.dirname(path) or "."
            os.makedirs(parent, exist_ok=True)
            self.file = open(path, "a", encoding="utf-8")

    def put(self, output: str, **kwargs):
        if kwargs.get("path"):
            target = kwargs["path"]
            if target not in self._extra_files:
                parent = os.path.dirname(target) or "."
                os.makedirs(parent, exist_ok=True)
                self._extra_files[target] = open(target, "a", encoding="utf-8")
            self._extra_files[target].write(output + "\n")
        elif self.file is not None:
            self.file.write(output + "\n")
        else:
            raise RuntimeError(
                "FileOutputDriver has no output path configured. "
                "Pass -o/--output <path> to specify a file destination."
            )

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
        for f in self._extra_files.values():
            f.close()
        self._extra_files.clear()