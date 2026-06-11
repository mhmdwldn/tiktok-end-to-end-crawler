"""Tests for helpers/output/driver/* — output drivers."""

from __future__ import annotations

import json
import os
import tempfile

import pytest

from helpers.output.driver.std import StdOutputDriver
from helpers.output.driver.file import FileOutputDriver
from helpers.output.driver.factory import OutputDriverFactory
from exception.exception import OutputDriverNotRecognizeException


class TestOutputDriverFactory:
    def test_create_std_driver(self) -> None:
        driver = OutputDriverFactory.create_output_driver(destination="std")
        assert isinstance(driver, StdOutputDriver)
        assert driver.name == "std"

    def test_create_file_driver(self) -> None:
        path = tempfile.mktemp(suffix=".json")
        driver = OutputDriverFactory.create_output_driver(
            destination="file", output=path,
        )
        try:
            assert isinstance(driver, FileOutputDriver)
            assert driver.name == "file"
        finally:
            driver.close()

    def test_unknown_destination_raises(self) -> None:
        with pytest.raises(OutputDriverNotRecognizeException):
            OutputDriverFactory.create_output_driver(destination="unknown")


class TestStdOutputDriver:
    def test_put_prints(self, capsys) -> None:
        driver = StdOutputDriver()
        driver.put("hello world")
        captured = capsys.readouterr()
        assert "hello world" in captured.out

    def test_close_noop(self) -> None:
        driver = StdOutputDriver()
        driver.close()  # should not raise


class TestFileOutputDriver:
    def test_put_writes_to_file(self) -> None:
        path = tempfile.mktemp(suffix=".json")
        driver = FileOutputDriver(path=path)
        try:
            driver.put("line1")
            driver.put("line2")
            driver.file.flush()  # ensure buffered writes hit disk

            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            assert len(lines) == 2
        finally:
            driver.close()
            if os.path.exists(path):
                os.remove(path)

    def test_close(self) -> None:
        path = tempfile.mktemp(suffix=".json")
        driver = FileOutputDriver(path=path)
        driver.close()
        assert driver.file is None
        if os.path.exists(path):
            os.remove(path)
