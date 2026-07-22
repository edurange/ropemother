#!/usr/bin/env python3
# ropemother/capture/filesink.py

"""JSON Lines capture sink for local files and fixtures."""

from pathlib import Path
from typing import override

from ropemother.capture.jsonrecords import (
    captured_message_record,
    format_registration_record,
    symbol_registration_record,
)
from ropemother.capture.sink import CaptureSink
from ropemother.format.registry import PortableFormatRegistration
from ropemother.message.records import CapturedMessage
from ropemother.message.symbols import MessageSymbolRegistration
from ropemother.util.onelinejson import JSONRecord, oneline_serialize

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T04:04:36+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


class JSONLinesCaptureSink(CaptureSink):
    """Capture sink that writes records as JSON Lines."""
    _path: Path
    _encoding: str

    def __init__(
        self,
        path: str | Path,
        *,
        encoding: str = "utf-8",
        append: bool = True,
    ) -> None:
        self._path = Path(path)
        self._encoding = encoding
        if not append:
            self._path.write_text("", encoding=self._encoding)

    @property
    def path(self) -> Path:
        return self._path

    @override
    def capture(self, message: CapturedMessage) -> None:
        record = captured_message_record(message)
        self._write_record(record)

    @override
    def capture_format_event(
        self, registration: PortableFormatRegistration
    ) -> None:
        record = format_registration_record(registration)
        self._write_record(record)

    @override
    def capture_symbol_event(
        self, registration: MessageSymbolRegistration
    ) -> None:
        record = symbol_registration_record(registration)
        self._write_record(record)

    def _write_record(self, record: JSONRecord) -> None:
        line = oneline_serialize(record)
        with self._path.open("a", encoding=self._encoding) as stream:
            stream.write(line)
            stream.write("\n")
