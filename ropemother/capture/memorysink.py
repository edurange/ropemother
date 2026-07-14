#!/usr/bin/env python3
# ropemother/capture/memorysink.py

"""In-memory capture sink for local development and fixtures."""

from typing import override

from ropemother.capture.sink import CaptureSink
from ropemother.capture.writer import CaptureRecord, CaptureRecordSource
from ropemother.format.registry import PortableFormatRegistration
from ropemother.message.records import CapturedMessage
from ropemother.message.symbols import MessageSymbolRegistration

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-06T18:35:51+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class InMemoryCaptureSink(CaptureSink, CaptureRecordSource):
    """Capture sink that stores records in memory."""
    _records: list[CaptureRecord]

    def __init__(self) -> None:
        self._records = []

    @property
    def records(self) -> tuple[CaptureRecord, ...]:
        return tuple(self._records)

    @property
    @override
    def capture_record_count(self) -> int:
        return len(self._records)

    @override
    def read_capture_records(
        self, start_index: int, count: int
    ) -> tuple[CaptureRecord, ...]:
        if start_index < 0:
            raise ValueError("start_index must be non-negative")
        if count < 0:
            raise ValueError("count must be non-negative")

        stop_index = start_index + count
        return tuple(self._records[start_index:stop_index])

    @override
    def capture(self, message: CapturedMessage) -> None:
        self._records.append(message)

    @override
    def capture_format_event(
        self, registration: PortableFormatRegistration
    ) -> None:
        self._records.append(registration)

    @override
    def capture_symbol_event(
        self, registration: MessageSymbolRegistration
    ) -> None:
        self._records.append(registration)
