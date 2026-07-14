#!/usr/bin/env python3
# ropemother/capture/filehistory.py

"""Read-side history over JSON Lines capture files."""

from pathlib import Path
from typing import Any, Iterable, TextIO, cast, override

from ropemother.capture.history import (
    DEFAULT_HISTORY_MAX_COUNT,
    InMemoryCaptureHistory,
    MessageHistory,
    MessageHistoryError,
    MessageHistoryPage,
)
from ropemother.capture.jsonrecords import capture_record_from_record
from ropemother.capture.writer import CaptureRecord, CaptureRecordSource
from ropemother.format.portableformat import PortableFormat
from ropemother.message.records import BusOperation
from ropemother.util.onelinejson import JSONRecord, oneline_deserialize

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T19:57:39+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class MessageHistorySourceError(OSError, MessageHistoryError):
    """Raised when a history source cannot be read."""
    pass


class MessageHistoryFormatError(ValueError, MessageHistoryError):
    """Raised when a history source contains invalid capture records."""
    pass


class _JSONLinesCaptureRecordSource(CaptureRecordSource):
    """Persistent indexed source backed by a JSON Lines capture file."""
    _path: Path
    _encoding: str
    _record_offsets: list[int]
    _indexed_position: int

    def __init__(self, path: Path, encoding: str) -> None:
        self._path = path
        self._encoding = encoding
        self._record_offsets = []
        self._indexed_position = 0

    @property
    def path(self) -> Path:
        return self._path

    @property
    @override
    def capture_record_count(self) -> int:
        self._refresh_index()
        return len(self._record_offsets)

    @override
    def read_capture_records(
        self, start_index: int, count: int
    ) -> tuple[CaptureRecord, ...]:
        if start_index < 0:
            raise ValueError("start_index must be non-negative")
        if count < 0:
            raise ValueError("count must be non-negative")

        self._refresh_index()

        stop_index = start_index + count
        offsets = self._record_offsets[start_index:stop_index]
        records = [self._record_at_offset(offset) for offset in offsets]
        return tuple(records)

    def _refresh_index(self) -> None:
        truncated = False
        new_offsets: tuple[int, ...] = ()
        indexed_position = self._indexed_position

        try:
            with self._path.open("r", encoding=self._encoding) as stream:
                stream.seek(0, 2)
                source_end = stream.tell()

                if source_end < self._indexed_position:
                    truncated = True
                else:
                    stream.seek(self._indexed_position)
                    new_offsets, indexed_position = (
                        self._new_record_offsets_from(stream)
                    )
        except UnicodeDecodeError as e:
            raise MessageHistoryFormatError(
                "history source is not valid " + self._encoding
            ) from e
        except OSError as e:
            raise MessageHistorySourceError(
                "history source could not be read: " + str(self._path)
            ) from e

        if truncated:
            raise MessageHistorySourceError(
                "history source appears to have been truncated: "
                + str(self._path)
            )

        self._record_offsets.extend(new_offsets)
        self._indexed_position = indexed_position

    def _new_record_offsets_from(
        self, stream: TextIO
    ) -> tuple[tuple[int, ...], int]:
        offsets: list[int] = []

        line_start = stream.tell()
        line = stream.readline()
        while line != "":
            if line.strip() != "":
                offsets.append(line_start)

            line_start = stream.tell()
            line = stream.readline()

        return (tuple(offsets), stream.tell())

    def _record_at_offset(self, offset: int) -> CaptureRecord:
        line = self._line_at_offset(offset)
        stripped = line.strip()

        if stripped == "":
            raise MessageHistorySourceError(
                "indexed history record could not be read: "
                + str(self._path)
            )

        return self._read_record(stripped)

    def _line_at_offset(self, offset: int) -> str:
        try:
            with self._path.open("r", encoding=self._encoding) as stream:
                stream.seek(offset)
                line = stream.readline()
        except UnicodeDecodeError as e:
            raise MessageHistoryFormatError(
                "history source is not valid " + self._encoding
            ) from e
        except OSError as e:
            raise MessageHistorySourceError(
                "history source could not be read: " + str(self._path)
            ) from e

        return line

    def _read_record(self, line: str) -> CaptureRecord:
        try:
            value = oneline_deserialize(line)
        except ValueError as e:
            raise MessageHistoryFormatError(
                "invalid JSONL capture record JSON"
            ) from e

        json_record = self._json_record_from_value(value)

        try:
            record = capture_record_from_record(json_record)
        except ValueError as e:
            raise MessageHistoryFormatError(
                "invalid JSONL capture record fields"
            ) from e

        return record

    def _json_record_from_value(self, value: object) -> JSONRecord:
        if not isinstance(value, dict):
            raise MessageHistoryFormatError(
                "JSONL capture record must be an object"
            )

        for key in value:
            if not isinstance(key, str):
                raise MessageHistoryFormatError(
                    "JSONL capture record keys must be strings"
                )

        return cast(JSONRecord, value)


class JSONLinesCaptureHistory(MessageHistory):
    """Message history view backed by a JSON Lines capture file."""
    _source: _JSONLinesCaptureRecordSource
    _history: InMemoryCaptureHistory

    def __init__(
        self,
        path: str | Path,
        *,
        encoding: str = "utf-8",
        extra_formats: Iterable[PortableFormat[Any, Any]] = (),
    ) -> None:
        self._source = _JSONLinesCaptureRecordSource(Path(path), encoding)
        self._history = InMemoryCaptureHistory(
            self._source, extra_formats=extra_formats
        )

    @property
    def path(self) -> Path:
        return self._source.path

    @override
    def select(
        self,
        *,
        msg_topic: str | None = None,
        msg_type: str | None = None,
        msg_producer: str | None = None,
        bus_operation: BusOperation | None = None,
        start_sequence: int | None = None,
        stop_sequence: int | None = None,
        max_count: int = DEFAULT_HISTORY_MAX_COUNT,
    ) -> MessageHistoryPage:
        page = self._history.select(
            msg_topic=msg_topic,
            msg_type=msg_type,
            msg_producer=msg_producer,
            bus_operation=bus_operation,
            start_sequence=start_sequence,
            stop_sequence=stop_sequence,
            max_count=max_count,
        )
        return page
