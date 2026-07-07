#!/usr/bin/env python3
# ropemother/bootstrap/buffer.py

"""Support for bootstrap buffering of capture records at startup."""

from dataclasses import dataclass

from ropemother.capture.sink import CaptureSink
from ropemother.capture.writer import CaptureRecord, CaptureRecordWriter
from ropemother.exceptions import MessageBusBaseException
from ropemother.format.registry import PortableFormatRegistration
from ropemother.message.records import CapturedMessage
from ropemother.message.symbols import MessageSymbolRegistration

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T07:00:19+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


DEFAULT_BOOTSTRAP_BUFFER_BYTES = 4 * 1024 * 1024
DEFAULT_BOOTSTRAP_BUFFER_RECORD_LIMIT = 256


class BootstrapBufferError(MessageBusBaseException):
    """Base exception for bootstrap buffer errors."""
    pass


class InvalidBootstrapBufferLimitError(ValueError, BootstrapBufferError):
    """Raised when bootstrap buffer limits are invalid."""
    pass


class BootstrapBufferLimitExceededError(BufferError, BootstrapBufferError):
    """Raised when bootstrap buffering exceeds configured limits."""
    pass


@dataclass(frozen=True, kw_only=True)
class BootstrapBufferLimits:
    """Record and payload byte limits for bootstrap buffering."""
    max_records: int = DEFAULT_BOOTSTRAP_BUFFER_RECORD_LIMIT
    max_payload_bytes: int = DEFAULT_BOOTSTRAP_BUFFER_BYTES

    def __post_init__(self) -> None:
        if self.max_records < 1:
            raise InvalidBootstrapBufferLimitError(
                "bootstrap buffer record limit must be positive"
            )
        if self.max_payload_bytes < 0:
            raise InvalidBootstrapBufferLimitError(
                "bootstrap buffer payload byte limit must not be negative"
            )


class BootstrapBuffer(CaptureRecordWriter):
    """Temporary capture record writer used during broker bootstrap."""
    _limits: BootstrapBufferLimits
    _records: list[CaptureRecord]
    _payload_bytes: int

    def __init__(self, *, limits: BootstrapBufferLimits | None = None) -> None:
        if limits is None:
            limits = BootstrapBufferLimits()

        self._limits = limits
        self._records = []
        self._payload_bytes = 0

    @property
    def pending_record_count(self) -> int:
        return len(self._records)

    @property
    def pending_payload_bytes(self) -> int:
        return self._payload_bytes

    def pending_records(self) -> tuple[CaptureRecord, ...]:
        return tuple(self._records)

    def clear(self) -> None:
        self._records.clear()
        self._payload_bytes = 0

    def flush_to(self, capture_sink: CaptureSink) -> None:
        for record in self._records:
            capture_sink.write_record(record)

        self.clear()

    def write_message_record(self, message: CapturedMessage) -> None:
        self._append_record(message)

    def write_format_registration(
        self, registration: PortableFormatRegistration
    ) -> None:
        self._append_record(registration)

    def write_symbol_registration(
        self, registration: MessageSymbolRegistration
    ) -> None:
        self._append_record(registration)

    def _append_record(self, record: CaptureRecord) -> None:
        candidate_payload_bytes = self._payload_byte_count(record)
        next_record_count = len(self._records) + 1
        next_payload_bytes = self._payload_bytes + candidate_payload_bytes

        if next_record_count > self._limits.max_records:
            raise BootstrapBufferLimitExceededError(
                "bootstrap buffer record limit exceeded"
            )
        if next_payload_bytes > self._limits.max_payload_bytes:
            raise BootstrapBufferLimitExceededError(
                "bootstrap buffer payload byte limit exceeded"
            )

        self._records.append(record)
        self._payload_bytes = next_payload_bytes

    def _payload_byte_count(self, record: CaptureRecord) -> int:
        if isinstance(record, CapturedMessage):
            return len(record.serialized_payload.payload_bytes)
        return 0
