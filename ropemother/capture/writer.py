#!/usr/bin/env python3
# ropemother/capture/writer.py

"""Shared capture-record writing contracts."""

from abc import ABC, abstractmethod
from typing import Iterable

from ropemother.format.registry import (
    PortableFormatRegistration,
    UnsupportedRegistrationTypeError,
)
from ropemother.message.records import CapturedMessage
from ropemother.message.registrationtable import MessageRegistration
from ropemother.message.symbols import MessageSymbolRegistration

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-06T18:34:14+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


RegistrationRecord = MessageRegistration
CaptureRecord = RegistrationRecord | CapturedMessage


class CaptureRecordSource(ABC):
    """Indexed source of capture records for history queries."""

    @property
    @abstractmethod
    def capture_record_count(self) -> int:
        ...

    @abstractmethod
    def read_capture_records(
        self, start_index: int, count: int
    ) -> tuple[CaptureRecord, ...]:
        ...

    def capture_records(self) -> tuple[CaptureRecord, ...]:
        return self.read_capture_records(0, self.capture_record_count)


class CaptureRecordWriter(ABC):
    """Abstract writer for capture and registration records."""
    @abstractmethod
    def write_message_record(self, message: CapturedMessage) -> None:
        ...

    @abstractmethod
    def write_format_registration(
        self, registration: PortableFormatRegistration
    ) -> None:
        ...

    @abstractmethod
    def write_symbol_registration(
        self, registration: MessageSymbolRegistration
    ) -> None:
        ...

    def write_record(self, record: CaptureRecord) -> None:
        if isinstance(record, CapturedMessage):
            self.write_message_record(record)
        else:
            self.write_registration(record)

    def write_records(self, records: Iterable[CaptureRecord]) -> None:
        for record in records:
            self.write_record(record)

    def write_registration(self, registration: RegistrationRecord) -> None:
        if isinstance(registration, MessageSymbolRegistration):
            self.write_symbol_registration(registration)
        elif isinstance(registration, PortableFormatRegistration):
            self.write_format_registration(registration)
        else:
            raise UnsupportedRegistrationTypeError(
                f"unsupported registration record: {registration!r}"
            )
