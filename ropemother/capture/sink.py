#!/usr/bin/env python3
# ropemother/capture/sink.py

"""Durable capture sink interface for message bus services."""

from abc import abstractmethod
from typing import Callable, Iterable

from ropemother.capture.writer import CaptureRecordWriter, RegistrationRecord
from ropemother.format.registry import PortableFormatRegistration
from ropemother.message.records import CapturedMessage
from ropemother.message.symbols import MessageSymbolRegistration

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T04:08:53+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


CapturedMessageHandler = Callable[[CapturedMessage], None]


class CaptureSink(CaptureRecordWriter):
    """Destination for captured bus records."""

    @abstractmethod
    def capture(self, message: CapturedMessage) -> None:
        ...

    @abstractmethod
    def capture_format_event(
        self, registration: PortableFormatRegistration
    ) -> None:
        ...

    @abstractmethod
    def capture_symbol_event(
        self, registration: MessageSymbolRegistration
    ) -> None:
        ...

    def write_message_record(self, message: CapturedMessage) -> None:
        self.capture(message)

    def write_format_registration(
        self, registration: PortableFormatRegistration
    ) -> None:
        self.capture_format_event(registration)

    def write_symbol_registration(
        self, registration: MessageSymbolRegistration
    ) -> None:
        self.capture_symbol_event(registration)

    def begin_capture(
        self, registrations: Iterable[RegistrationRecord]
    ) -> None:
        for registration in registrations:
            self.capture_registration(registration)

    def capture_registration(self, registration: RegistrationRecord) -> None:
        self.write_registration(registration)
