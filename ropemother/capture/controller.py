#!/usr/bin/env python3
# ropemother/capture/controller.py

"""Capture state controller for broker-owned capture posture."""

from enum import Enum
from threading import Lock
from typing import Iterable

from ropemother.bootstrap.buffer import BootstrapBuffer, BootstrapBufferLimits
from ropemother.capture.sink import CaptureSink
from ropemother.capture.writer import (
    CaptureRecordSource,
    CaptureRecordWriter,
    RegistrationRecord,
)
from ropemother.exceptions import (
    CaptureDisabledError,
    CaptureUnavailableError,
    MessageBusBaseException,
)
from ropemother.format.registry import PortableFormatRegistration
from ropemother.message.records import CapturedMessage
from ropemother.message.symbols import MessageSymbolRegistration

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T02:55:12+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


class CaptureState(Enum):
    """Lifecycle state for broker-owned capture handling."""
    DISABLED = "disabled"
    WAITING_FOR_SINK = "waiting-for-sink"
    BOOTSTRAPPING = "bootstrapping"
    SWAPPING = "swapping"
    ACTIVE = "active"
    FAILED = "failed"


class CaptureControllerError(MessageBusBaseException):
    """Base exception for capture controller errors."""
    pass


class CaptureControllerAlreadyActiveError(
    RuntimeError, CaptureControllerError
):
    """Raised when an active capture controller is activated again."""
    pass


class CaptureController(CaptureRecordWriter):
    """Broker-owned writer that manages capture sink activation."""
    _state: CaptureState
    _buffer: BootstrapBuffer | None
    _capture_sink: CaptureSink | None
    _lock: Lock

    def __init__(
        self,
        *,
        capture_enabled: bool = True,
        bootstrap_enabled: bool = False,
        bootstrap_limits: BootstrapBufferLimits | None = None,
        capture_sink: CaptureSink | None = None,
    ) -> None:
        self._lock = Lock()
        self._buffer = None
        self._capture_sink = None

        if not capture_enabled:
            if capture_sink is not None:
                raise CaptureDisabledError(
                    "capture sink cannot be attached to a transport-only bus"
                )
            self._state = CaptureState.DISABLED
        elif capture_sink is not None:
            self._capture_sink = capture_sink
            self._state = CaptureState.ACTIVE
        elif bootstrap_enabled:
            self._buffer = BootstrapBuffer(limits=bootstrap_limits)
            self._state = CaptureState.BOOTSTRAPPING
        else:
            self._state = CaptureState.WAITING_FOR_SINK

    @property
    def state(self) -> CaptureState:
        return self._state

    @property
    def capture_active(self) -> bool:
        return self._state is CaptureState.ACTIVE

    @property
    def pending_record_count(self) -> int:
        if self._buffer is None:
            return 0
        return self._buffer.pending_record_count

    @property
    def pending_payload_bytes(self) -> int:
        if self._buffer is None:
            return 0
        return self._buffer.pending_payload_bytes

    def capture_source(self) -> CaptureRecordSource | None:
        if isinstance(self._capture_sink, CaptureRecordSource):
            return self._capture_sink
        return None

    def activate_capture_sink(
        self,
        capture_sink: CaptureSink,
        *,
        registrations: Iterable[RegistrationRecord] = (),
    ) -> None:
        with self._lock:
            if self._state is CaptureState.DISABLED:
                raise CaptureDisabledError(
                    "capture sink cannot be attached to a transport-only bus"
                )
            if self._state is CaptureState.ACTIVE:
                raise CaptureControllerAlreadyActiveError(
                    "capture controller already has an active capture sink"
                )
            if self._state is CaptureState.FAILED:
                raise CaptureUnavailableError(
                    "capture controller cannot activate after capture failure"
                )
            if self._state is CaptureState.SWAPPING:
                raise CaptureUnavailableError(
                    "capture controller is already activating a capture sink"
                )

            self._state = CaptureState.SWAPPING
            try:
                if self._buffer is None:
                    capture_sink.begin_capture(registrations)
                else:
                    self._buffer.flush_to(capture_sink)
            except Exception:
                self._state = CaptureState.FAILED
                raise

            self._buffer = None
            self._capture_sink = capture_sink
            self._state = CaptureState.ACTIVE

    def write_message_record(self, message: CapturedMessage) -> None:
        with self._lock:
            if self._state is CaptureState.DISABLED:
                return
            if self._state is CaptureState.WAITING_FOR_SINK:
                raise CaptureUnavailableError(
                    "capture sink is required before delivery"
                )

            writer = self._current_writer()
            writer.write_message_record(message)

    def write_format_registration(
        self, registration: PortableFormatRegistration
    ) -> None:
        self._write_registration_record(registration)

    def write_symbol_registration(
        self, registration: MessageSymbolRegistration
    ) -> None:
        self._write_registration_record(registration)

    def _write_registration_record(
        self, registration: RegistrationRecord
    ) -> None:
        with self._lock:
            if self._state is CaptureState.DISABLED:
                return
            if self._state is CaptureState.WAITING_FOR_SINK:
                return

            writer = self._current_writer()
            writer.write_registration(registration)

    def _current_writer(self) -> CaptureRecordWriter:
        if self._state is CaptureState.BOOTSTRAPPING:
            if self._buffer is None:
                raise CaptureUnavailableError(
                    "capture controller bootstrap buffer is unavailable"
                )
            return self._buffer

        if self._state is CaptureState.ACTIVE:
            if self._capture_sink is None:
                raise CaptureUnavailableError(
                    "capture controller active sink is unavailable"
                )
            return self._capture_sink

        if self._state is CaptureState.SWAPPING:
            raise CaptureUnavailableError(
                "capture controller is activating a capture sink"
            )

        raise CaptureUnavailableError("capture controller is not available")
