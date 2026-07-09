#!/usr/bin/env python3
# ropemother/service/service.py

"""Programmatic foreground service wrapper for message bus brokers."""

from collections.abc import Mapping
from threading import Event
from typing import Self

from ropemother.bootstrap.buffer import BootstrapBufferLimits
from ropemother.bootstrap.policy import BootstrapPolicy
from ropemother.broker.direct import DirectMessageBus
from ropemother.capture.sink import CaptureSink
from ropemother.exceptions import MessageBusBaseException
from ropemother.service.descriptor import ConnectionDescriptor
from ropemother.service.environment import (
    BUS_CONTACT_URI_VARIABLE,
    bus_contact_variables,
)
from ropemother.service.listener import FrameConnectionListener
from ropemother.transport.connection import FrameChannel, FrameConnection
from ropemother.transport.sessionrunner import BrokerTransportSessionRunner

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T17:06:57+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


class MessageBusServiceError(MessageBusBaseException):
    """Base exception for message bus service errors."""
    pass


class MessageBusServiceAlreadyStartedError(
    RuntimeError, MessageBusServiceError
):
    """Raised when a message bus service is started more than once."""


class MessageBusServiceStoppedError(RuntimeError, MessageBusServiceError):
    """Raised when a stopped message bus service is reused."""
    pass


class MessageBusService:
    """Foreground service wrapper for a message bus broker."""
    _bus: DirectMessageBus
    _listener: FrameConnectionListener
    _session_runners: list[BrokerTransportSessionRunner]
    _stop_requested: Event
    _started: bool
    _closed: bool
    _daemon_sessions: bool

    def __init__(
        self,
        *,
        bus: DirectMessageBus,
        listener: FrameConnectionListener,
        daemon_sessions: bool = True,
    ) -> None:
        self._bus = bus
        self._listener = listener
        self._session_runners = []
        self._stop_requested = Event()
        self._started = False
        self._closed = False
        self._daemon_sessions = daemon_sessions

    @classmethod
    def from_listener(
        cls,
        *,
        bus: DirectMessageBus,
        listener: FrameConnectionListener,
        daemon_sessions: bool = True,
    ) -> Self:
        service = cls(
            bus=bus,
            listener=listener,
            daemon_sessions=daemon_sessions,
        )
        return service

    @classmethod
    def capture_bootstrap(
        cls,
        *,
        listener: FrameConnectionListener,
        bootstrap_policy: BootstrapPolicy | None = None,
        bootstrap_limits: BootstrapBufferLimits | None = None,
        daemon_sessions: bool = True,
    ) -> Self:
        bus = DirectMessageBus.capture_bootstrap(
            bootstrap_policy=bootstrap_policy,
            bootstrap_limits=bootstrap_limits,
        )
        service = cls.from_listener(
            bus=bus, listener=listener, daemon_sessions=daemon_sessions
        )
        return service

    def set_capture_sink(self, capture_sink: CaptureSink) -> None:
        self._bus.set_capture_sink(capture_sink)

    def connection_descriptor(self) -> ConnectionDescriptor:
        return self._listener.connection_descriptor()

    def bus_contact_variables(
        self,
        *,
        variables: Mapping[str, str] | None = None,
        name: str = BUS_CONTACT_URI_VARIABLE,
    ) -> dict[str, str]:
        contact_variables = bus_contact_variables(
            self.connection_descriptor(), variables=variables, name=name
        )
        return contact_variables

    def serve_forever(self) -> None:
        if self._closed:
            raise MessageBusServiceStoppedError(
                "message bus service has already been closed"
            )
        if self._started:
            raise MessageBusServiceAlreadyStartedError(
                "message bus service has already been started"
            )

        self._started = True
        try:
            self._serve_until_stopped()
        finally:
            self.close()

    def request_stop(self) -> None:
        first_request = not self._stop_requested.is_set()
        self._stop_requested.set()
        if first_request:
            self._listener.close()
        self._request_session_stops()

    def close(self) -> None:
        if self._closed:
            return

        try:
            self.request_stop()
            self._join_sessions()
        finally:
            self._closed = True

    def _serve_until_stopped(self) -> None:
        while not self._stop_requested.is_set():
            try:
                connection = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                if self._stop_requested.is_set():
                    continue
                raise

            self._start_session(connection)

    def _start_session(self, connection: FrameConnection) -> None:
        channel = FrameChannel(connection)
        session = self._bus.create_transport_session(channel=channel)
        runner = BrokerTransportSessionRunner(
            session=session,
            close_connection=connection.close,
            daemon=self._daemon_sessions,
        )
        self._session_runners.append(runner)
        runner.start()

    def _request_session_stops(self) -> None:
        for runner in self._session_runners:
            runner.request_stop()

    def _join_sessions(self) -> None:
        for runner in self._session_runners:
            runner.join()
