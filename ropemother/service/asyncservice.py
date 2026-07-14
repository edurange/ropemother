#!/usr/bin/env python3
# ropemother/service/asyncservice.py

"""Programmatic async service wrapper for message bus brokers."""

from asyncio import Event, wait_for
from collections.abc import Mapping
from typing import Self

from ropemother.bootstrap.buffer import BootstrapBufferLimits
from ropemother.bootstrap.policy import BootstrapPolicy
from ropemother.broker.asyncdirect import AsyncDirectMessageBus
from ropemother.capture.sink import CaptureSink
from ropemother.exceptions import MessageBusBaseException
from ropemother.service.descriptor import ConnectionDescriptor
from ropemother.service.environment import (
    BUS_CONTACT_URI_VARIABLE,
    bus_contact_variables,
)
from ropemother.service.listener import AsyncFrameConnectionListener
from ropemother.transport.asyncconnection import (
    AsyncFrameChannel,
    AsyncFrameConnection,
)
from ropemother.transport.asyncsessionrunner import (
    AsyncBrokerTransportSessionRunner,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T17:07:37+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


_ASYNC_ACCEPT_TIMEOUT = 0.1


class AsyncMessageBusServiceError(MessageBusBaseException):
    """Base exception for async message bus service errors."""
    pass


class AsyncMessageBusServiceAlreadyStartedError(
    RuntimeError, AsyncMessageBusServiceError
):
    """Raised when an async message bus service starts twice."""
    pass


class AsyncMessageBusServiceStoppedError(
    RuntimeError, AsyncMessageBusServiceError
):
    """Raised when a stopped async message bus service is reused."""
    pass


class AsyncMessageBusService:
    """Foreground async service wrapper for a message bus broker."""
    _bus: AsyncDirectMessageBus
    _listener: AsyncFrameConnectionListener
    _session_runners: list[AsyncBrokerTransportSessionRunner]
    _stop_requested: Event
    _started: bool
    _closed: bool

    def __init__(
        self,
        *,
        bus: AsyncDirectMessageBus,
        listener: AsyncFrameConnectionListener,
    ) -> None:
        self._bus = bus
        self._listener = listener
        self._session_runners = []
        self._stop_requested = Event()
        self._started = False
        self._closed = False

    @classmethod
    def from_listener(
        cls,
        *,
        bus: AsyncDirectMessageBus,
        listener: AsyncFrameConnectionListener,
    ) -> Self:
        return cls(bus=bus, listener=listener)

    @classmethod
    def capture_bootstrap(
        cls,
        *,
        listener: AsyncFrameConnectionListener,
        bootstrap_policy: BootstrapPolicy | None = None,
        bootstrap_limits: BootstrapBufferLimits | None = None,
    ) -> Self:
        bus = AsyncDirectMessageBus.capture_bootstrap(
            bootstrap_policy=bootstrap_policy,
            bootstrap_limits=bootstrap_limits,
        )
        return cls.from_listener(bus=bus, listener=listener)

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

    async def serve_forever(self) -> None:
        if self._closed:
            raise AsyncMessageBusServiceStoppedError(
                "async message bus service has already been closed"
            )
        if self._started:
            raise AsyncMessageBusServiceAlreadyStartedError(
                "async message bus service has already been started"
            )

        self._started = True
        try:
            await self._serve_until_stopped()
        finally:
            await self.close()

    def request_stop(self) -> None:
        first_request = not self._stop_requested.is_set()
        self._stop_requested.set()
        if first_request:
            self._listener.close()
        self._request_session_stops()

    async def close(self) -> None:
        if self._closed:
            return

        try:
            self.request_stop()
            await self._wait_sessions()
        finally:
            self._closed = True

    async def _serve_until_stopped(self) -> None:
        while not self._stop_requested.is_set():
            try:
                connection = await wait_for(
                    self._listener.accept(), timeout=_ASYNC_ACCEPT_TIMEOUT
                )
            except TimeoutError:
                continue
            except OSError:
                if self._stop_requested.is_set():
                    continue
                raise

            self._start_session(connection)

    def _start_session(self, connection: AsyncFrameConnection) -> None:
        channel = AsyncFrameChannel(connection)
        session = self._bus.create_transport_session(channel=channel)
        runner = AsyncBrokerTransportSessionRunner(
            session=session, close_connection=connection.close
        )
        self._session_runners.append(runner)
        runner.start()

    def _request_session_stops(self) -> None:
        for runner in self._session_runners:
            runner.request_stop()

    async def _wait_sessions(self) -> None:
        for runner in self._session_runners:
            await runner.wait()
