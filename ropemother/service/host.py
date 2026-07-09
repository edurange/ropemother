#!/usr/bin/env python3
# ropemother/service/host.py

"""Host abstractions for message bus service lifecycles."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from types import TracebackType
from typing import Any, Self

from ropemother.broker.direct import DirectMessageBus
from ropemother.broker.directcore import CaptureMode
from ropemother.capture.sink import CaptureSink
from ropemother.client.endpointfactory import MessageEndpointFactory
from ropemother.exceptions import MessageBusBaseException
from ropemother.format.defaults import default_portable_format_registry
from ropemother.format.portableformat import PortableFormat
from ropemother.format.registry import PortableFormatRegistry
from ropemother.service.brokerextension import (
    BrokerExtension,
    BrokerExtensionRunner,
)
from ropemother.service.connector import connect_transport_client
from ropemother.service.descriptor import ConnectionDescriptor
from ropemother.service.environment import (
    BUS_CONTACT_URI_VARIABLE,
    bus_contact_variables,
)
from ropemother.service.service import MessageBusService
from ropemother.service.socketlistener import LocalBusServiceListener
from ropemother.transport.client import TransportClient

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T17:53:48+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


_DEFAULT_SOCKET_NAME = "ropemother.sock"


class MessageBusHostError(MessageBusBaseException):
    """Base exception for message bus host errors."""
    pass


class MessageBusHostClosedError(RuntimeError, MessageBusHostError):
    """Raised when a closed message bus host is reused."""
    pass


class MessageBusHostUnavailableError(RuntimeError, MessageBusHostError):
    """Raised when a message bus host has no active connection."""
    pass


class InvalidLocalMessageBusHostError(ValueError, MessageBusHostError):
    """Raised when local host configuration is invalid."""
    pass


class MessageBusHost(ABC):
    """Lifecycle manager for a hosted message bus service."""

    @abstractmethod
    def start(self) -> None:
        ...

    @abstractmethod
    def stop(self) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    @abstractmethod
    def connection_descriptor(self) -> ConnectionDescriptor:
        ...

    @abstractmethod
    def client(self, name: str | None = None) -> MessageEndpointFactory:
        ...

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

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class LocalMessageBusHost(MessageBusHost):
    """Local host for a freestanding message bus service."""
    _runtime_directory: TemporaryDirectory[str] | None
    _runtime_path: Path | None
    _socket_path: Path | None
    _configured_runtime_path: Path | None
    _configured_socket_path: Path | None
    _bus: DirectMessageBus | None
    _service: MessageBusService | None
    _service_thread: Thread | None
    _broker_extensions: list[BrokerExtension]
    _broker_extension_runners: list[BrokerExtensionRunner]
    _clients: list[tuple[str | None, TransportClient]]
    _started: bool
    _closed: bool
    _capture_sink: CaptureSink | None
    _daemon_service: bool
    _replace_existing_socket: bool

    def __init__(
        self,
        *,
        extra_formats: Iterable[PortableFormat[Any, Any]] = (),
        capture_mode: CaptureMode = CaptureMode.CAPTURE_ENABLED,
        capture_sink: CaptureSink | None = None,
        broker_extensions: list[BrokerExtension] | None = None,
        daemon_service: bool = True,
        runtime_directory: Path | str | None = None,
        socket_path: Path | str | None = None,
        replace_existing_socket: bool = False,
    ) -> None:
        if runtime_directory is not None and socket_path is not None:
            raise InvalidLocalMessageBusHostError(
                "local message bus host accepts runtime_directory or "
                "socket_path, not both"
            )

        format_registry = default_portable_format_registry(
            extra_formats=extra_formats
        )

        self._runtime_directory = None
        self._runtime_path = None
        self._socket_path = None
        self._configured_runtime_path = self._normalize_path(
            runtime_directory
        )
        self._configured_socket_path = self._normalize_path(socket_path)
        self._format_registry = format_registry
        self._bus = None
        self._service = None
        self._service_thread = None
        if broker_extensions is None:
            broker_extensions = []
        self._broker_extensions = list(broker_extensions)
        self._broker_extension_runners = []
        self._clients = []
        self._started = False
        self._closed = False
        self._capture_mode = capture_mode
        self._capture_sink = capture_sink
        self._daemon_service = daemon_service
        self._replace_existing_socket = replace_existing_socket

    def start(self) -> None:
        if self._started:
            return
        if self._closed:
            raise MessageBusHostClosedError(
                "local message bus host is already closed"
            )

        socket_path = self._prepare_socket_path()
        bus = DirectMessageBus(
            capture_mode=self._capture_mode,
            capture_sink=self._capture_sink,
            extra_formats=self._format_registry.formats(),
        )

        listener = LocalBusServiceListener.from_socket_path(
            socket_path, replace_existing=self._replace_existing_socket
        )
        service = MessageBusService.from_listener(bus=bus, listener=listener)
        service_thread = Thread(
            target=service.serve_forever, daemon=self._daemon_service
        )
        broker_extension_runners = []
        for broker_extension in self._broker_extensions:
            runner = broker_extension.create_runner(
                bus, daemon=self._daemon_service
            )
            broker_extension_runners.append(runner)
        self._socket_path = socket_path
        self._bus = bus
        self._service = service
        self._service_thread = service_thread
        self._broker_extension_runners = broker_extension_runners
        service_thread.start()
        for runner in broker_extension_runners:
            runner.start()
        self._started = True

    def stop(self) -> None:
        for runner in self._broker_extension_runners:
            runner.request_stop()
        if self._service is not None:
            self._service.request_stop()
        if self._service_thread is not None:
            self._service_thread.join()
        for runner in self._broker_extension_runners:
            runner.join()

    def close(self) -> None:
        if self._closed:
            return

        try:
            for _, client in self._clients:
                client.close()
            self.stop()
        finally:
            if self._runtime_directory is not None:
                self._runtime_directory.cleanup()
            self._closed = True

    def connection_descriptor(self) -> ConnectionDescriptor:
        self.start()
        if self._service is None:
            raise MessageBusHostUnavailableError(
                "local message bus service is unavailable"
            )

        return self._service.connection_descriptor()

    def client(self, name: str | None = None) -> TransportClient:
        self.start()
        descriptor = self.connection_descriptor()
        client = connect_transport_client(
            descriptor=descriptor,
            extra_formats=self._format_registry.formats(),
        )
        self._clients.append((name, client))
        return client

    def _prepare_socket_path(self) -> Path:
        if self._configured_socket_path is not None:
            socket_path = self._configured_socket_path
            socket_path.parent.mkdir(parents=True, exist_ok=True)
            return socket_path

        if self._configured_runtime_path is not None:
            runtime_path = self._configured_runtime_path
            runtime_path.mkdir(parents=True, exist_ok=True)
            socket_path = runtime_path / _DEFAULT_SOCKET_NAME
            self._runtime_path = runtime_path
            return socket_path

        runtime_directory = TemporaryDirectory()
        runtime_path = Path(runtime_directory.name)
        socket_path = runtime_path / _DEFAULT_SOCKET_NAME
        self._runtime_directory = runtime_directory
        self._runtime_path = runtime_path
        return socket_path

    def _normalize_path(self, path: Path | str | None) -> Path | None:
        if path is None:
            return None

        return Path(path).expanduser().resolve()
