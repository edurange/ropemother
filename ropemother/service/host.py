#!/usr/bin/env python3
# ropemother/service/host.py

"""Host abstractions for message bus service lifecycles."""

import abc
import collections.abc
import pathlib
import tempfile
import threading
import types
import typing

from ropemother.broker.direct import DirectMessageBus
from ropemother.broker.directcore import CaptureMode
from ropemother.capture.sink import CaptureSink
from ropemother.client.endpointfactory import MessageEndpointFactory
from ropemother.exceptions import MessageBusBaseException
from ropemother.format.defaults import default_portable_format_registry
from ropemother.format.portableformat import PortableFormat
from ropemother.format.registry import PortableFormatRegistry
from ropemother.service.brokerextension import BrokerExtension
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
__date__ = "2026-08-20T23:37:19+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev7"
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


class MessageBusHost(abc.ABC):
    """Lifecycle manager for a hosted message bus service."""

    @abc.abstractmethod
    def start(self) -> None:
        ...

    @abc.abstractmethod
    def stop(self) -> None:
        ...

    @abc.abstractmethod
    def close(self) -> None:
        ...

    @abc.abstractmethod
    def connection_descriptor(self) -> ConnectionDescriptor:
        ...

    @abc.abstractmethod
    def client(self, name: str | None = None) -> MessageEndpointFactory:
        ...

    def bus_contact_variables(
        self,
        *,
        variables: collections.abc.Mapping[str, str] | None = None,
        name: str = BUS_CONTACT_URI_VARIABLE,
    ) -> dict[str, str]:
        contact_variables = bus_contact_variables(
            self.connection_descriptor(), variables=variables, name=name
        )
        return contact_variables

    def __enter__(self) -> typing.Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        self.close()


class LocalMessageBusHost(MessageBusHost):
    """Local host for a freestanding message bus service."""
    _runtime_directory: tempfile.TemporaryDirectory[str] | None
    _runtime_path: pathlib.Path | None
    _socket_path: pathlib.Path | None
    _configured_runtime_path: pathlib.Path | None
    _configured_socket_path: pathlib.Path | None
    _bus: DirectMessageBus | None
    _service: MessageBusService | None
    _service_thread: threading.Thread | None
    _extensions: tuple[BrokerExtension, ...]
    _clients: list[tuple[str | None, TransportClient]]
    _started: bool
    _closed: bool
    _capture_sink: CaptureSink | None
    _daemon_service: bool
    _replace_existing_socket: bool

    def __init__(
        self,
        *extensions: BrokerExtension,
        extra_formats: collections.abc.Iterable[PortableFormat] = (),
        capture_mode: CaptureMode = CaptureMode.CAPTURE_ENABLED,
        capture_sink: CaptureSink | None = None,
        daemon_service: bool = True,
        runtime_directory: pathlib.Path | str | None = None,
        socket_path: pathlib.Path | str | None = None,
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
        self._extensions = extensions
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

        extension_handlers = []
        for extension in self._extensions:
            handler = extension.create_handler(bus)
            extension_handlers.append(handler)

        listener = LocalBusServiceListener.from_socket_path(
            socket_path, replace_existing=self._replace_existing_socket
        )
        service = MessageBusService.from_listener(
            bus=bus,
            listener=listener,
            extension_handlers=extension_handlers,
        )
        service_thread = threading.Thread(
            target=service.serve_forever, daemon=self._daemon_service
        )
        self._socket_path = socket_path
        self._bus = bus
        self._service = service
        self._service_thread = service_thread
        service_thread.start()
        self._started = True

    def stop(self) -> None:
        if self._service is not None:
            self._service.request_stop()
        if self._service_thread is not None:
            self._service_thread.join()

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

    def _prepare_socket_path(self) -> pathlib.Path:
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

        runtime_directory = tempfile.TemporaryDirectory()
        runtime_path = pathlib.Path(runtime_directory.name)
        socket_path = runtime_path / _DEFAULT_SOCKET_NAME
        self._runtime_directory = runtime_directory
        self._runtime_path = runtime_path
        return socket_path

    def _normalize_path(
        self, path: pathlib.Path | str | None
    ) -> pathlib.Path | None:
        if path is None:
            return None

        return pathlib.Path(path).expanduser().resolve()
