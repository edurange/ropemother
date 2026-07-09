#!/usr/bin/env python3
# ropemother/service/socketlistener.py

"""Unix-domain socket listener for freestanding message bus services."""

import asyncio
from dataclasses import dataclass
from pathlib import Path
import socket
import stat
from typing import Self

from ropemother.exceptions import MessageBusBaseException
from ropemother.service.descriptor import ConnectionDescriptor
from ropemother.service.listener import (
    AsyncFrameConnectionListener,
    FrameConnectionListener,
)
from ropemother.service.resource import LocalSocketBusResource
from ropemother.transport.asyncconnection import AsyncFrameConnection
from ropemother.transport.asyncsocketconnection import (
    AsyncSocketFrameConnection,
)
from ropemother.transport.connection import FrameConnection
from ropemother.transport.socketconnection import SocketFrameConnection

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-04T23:16:38+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


_DEFAULT_ACCEPT_TIMEOUT = 0.1
_DEFAULT_LISTEN_BACKLOG = 128


class LocalBusServiceListenerError(MessageBusBaseException):
    """Base exception for local socket service listener errors."""
    pass


class InvalidLocalSocketListenerError(
    ValueError, LocalBusServiceListenerError
):
    """Raised when local socket listener configuration is invalid."""
    pass


class LocalSocketPathExistsError(
    FileExistsError, LocalBusServiceListenerError
):
    """Raised when a local socket path already exists."""
    pass


@dataclass
class _SocketPathStatus:
    exists: bool
    is_socket: bool


def _socket_path_status(socket_path: Path) -> _SocketPathStatus:
    try:
        path_stat = socket_path.lstat()
    except FileNotFoundError:
        return _SocketPathStatus(exists=False, is_socket=False)

    is_socket = stat.S_ISSOCK(path_stat.st_mode)
    status = _SocketPathStatus(exists=True, is_socket=is_socket)
    return status


def _prepare_socket_path(
    *, socket_path: Path, replace_existing: bool
) -> None:
    status = _socket_path_status(socket_path)
    if not status.exists:
        return

    if not status.is_socket:
        raise InvalidLocalSocketListenerError(
            "local socket listener path already exists and is not a socket"
        )

    if not replace_existing:
        raise LocalSocketPathExistsError(
            "local socket listener path already exists"
        )

    socket_path.unlink()


def _remove_socket_path(socket_path: Path) -> None:
    status = _socket_path_status(socket_path)
    if status.exists and status.is_socket:
        socket_path.unlink()


class LocalBusServiceListener(FrameConnectionListener):
    """Unix-domain socket listener for a local message bus service."""
    _resource: LocalSocketBusResource
    _listener_socket: socket.socket
    _bound: bool
    _closed: bool

    def __init__(
        self,
        *,
        resource: LocalSocketBusResource,
        backlog: int = _DEFAULT_LISTEN_BACKLOG,
        replace_existing: bool = False,
        accept_timeout: float | None = _DEFAULT_ACCEPT_TIMEOUT,
    ) -> None:
        if backlog < 1:
            raise InvalidLocalSocketListenerError(
                "local socket listener backlog must be positive"
            )

        self._resource = resource
        self._listener_socket = socket.socket(
            socket.AF_UNIX, socket.SOCK_STREAM
        )
        self._listener_socket.settimeout(accept_timeout)
        self._bound = False
        self._closed = False

        _prepare_socket_path(
            socket_path=resource.socket_path,
            replace_existing=replace_existing,
        )
        try:
            self._listener_socket.bind(str(resource.socket_path))
            self._listener_socket.listen(backlog)
            self._bound = True
        except OSError:
            self._listener_socket.close()
            self._closed = True
            raise

    @classmethod
    def from_socket_path(
        cls,
        socket_path: Path | str,
        *,
        backlog: int = _DEFAULT_LISTEN_BACKLOG,
        replace_existing: bool = False,
        accept_timeout: float | None = _DEFAULT_ACCEPT_TIMEOUT,
    ) -> Self:
        resource = LocalSocketBusResource.from_path(socket_path)
        listener = cls(
            resource=resource,
            backlog=backlog,
            replace_existing=replace_existing,
            accept_timeout=accept_timeout,
        )
        return listener

    @classmethod
    def from_path(
        cls,
        socket_path: Path | str,
        *,
        backlog: int = _DEFAULT_LISTEN_BACKLOG,
        replace_existing: bool = False,
        accept_timeout: float | None = _DEFAULT_ACCEPT_TIMEOUT,
    ) -> Self:
        listener = cls.from_socket_path(
            socket_path,
            backlog=backlog,
            replace_existing=replace_existing,
            accept_timeout=accept_timeout,
        )
        return listener

    def connection_descriptor(self) -> ConnectionDescriptor:
        return self._resource.connection_descriptor()

    def accept(self) -> FrameConnection:
        stream_socket, _ = self._listener_socket.accept()
        return SocketFrameConnection(stream_socket)

    def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        self._listener_socket.close()
        if self._bound:
            _remove_socket_path(self._resource.socket_path)


class AsyncLocalBusServiceListener(AsyncFrameConnectionListener):
    """Async Unix-domain socket listener for a local message bus service."""
    _resource: LocalSocketBusResource
    _listener_socket: socket.socket
    _bound: bool
    _closed: bool

    def __init__(
        self,
        *,
        resource: LocalSocketBusResource,
        backlog: int = _DEFAULT_LISTEN_BACKLOG,
        replace_existing: bool = False,
    ) -> None:
        if backlog < 1:
            raise InvalidLocalSocketListenerError(
                "local socket listener backlog must be positive"
            )

        self._resource = resource
        self._listener_socket = socket.socket(
            socket.AF_UNIX, socket.SOCK_STREAM
        )
        self._listener_socket.setblocking(False)
        self._bound = False
        self._closed = False

        _prepare_socket_path(
            socket_path=resource.socket_path,
            replace_existing=replace_existing,
        )
        try:
            self._listener_socket.bind(str(resource.socket_path))
            self._listener_socket.listen(backlog)
            self._bound = True
        except OSError:
            self._listener_socket.close()
            self._closed = True
            raise

    @classmethod
    def from_socket_path(
        cls,
        socket_path: Path | str,
        *,
        backlog: int = _DEFAULT_LISTEN_BACKLOG,
        replace_existing: bool = False,
    ) -> Self:
        resource = LocalSocketBusResource.from_path(socket_path)
        listener = cls(
            resource=resource,
            backlog=backlog,
            replace_existing=replace_existing,
        )
        return listener

    @classmethod
    def from_path(
        cls,
        socket_path: Path | str,
        *,
        backlog: int = _DEFAULT_LISTEN_BACKLOG,
        replace_existing: bool = False,
    ) -> Self:
        listener = cls.from_socket_path(
            socket_path,
            backlog=backlog,
            replace_existing=replace_existing,
        )
        return listener

    def connection_descriptor(self) -> ConnectionDescriptor:
        return self._resource.connection_descriptor()

    async def accept(self) -> AsyncFrameConnection:
        loop = asyncio.get_running_loop()
        stream_socket, _ = await loop.sock_accept(self._listener_socket)
        return AsyncSocketFrameConnection(stream_socket)

    def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        self._listener_socket.close()
        if self._bound:
            _remove_socket_path(self._resource.socket_path)
