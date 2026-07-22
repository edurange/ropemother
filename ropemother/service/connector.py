#!/usr/bin/env python3
# ropemother/service/connector.py

"""Client-side connection helpers for freestanding message bus services."""

from collections.abc import Iterable
import socket
from typing import Any

from ropemother.exceptions import MessageBusBaseException
from ropemother.format.portableformat import PortableFormat
from ropemother.service.descriptor import ConnectionDescriptor
from ropemother.transport.asyncclient import AsyncTransportClient
from ropemother.transport.asyncconnection import AsyncFrameChannel
from ropemother.transport.asyncsocketconnection import (
    AsyncSocketFrameConnection,
    open_async_socket_frame_connection,
)
from ropemother.transport.client import TransportClient
from ropemother.transport.connection import FrameChannel, FrameConnection
from ropemother.transport.socketconnection import SocketFrameConnection

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T17:39:17+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


class ServiceConnectorError(MessageBusBaseException):
    """Base exception for message bus service connector errors."""
    pass


class ServiceConnectionFailedError(ConnectionError, ServiceConnectorError):
    """Raised when a message bus service connection fails."""
    pass


def connect_frame_connection(
    descriptor: ConnectionDescriptor,
) -> FrameConnection:
    """Open a frame connection to a described message bus service."""
    socket_path = descriptor.unix_socket_path()
    stream_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        stream_socket.connect(str(socket_path))
    except OSError as error:
        stream_socket.close()
        raise ServiceConnectionFailedError(
            "failed to connect to message bus service"
        ) from error

    return SocketFrameConnection(stream_socket)


def connect_transport_client(
    *,
    descriptor: ConnectionDescriptor,
    extra_formats: Iterable[PortableFormat[Any, Any]] = (),
) -> TransportClient:
    """Open a transport client for a described message bus service."""
    connection = connect_frame_connection(descriptor)
    channel = FrameChannel(connection)
    return TransportClient(channel=channel, extra_formats=extra_formats)


async def connect_async_frame_connection(
    descriptor: ConnectionDescriptor,
) -> AsyncSocketFrameConnection:
    """Open an async frame connection to a described message bus service."""
    socket_path = descriptor.unix_socket_path()
    try:
        connection = await open_async_socket_frame_connection(socket_path)
    except OSError as error:
        raise ServiceConnectionFailedError(
            "failed to connect to message bus service"
        ) from error

    return connection


async def connect_async_transport_client(
    *,
    descriptor: ConnectionDescriptor,
    extra_formats: Iterable[PortableFormat[Any, Any]] = (),
) -> AsyncTransportClient:
    """Open an async transport client for a described message bus service."""
    connection = await connect_async_frame_connection(descriptor)
    channel = AsyncFrameChannel(connection)
    return AsyncTransportClient(channel=channel, extra_formats=extra_formats)
