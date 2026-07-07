#!/usr/bin/env python3
# ropemother/transport/zeromq/connection.py

"""ZMQ-backed transport connection for encoded message bus frame parts."""

from typing import Self
from uuid import uuid4

from ropemother.exceptions import MessageBusBaseException
from ropemother.transport.codec import FrameParts
from ropemother.transport.connection import FrameConnection
from ropemother.transport.zeromq.address import ZMQAddress

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T20:19:37+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


class ZMQFrameConnectionError(MessageBusBaseException):
    """Base exception for ZeroMQ frame connection errors."""
    pass


class ZMQFrameConnectionTimeoutError(TimeoutError, ZMQFrameConnectionError):
    """Raised when a ZeroMQ frame connection times out."""
    pass


class ZMQTransportUnavailableError(ImportError, ZMQFrameConnectionError):
    """Raised when ZeroMQ transport support is unavailable."""
    pass


try:
    import zmq
except ImportError as error:
    raise ZMQTransportUnavailableError(
        "ZMQ transport requires pyzmq"
    ) from error


class ZMQFrameConnectionClosedError(EOFError, ZMQFrameConnectionError):
    """Raised when a ZeroMQ connection closes while receiving frame data."""
    pass


class InvalidZMQFramePartsError(ValueError, ZMQFrameConnectionError):
    """Raised when ZeroMQ frame parts are malformed."""
    pass


class ZMQFrameConnection(FrameConnection):
    """ZeroMQ-backed connection for message bus frame parts."""
    def __init__(
        self, socket: zmq.Socket, *, receive_timeout_ms: int | None = None
    ) -> None:
        self._socket = socket
        self._socket.setsockopt(zmq.LINGER, 0)
        if receive_timeout_ms is not None:
            self._socket.setsockopt(zmq.RCVTIMEO, receive_timeout_ms)

    def send_frame_parts(self, parts: FrameParts) -> None:
        if len(parts) == 0:
            raise InvalidZMQFramePartsError(
                "ZMQ frame must contain at least one part"
            )

        try:
            self._socket.send_multipart(parts)
        except zmq.ZMQError as error:
            self._raise_connection_error(error)

    def receive_frame_parts(self) -> FrameParts:
        try:
            parts = tuple(self._socket.recv_multipart())
        except zmq.ZMQError as error:
            self._raise_connection_error(error)

        if len(parts) == 0:
            raise InvalidZMQFramePartsError(
                "ZMQ frame must contain at least one part"
            )

        return parts

    def receive_frame_parts_nowait(self) -> FrameParts | None:
        parts = None
        try:
            parts = tuple(self._socket.recv_multipart(flags=zmq.NOBLOCK))
        except zmq.ZMQError as error:
            if error.errno != zmq.EAGAIN:
                self._raise_connection_error(error)

        if parts is not None and len(parts) == 0:
            raise InvalidZMQFramePartsError(
                "ZMQ frame must contain at least one part"
            )

        return parts

    def close(self) -> None:
        self._socket.close(linger=0)

    def _raise_connection_error(self, error: zmq.ZMQError) -> None:
        if error.errno == zmq.EAGAIN:
            raise ZMQFrameConnectionTimeoutError(
                "ZMQ receive timed out"
            ) from error
        if error.errno in (zmq.ENOTSOCK, zmq.ETERM):
            raise ZMQFrameConnectionClosedError(
                "ZMQ connection is closed"
            ) from error
        raise ZMQFrameConnectionError("ZMQ frame connection failed") from error

    @classmethod
    def make_pair(
        cls,
        *,
        first_receive_timeout_ms: int | None = None,
        second_receive_timeout_ms: int | None = None,
    ) -> tuple[Self, Self]:
        context = zmq.Context.instance()
        address = ZMQAddress.inproc(f"ropemother-{uuid4().hex}")
        first = cls.bind_pair(
            context=context,
            address=address,
            receive_timeout_ms=first_receive_timeout_ms,
        )
        second = cls.connect_pair(
            context=context,
            address=address,
            receive_timeout_ms=second_receive_timeout_ms,
        )
        return first, second

    @classmethod
    def bind_pair(
        cls,
        *,
        address: ZMQAddress | str,
        context: zmq.Context | None = None,
        receive_timeout_ms: int | None = None,
    ) -> Self:
        resolved_context = context
        if resolved_context is None:
            resolved_context = zmq.Context.instance()
        socket = resolved_context.socket(zmq.PAIR)
        socket.bind(str(address))

        return cls(socket, receive_timeout_ms=receive_timeout_ms)

    @classmethod
    def connect_pair(
        cls,
        *,
        address: ZMQAddress | str,
        context: zmq.Context | None = None,
        receive_timeout_ms: int | None = None,
    ) -> Self:
        resolved_context = context
        if resolved_context is None:
            resolved_context = zmq.Context.instance()
        socket = resolved_context.socket(zmq.PAIR)
        socket.connect(str(address))

        return cls(socket, receive_timeout_ms=receive_timeout_ms)
