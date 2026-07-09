#!/usr/bin/env python3
# ropemother/transport/socketconnection.py

"""Socket-backed transport connection for encoded message bus frame parts."""

import select
import socket
from typing import Self

from ropemother.transport.codec import FrameParts
from ropemother.transport.connection import FrameConnection
from ropemother.transport.socketframing import (
    InvalidSocketFramePartsError,
    SOCKET_READ_SIZE,
    SocketFrameConnectionClosedError,
    SocketFrameConnectionError,
    SocketFramePartsBuffer,
    socket_frame_parts_bytes,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-04T22:24:04+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


class SocketFrameConnection(FrameConnection):
    """Socket-backed connection for sending and receiving frame parts."""
    _socket: socket.socket
    _parts_buffer: SocketFramePartsBuffer

    def __init__(self, stream_socket: socket.socket) -> None:
        self._socket = stream_socket
        self._parts_buffer = SocketFramePartsBuffer()

    def send_frame_parts(self, parts: FrameParts) -> None:
        frame_data = socket_frame_parts_bytes(parts)
        self._socket.sendall(frame_data)

    def receive_frame_parts(self) -> FrameParts:
        parts = self._parts_buffer.take_frame_parts()
        while parts is None:
            self._parts_buffer.extend(self._recv_some())
            parts = self._parts_buffer.take_frame_parts()
        return parts

    def receive_frame_parts_nowait(self) -> FrameParts | None:
        self._recv_available()
        parts = self._parts_buffer.take_frame_parts()
        return parts

    def close(self) -> None:
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._socket.close()

    def _recv_some(self) -> bytes:
        chunk = self._socket.recv(SOCKET_READ_SIZE)
        if chunk == b"":
            raise SocketFrameConnectionClosedError(
                "socket closed while receiving frame parts"
            )

        return chunk

    def _recv_available(self) -> None:
        readable, _, _ = select.select([self._socket], [], [], 0)
        while readable:
            self._parts_buffer.extend(self._recv_some())
            readable, _, _ = select.select([self._socket], [], [], 0)

    @classmethod
    def make_pair(cls) -> tuple[Self, Self]:
        socket_a, socket_b = socket.socketpair()
        a = cls(socket_a)
        b = cls(socket_b)
        return a, b
