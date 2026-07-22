#!/usr/bin/env python3
# ropemother/transport/asyncsocketconnection.py

"""Async socket-backed transport connection for message bus frame parts."""

import asyncio
from pathlib import Path
import select
import socket

from ropemother.transport.asyncconnection import AsyncFrameConnection
from ropemother.transport.codec import FrameParts
from ropemother.transport.socketframing import (
    SOCKET_READ_SIZE,
    SocketFrameConnectionClosedError,
    SocketFramePartsBuffer,
    socket_frame_parts_bytes,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-04T22:33:23+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


class AsyncSocketFrameConnection(AsyncFrameConnection):
    """Async socket-backed connection for sending and receiving frame parts."""
    _socket: socket.socket
    _parts_buffer: SocketFramePartsBuffer

    def __init__(self, stream_socket: socket.socket) -> None:
        self._socket = stream_socket
        self._socket.setblocking(False)
        self._parts_buffer = SocketFramePartsBuffer()

    async def send_frame_parts(self, parts: FrameParts) -> None:
        frame_data = socket_frame_parts_bytes(parts)
        loop = asyncio.get_running_loop()
        await loop.sock_sendall(self._socket, frame_data)

    async def receive_frame_parts(self) -> FrameParts:
        parts = self._parts_buffer.take_frame_parts()
        while parts is None:
            self._parts_buffer.extend(await self._recv_some())
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

    async def _recv_some(self) -> bytes:
        loop = asyncio.get_running_loop()
        chunk = await loop.sock_recv(self._socket, SOCKET_READ_SIZE)
        if chunk == b"":
            raise SocketFrameConnectionClosedError(
                "socket closed while receiving frame parts"
            )

        return chunk

    def _recv_available(self) -> None:
        readable, _, _ = select.select([self._socket], [], [], 0)
        while readable:
            try:
                chunk = self._socket.recv(SOCKET_READ_SIZE)
            except BlockingIOError:
                return

            if chunk == b"":
                raise SocketFrameConnectionClosedError(
                    "socket closed while receiving frame parts"
                )

            self._parts_buffer.extend(chunk)
            readable, _, _ = select.select([self._socket], [], [], 0)


async def open_async_socket_frame_connection(
    socket_path: Path | str,
) -> AsyncSocketFrameConnection:
    """Open an async socket-backed frame connection."""
    stream_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stream_socket.setblocking(False)
    try:
        loop = asyncio.get_running_loop()
        await loop.sock_connect(stream_socket, str(socket_path))
    except OSError:
        stream_socket.close()
        raise

    return AsyncSocketFrameConnection(stream_socket)
