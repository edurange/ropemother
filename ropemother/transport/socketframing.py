#!/usr/bin/env python3
# ropemother/transport/socketframing.py

"""Shared frame-parts encoding for socket-backed transport connections."""

import struct

from ropemother.exceptions import MessageBusBaseException
from ropemother.transport.codec import FrameParts

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-04T22:22:21+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


_COUNT_FORMAT = "!I"
_LENGTH_FORMAT = "!Q"
_COUNT_SIZE = struct.calcsize(_COUNT_FORMAT)
_LENGTH_SIZE = struct.calcsize(_LENGTH_FORMAT)
SOCKET_READ_SIZE = 4096


class SocketFrameConnectionError(MessageBusBaseException):
    """Base exception for socket-backed frame connection errors."""
    pass


class SocketFrameConnectionClosedError(EOFError, SocketFrameConnectionError):
    """Raised when a socket closes while receiving frame data."""
    pass


class InvalidSocketFramePartsError(ValueError, SocketFrameConnectionError):
    """Raised when socket frame parts are malformed."""
    pass


def socket_frame_parts_bytes(parts: FrameParts) -> bytes:
    """Encode frame parts into the shared socket wire representation."""
    if len(parts) == 0:
        raise InvalidSocketFramePartsError(
            "socket frame must contain at least one part"
        )

    chunks = [struct.pack(_COUNT_FORMAT, len(parts))]
    for part in parts:
        chunks.append(struct.pack(_LENGTH_FORMAT, len(part)))
        chunks.append(part)
    return b"".join(chunks)


class SocketFramePartsBuffer:
    """Incremental decoder for socket-framed frame parts."""
    _receive_buffer: bytearray

    def __init__(self) -> None:
        self._receive_buffer = bytearray()

    def extend(self, data: bytes) -> None:
        self._receive_buffer.extend(data)

    def take_frame_parts(self) -> FrameParts | None:
        if len(self._receive_buffer) < _COUNT_SIZE:
            return None

        cursor = 0
        count_end = cursor + _COUNT_SIZE
        count_data = self._receive_buffer[cursor:count_end]
        part_count = struct.unpack(_COUNT_FORMAT, count_data)[0]
        if part_count == 0:
            raise InvalidSocketFramePartsError(
                "socket frame must contain at least one part"
            )

        cursor = count_end
        parts = []
        for _ in range(part_count):
            length_end = cursor + _LENGTH_SIZE
            if len(self._receive_buffer) < length_end:
                return None

            length_data = self._receive_buffer[cursor:length_end]
            part_length = struct.unpack(_LENGTH_FORMAT, length_data)[0]
            cursor = length_end

            part_end = cursor + part_length
            if len(self._receive_buffer) < part_end:
                return None

            part = bytes(self._receive_buffer[cursor:part_end])
            parts.append(part)
            cursor = part_end

        del self._receive_buffer[:cursor]
        return tuple(parts)
