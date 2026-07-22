#!/usr/bin/env python3
# ropemother/transport/connection.py

"""Transport-neutral connections for encoded message bus frame parts."""

from abc import ABC, abstractmethod
from queue import Empty, Queue
from threading import Lock
from typing import Self

from ropemother.transport.codec import (
    FrameParts,
    TransportFrame,
    decode_frame,
    encode_frame,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-04T21:26:57+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


class FrameConnection(ABC):
    """Transport-neutral connection for sending and receiving frame parts."""
    @abstractmethod
    def send_frame_parts(self, parts: FrameParts) -> None:
        pass

    @abstractmethod
    def receive_frame_parts(self) -> FrameParts:
        pass

    @abstractmethod
    def receive_frame_parts_nowait(self) -> FrameParts | None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class MemoryFrameConnection(FrameConnection):
    """In-memory frame connection backed by paired queues."""
    def __init__(
        self, incoming: Queue[FrameParts], outgoing: Queue[FrameParts]
    ) -> None:
        self._incoming = incoming
        self._outgoing = outgoing

    def send_frame_parts(self, parts: FrameParts) -> None:
        self._outgoing.put(parts)

    def receive_frame_parts(self) -> FrameParts:
        return self._incoming.get()

    def receive_frame_parts_nowait(self) -> FrameParts | None:
        parts = None
        try:
            parts = self._incoming.get_nowait()
        except Empty:
            pass
        return parts

    def close(self) -> None:
        pass

    @classmethod
    def make_pair(cls) -> tuple[Self, Self]:
        a_to_b: Queue[FrameParts] = Queue()
        b_to_a: Queue[FrameParts] = Queue()
        a = cls(incoming=b_to_a, outgoing=a_to_b)
        b = cls(incoming=a_to_b, outgoing=b_to_a)
        return a, b


class FrameChannel:
    """Connection wrapper that encodes and decodes transport frames."""
    _conncetion: FrameConnection
    _send_lock: Lock

    def __init__(self, connection: FrameConnection) -> None:
        self._connection = connection
        self._send_lock = Lock()

    def send_frame(self, frame: TransportFrame) -> None:
        parts = encode_frame(frame)
        with self._send_lock:
            self._connection.send_frame_parts(parts)

    def receive_frame(self) -> TransportFrame:
        parts = self._connection.receive_frame_parts()
        return decode_frame(parts)

    def receive_frame_nowait(self) -> TransportFrame | None:
        parts = self._connection.receive_frame_parts_nowait()
        frame = None
        if parts is not None:
            frame = decode_frame(parts)
        return frame

    def close(self) -> None:
        self._connection.close()
