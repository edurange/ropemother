#!/usr/bin/env python3
# ropemother/transport/asyncconnection.py

"""Asynchronous transport-neutral frame connections."""

from abc import ABC, abstractmethod
from asyncio import Lock, Queue, QueueEmpty
from typing import Self

from ropemother.transport.codec import (
    FrameParts,
    TransportFrame,
    decode_frame,
    encode_frame,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-04T22:32:27+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class AsyncFrameConnection(ABC):
    """Async transport-neutral connection for frame parts."""
    @abstractmethod
    async def send_frame_parts(self, parts: FrameParts) -> None:
        pass

    @abstractmethod
    async def receive_frame_parts(self) -> FrameParts:
        pass

    @abstractmethod
    def receive_frame_parts_nowait(self) -> FrameParts | None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class AsyncMemoryFrameConnection(AsyncFrameConnection):
    """Async in-memory frame connection backed by paired queues."""
    def __init__(
        self, incoming: Queue[FrameParts], outgoing: Queue[FrameParts]
    ) -> None:
        self._incoming = incoming
        self._outgoing = outgoing

    async def send_frame_parts(self, parts: FrameParts) -> None:
        await self._outgoing.put(parts)

    async def receive_frame_parts(self) -> FrameParts:
        return await self._incoming.get()

    def receive_frame_parts_nowait(self) -> FrameParts | None:
        parts = None
        try:
            parts = self._incoming.get_nowait()
        except QueueEmpty:
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


class AsyncFrameChannel:
    """Async connection wrapper that encodes and decodes transport frames."""
    def __init__(self, connection: AsyncFrameConnection) -> None:
        self._connection = connection
        self._send_lock = Lock()

    async def send_frame(self, frame: TransportFrame) -> None:
        parts = encode_frame(frame)
        async with self._send_lock:
            await self._connection.send_frame_parts(parts)

    async def receive_frame(self) -> TransportFrame:
        parts = await self._connection.receive_frame_parts()
        return decode_frame(parts)

    def receive_frame_nowait(self) -> TransportFrame | None:
        parts = self._connection.receive_frame_parts_nowait()
        frame = None
        if parts is not None:
            frame = decode_frame(parts)
        return frame

    def close(self) -> None:
        self._connection.close()
