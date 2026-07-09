#!/usr/bin/env python3
# ropemother/broker/asyncendpoints.py

"""Asynchronous abstract interfaces for message bus participants."""

from abc import ABC, abstractmethod
from typing import Any

from ropemother.exceptions import InvalidReceiveCountError
from ropemother.format.portableformat import PortableFormat
from ropemother.message.messageidentity import CorrelationID
from ropemother.message.records import ReceivedMessage

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T03:21:36+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


class AsyncEmitter(ABC):
    """Async endpoint that publishes messages from a registered producer."""

    @abstractmethod
    async def emit(
        self,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        ...

    @abstractmethod
    async def emit_request(
        self,
        payload: Any,
        *,
        correlation_id: CorrelationID,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        ...

    @abstractmethod
    async def emit_reply(
        self,
        request: ReceivedMessage,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        ...


class AsyncReceiver(ABC):
    """Async subscriber endpoint with convenience methods."""

    @abstractmethod
    async def _receive_batch(
        self, *, min_count: int, max_count: int | None
    ) -> list[ReceivedMessage]:
        ...

    @abstractmethod
    def _receive_batch_nowait(
        self, *, max_count: int | None
    ) -> list[ReceivedMessage]:
        ...

    async def receive(self) -> ReceivedMessage:
        messages = await self._receive_batch(min_count=1, max_count=1)
        return messages[0]

    def receive_available(self) -> list[ReceivedMessage]:
        return self._receive_batch_nowait(max_count=None)

    async def receive_batch(
        self, *, min_count: int, max_count: int | None
    ) -> list[ReceivedMessage]:
        if min_count < 0:
            raise InvalidReceiveCountError("Minimum count cannot be negative")
        elif max_count is not None and max_count < 0:
            raise InvalidReceiveCountError("Maximum count cannot be negative")
        elif max_count is not None and min_count > max_count:
            raise InvalidReceiveCountError(
                "minimum count cannot be greater than maximum count"
            )
        messages = await self._receive_batch(
            min_count=min_count, max_count=max_count
        )
        return messages

    async def receive_many(self, max_count: int) -> list[ReceivedMessage]:
        if max_count < 1:
            raise InvalidReceiveCountError("Maximum count must be at least 1")
        return await self._receive_batch(min_count=1, max_count=max_count)

    def receive_nowait(self) -> ReceivedMessage | None:
        messages = self._receive_batch_nowait(max_count=1)
        message = None
        if messages:
            message = messages[0]
        return message
