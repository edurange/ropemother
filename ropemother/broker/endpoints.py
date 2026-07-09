#!/usr/bin/env python3
# ropemother/broker/endpoints.py

"""Abstract interfaces for message bus participants."""

from abc import ABC, abstractmethod
from typing import Any

from ropemother.exceptions import (
    InvalidReceiveCountError,
    MessageBusBaseException,
)
from ropemother.format.portableformat import PortableFormat
from ropemother.message.messageidentity import CorrelationID, MessageID
from ropemother.message.records import BusOperation, ReceivedMessage

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T03:17:53+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


class EndpointUsageError(MessageBusBaseException):
    """Base exception for endpoint usage errors."""
    pass


class InvalidReplyRequestError(ValueError, EndpointUsageError):
    """Raised when reply metadata cannot be derived from a request."""
    pass


class UnlistedMessageTypeError(ValueError, EndpointUsageError):
    """Raised when an emitter uses an undeclared message type."""
    pass


class UnsupportedTypeFormatError(ValueError, EndpointUsageError):
    """Raised when a message type does not support a payload format."""
    pass


class Emitter(ABC):
    """Endpoint that publishes messages from a registered producer."""

    @abstractmethod
    def emit(
        self,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        ...

    @abstractmethod
    def emit_request(
        self,
        payload: Any,
        *,
        correlation_id: CorrelationID,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        ...

    @abstractmethod
    def emit_reply(
        self,
        request: ReceivedMessage,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        ...


class ReceiveEndpoint(ABC):
    """Minimal endpoint that can receive one message."""

    @abstractmethod
    def receive(self) -> ReceivedMessage:
        ...


class Receiver(ReceiveEndpoint):
    """Endpoint that receives subscribed messages with convenience methods."""

    @abstractmethod
    def _receive_batch(
        self, *, min_count: int, max_count: int | None
    ) -> list[ReceivedMessage]:
        ...

    def receive(self) -> ReceivedMessage:
        messages = self._receive_batch(min_count=1, max_count=1)
        return messages[0]

    def receive_available(self) -> list[ReceivedMessage]:
        return self._receive_batch(min_count=0, max_count=None)

    def receive_batch(
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
        return self._receive_batch(min_count=min_count, max_count=max_count)

    def receive_many(self, max_count: int) -> list[ReceivedMessage]:
        if max_count < 1:
            raise InvalidReceiveCountError("Maximum count must be at least 1")
        return self._receive_batch(min_count=1, max_count=max_count)

    def receive_nowait(self) -> ReceivedMessage | None:
        messages = self._receive_batch(min_count=0, max_count=1)
        message = None
        if messages:
            message = messages[0]
        return message


def reply_metadata_for(
    request: ReceivedMessage
) -> tuple[CorrelationID, MessageID]:
    """Return correlation and reply target metadata for a request."""
    if request.bus_operation != BusOperation.REQUEST:
        raise InvalidReplyRequestError(
            "cannot reply to a message that is not a request"
        )

    correlation_id = request.correlation_id
    if correlation_id is None:
        raise InvalidReplyRequestError(
            "cannot reply to a request without a correlation ID"
        )

    reply_to = request.msg_id
    return correlation_id, reply_to
