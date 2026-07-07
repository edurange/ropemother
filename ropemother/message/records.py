#!/usr/bin/env python3
# ropemother/message/records.py

"""Core messaging value types."""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from ropemother.format.registry import PortableFormatID
from ropemother.message.messageidentity import CorrelationID, MessageID
from ropemother.message.symbols import MessageTypeID, ProducerID, TopicID

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T03:22:17+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


class BusOperation(Enum):
    """Message operation kind used for publish, request, and reply routing."""
    PUBLISH = "publish"
    REQUEST = "request"
    REPLY = "reply"


@dataclass(frozen=True, kw_only=True)
class ReceivedMessage[DomainT]:
    """Subscriber-facing message with readable metadata and decoded payload."""
    payload: Any
    msg_topic: str
    msg_type: str
    msg_producer: str
    msg_id: MessageID
    bus_operation: BusOperation = BusOperation.PUBLISH
    correlation_id: CorrelationID | None = None
    reply_to: MessageID | None = None


@dataclass(frozen=True, kw_only=True)
class SerializedPayload:
    """Portable payload bytes with the format used to decode them."""
    format_id: PortableFormatID
    payload_bytes: bytes


@dataclass(frozen=True, kw_only=True)
class CapturedMessage:
    """Compact capture record for a serialized bus message."""
    # Can this be just serialized?
    serialized_payload: SerializedPayload
    msg_id: MessageID
    msg_topic_id: TopicID
    msg_type_id: MessageTypeID
    msg_producer_id: ProducerID
    bus_operation: BusOperation
    bus_sequence: int
    topic_sequence: int
    bus_received_at: int
    correlation_id: CorrelationID | None = None
    reply_to: MessageID | None = None


@dataclass(frozen=True, kw_only=True)
class EmitRequest:
    """Local emit request before broker sequencing and capture metadata."""
    payload: Any
    msg_topic: str
    msg_type: str


@dataclass(frozen=True, kw_only=True)
class BusMessage[DomainT]:
    """Internal broker message carrying readable symbols and compact IDs."""
    payload: Any
    # Can this be just serialized?
    serialized_payload: SerializedPayload
    msg_id: MessageID
    msg_topic: str
    msg_type: str
    msg_producer: str
    msg_topic_id: TopicID
    msg_type_id: MessageTypeID
    msg_producer_id: ProducerID
    bus_operation: BusOperation
    bus_sequence: int
    topic_sequence: int
    bus_received_at: int
    correlation_id: CorrelationID | None = None
    reply_to: MessageID | None = None

    def received_view(self) -> ReceivedMessage:
        message = ReceivedMessage(
            payload=self.payload,
            msg_topic=self.msg_topic,
            msg_type=self.msg_type,
            msg_producer=self.msg_producer,
            bus_operation=self.bus_operation,
            msg_id=self.msg_id,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
        )
        return message

    def captured_view(self) -> CapturedMessage:
        message = CapturedMessage(
            serialized_payload=self.serialized_payload,
            msg_id=self.msg_id,
            msg_topic_id=self.msg_topic_id,
            msg_type_id=self.msg_type_id,
            msg_producer_id=self.msg_producer_id,
            bus_operation=self.bus_operation,
            bus_sequence=self.bus_sequence,
            topic_sequence=self.topic_sequence,
            bus_received_at=self.bus_received_at,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
        )
        return message