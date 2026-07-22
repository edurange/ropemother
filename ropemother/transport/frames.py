#!/usr/bin/env python3
# ropemother/transport/frames.py

"""Transport frame vocabulary for setup and compact message delivery."""

from dataclasses import dataclass

from ropemother.capture.writer import RegistrationRecord
from ropemother.format.portableformat import PortableFormatKey
from ropemother.format.registry import PortableFormatID
from ropemother.message.messageidentity import CorrelationID, MessageID
from ropemother.message.records import BusOperation
from ropemother.message.selectors import (
    OptionalSymbolInput,
    SubscriptionTopicSelector,
    SymbolCollectionInput,
)
from ropemother.message.symbols import MessageTypeID, ProducerID, TopicID
from ropemother.util.typedid import TypedID

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-05T16:42:54+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


class TransportSubscriptionID(TypedID):
    """Session-local typed identifier for a transport subscription."""
    pass


@dataclass(frozen=True, kw_only=True)
class TransportTypeFormatSupport:
    """Transport-safe payload format support for one message type."""
    msg_type: str
    format_keys: tuple[PortableFormatKey, ...]


@dataclass(frozen=True, kw_only=True)
class RegisterEmitterFrame:
    """Transport request to register an emitter endpoint."""
    msg_topic: str
    msg_producer: str
    msg_type: str
    format_key: PortableFormatKey
    additional_msg_types: SymbolCollectionInput = ()
    allow_unlisted_type_formats: bool = False
    supported_type_formats: tuple[TransportTypeFormatSupport, ...] = ()


@dataclass(frozen=True, kw_only=True)
class RegisterEmitterResultFrame:
    """Transport reply containing registered emitter identifiers."""
    msg_topic_id: TopicID
    msg_producer_id: ProducerID
    msg_type_id: MessageTypeID
    msg_format_id: PortableFormatID
    registrations: tuple[RegistrationRecord, ...]


@dataclass(frozen=True, kw_only=True)
class RegisterMessageTypeFrame:
    """Transport request to register an additional message type."""
    msg_type: str


@dataclass(frozen=True, kw_only=True)
class RegisterMessageTypeResultFrame:
    """Transport reply containing a registered message type identifier."""
    msg_type_id: MessageTypeID
    registrations: tuple[RegistrationRecord, ...]


@dataclass(frozen=True, kw_only=True)
class RegisterPayloadFormatFrame:
    """Transport request to register a portable payload format."""
    format_key: PortableFormatKey


@dataclass(frozen=True, kw_only=True)
class RegisterPayloadFormatResultFrame:
    """Transport reply containing a registered payload format identifier."""
    format_id: PortableFormatID
    registrations: tuple[RegistrationRecord, ...]


@dataclass(frozen=True, kw_only=True)
class EmitFrame:
    """Transport request to emit a serialized message."""
    msg_topic_id: TopicID
    msg_producer_id: ProducerID
    msg_type_id: MessageTypeID
    msg_format_id: PortableFormatID
    payload_bytes: bytes
    bus_operation: BusOperation = BusOperation.PUBLISH
    correlation_id: CorrelationID | None = None
    reply_to: MessageID | None = None
    result_requested: bool = True


@dataclass(frozen=True, kw_only=True)
class EmitResultFrame:
    """Transport reply acknowledging an emit request."""
    pass


@dataclass(frozen=True, kw_only=True)
class SubscribeFrame:
    """Transport request to register a receiver subscription."""
    msg_topic: tuple[SubscriptionTopicSelector, ...]
    msg_producer: OptionalSymbolInput
    msg_type: OptionalSymbolInput


@dataclass(frozen=True, kw_only=True)
class SubscribeResultFrame:
    """Transport reply containing a registered subscription identifier."""
    subscription_id: TransportSubscriptionID
    registrations: tuple[RegistrationRecord, ...]


@dataclass(frozen=True, kw_only=True)
class DeliveryFrame:
    """Transport frame carrying a delivered serialized message."""
    subscription_id: TransportSubscriptionID
    msg_topic_id: TopicID
    msg_producer_id: ProducerID
    msg_type_id: MessageTypeID
    msg_format_id: PortableFormatID
    msg_id: MessageID
    payload_bytes: bytes
    bus_operation: BusOperation = BusOperation.PUBLISH
    correlation_id: CorrelationID | None = None
    reply_to: MessageID | None = None


@dataclass(frozen=True, kw_only=True)
class TransportErrorFrame:
    """Transport frame carrying a protocol or session error."""
    error_code: str
    error_message: str
    request_id: int | None = None


@dataclass(frozen=True, kw_only=True)
class RegistrationFrame:
    """Captured registration frame mirrored over a transport session."""
    registrations: tuple[RegistrationRecord, ...]
