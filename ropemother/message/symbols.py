#!/usr/bin/env python3
# ropemother/message/symbols.py

"""Implements numeric run-local symbol IDs for message bus records."""

from dataclasses import dataclass
from enum import Enum
from typing import TypeVar

from ropemother.exceptions import MessageBusBaseException
from ropemother.util.symbol import (
    is_ascii_alphanumeric,
    is_simple_symbol_character,
)
from ropemother.util.typedid import TypedID

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-04T15:06:40+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


_RESERVED_TOPIC_ROOTS = frozenset({"bus", "system"})
# In future iterations, reserved roots should be configurable, and emitting to
# them should enforce a simple capability check so that only host-approved
# emitters can publish to them. Reserved roots protect control namespaces or
# topics whose messages may be interpreted as bus-, host-, or downstream-
# coordination data. Ordinary emitter registration should not be able to
# publish under these roots. The host application can distribute explicitly
# approved reserved-emitter paths for code responsible for authoring those
# messages.


class MessageSymbolError(MessageBusBaseException):
    """Base exception for message symbol errors."""
    pass


class InvalidMessageSymbolError(ValueError, MessageSymbolError):
    """Raised when a message symbol is invalid."""
    pass


class InvalidMessageSymbolTypeError(TypeError, MessageSymbolError):
    """Raised when a message symbol value has the wrong type."""
    pass


class ReservedMessageSymbolError(ValueError, MessageSymbolError):
    """Raised when a message topic uses a reserved root."""
    pass


class MessageSymbolKind(Enum):
    """Kinds of message symbols managed by the bus."""
    TOPIC = 1
    MSG_TYPE = 2
    PRODUCER = 3


# Can these be reconciled with utility symbol types from intarsia?
class TopicID(TypedID):
    """Compact identifier for a registered message topic."""
    pass


class MessageTypeID(TypedID):
    """Compact identifier for a registered message type."""
    pass


class ProducerID(TypedID):
    """Compact identifier for a registered message producer."""
    pass


type AnyMessageSymbolID = TopicID | MessageTypeID | ProducerID
ID = TypeVar("ID", TopicID, MessageTypeID, ProducerID)


@dataclass(frozen=True, kw_only=True, slots=True)
class MessageSymbolRegistration:
    """Captured binding from a compact ID to a message symbol."""
    symbol_kind: MessageSymbolKind
    symbol_id: AnyMessageSymbolID
    symbol: str


class MessageSymbolRegistry:
    """Registry that assigns compact IDs to readable message symbols."""
    _topic_ids: dict[str, TopicID]
    _msg_type_id: dict[str, MessageTypeID]
    _producer_ids: dict[str, ProducerID]
    _registrations: list[MessageSymbolRegistration]

    def __init__(self) -> None:
        self._topic_ids = {}
        self._msg_type_id = {}
        self._producer_ids = {}
        self._registrations = []

    def ensure_topic_id(
        self, msg_topic: str
    ) -> tuple[TopicID, MessageSymbolRegistration | None]:
        _validate_msg_topic(msg_topic)
        result = self._ensure_symbol_id(
            table=self._topic_ids,
            id_type=TopicID,
            symbol_kind=MessageSymbolKind.TOPIC,
            symbol=msg_topic,
        )
        return result

    def ensure_msg_type_id(
        self, msg_type: str
    ) -> tuple[MessageTypeID, MessageSymbolRegistration | None]:
        validate_msg_type(msg_type)
        result = self._ensure_symbol_id(
            table=self._msg_type_id,
            id_type=MessageTypeID,
            symbol_kind=MessageSymbolKind.MSG_TYPE,
            symbol=msg_type,
        )
        return result

    def ensure_producer_id(
        self, msg_producer: str
    ) -> tuple[ProducerID, MessageSymbolRegistration | None]:
        _validate_msg_producer(msg_producer)
        result = self._ensure_symbol_id(
            table=self._producer_ids,
            id_type=ProducerID,
            symbol_kind=MessageSymbolKind.PRODUCER,
            symbol=msg_producer,
        )
        return result

    def registrations(self) -> tuple[MessageSymbolRegistration, ...]:
        return tuple(self._registrations)

    def _ensure_symbol_id(
        self,
        *,
        table: dict[str, ID],
        id_type: type[ID],
        symbol_kind: MessageSymbolKind,
        symbol: str,
    ) -> tuple[ID, MessageSymbolRegistration | None]:
        if symbol in table:
            symbol_id = table[symbol]
            registration = None
        else:
            symbol_id = id_type(len(table))
            table[symbol] = symbol_id
            registration = MessageSymbolRegistration(
                symbol_kind=symbol_kind, symbol_id=symbol_id, symbol=symbol
            )
            self._registrations.append(registration)
        return symbol_id, registration


def coerce_topic_id(value: int) -> TopicID:
    return TopicID(value)


def coerce_msg_type_id(value: int) -> MessageTypeID:
    return MessageTypeID(value)


def coerce_producer_id(value: int) -> ProducerID:
    return ProducerID(value)


def _validate_msg_topic(msg_topic: str) -> None:
    _validate_symbol_type(msg_topic, "message topic")

    segments = msg_topic.split(".")
    if any(segment == "" for segment in segments):
        raise InvalidMessageSymbolError(
            "message topic must not contain empty segments"
        )

    for segment in segments:
        _validate_simple_symbol_segment(segment, "message topic segment")

    if segments[0] in _RESERVED_TOPIC_ROOTS:
        raise ReservedMessageSymbolError(
            f"reserved message topic root: {segments[0]}"
        )


def _validate_msg_producer(msg_producer: str) -> None:
    _validate_symbol_type(msg_producer, "message producer")
    _validate_simple_symbol_segment(msg_producer, "message producer")


def validate_msg_type(msg_type: str) -> None:
    _validate_symbol_type(msg_type, "message type")
    _validate_simple_symbol_segment(msg_type, "message type")


def _validate_simple_symbol_segment(segment: str, label: str) -> None:
    if segment == "":
        raise InvalidMessageSymbolError(f"{label} must not be empty")

    first = segment[0]
    if not is_ascii_alphanumeric(first):
        raise InvalidMessageSymbolError(
            f"{label} must start with an ASCII letter or digit"
        )

    for char in segment[1:]:
        if not is_simple_symbol_character(char):
            raise InvalidMessageSymbolError(
                f"{label} may contain only ASCII letters, digits, "
                "hyphen, or underscore"
            )


def _validate_symbol(symbol: str) -> None:
    if not isinstance(symbol, str):
        raise InvalidMessageSymbolTypeError(
            f"message symbol must be a string: got {symbol!r}"
        )

    if symbol == "":
        raise InvalidMessageSymbolError(
            "message symbol must not be empty"
        )


def _validate_symbol_type(symbol: object, label: str) -> None:
    if not isinstance(symbol, str):
        raise InvalidMessageSymbolTypeError(
            f"{label} must be a string: got {symbol!r}"
        )

    if symbol == "":
        raise InvalidMessageSymbolError(f"{label} must not be empty")
