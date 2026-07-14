#!/usr/bin/env python3
# ropemother/message/registrationtable.py

"""Registration table for compact message and format IDs."""

from collections.abc import Hashable, Iterable
from dataclasses import dataclass, field

from ropemother.exceptions import MessageBusBaseException
from ropemother.format.portableformat import PortableFormatKey
from ropemother.format.registry import (
    PortableFormatID,
    PortableFormatRegistration,
)
from ropemother.message.symbols import (
    MessageSymbolKind,
    MessageSymbolRegistration,
    MessageTypeID,
    ProducerID,
    TopicID,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T15:49:56+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class MessageRegistrationTableError(MessageBusBaseException):
    """Base exception for message registration table errors."""
    pass


class ConflictingMessageRegistrationError(
    ValueError, MessageRegistrationTableError
):
    """Raised when a compact ID conflicts with an existing registration."""
    pass


class UnknownMessageRegistrationError(KeyError, MessageRegistrationTableError):
    """Raised when a compact ID or registered value is unknown."""
    pass


type MessageRegistration = (
    MessageSymbolRegistration | PortableFormatRegistration
)


@dataclass
class _IDTable[ID: Hashable, T: Hashable]:
    label: str
    objects_by_id: dict[ID, T] = field(default_factory=dict)
    ids_by_object: dict[T, ID] = field(default_factory=dict)

    def bind(self, object_id: ID, obj: T) -> None:
        existing_obj = self.objects_by_id.get(object_id)
        if existing_obj is not None and existing_obj != obj:
            raise ConflictingMessageRegistrationError(
                f"conflicting {self.label} registration for ID {object_id}"
            )
        existing_id = self.ids_by_object.get(obj)
        if existing_id is not None and existing_id != object_id:
            raise ConflictingMessageRegistrationError(
                f"conflicting {self.label} registration for {obj!r}"
            )

        self.objects_by_id[object_id] = obj
        self.ids_by_object[obj] = object_id

    def object_for_id(self, object_id: ID) -> T:
        try:
            obj = self.objects_by_id[object_id]
        except KeyError as e:
            raise UnknownMessageRegistrationError(
                f"unknown {self.label} ID: {object_id}"
            ) from e
        return obj

    def id_for_object(self, obj: T) -> ID:
        try:
            object_id = self.ids_by_object[obj]
        except KeyError as e:
            raise UnknownMessageRegistrationError(
                f"unknown {self.label}: {obj!r}"
            ) from e
        return object_id

    def find_id_for_object(self, obj: T) -> ID | None:
        return self.ids_by_object.get(obj)


class MessageRegistrationTable:
    """Lookup table for compact message symbols and payload formats."""
    _topics: _IDTable[TopicID, str]
    _msg_types: _IDTable[MessageTypeID, str]
    _producers: _IDTable[ProducerID, str]
    _formats: _IDTable[PortableFormatID, PortableFormatKey]

    def __init__(self) -> None:
        self._topics = _IDTable("topic")
        self._msg_types = _IDTable("message type")
        self._producers = _IDTable("producer")
        self._formats = _IDTable("portable format")

    def apply_registrations(
        self, registrations: Iterable[MessageRegistration]
    ) -> None:
        for registration in registrations:
            self._apply_registration(registration)

    def _apply_registration(
        self, registration: MessageRegistration
    ) -> None:
        if isinstance(registration, MessageSymbolRegistration):
            self._apply_message_symbol_registration(registration)
        elif isinstance(registration, PortableFormatRegistration):
            self._formats.bind(registration.format_id, registration.key)
        else:
            registration_type = type(registration).__name__
            raise MessageRegistrationTableError(
                "unsupported message registration: " + registration_type
            )

    def topic_for_id(self, topic_id: TopicID) -> str:
        return self._topics.object_for_id(topic_id)

    def topic_id_for(self, msg_topic: str) -> TopicID:
        return self._topics.id_for_object(msg_topic)

    def find_topic_id_for(self, msg_topic: str) -> TopicID | None:
        return self._topics.find_id_for_object(msg_topic)

    def msg_type_for_id(self, msg_type_id: MessageTypeID) -> str:
        return self._msg_types.object_for_id(msg_type_id)

    def msg_type_id_for(self, msg_type: str) -> MessageTypeID:
        return self._msg_types.id_for_object(msg_type)

    def find_msg_type_id_for(self, msg_type: str) -> MessageTypeID | None:
        return self._msg_types.find_id_for_object(msg_type)

    def producer_for_id(self, producer_id: ProducerID) -> str:
        return self._producers.object_for_id(producer_id)

    def producer_id_for(self, msg_producer: str) -> ProducerID:
        return self._producers.id_for_object(msg_producer)

    def find_producer_id_for(
        self, msg_producer: str
    ) -> ProducerID | None:
        return self._producers.find_id_for_object(msg_producer)

    def format_key_for_id(
        self, format_id: PortableFormatID
    ) -> PortableFormatKey:
        return self._formats.object_for_id(format_id)

    def format_id_for(
        self, format_key: PortableFormatKey
    ) -> PortableFormatID:
        return self._formats.id_for_object(format_key)

    def find_format_id_for(
        self, format_key: PortableFormatKey
    ) -> PortableFormatID | None:
        return self._formats.find_id_for_object(format_key)

    def _apply_message_symbol_registration(
        self, registration: MessageSymbolRegistration
    ) -> None:
        if registration.symbol_kind is MessageSymbolKind.TOPIC:
            self._topics.bind(registration.symbol_id, registration.symbol)
        elif registration.symbol_kind is MessageSymbolKind.MSG_TYPE:
            self._msg_types.bind(registration.symbol_id, registration.symbol)
        elif registration.symbol_kind is MessageSymbolKind.PRODUCER:
            self._producers.bind(registration.symbol_id, registration.symbol)
        else:
            raise MessageRegistrationTableError(
                "unsupported message symbol kind: "
                + registration.symbol_kind.name
            )
