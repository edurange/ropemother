#!/usr/bin/env python3
# ropemother/transport/endpointregistration.py

"""Endpoint-local registration state for transport sessions."""

from collections.abc import Iterable

from ropemother.capture.writer import RegistrationRecord
from ropemother.format.portableformat import PortableFormatKey
from ropemother.format.registry import PortableFormatID
from ropemother.message.registrationtable import MessageRegistrationTable
from ropemother.message.symbols import MessageTypeID, ProducerID, TopicID

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T20:03:19+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


class EndpointRegistrationView:
    """Transport-safe view of registered endpoint defaults."""
    _symbols: MessageRegistrationTable
    _sent_registrations: set[RegistrationRecord]

    def __init__(self) -> None:
        self._symbols = MessageRegistrationTable()
        self._sent_registrations = set()

    def apply_registrations(
        self, registrations: Iterable[RegistrationRecord]
    ) -> None:
        self._symbols.apply_registrations(registrations)

    def take_unsent(
        self, registrations: Iterable[RegistrationRecord]
    ) -> tuple[RegistrationRecord, ...]:
        unsent: list[RegistrationRecord] = []
        for registration in registrations:
            if registration not in self._sent_registrations:
                unsent.append(registration)
                self._sent_registrations.add(registration)
        return tuple(unsent)

    def topic_for_id(self, topic_id: TopicID) -> str:
        return self._symbols.topic_for_id(topic_id)

    def topic_id_for(self, msg_topic: str) -> TopicID:
        return self._symbols.topic_id_for(msg_topic)

    def find_topic_id_for(self, msg_topic: str) -> TopicID | None:
        return self._symbols.find_topic_id_for(msg_topic)

    def msg_type_for_id(self, msg_type_id: MessageTypeID) -> str:
        return self._symbols.msg_type_for_id(msg_type_id)

    def msg_type_id_for(self, msg_type: str) -> MessageTypeID:
        return self._symbols.msg_type_id_for(msg_type)

    def find_msg_type_id_for(self, msg_type: str) -> MessageTypeID | None:
        return self._symbols.find_msg_type_id_for(msg_type)

    def producer_for_id(self, producer_id: ProducerID) -> str:
        return self._symbols.producer_for_id(producer_id)

    def producer_id_for(self, msg_producer: str) -> ProducerID:
        return self._symbols.producer_id_for(msg_producer)

    def find_producer_id_for(
        self, msg_producer: str
    ) -> ProducerID | None:
        return self._symbols.find_producer_id_for(msg_producer)

    def format_key_for_id(
        self, format_id: PortableFormatID
    ) -> PortableFormatKey:
        return self._symbols.format_key_for_id(format_id)

    def format_id_for(self, format_key: PortableFormatKey) -> PortableFormatID:
        return self._symbols.format_id_for(format_key)

    def find_format_id_for(
        self, format_key: PortableFormatKey
    ) -> PortableFormatID | None:
        return self._symbols.find_format_id_for(format_key)
