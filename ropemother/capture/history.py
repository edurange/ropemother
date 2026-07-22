#!/usr/bin/env python3
# ropemother/capture/history.py

"""Read-side history views over captured message records."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, override

from ropemother.capture.historyselection import (
    DEFAULT_HISTORY_MAX_COUNT,
    HistorySelection,
    history_selection_from_args,
)
from ropemother.capture.writer import CaptureRecord, CaptureRecordSource
from ropemother.exceptions import MessageBusBaseException
from ropemother.format.defaults import default_portable_format_registry
from ropemother.format.formattable import (
    PortableFormatTable,
    PortableFormatTableError,
)
from ropemother.format.portableformat import PortableFormat, PortableFormatKey
from ropemother.message.messageidentity import CorrelationID, MessageID
from ropemother.message.records import BusOperation, CapturedMessage
from ropemother.message.registrationtable import (
    MessageRegistration,
    MessageRegistrationTable,
    UnknownMessageRegistrationError,
)
from ropemother.message.symbols import MessageTypeID, ProducerID, TopicID

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-22T15:23:09+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class MessageHistoryError(MessageBusBaseException):
    """Base exception for message history query errors."""
    pass


class InvalidHistorySelectionError(ValueError, MessageHistoryError):
    """Raised when a message history query is invalid."""
    pass


class IncompleteMessageHistoryError(LookupError, MessageHistoryError):
    """Raised when captured history is missing required registrations."""
    pass


class MessageHistoryPayloadDecodeError(ValueError, MessageHistoryError):
    """Raised when a captured payload cannot be decoded."""
    pass


@dataclass(frozen=True, kw_only=True)
class MessageHistoryEntry:
    """Decoded captured message returned from a history query."""
    payload: Any
    payload_format_key: PortableFormatKey
    payload_bytes: bytes
    msg_topic: str
    msg_type: str
    msg_producer: str
    msg_id: MessageID
    bus_operation: BusOperation
    bus_sequence: int
    topic_sequence: int
    bus_received_at: int
    correlation_id: CorrelationID | None = None
    reply_to: MessageID | None = None


@dataclass(frozen=True, kw_only=True)
class MessageHistoryPage:
    """Page of decoded history entries and the next sequence cursor."""
    entries: tuple[MessageHistoryEntry, ...]
    next_sequence: int | None = None


class MessageHistory(ABC):
    """Read-side interface for querying captured message history."""

    @abstractmethod
    def select(
        self,
        *,
        msg_topic: str | None = None,
        msg_type: str | None = None,
        msg_producer: str | None = None,
        bus_operation: BusOperation | None = None,
        start_sequence: int | None = None,
        stop_sequence: int | None = None,
        max_count: int = DEFAULT_HISTORY_MAX_COUNT,
    ) -> MessageHistoryPage:
        ...


class InMemoryCaptureHistory(MessageHistory):
    """Message history view backed by in-memory capture records."""
    _source: CaptureRecordSource
    _format_registry: PortableFormatTable
    _registrations: MessageRegistrationTable
    _indexed_record_count: int

    def __init__(
        self,
        source: CaptureRecordSource,
        *,
        extra_formats: Iterable[PortableFormat[Any, Any]] = (),
    ) -> None:
        format_registry = default_portable_format_registry(
            extra_formats=extra_formats
        )
        self._initialize(source, format_registry=format_registry)

    @classmethod
    def _from_format_registry(
        cls,
        source: CaptureRecordSource,
        *,
        format_registry: PortableFormatTable,
    ) -> "InMemoryCaptureHistory":
        history = cls.__new__(cls)
        history._initialize(source, format_registry=format_registry)
        return history

    @override
    def select(
        self,
        *,
        msg_topic: str | None = None,
        msg_type: str | None = None,
        msg_producer: str | None = None,
        bus_operation: BusOperation | None = None,
        start_sequence: int | None = None,
        stop_sequence: int | None = None,
        max_count: int = DEFAULT_HISTORY_MAX_COUNT,
    ) -> MessageHistoryPage:
        selection = history_selection_from_args(
            msg_topic=msg_topic,
            msg_type=msg_type,
            msg_producer=msg_producer,
            bus_operation=bus_operation,
            start_sequence=start_sequence,
            stop_sequence=stop_sequence,
            max_count=max_count,
        )
        _validate_selection(selection)

        self._refresh_registrations()

        topic_id = self._topic_id_for(selection.msg_topic)
        msg_type_id = self._msg_type_id_for(selection.msg_type)
        producer_id = self._producer_id_for(selection.msg_producer)
        if _missing_requested_symbol(
            selection, topic_id, msg_type_id, producer_id
        ):
            return MessageHistoryPage(entries=())

        entries: list[MessageHistoryEntry] = []
        next_sequence = None
        record_count = self._indexed_record_count
        records = self._source.read_capture_records(0, record_count)
        if len(records) != record_count:
            raise IncompleteMessageHistoryError(
                "history source returned fewer indexed records than requested"
            )
        for record in records:
            if not isinstance(record, CapturedMessage):
                continue
            if not _message_matches(
                record,
                selection=selection,
                topic_id=topic_id,
                msg_type_id=msg_type_id,
                producer_id=producer_id,
            ):
                continue
            if len(entries) >= selection.max_count:
                next_sequence = record.bus_sequence
                break
            entries.append(self._entry_for(record))

        page = MessageHistoryPage(
            entries=tuple(entries), next_sequence=next_sequence
        )
        return page

    def _initialize(
        self,
        source: CaptureRecordSource,
        *,
        format_registry: PortableFormatTable,
    ) -> None:
        self._source = source
        self._format_registry = format_registry
        self._registrations = MessageRegistrationTable()
        self._indexed_record_count = 0

    def _refresh_registrations(self) -> None:
        record_count = self._source.capture_record_count
        new_count = record_count - self._indexed_record_count

        if new_count < 0:
            raise IncompleteMessageHistoryError(
                "history source returned fewer records than before"
            )

        records = self._source.read_capture_records(
            self._indexed_record_count, new_count
        )
        if len(records) != new_count:
            raise IncompleteMessageHistoryError(
                "history source returned fewer indexed records than requested"
            )

        registrations = _registrations_from(records)

        self._registrations.apply_registrations(registrations)
        self._indexed_record_count = record_count

    def _topic_id_for(self, msg_topic: str | None) -> TopicID | None:
        if msg_topic is None:
            return None
        return self._registrations.find_topic_id_for(msg_topic)

    def _msg_type_id_for(
        self, msg_type: str | None
    ) -> MessageTypeID | None:
        if msg_type is None:
            return None
        return self._registrations.find_msg_type_id_for(msg_type)

    def _producer_id_for(
        self, msg_producer: str | None
    ) -> ProducerID | None:
        if msg_producer is None:
            return None
        return self._registrations.find_producer_id_for(msg_producer)

    def _entry_for(self, message: CapturedMessage) -> MessageHistoryEntry:
        serialized_payload = message.serialized_payload

        try:
            payload_format_key = self._registrations.format_key_for_id(
                serialized_payload.format_id
            )
        except UnknownMessageRegistrationError as e:
            raise IncompleteMessageHistoryError(
                "history is missing a payload format registration needed to "
                "decode a message"
            ) from e

        try:
            msg_topic = self._registrations.topic_for_id(message.msg_topic_id)
            msg_type = self._registrations.msg_type_for_id(message.msg_type_id)
            msg_producer = self._registrations.producer_for_id(
                message.msg_producer_id
            )
        except UnknownMessageRegistrationError as e:
            raise IncompleteMessageHistoryError(
                "history is missing a registration needed to read a message"
            ) from e

        payload_bytes = serialized_payload.payload_bytes
        payload = decode_history_payload(
            self._format_registry, payload_format_key, payload_bytes
        )
        entry = MessageHistoryEntry(
            payload=payload,
            payload_format_key=payload_format_key,
            payload_bytes=payload_bytes,
            msg_topic=msg_topic,
            msg_type=msg_type,
            msg_producer=msg_producer,
            msg_id=message.msg_id,
            bus_operation=message.bus_operation,
            bus_sequence=message.bus_sequence,
            topic_sequence=message.topic_sequence,
            bus_received_at=message.bus_received_at,
            correlation_id=message.correlation_id,
            reply_to=message.reply_to,
        )
        return entry


def decode_history_payload(
    format_table: PortableFormatTable,
    format_key: PortableFormatKey,
    payload_bytes: bytes,
) -> Any:
    try:
        payload_format = format_table.from_key(format_key)
    except PortableFormatTableError as e:
        raise MessageHistoryPayloadDecodeError(
            "history has no local decoder for payload format "
            f"{format_key.registration_key!r}"
        ) from e

    try:
        payload = payload_format.decode(payload_bytes)
    except (TypeError, ValueError) as e:
        raise MessageHistoryPayloadDecodeError(
            "history payload could not be decoded with format "
            f"{format_key.registration_key!r}"
        ) from e
    return payload


def _validate_selection(selection: HistorySelection) -> None:
    if selection.max_count < 1:
        raise InvalidHistorySelectionError(
            f"max_count must be positive: got {selection.max_count}"
        )
    if selection.start_sequence is not None and selection.start_sequence < 0:
        raise InvalidHistorySelectionError(
            f"start_sequence must be non-negative: {selection.start_sequence}"
        )
    if selection.stop_sequence is not None and selection.stop_sequence < 0:
        raise InvalidHistorySelectionError(
            f"stop_sequence must be non-negative: {selection.stop_sequence}"
        )


def _missing_requested_symbol(
    selection: HistorySelection,
    topic_id: TopicID | None,
    msg_type_id: MessageTypeID | None,
    producer_id: ProducerID | None,
) -> bool:
    missing_topic = selection.msg_topic is not None and topic_id is None
    missing_msg_type = selection.msg_type is not None and msg_type_id is None
    missing_producer = (
        selection.msg_producer is not None and producer_id is None
    )
    return missing_topic or missing_msg_type or missing_producer


def _message_matches(
    message: CapturedMessage,
    *,
    selection: HistorySelection,
    topic_id: TopicID | None,
    msg_type_id: MessageTypeID | None,
    producer_id: ProducerID | None,
) -> bool:
    result = True
    if selection.bus_operation is not None:
        result = result and message.bus_operation is selection.bus_operation
    if selection.start_sequence is not None:
        result = result and message.bus_sequence >= selection.start_sequence
    if selection.stop_sequence is not None:
        result = result and message.bus_sequence < selection.stop_sequence
    if topic_id is not None:
        result = result and message.msg_topic_id == topic_id
    if msg_type_id is not None:
        result = result and message.msg_type_id == msg_type_id
    if producer_id is not None:
        result = result and message.msg_producer_id == producer_id
    return result


def _registrations_from(
    records: Iterable[CaptureRecord],
) -> tuple[MessageRegistration, ...]:
    registrations: list[MessageRegistration] = []
    for record in records:
        if not isinstance(record, CapturedMessage):
            registrations.append(record)
    return tuple(registrations)
