#!/usr/bin/env python3
# ropemother/capture/jsonrecords.py

"""JSON-compatible projections of capture records."""

import base64
import binascii

from ropemother.capture.writer import CaptureRecord
from ropemother.format.registry import PortableFormatID
from ropemother.format.portableformat import PortableFormatKey
from ropemother.format.registry import PortableFormatRegistration
from ropemother.message.messageidentity import CorrelationID, MessageID
from ropemother.message.records import (
    BusOperation,
    CapturedMessage,
    SerializedPayload,
)
from ropemother.message.symbols import (
    MessageSymbolKind,
    MessageSymbolRegistration,
    MessageTypeID,
    ProducerID,
    TopicID,
)
from ropemother.util.onelinejson import JSONRecord

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-01T14:40:52+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


def captured_message_record(message: CapturedMessage) -> JSONRecord:
    payload_bytes = message.serialized_payload.payload_bytes
    payload_base64 = base64.b64encode(payload_bytes).decode("ascii")
    record: JSONRecord = {
        "record_type": "CapturedMessage",
        "msg_id": int(message.msg_id),
        "msg_topic_id": int(message.msg_topic_id),
        "msg_type_id": int(message.msg_type_id),
        "msg_producer_id": int(message.msg_producer_id),
        "bus_operation": message.bus_operation.value,
        "bus_sequence": message.bus_sequence,
        "topic_sequence": message.topic_sequence,
        "bus_received_at": message.bus_received_at,
        "correlation_id": optional_int(message.correlation_id),
        "reply_to": optional_int(message.reply_to),
        "payload_format_id": message.serialized_payload.format_id.value,
        "payload_base64": payload_base64,
    }
    return record


def format_registration_record(
    registration: PortableFormatRegistration,
) -> JSONRecord:
    record: JSONRecord = {
        "record_type": "PortableFormatRegistration",
        "format_id": registration.format_id.value,
        "registration_key": registration.key.registration_key,
    }
    return record


def symbol_registration_record(
    registration: MessageSymbolRegistration,
) -> JSONRecord:
    record: JSONRecord = {
        "record_type": "MessageSymbolRegistration",
        "symbol_kind": registration.symbol_kind.value,
        "symbol_id": int(registration.symbol_id),
        "symbol": registration.symbol,
    }
    return record


def optional_int(value: int | None) -> int | None:
    result = None
    if value is not None:
        result = int(value)
    return result


def capture_record_from_record(record: JSONRecord) -> CaptureRecord:
    record_type = record.get("record_type")

    if record_type == "CapturedMessage":
        capture_record = captured_message_from_record(record)
    elif record_type == "PortableFormatRegistration":
        capture_record = format_registration_from_record(record)
    elif record_type == "MessageSymbolRegistration":
        capture_record = symbol_registration_from_record(record)
    else:
        raise ValueError("unknown capture record type")

    return capture_record


def captured_message_from_record(record: JSONRecord) -> CapturedMessage:
    payload_format_id_value = record.get("payload_format_id")
    if type(payload_format_id_value) is not int:
        raise ValueError("captured message payload format ID is invalid")
    payload_format_id = PortableFormatID(payload_format_id_value)

    payload_base64 = record.get("payload_base64")
    if not isinstance(payload_base64, str):
        raise ValueError("captured message payload bytes are invalid")

    try:
        payload_bytes = base64.b64decode(payload_base64, validate=True)
    except (binascii.Error, ValueError) as e:
        raise ValueError("captured message payload bytes are invalid") from e

    serialized_payload = SerializedPayload(
        format_id=payload_format_id, payload_bytes=payload_bytes
    )

    msg_id_value = record.get("msg_id")
    if type(msg_id_value) is not int:
        raise ValueError("captured message ID is invalid")
    msg_id = MessageID(msg_id_value)

    msg_topic_id_value = record.get("msg_topic_id")
    if type(msg_topic_id_value) is not int:
        raise ValueError("captured message topic ID is invalid")
    msg_topic_id = TopicID(msg_topic_id_value)

    msg_type_id_value = record.get("msg_type_id")
    if type(msg_type_id_value) is not int:
        raise ValueError("captured message type ID is invalid")
    msg_type_id = MessageTypeID(msg_type_id_value)

    msg_producer_id_value = record.get("msg_producer_id")
    if type(msg_producer_id_value) is not int:
        raise ValueError("captured message producer ID is invalid")
    msg_producer_id = ProducerID(msg_producer_id_value)

    bus_operation_value = record.get("bus_operation")
    if not isinstance(bus_operation_value, str):
        raise ValueError("captured message bus operation is invalid")

    try:
        bus_operation = BusOperation(bus_operation_value)
    except ValueError as e:
        raise ValueError("captured message bus operation is invalid") from e

    bus_sequence = record.get("bus_sequence")
    if type(bus_sequence) is not int or bus_sequence < 0:
        raise ValueError("captured message bus sequence is invalid")

    topic_sequence = record.get("topic_sequence")
    if type(topic_sequence) is not int or topic_sequence < 0:
        raise ValueError("captured message topic sequence is invalid")

    bus_received_at = record.get("bus_received_at")
    if type(bus_received_at) is not int or bus_received_at < 0:
        raise ValueError("captured message receive time is invalid")

    correlation_id_value = record.get("correlation_id")
    if correlation_id_value is None:
        correlation_id = None
    elif type(correlation_id_value) is int:
        correlation_id = CorrelationID(correlation_id_value)
    else:
        raise ValueError("captured message correlation ID is invalid")

    reply_to_value = record.get("reply_to")
    if reply_to_value is None:
        reply_to = None
    elif type(reply_to_value) is int:
        reply_to = MessageID(reply_to_value)
    else:
        raise ValueError("captured message reply target is invalid")

    message = CapturedMessage(
        serialized_payload=serialized_payload,
        msg_id=msg_id,
        msg_topic_id=msg_topic_id,
        msg_type_id=msg_type_id,
        msg_producer_id=msg_producer_id,
        bus_operation=bus_operation,
        bus_sequence=bus_sequence,
        topic_sequence=topic_sequence,
        bus_received_at=bus_received_at,
        correlation_id=correlation_id,
        reply_to=reply_to,
    )
    return message


def format_registration_from_record(
    record: JSONRecord,
) -> PortableFormatRegistration:
    format_id_value = record.get("format_id")
    if type(format_id_value) is not int:
        raise ValueError("portable format registration ID is invalid")
    format_id = PortableFormatID(format_id_value)

    registration_key = record.get("registration_key")
    if not isinstance(registration_key, str):
        raise ValueError("portable format registration key is invalid")
    key = portable_format_key_from_registration_key(registration_key)

    registration = PortableFormatRegistration(format_id=format_id, key=key)
    return registration


def symbol_registration_from_record(
    record: JSONRecord,
) -> MessageSymbolRegistration:
    symbol_kind_value = record.get("symbol_kind")
    if type(symbol_kind_value) is not int:
        raise ValueError("message symbol registration kind is invalid")

    try:
        symbol_kind = MessageSymbolKind(symbol_kind_value)
    except ValueError as e:
        raise ValueError("message symbol registration kind is invalid") from e

    symbol_id_value = record.get("symbol_id")
    if type(symbol_id_value) is not int:
        raise ValueError("message symbol registration ID is invalid")
    symbol_id = message_symbol_id(symbol_kind, symbol_id_value)

    symbol = record.get("symbol")
    if not isinstance(symbol, str):
        raise ValueError("message symbol registration symbol is invalid")

    registration = MessageSymbolRegistration(
        symbol_kind=symbol_kind, symbol_id=symbol_id, symbol=symbol
    )
    return registration


def portable_format_key_from_registration_key(
    registration_key: str,
) -> PortableFormatKey:
    symbol, separator, version = registration_key.partition(":")

    if separator == "":
        key = PortableFormatKey.from_str(symbol)
    else:
        key = PortableFormatKey.from_str(symbol, version=version)

    return key


def message_symbol_id(
    symbol_kind: MessageSymbolKind,
    value: int,
) -> TopicID | MessageTypeID | ProducerID:
    if symbol_kind is MessageSymbolKind.TOPIC:
        symbol_id = TopicID(value)
    elif symbol_kind is MessageSymbolKind.MSG_TYPE:
        symbol_id = MessageTypeID(value)
    elif symbol_kind is MessageSymbolKind.PRODUCER:
        symbol_id = ProducerID(value)
    else:
        raise ValueError("message symbol registration kind is invalid")

    return symbol_id
