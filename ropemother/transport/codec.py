#!/usr/bin/env python3
# ropemother/transport/codec.py

"""Transport frame codec for metadata plus opaque payload byte parts."""

import json
from typing import Any

from ropemother.capture.writer import RegistrationRecord
from ropemother.exceptions import MessageBusBaseException
from ropemother.format.portableformat import PortableFormatKey
from ropemother.format.registry import (
    PortableFormatID,
    PortableFormatRegistration,
)
from ropemother.message.messageidentity import CorrelationID, MessageID
from ropemother.message.records import BusOperation
from ropemother.message.selectors import SubscriptionTopicSelector
from ropemother.message.symbols import (
    MessageSymbolKind,
    MessageSymbolRegistration,
    MessageTypeID,
    ProducerID,
    TopicID,
)
from ropemother.transport.frames import (
    DeliveryFrame,
    EmitFrame,
    EmitResultFrame,
    RegisterEmitterFrame,
    RegisterEmitterResultFrame,
    RegisterMessageTypeFrame,
    RegisterMessageTypeResultFrame,
    RegisterPayloadFormatFrame,
    RegisterPayloadFormatResultFrame,
    RegistrationFrame,
    SubscribeFrame,
    SubscribeResultFrame,
    TransportErrorFrame,
    TransportSubscriptionID,
    TransportTypeFormatSupport,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-05T16:43:31+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class TransportCodecError(MessageBusBaseException):
    """Base exception for transport frame codec errors."""
    pass


class UnsupportedTransportFrameError(TypeError, TransportCodecError):
    """Raised when a transport frame type is unsupported."""
    pass


class InvalidTransportFrameError(ValueError, TransportCodecError):
    """Raised when encoded transport frame data is invalid."""
    pass


class MissingTransportMetadataKeyError(KeyError, TransportCodecError):
    """Raised when required transport frame metadata is missing."""
    pass


type TransportFrame = (
    RegisterEmitterFrame
    | RegisterEmitterResultFrame
    | RegisterMessageTypeFrame
    | RegisterMessageTypeResultFrame
    | RegistrationFrame
    | EmitFrame
    | EmitResultFrame
    | SubscribeFrame
    | SubscribeResultFrame
    | DeliveryFrame
    | TransportErrorFrame
    | TransportTypeFormatSupport
)

type FrameParts = tuple[bytes, ...]
type Metadata = dict[str, Any]


def encode_frame(frame: TransportFrame) -> FrameParts:
    if isinstance(frame, RegisterEmitterFrame):
        return _encode_register_emitter_frame(frame)
    if isinstance(frame, RegisterEmitterResultFrame):
        return _encode_register_emitter_result_frame(frame)
    if isinstance(frame, RegisterMessageTypeFrame):
        return _encode_register_msg_type_frame(frame)
    if isinstance(frame, RegisterMessageTypeResultFrame):
        return _encode_register_msg_type_result_frame(frame)
    if isinstance(frame, RegisterPayloadFormatFrame):
        return _encode_register_payload_format_frame(frame)
    if isinstance(frame, RegisterPayloadFormatResultFrame):
        return _encode_register_payload_format_result_frame(frame)
    if isinstance(frame, RegistrationFrame):
        return _encode_registration_frame(frame)
    if isinstance(frame, EmitFrame):
        return _encode_emit_frame(frame)
    if isinstance(frame, EmitResultFrame):
        return _encode_emit_result_frame(frame)
    if isinstance(frame, SubscribeFrame):
        return _encode_subscribe_frame(frame)
    if isinstance(frame, SubscribeResultFrame):
        return _encode_subscribe_result_frame(frame)
    if isinstance(frame, DeliveryFrame):
        return _encode_delivery_frame(frame)
    if isinstance(frame, TransportErrorFrame):
        return _encode_transport_error_frame(frame)
    raise UnsupportedTransportFrameError(
        f"unsupported transport frame: {frame!r}"
    )


# These literals should probably be given proper names somewhere
def decode_frame(parts: FrameParts) -> TransportFrame:
    metadata, payload_bytes = _decode_parts(parts)
    try:
        frame_type = metadata["frame_type"]
    except KeyError as e:
        raise MissingTransportMetadataKeyError("frame_type") from e

    if frame_type == "register_emitter":
        return _decode_register_emitter_frame(metadata, payload_bytes)
    if frame_type == "register_emitter_result":
        return _decode_register_emitter_result_frame(metadata, payload_bytes)
    if frame_type == "register_msg_type":
        return _decode_register_msg_type_frame(metadata, payload_bytes)
    if frame_type == "register_msg_type_result":
        return _decode_register_msg_type_result_frame(metadata, payload_bytes)
    if frame_type == "register_payload_format":
        return _decode_register_payload_format_frame(metadata, payload_bytes)
    if frame_type == "register_payload_format_result":
        return _decode_register_payload_format_result_frame(
            metadata, payload_bytes
        )
    if frame_type == "registration":
        return _decode_registration_frame(metadata, payload_bytes)
    if frame_type == "emit":
        return _decode_emit_frame(metadata, payload_bytes)
    if frame_type == "emit_result":
        return _decode_emit_result_frame(metadata, payload_bytes)
    if frame_type == "subscribe":
        return _decode_subscribe_frame(metadata, payload_bytes)
    if frame_type == "subscribe_result":
        return _decode_subscribe_result_frame(metadata, payload_bytes)
    if frame_type == "delivery":
        return _decode_delivery_frame(metadata, payload_bytes)
    if frame_type == "transport_error":
        return _decode_transport_error_frame(metadata, payload_bytes)
    raise InvalidTransportFrameError(f"unknown frame type: {frame_type!r}")


def _encode_metadata(metadata: Metadata) -> bytes:
    data = json.dumps(
        metadata, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return data


def _decode_metadata(data: bytes) -> Metadata:
    metadata = json.loads(data.decode("utf-8"))
    if not isinstance(metadata, dict):
        raise InvalidTransportFrameError(
            f"frame metadata must decode to a dict: {metadata!r}"
        )
    return metadata


def _metadata_parts(metadata: Metadata) -> FrameParts:
    return (_encode_metadata(metadata),)


def _payload_parts(metadata: Metadata, payload_bytes: bytes) -> FrameParts:
    return (_encode_metadata(metadata), payload_bytes)


def _decode_parts(parts: FrameParts) -> tuple[Metadata, bytes | None]:
    if len(parts) == 1:
        metadata = _decode_metadata(parts[0])
        payload_bytes = None
    elif len(parts) == 2:
        metadata = _decode_metadata(parts[0])
        payload_bytes = parts[1]
    else:
        raise InvalidTransportFrameError(
            f"transport frame must have 1 or 2 parts: got {len(parts)}"
        )
    return metadata, payload_bytes


def _encode_format_key(format_key: PortableFormatKey) -> Metadata:
    encoded = {
        "symbol": format_key.symbol.value,
        "version": format_key.version,
    }
    return encoded


def _decode_format_key(encoded: Metadata) -> PortableFormatKey:
    symbol = encoded["symbol"]
    version = encoded["version"]
    return PortableFormatKey.from_str(symbol, version=version)


def _encode_type_format_support(
    support: TransportTypeFormatSupport,
) -> Metadata:
    format_keys = tuple(
        _encode_format_key(format_key) for format_key in support.format_keys
    )
    return {"msg_type": support.msg_type, "format_keys": format_keys}


def _decode_type_format_support(
    encoded: Metadata,
) -> TransportTypeFormatSupport:
    format_keys = tuple(
        _decode_format_key(format_key)
        for format_key in encoded.get("format_keys", ())
    )
    support = TransportTypeFormatSupport(
        msg_type=encoded["msg_type"], format_keys=format_keys
    )
    return support


def _encode_registration(registration: RegistrationRecord) -> Metadata:
    if isinstance(registration, MessageSymbolRegistration):
        encoded = _encode_symbol_registration(registration)
    elif isinstance(registration, PortableFormatRegistration):
        encoded = _encode_format_registration(registration)
    else:
        raise UnsupportedTransportFrameError(
            f"unsupported registration record: {registration!r}"
        )
    return encoded


def _encode_symbol_registration(
    registration: MessageSymbolRegistration,
) -> Metadata:
    encoded = {
        "record_type": "message_symbol_registration",
        "symbol_kind": registration.symbol_kind.name,
        "symbol_id": int(registration.symbol_id),
        "symbol": registration.symbol,
    }
    return encoded


def _encode_format_registration(
    registration: PortableFormatRegistration,
) -> Metadata:
    encoded = {
        "record_type": "portable_format_registration",
        "format_id": registration.format_id.value,
        "format_key": _encode_format_key(registration.key),
    }
    return encoded


def _decode_registration(encoded: Metadata) -> RegistrationRecord:
    record_type = encoded["record_type"]
    if record_type == "message_symbol_registration":
        registration = _decode_symbol_registration(encoded)
    elif record_type == "portable_format_registration":
        registration = _decode_format_registration(encoded)
    else:
        raise InvalidTransportFrameError(
            f"unknown registration record type: {record_type!r}"
        )
    return registration


def _decode_symbol_registration(
    encoded: Metadata,
) -> MessageSymbolRegistration:
    symbol_kind = MessageSymbolKind[encoded["symbol_kind"]]
    raw_id = encoded["symbol_id"]

    if symbol_kind is MessageSymbolKind.TOPIC:
        symbol_id = TopicID(raw_id)
    elif symbol_kind is MessageSymbolKind.MSG_TYPE:
        symbol_id = MessageTypeID(raw_id)
    elif symbol_kind is MessageSymbolKind.PRODUCER:
        symbol_id = ProducerID(raw_id)
    else:
        raise InvalidTransportFrameError(
            f"unknown symbol kind: {symbol_kind!r}"
        )

    registration = MessageSymbolRegistration(
        symbol_kind=symbol_kind,
        symbol_id=symbol_id,
        symbol=encoded["symbol"],
    )
    return registration


def _decode_format_registration(
    encoded: Metadata,
) -> PortableFormatRegistration:
    registration = PortableFormatRegistration(
        format_id=PortableFormatID(encoded["format_id"]),
        key=_decode_format_key(encoded["format_key"]),
    )
    return registration


def _encode_register_payload_format_frame(
    frame: RegisterPayloadFormatFrame,
) -> FrameParts:
    metadata = {
        "frame_type": "register_payload_format",
        "format_key": _encode_format_key(frame.format_key),
    }
    return _metadata_parts(metadata)


def _decode_register_payload_format_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> RegisterPayloadFormatFrame:
    _reject_unexpected_payload(payload_bytes)
    frame = RegisterPayloadFormatFrame(
        format_key=_decode_format_key(metadata["format_key"])
    )
    return frame


def _encode_register_payload_format_result_frame(
    frame: RegisterPayloadFormatResultFrame,
) -> FrameParts:
    registrations = tuple(
        _encode_registration(registration)
        for registration in frame.registrations
    )
    metadata = {
        "frame_type": "register_payload_format_result",
        "format_id": frame.format_id.value,
        "registrations": registrations,
    }
    return _metadata_parts(metadata)


def _decode_register_payload_format_result_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> RegisterPayloadFormatResultFrame:
    _reject_unexpected_payload(payload_bytes)
    registrations = tuple(
        _decode_registration(registration)
        for registration in metadata.get("registrations", ())
    )
    frame = RegisterPayloadFormatResultFrame(
        format_id=PortableFormatID(metadata["format_id"]),
        registrations=registrations,
    )
    return frame


def _encode_register_emitter_frame(frame: RegisterEmitterFrame) -> FrameParts:
    supported_type_formats = tuple(
        _encode_type_format_support(support)
        for support in frame.supported_type_formats
    )
    metadata = {
        "frame_type": "register_emitter",
        "msg_topic": frame.msg_topic,
        "msg_producer": frame.msg_producer,
        "msg_type": frame.msg_type,
        "format_key": _encode_format_key(frame.format_key),
        "additional_msg_types": list(frame.additional_msg_types),
        "allow_unlisted_type_formats": frame.allow_unlisted_type_formats,
        "supported_type_formats": supported_type_formats,
    }
    return _metadata_parts(metadata)


def _encode_emit_frame(frame: EmitFrame) -> FrameParts:
    correlation_id = None
    if frame.correlation_id is not None:
        correlation_id = int(frame.correlation_id)
    reply_to = None
    if frame.reply_to is not None:
        reply_to = int(frame.reply_to)
    metadata = {
        "frame_type": "emit",
        "msg_topic_id": int(frame.msg_topic_id),
        "msg_producer_id": int(frame.msg_producer_id),
        "msg_type_id": int(frame.msg_type_id),
        "msg_format_id": frame.msg_format_id.value,
        "bus_operation": frame.bus_operation.value,
        "correlation_id": correlation_id,
        "reply_to": reply_to,
        "result_requested": frame.result_requested,
    }
    return _payload_parts(metadata, frame.payload_bytes)


def _decode_register_emitter_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> RegisterEmitterFrame:
    _reject_unexpected_payload(payload_bytes)
    additional_msg_types = tuple(metadata.get("additional_msg_types", ()))
    allow_unlisted_type_formats = metadata.get(
        "allow_unlisted_type_formats", False
    )
    supported_type_formats = tuple(
        _decode_type_format_support(support)
        for support in metadata.get("supported_type_formats", ())
    )
    frame = RegisterEmitterFrame(
        msg_topic=metadata["msg_topic"],
        msg_producer=metadata["msg_producer"],
        msg_type=metadata["msg_type"],
        format_key=_decode_format_key(metadata["format_key"]),
        additional_msg_types=additional_msg_types,
        allow_unlisted_type_formats=allow_unlisted_type_formats,
        supported_type_formats=supported_type_formats,
    )
    return frame


def _decode_emit_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> EmitFrame:
    payload_bytes = _require_payload(payload_bytes)
    raw_correlation_id = metadata.get("correlation_id")
    bus_operation = BusOperation(
        metadata.get("bus_operation", BusOperation.PUBLISH.value)
    )
    correlation_id = None
    if raw_correlation_id is not None:
        correlation_id = CorrelationID(raw_correlation_id)
    raw_reply_to = metadata.get("reply_to")
    reply_to = None
    if raw_reply_to is not None:
        reply_to = MessageID(raw_reply_to)
    frame = EmitFrame(
        msg_topic_id=TopicID(metadata["msg_topic_id"]),
        msg_producer_id=ProducerID(metadata["msg_producer_id"]),
        msg_type_id=MessageTypeID(metadata["msg_type_id"]),
        msg_format_id=PortableFormatID(metadata["msg_format_id"]),
        payload_bytes=payload_bytes,
        bus_operation=bus_operation,
        correlation_id=correlation_id,
        reply_to=reply_to,
        result_requested=bool(metadata.get("result_requested", True)),
    )
    return frame


def _encode_register_msg_type_frame(
    frame: RegisterMessageTypeFrame,
) -> FrameParts:
    metadata = {
        "frame_type": "register_msg_type", "msg_type": frame.msg_type
    }
    return _metadata_parts(metadata)


def _decode_register_msg_type_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> RegisterMessageTypeFrame:
    _reject_unexpected_payload(payload_bytes)
    return RegisterMessageTypeFrame(msg_type=metadata["msg_type"])


def _encode_register_msg_type_result_frame(
    frame: RegisterMessageTypeResultFrame,
) -> FrameParts:
    registrations = tuple(
        _encode_registration(registration)
        for registration in frame.registrations
    )
    metadata = {
        "frame_type": "register_msg_type_result",
        "msg_type_id": int(frame.msg_type_id),
        "registrations": registrations,
    }
    return _metadata_parts(metadata)


def _decode_register_msg_type_result_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> RegisterMessageTypeResultFrame:
    _reject_unexpected_payload(payload_bytes)
    registrations = tuple(
        _decode_registration(registration)
        for registration in metadata["registrations"]
    )
    frame = RegisterMessageTypeResultFrame(
        msg_type_id=MessageTypeID(metadata["msg_type_id"]),
        registrations=registrations,
    )
    return frame


def _encode_emit_result_frame(frame: EmitResultFrame) -> FrameParts:
    metadata = {"frame_type": "emit_result"}
    return _metadata_parts(metadata)


def _decode_emit_result_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> EmitResultFrame:
    _reject_unexpected_payload(payload_bytes)
    return EmitResultFrame()


def _reject_unexpected_payload(payload_bytes: bytes | None) -> None:
    if payload_bytes is not None:
        raise InvalidTransportFrameError(
            "frame type does not accept a payload byte part"
        )


def _require_payload(payload_bytes: bytes | None) -> bytes:
    if payload_bytes is None:
        raise InvalidTransportFrameError(
            "frame type requires a payload byte part"
        )
    return payload_bytes


def _encode_register_emitter_result_frame(
    frame: RegisterEmitterResultFrame,
) -> FrameParts:
    msg_type_id = int(frame.msg_type_id)
    registrations = [
        _encode_registration(registration)
        for registration in frame.registrations
    ]
    metadata = {
        "frame_type": "register_emitter_result",
        "msg_topic_id": int(frame.msg_topic_id),
        "msg_producer_id": int(frame.msg_producer_id),
        "msg_type_id": msg_type_id,
        "msg_format_id": frame.msg_format_id.value,
        "registrations": registrations,
    }
    return _metadata_parts(metadata)


def _decode_register_emitter_result_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> RegisterEmitterResultFrame:
    _reject_unexpected_payload(payload_bytes)
    msg_type_id = MessageTypeID(metadata["msg_type_id"])
    frame = RegisterEmitterResultFrame(
        msg_topic_id=TopicID(metadata["msg_topic_id"]),
        msg_producer_id=ProducerID(metadata["msg_producer_id"]),
        msg_type_id=msg_type_id,
        msg_format_id=PortableFormatID(metadata["msg_format_id"]),
        registrations=tuple(
            _decode_registration(registration)
            for registration in metadata["registrations"]
        ),
    )
    return frame


def _encode_topic_selector(
    selector: SubscriptionTopicSelector,
) -> Metadata:
    metadata = {
        "topic": selector.topic,
        "include_subtopics": selector.include_subtopics,
    }
    return metadata


def _decode_topic_selector(
    metadata: Metadata,
) -> SubscriptionTopicSelector:
    selector = SubscriptionTopicSelector(
        topic=metadata["topic"],
        include_subtopics=metadata["include_subtopics"],
    )
    return selector


def _encode_subscribe_frame(frame: SubscribeFrame) -> FrameParts:
    topic_selectors = [
        _encode_topic_selector(selector) for selector in frame.msg_topic
    ]
    metadata = {
        "frame_type": "subscribe",
        "msg_topic": topic_selectors,
        "msg_producer": frame.msg_producer,
        "msg_type": frame.msg_type,
    }
    return _metadata_parts(metadata)


def _decode_subscribe_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> SubscribeFrame:
    _reject_unexpected_payload(payload_bytes)
    topic_selectors = []
    for raw_selector in metadata["msg_topic"]:
        topic_selector = _decode_topic_selector(raw_selector)
        topic_selectors.append(topic_selector)
    frame = SubscribeFrame(
        msg_topic=tuple(topic_selectors),
        msg_producer=metadata["msg_producer"],
        msg_type=metadata["msg_type"],
    )
    return frame


def _encode_subscribe_result_frame(
    frame: SubscribeResultFrame,
) -> FrameParts:
    metadata = {
        "frame_type": "subscribe_result",
        "subscription_id": int(frame.subscription_id),
        "registrations": [
            _encode_registration(registration)
            for registration in frame.registrations
        ],
    }
    return _metadata_parts(metadata)


def _decode_subscribe_result_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> SubscribeResultFrame:
    _reject_unexpected_payload(payload_bytes)
    frame = SubscribeResultFrame(
        subscription_id=TransportSubscriptionID(metadata["subscription_id"]),
        registrations=tuple(
            _decode_registration(registration)
            for registration in metadata["registrations"]
        ),
    )
    return frame


def _encode_delivery_frame(frame: DeliveryFrame) -> FrameParts:
    correlation_id = None
    if frame.correlation_id is not None:
        correlation_id = int(frame.correlation_id)
    reply_to = None
    if frame.reply_to is not None:
        reply_to = int(frame.reply_to)
    metadata = {
        "frame_type": "delivery",
        "subscription_id": int(frame.subscription_id),
        "msg_topic_id": int(frame.msg_topic_id),
        "msg_producer_id": int(frame.msg_producer_id),
        "msg_type_id": int(frame.msg_type_id),
        "msg_format_id": frame.msg_format_id.value,
        "msg_id": int(frame.msg_id),
        "bus_operation": frame.bus_operation.value,
        "correlation_id": correlation_id,
        "reply_to": reply_to,
    }
    return _payload_parts(metadata, frame.payload_bytes)


def _decode_delivery_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> DeliveryFrame:
    payload_bytes = _require_payload(payload_bytes)
    raw_correlation_id = metadata.get("correlation_id")
    bus_operation = BusOperation(
        metadata.get("bus_operation", BusOperation.PUBLISH.value)
    )
    correlation_id = None
    if raw_correlation_id is not None:
        correlation_id = CorrelationID(raw_correlation_id)
    raw_reply_to = metadata.get("reply_to")
    reply_to = None
    if raw_reply_to is not None:
        reply_to = MessageID(raw_reply_to)
    frame = DeliveryFrame(
        subscription_id=TransportSubscriptionID(metadata["subscription_id"]),
        msg_topic_id=TopicID(metadata["msg_topic_id"]),
        msg_producer_id=ProducerID(metadata["msg_producer_id"]),
        msg_type_id=MessageTypeID(metadata["msg_type_id"]),
        msg_format_id=PortableFormatID(metadata["msg_format_id"]),
        msg_id=MessageID(metadata["msg_id"]),
        payload_bytes=payload_bytes,
        bus_operation=bus_operation,
        correlation_id=correlation_id,
        reply_to=reply_to,
    )
    return frame


def _encode_transport_error_frame(frame: TransportErrorFrame) -> FrameParts:
    metadata = {
        "frame_type": "transport_error",
        "error_code": frame.error_code,
        "error_message": frame.error_message,
        "request_id": frame.request_id,
    }
    return _metadata_parts(metadata)


def _decode_transport_error_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> TransportErrorFrame:
    _reject_unexpected_payload(payload_bytes)
    frame = TransportErrorFrame(
        error_code=metadata["error_code"],
        error_message=metadata["error_message"],
        request_id=metadata["request_id"],
    )
    return frame


def _encode_registration_frame(frame: RegistrationFrame) -> FrameParts:
    metadata = {
        "frame_type": "registration",
        "registrations": [
            _encode_registration(registration)
            for registration in frame.registrations
        ],
    }
    return _metadata_parts(metadata)


def _decode_registration_frame(
    metadata: Metadata, payload_bytes: bytes | None
) -> RegistrationFrame:
    _reject_unexpected_payload(payload_bytes)
    frame = RegistrationFrame(
        registrations=tuple(
            _decode_registration(registration)
            for registration in metadata["registrations"]
        ),
    )
    return frame
