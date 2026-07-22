#!/usr/bin/env python3
# ropemother/transport/asyncclient.py

"""Asynchronous endpoint-side client facade for ropemother transport frames."""

from collections import deque
from collections.abc import Iterable
from typing import Any

from ropemother.broker.asyncendpoints import AsyncEmitter, AsyncReceiver
from ropemother.broker.endpoints import (
    UnlistedMessageTypeError,
    UnsupportedTypeFormatError,
    reply_metadata_for,
)
from ropemother.client.asyncendpointprovisioner import AsyncEndpointProvisioner
from ropemother.exceptions import PayloadSerializationError
from ropemother.format.defaults import default_portable_format_registry
from ropemother.format.formattable import PortableFormatTableError
from ropemother.format.portableformat import (
    JSON_PORTABLE_FORMAT,
    PortableFormat,
)
from ropemother.format.registry import PortableFormatID, PortableFormatRegistry
from ropemother.message.messageidentity import CorrelationID, MessageID
from ropemother.message.records import BusOperation, ReceivedMessage
from ropemother.message.selectors import (
    OptionalSymbolInput,
    SubscriptionTopicInput,
    SymbolCollectionInput,
    normalize_symbol_collection_input,
    topic_filter_from_input,
)
from ropemother.message.symbols import MessageTypeID
from ropemother.message.typeformats import (
    SupportedTypeFormatsInput,
    TypeFormatPolicy,
    normalize_type_format_policy,
)
from ropemother.transport.asyncconnection import AsyncFrameChannel
from ropemother.transport.client import (
    TransportPayloadDecodeError,
    TransportRequestError,
    UnexpectedTransportFrameError,
)
from ropemother.transport.endpointregistration import EndpointRegistrationView
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
__date__ = "2026-07-22T16:15:20+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


# Resolve undefined/ambiguous behavior around overlapping/simultaneous calls:
# Single-reader discipline? Supervised read loop?
class AsyncTransportClient(AsyncEndpointProvisioner):
    """Async endpoint factory backed by transport frames."""
    _channel: AsyncFrameChannel
    _delivery_queues: dict[TransportSubscriptionID, deque[DeliveryFrame]]
    _format_registry: PortableFormatRegistry
    _registrations: EndpointRegistrationView

    def __init__(
        self,
        *,
        channel: AsyncFrameChannel,
        extra_formats: Iterable[PortableFormat[Any, Any]] = (),
    ) -> None:
        self._channel = channel
        self._delivery_queues = {}
        self._format_registry = default_portable_format_registry(
            extra_formats=extra_formats
        )
        self._registrations = EndpointRegistrationView()

    def close(self) -> None:
        self._channel.close()

    async def register_emitter(
        self,
        *,
        msg_topic: str,
        msg_producer: str,
        msg_type: str,
        additional_msg_types: SymbolCollectionInput = (),
        allow_unlisted_type_formats: bool = False,
        payload_format: PortableFormat[Any, Any] = JSON_PORTABLE_FORMAT,
        supported_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> "AsyncTransportEmitter":
        additional_types = normalize_symbol_collection_input(
            additional_msg_types, argument_name="additional_msg_types"
        )
        format_policy = normalize_type_format_policy(
            msg_type=msg_type,
            payload_format=payload_format,
            additional_msg_types=additional_types,
            supported_type_formats=supported_type_formats,
            allow_unlisted_type_formats=allow_unlisted_type_formats,
        )
        self._install_format_policy_formats(format_policy)
        type_format_support = _transport_type_format_support(format_policy)
        frame = RegisterEmitterFrame(
            msg_topic=msg_topic,
            msg_producer=msg_producer,
            msg_type=msg_type,
            format_key=payload_format.key,
            additional_msg_types=additional_types,
            allow_unlisted_type_formats=allow_unlisted_type_formats,
            supported_type_formats=type_format_support,
        )
        await self._channel.send_frame(frame)

        response = await self._receive_expected_frame(
            RegisterEmitterResultFrame
        )

        self._registrations.apply_registrations(response.registrations)

        emitter = AsyncTransportEmitter(
            client=self,
            channel=self._channel,
            format_registry=self._format_registry,
            registrations=self._registrations,
            msg_topic=msg_topic,
            msg_producer=msg_producer,
            msg_type=msg_type,
            allow_unlisted_type_formats=allow_unlisted_type_formats,
            format_policy=format_policy,
        )
        return emitter

    async def subscribe(
        self,
        *,
        msg_topic: SubscriptionTopicInput,
        msg_producer: OptionalSymbolInput = None,
        msg_type: OptionalSymbolInput = None,
    ) -> "AsyncTransportReceiver":
        msg_topic_filter = topic_filter_from_input(msg_topic)
        frame = SubscribeFrame(
            msg_topic=msg_topic_filter.selectors,
            msg_producer=msg_producer,
            msg_type=msg_type,
        )
        await self._channel.send_frame(frame)

        response = await self._receive_expected_frame(SubscribeResultFrame)
        self._registrations.apply_registrations(response.registrations)

        receiver = AsyncTransportReceiver(
            client=self, subscription_id=response.subscription_id
        )
        return receiver

    async def _receive_expected_frame[T](self, expected_type: type[T]) -> T:
        while True:
            frame = await self._channel.receive_frame()
            if isinstance(frame, TransportErrorFrame):
                raise TransportRequestError(
                    frame.error_message, error_code=frame.error_code
                )
            if isinstance(frame, RegistrationFrame):
                self._registrations.apply_registrations(frame.registrations)
                continue
            if isinstance(frame, DeliveryFrame):
                self._queue_delivery_frame(frame)
                continue
            if isinstance(frame, expected_type):
                return frame
            if isinstance(frame, EmitResultFrame):
                continue

            frame_type = type(frame).__name__
            expected_name = expected_type.__name__
            raise UnexpectedTransportFrameError(
                f"expected {expected_name}, got {frame_type}"
            )

    def _queue_delivery_frame(self, frame: DeliveryFrame) -> None:
        delivery_queue = self._delivery_queues.setdefault(
            frame.subscription_id, deque()
        )
        delivery_queue.append(frame)

    def _take_queued_delivery_frame(
        self, subscription_id: TransportSubscriptionID
    ) -> DeliveryFrame | None:
        delivery_queue = self._delivery_queues.setdefault(
            subscription_id, deque()
        )
        frame = None
        if delivery_queue:
            frame = delivery_queue.popleft()

        return frame

    async def _receive_delivery_frame(
        self, subscription_id: TransportSubscriptionID
    ) -> DeliveryFrame:
        frames = await self._receive_delivery_batch(
            subscription_id, min_count=1, max_count=1
        )
        return frames[0]

    async def _receive_delivery_batch(
        self,
        subscription_id: TransportSubscriptionID,
        *,
        min_count: int,
        max_count: int | None,
    ) -> list[DeliveryFrame]:
        frames = []
        while max_count is None or len(frames) < max_count:
            frame = self._take_queued_delivery_frame(subscription_id)
            if frame is None and len(frames) < min_count:
                frame = await self._receive_relevant_delivery_frame(
                    subscription_id
                )
            elif frame is None:
                frame = self._receive_relevant_delivery_frame_nowait(
                    subscription_id
                )

            if frame is None:
                break

            frames.append(frame)

        return frames

    def _receive_delivery_batch_nowait(
        self,
        subscription_id: TransportSubscriptionID,
        *,
        max_count: int | None,
    ) -> list[DeliveryFrame]:
        frames = []
        while max_count is None or len(frames) < max_count:
            frame = self._take_queued_delivery_frame(subscription_id)
            if frame is None:
                frame = self._receive_relevant_delivery_frame_nowait(
                    subscription_id
                )

            if frame is None:
                break

            frames.append(frame)

        return frames

    async def _receive_relevant_delivery_frame(
        self, subscription_id: TransportSubscriptionID
    ) -> DeliveryFrame:
        while True:
            frame = await self._channel.receive_frame()
            delivery_frame = self._handle_delivery_candidate(
                frame, subscription_id
            )
            if delivery_frame is not None:
                return delivery_frame

    def _receive_relevant_delivery_frame_nowait(
        self, subscription_id: TransportSubscriptionID
    ) -> DeliveryFrame | None:
        delivery_frame = None
        while delivery_frame is None:
            frame = self._channel.receive_frame_nowait()
            if frame is None:
                break

            delivery_frame = self._handle_delivery_candidate(
                frame, subscription_id
            )

        return delivery_frame

    def _handle_delivery_candidate(
        self, frame: Any, subscription_id: TransportSubscriptionID
    ) -> DeliveryFrame | None:
        delivery_frame = None
        if isinstance(frame, TransportErrorFrame):
            raise TransportRequestError(
                frame.error_message, error_code=frame.error_code
            )
        elif isinstance(frame, RegistrationFrame):
            self._registrations.apply_registrations(frame.registrations)
        elif isinstance(frame, DeliveryFrame):
            if frame.subscription_id == subscription_id:
                delivery_frame = frame
            else:
                self._queue_delivery_frame(frame)
        elif isinstance(frame, EmitResultFrame):
            pass
        else:
            raise UnexpectedTransportFrameError(
                f"expected DeliveryFrame, got {type(frame).__name__}"
            )

        return delivery_frame

    def _received_message_from_frame(
        self, frame: DeliveryFrame
    ) -> ReceivedMessage:
        format_key = self._registrations.format_key_for_id(frame.msg_format_id)
        try:
            portable_format = self._format_registry.from_key(format_key)
        except PortableFormatTableError as e:
            raise TransportPayloadDecodeError(
                "async transport client has no local decoder for payload "
                f"format {format_key.registration_key!r}"
            ) from e

        try:
            payload = portable_format.decode(frame.payload_bytes)
        except (TypeError, ValueError) as e:
            raise TransportPayloadDecodeError(
                "async transport payload could not be decoded with format "
                f"{format_key.registration_key!r}"
            ) from e

        msg_topic = self._registrations.topic_for_id(frame.msg_topic_id)
        msg_type = self._registrations.msg_type_for_id(frame.msg_type_id)
        msg_producer = self._registrations.producer_for_id(
            frame.msg_producer_id
        )
        message = ReceivedMessage(
            payload=payload,
            msg_topic=msg_topic,
            msg_type=msg_type,
            msg_producer=msg_producer,
            bus_operation=frame.bus_operation,
            msg_id=frame.msg_id,
            correlation_id=frame.correlation_id,
            reply_to=frame.reply_to,
        )
        return message

    def _portable_format_table(self) -> PortableFormatRegistry:
        return self._format_registry

    # Clarify this name later
    def _install_format_policy_formats(
        self, format_policy: TypeFormatPolicy
    ) -> None:
        self._format_registry.install_format(
            format_policy.default_payload_format
        )
        for support in format_policy.supported_type_formats.values():
            self._format_registry.install_formats(support.supported_formats)


class AsyncTransportEmitter(AsyncEmitter):
    """Async emitter backed by transport frames."""
    _client: AsyncTransportClient
    _channel: AsyncFrameChannel
    _format_registry: PortableFormatRegistry
    _registrations: EndpointRegistrationView
    _msg_topic: str
    _msg_producer: str
    _msg_type: str
    _allow_unlisted_type_formats: bool
    _format_policy: TypeFormatPolicy

    def __init__(
        self,
        *,
        client: AsyncTransportClient,
        channel: AsyncFrameChannel,
        format_registry: PortableFormatRegistry,
        registrations: EndpointRegistrationView,
        msg_topic: str,
        msg_producer: str,
        msg_type: str,
        allow_unlisted_type_formats: bool,
        format_policy: TypeFormatPolicy,
    ) -> None:
        self._client = client
        self._channel = channel
        self._format_registry = format_registry
        self._registrations = registrations
        self._msg_topic = msg_topic
        self._msg_producer = msg_producer
        self._msg_type = msg_type
        self._allow_unlisted_type_formats = allow_unlisted_type_formats
        self._format_policy = format_policy

    async def emit(
        self,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        await self._emit_frame(
            payload=payload,
            msg_type=msg_type,
            payload_format=payload_format,
            bus_operation=BusOperation.PUBLISH,
        )
        await self._client._receive_expected_frame(EmitResultFrame)

    async def emit_request(
        self,
        payload: Any,
        *,
        correlation_id: CorrelationID,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        await self._emit_frame(
            payload=payload,
            msg_type=msg_type,
            payload_format=payload_format,
            bus_operation=BusOperation.REQUEST,
            correlation_id=correlation_id,
        )
        await self._client._receive_expected_frame(EmitResultFrame)

    async def emit_reply(
        self,
        request: ReceivedMessage,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        correlation_id, reply_to = reply_metadata_for(request)
        await self._emit_frame(
            payload=payload,
            msg_type=msg_type,
            payload_format=payload_format,
            bus_operation=BusOperation.REPLY,
            correlation_id=correlation_id,
            reply_to=reply_to,
        )
        await self._client._receive_expected_frame(EmitResultFrame)

    async def _emit_frame(
        self,
        *,
        payload: Any,
        msg_type: str | None,
        payload_format: PortableFormat[Any, Any] | None,
        bus_operation: BusOperation,
        correlation_id: CorrelationID | None = None,
        reply_to: MessageID | None = None,
        result_requested: bool = True,
    ) -> None:
        resolved_msg_type = self._format_policy.resolve_msg_type(msg_type)
        msg_type_id = await self._msg_type_id_for_emit(resolved_msg_type)
        resolved_format = self._format_policy.resolve_payload_format(
            msg_type=resolved_msg_type, payload_format=payload_format
        )
        self._ensure_type_format_supported(
            msg_type=resolved_msg_type, payload_format=resolved_format
        )
        payload_bytes = self._serialize_payload(payload, resolved_format)

        msg_topic_id = self._registrations.topic_id_for(self._msg_topic)
        msg_producer_id = self._registrations.producer_id_for(
            self._msg_producer
        )
        msg_format_id = await self._format_id_for_emit(resolved_format)
        frame = EmitFrame(
            msg_topic_id=msg_topic_id,
            msg_producer_id=msg_producer_id,
            msg_type_id=msg_type_id,
            msg_format_id=msg_format_id,
            payload_bytes=payload_bytes,
            bus_operation=bus_operation,
            correlation_id=correlation_id,
            reply_to=reply_to,
            result_requested=result_requested,
        )
        await self._channel.send_frame(frame)

    async def _msg_type_id_for_emit(self, msg_type: str) -> MessageTypeID:
        msg_type_id = self._registrations.find_msg_type_id_for(msg_type)
        if msg_type_id is not None:
            return msg_type_id

        if not self._allow_unlisted_type_formats:
            raise UnlistedMessageTypeError(
                f"message type is not listed for this emitter: {msg_type!r}"
            )

        frame = RegisterMessageTypeFrame(msg_type=msg_type)
        await self._channel.send_frame(frame)
        response = await self._client._receive_expected_frame(
            RegisterMessageTypeResultFrame
        )
        self._registrations.apply_registrations(response.registrations)
        return response.msg_type_id

    def _ensure_type_format_supported(
        self,
        *,
        msg_type: str,
        payload_format: PortableFormat[Any, Any],
    ) -> None:
        if self._format_policy.supports(
            msg_type=msg_type, payload_format=payload_format
        ):
            return

        raise UnsupportedTypeFormatError(
            "message type and payload format are not listed for this emitter: "
            f"{msg_type!r}, {payload_format.key.registration_key!r}"
        )

    async def _format_id_for_emit(
        self, payload_format: PortableFormat[Any, Any]
    ) -> PortableFormatID:
        format_id = self._registrations.find_format_id_for(
            payload_format.key
        )
        if format_id is not None:
            return format_id

        frame = RegisterPayloadFormatFrame(format_key=payload_format.key)
        await self._channel.send_frame(frame)
        response = await self._client._receive_expected_frame(
            RegisterPayloadFormatResultFrame
        )
        self._registrations.apply_registrations(response.registrations)
        return response.format_id

    def _serialize_payload(
        self, payload: Any, payload_format: PortableFormat[Any, Any]
    ) -> bytes:
        try:
            payload_bytes = payload_format.encode(payload)
        except (TypeError, ValueError) as e:
            raise PayloadSerializationError(
                "payload could not be serialized with message format: "
                f"{payload_format.key.registration_key!r}"
            ) from e

        if not isinstance(payload_bytes, bytes):
            raise PayloadSerializationError(
                "message format produced a non-bytes payload: "
                f"{payload_format.key.registration_key!r}"
            )

        return payload_bytes


class AsyncTransportReceiver(AsyncReceiver):
    """Async receiver backed by transport frames."""
    _client: AsyncTransportClient
    _subscription_id: TransportSubscriptionID

    def __init__(
        self,
        *,
        client: AsyncTransportClient,
        subscription_id: TransportSubscriptionID,
    ) -> None:
        self._client = client
        self._subscription_id = subscription_id

    async def _receive_batch(
        self, *, min_count: int, max_count: int | None
    ) -> list[ReceivedMessage]:
        frames = await self._client._receive_delivery_batch(
            self._subscription_id, min_count=min_count, max_count=max_count
        )
        messages = []
        for frame in frames:
            message = self._client._received_message_from_frame(frame)
            messages.append(message)
        return messages

    def _receive_batch_nowait(
        self, *, max_count: int | None
    ) -> list[ReceivedMessage]:
        frames = self._client._receive_delivery_batch_nowait(
            self._subscription_id, max_count=max_count
        )
        messages = []
        for frame in frames:
            message = self._client._received_message_from_frame(frame)
            messages.append(message)
        return messages


def _transport_type_format_support(
    policy: TypeFormatPolicy,
) -> tuple[TransportTypeFormatSupport, ...]:
    entries = []
    for msg_type, support in policy.supported_type_formats.items():
        format_keys = tuple(
            payload_format.key
            for payload_format in support.supported_formats
        )
        entry = TransportTypeFormatSupport(
            msg_type=msg_type, format_keys=format_keys
        )
        entries.append(entry)

    return tuple(entries)
