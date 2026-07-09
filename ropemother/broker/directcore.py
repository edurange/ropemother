#!/usr/bin/env python3
# ropemother/broker/directcore.py

"""Shared implementation for in-process brokers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from time import monotonic_ns
from typing import Any

from ropemother.bootstrap.buffer import BootstrapBufferLimits
from ropemother.bootstrap.policy import (
    BootstrapMessageRejectedError,
    BootstrapPolicy,
    LifecycleBootstrapPolicy,
)
from ropemother.broker.endpoints import (
    UnlistedMessageTypeError,
    UnsupportedTypeFormatError,
)
from ropemother.broker.subscription import Subscription
from ropemother.capture.controller import CaptureController, CaptureState
from ropemother.capture.sink import CaptureSink
from ropemother.capture.writer import CaptureRecordSource, RegistrationRecord
from ropemother.exceptions import (
    MessageBusBaseException,
    PayloadSerializationError,
)
from ropemother.format.formattable import PortableFormatTable
from ropemother.format.portableformat import (
    PortableFormat,
    PortableFormatKey,
    JSON_PORTABLE_FORMAT,
)
from ropemother.format.registry import (
    PortableFormatID,
    PortableFormatRegistration,
    PortableFormatRegistry,
)
from ropemother.message.messageidentity import CorrelationID, MessageID
from ropemother.message.records import (
    BusMessage,
    BusOperation,
    ReceivedMessage,
    SerializedPayload,
)
from ropemother.message.selectors import (
    OptionalSymbolInput,
    SubscriptionTopicFilter,
    SymbolCollectionInput,
    normalize_symbol_collection_input,
    symbol_selector_from_input,
)
from ropemother.message.symbols import (
    MessageSymbolKind,
    MessageSymbolRegistration,
    MessageSymbolRegistry,
    MessageTypeID,
    ProducerID,
    TopicID,
    validate_msg_type,
)
from ropemother.message.typeformats import (
    SupportedTypeFormatsInput,
    TypeFormatPolicy,
    normalize_type_format_policy,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T02:56:26+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


class RegistrationRecordLookupError(MessageBusBaseException):
    """Base exception for registration record lookup errors."""
    pass


class MissingRegistrationRecordError(
    LookupError, RegistrationRecordLookupError
):
    """Raised when a captured registration record is unavailable."""
    pass


class UnsupportedRegistrationSourceError(
    TypeError, RegistrationRecordLookupError
):
    """Raised when a registration source cannot be queried."""
    pass


class CaptureMode(Enum):
    """Capture posture for a direct broker core."""
    CAPTURE_ENABLED = "capture-enabled"
    TRANSPORT_ONLY = "transport-only"

    @property
    def capture_enabled(self) -> bool:
        return self is CaptureMode.CAPTURE_ENABLED


class BrokerDeliveryTarget(ABC):
    """Receiver-side target that accepts broker-delivered messages."""

    @abstractmethod
    def deliver(self, message: BusMessage) -> None:
        ...


@dataclass(frozen=True, kw_only=True)
class EmitterBinding:
    """Registered producer defaults used when emitting messages."""
    msg_topic: str
    msg_producer: str
    msg_type: str
    additional_msg_types: tuple[str, ...]
    allow_unlisted_type_formats: bool
    msg_topic_id: TopicID
    msg_producer_id: ProducerID
    msg_type_id: MessageTypeID
    additional_msg_type_ids: dict[str, MessageTypeID]
    format_policy: TypeFormatPolicy
    format_ids: dict[PortableFormatKey, PortableFormatID]

    @property
    def default_format_id(self) -> PortableFormatID:
        payload_format = self.format_policy.default_payload_format
        return self.format_ids[payload_format.key]

    def resolve_msg_type(self, msg_type: str | None) -> str:
        return self.format_policy.resolve_msg_type(msg_type)


@dataclass(frozen=True, kw_only=True)
class SubscriptionBinding:
    """Registered subscriber filter and delivery target."""
    subscription: Subscription
    msg_topic_id: tuple[TopicID, ...]
    msg_producer_id: tuple[ProducerID, ...]
    msg_type_id: tuple[MessageTypeID, ...]


class DirectBrokerCore:
    """Shared routing, registration, serialization, and capture core."""
    _bootstrap_policy: BootstrapPolicy
    _capture_controller: CaptureController
    _format_registry: PortableFormatRegistry
    _symbol_registry: MessageSymbolRegistry
    _registrations: list[RegistrationRecord]
    _bus_sequence: int
    _topic_sequences: dict[TopicID, int]
    _receivers: list[tuple[Subscription, BrokerDeliveryTarget]]

    def __init__(
        self,
        *,
        capture_enabled: bool = True,
        bootstrap_enabled: bool = False,
        bootstrap_policy: BootstrapPolicy | None = None,
        bootstrap_limits: BootstrapBufferLimits | None = None,
        capture_sink: CaptureSink | None = None,
    ) -> None:
        if bootstrap_policy is None:
            bootstrap_policy = LifecycleBootstrapPolicy()
        self._bootstrap_policy = bootstrap_policy
        self._capture_controller = CaptureController(
            capture_enabled=capture_enabled,
            bootstrap_enabled=bootstrap_enabled,
            bootstrap_limits=bootstrap_limits,
        )
        self._format_registry = PortableFormatRegistry()
        self._symbol_registry = MessageSymbolRegistry()
        self._registrations = []
        self._bus_sequence = 0
        self._topic_sequences = {}
        self._receivers = []
        if capture_sink is not None:
            self.set_capture_sink(capture_sink)

    def bind_emitter(
        self,
        *,
        msg_topic: str,
        msg_producer: str,
        msg_type: str,
        additional_msg_types: SymbolCollectionInput = (),
        allow_unlisted_type_formats: bool = False,
        payload_format: PortableFormat[Any, Any] = JSON_PORTABLE_FORMAT,
        supported_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> tuple[EmitterBinding, tuple[RegistrationRecord, ...]]:
        new_registrations: list[RegistrationRecord] = []

        topic_id, topic_reg = self._symbol_registry.ensure_topic_id(msg_topic)
        self._capture_symbol_event(topic_reg)
        if topic_reg is not None:
            new_registrations.append(topic_reg)

        producer_id, producer_reg = self._symbol_registry.ensure_producer_id(
            msg_producer
        )
        self._capture_symbol_event(producer_reg)
        if producer_reg is not None:
            new_registrations.append(producer_reg)

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

        msg_type_id, msg_type_reg = self._symbol_registry.ensure_msg_type_id(
            msg_type
        )
        self._capture_symbol_event(msg_type_reg)
        if msg_type_reg is not None:
            new_registrations.append(msg_type_reg)

        additional_msg_type_ids = {}
        for supported_msg_type in format_policy.supported_type_formats:
            if supported_msg_type == msg_type:
                continue

            supported_msg_type_id, supported_msg_type_reg = (
                self._symbol_registry.ensure_msg_type_id(supported_msg_type)
            )
            self._capture_symbol_event(supported_msg_type_reg)
            if supported_msg_type_reg is not None:
                new_registrations.append(supported_msg_type_reg)
            additional_msg_type_ids[supported_msg_type] = supported_msg_type_id

        format_ids = self._ensure_format_ids_for_policy(
            format_policy, registrations=new_registrations
        )

        binding = EmitterBinding(
            msg_topic=msg_topic,
            msg_producer=msg_producer,
            msg_type=msg_type,
            additional_msg_types=additional_types,
            allow_unlisted_type_formats=allow_unlisted_type_formats,
            msg_topic_id=topic_id,
            msg_producer_id=producer_id,
            msg_type_id=msg_type_id,
            additional_msg_type_ids=additional_msg_type_ids,
            format_policy=format_policy,
            format_ids=format_ids,
        )
        return binding, tuple(new_registrations)

    def bind_subscription(
        self,
        *,
        msg_topic_filter: SubscriptionTopicFilter,
        msg_producer: OptionalSymbolInput = None,
        msg_type: OptionalSymbolInput = None,
    ) -> tuple[SubscriptionBinding, tuple[RegistrationRecord, ...]]:
        new_registrations: list[RegistrationRecord] = []

        msg_topic_ids = []
        for selector in msg_topic_filter.selectors:
            topic_id, topic_reg = self._symbol_registry.ensure_topic_id(
                selector.topic
            )
            self._capture_symbol_event(topic_reg)
            if topic_reg is not None:
                new_registrations.append(topic_reg)
            msg_topic_ids.append(topic_id)

        msg_producer_filter = symbol_selector_from_input(
            msg_producer, argument_name="msg_producer"
        )
        msg_producer_ids = []
        if msg_producer_filter.symbols is not None:
            for producer in msg_producer_filter.symbols:
                producer_id, producer_reg = (
                    self._symbol_registry.ensure_producer_id(producer)
                )
                self._capture_symbol_event(producer_reg)
                if producer_reg is not None:
                    new_registrations.append(producer_reg)
                msg_producer_ids.append(producer_id)

        msg_type_filter = symbol_selector_from_input(
            msg_type, argument_name="msg_type"
        )
        msg_type_ids = []
        if msg_type_filter.symbols is not None:
            for current_msg_type in msg_type_filter.symbols:
                msg_type_id, msg_type_reg = (
                    self._symbol_registry.ensure_msg_type_id(current_msg_type)
                )
                self._capture_symbol_event(msg_type_reg)
                if msg_type_reg is not None:
                    new_registrations.append(msg_type_reg)
                msg_type_ids.append(msg_type_id)

        subscription = Subscription(
            msg_topic_filter=msg_topic_filter,
            msg_producer_filter=msg_producer_filter,
            msg_type_filter=msg_type_filter,
        )
        binding = SubscriptionBinding(
            subscription=subscription,
            msg_topic_id=tuple(msg_topic_ids),
            msg_producer_id=tuple(msg_producer_ids),
            msg_type_id=tuple(msg_type_ids),
        )
        return binding, tuple(new_registrations)

    def add_receiver(
        self,
        *,
        subscription: Subscription,
        delivery_target: BrokerDeliveryTarget,
    ) -> None:
        self._receivers.append((subscription, delivery_target))

    def register_msg_type(
        self, msg_type: str
    ) -> tuple[MessageTypeID, tuple[RegistrationRecord, ...]]:
        msg_type_id = self._ensure_msg_type_id(msg_type)
        registration = self._message_registration_for(
            symbol_kind=MessageSymbolKind.MSG_TYPE, symbol_id=msg_type_id
        )
        return (msg_type_id, (registration,))

    def register_payload_format(
        self, payload_format: PortableFormat[Any, Any]
    ) -> tuple[PortableFormatID, tuple[RegistrationRecord, ...]]:
        format_id = self._ensure_format_id(payload_format)
        registration = self._format_registration_for(format_id)
        return (format_id, (registration,))

    def set_capture_sink(self, capture_sink: CaptureSink) -> None:
        self._capture_controller.activate_capture_sink(
            capture_sink, registrations=self._registrations
        )

    def capture_source(self) -> CaptureRecordSource | None:
        return self._capture_controller.capture_source()

    def format_table(self) -> PortableFormatTable:
        return self._format_registry

    # Is it possible to preserve more type information than Any here?
    def emit_from(
        self,
        *,
        binding: EmitterBinding,
        payload: Any,
        msg_type: str | None,
        payload_format: PortableFormat[Any, Any] | None,
        bus_operation: BusOperation,
        correlation_id: CorrelationID | None = None,
        reply_to: MessageID | None = None,
    ) -> None:
        resolved_msg_type = binding.resolve_msg_type(msg_type)
        resolved_payload_format = binding.format_policy.resolve_payload_format(
            msg_type=resolved_msg_type, payload_format=payload_format
        )
        resolved_msg_type_id = self._resolve_msg_type_id(
            binding=binding, msg_type=resolved_msg_type
        )
        self._ensure_type_format_supported(
            binding=binding,
            msg_type=resolved_msg_type,
            payload_format=resolved_payload_format,
        )
        resolved_format_id = self._resolve_format_id(
            binding=binding, payload_format=resolved_payload_format
        )

        self._emit(
            payload=payload,
            msg_format=resolved_payload_format,
            msg_format_id=resolved_format_id,
            msg_topic=binding.msg_topic,
            msg_type=resolved_msg_type,
            msg_producer=binding.msg_producer,
            msg_topic_id=binding.msg_topic_id,
            msg_type_id=resolved_msg_type_id,
            msg_producer_id=binding.msg_producer_id,
            bus_operation=bus_operation,
            correlation_id=correlation_id,
            reply_to=reply_to,
        )

    def emit_serialized_from(
        self,
        *,
        binding: EmitterBinding,
        serialized_payload: SerializedPayload,
        msg_type: str | None,
        bus_operation: BusOperation,
        correlation_id: CorrelationID | None = None,
        reply_to: MessageID | None = None,
    ) -> None:
        resolved_msg_type = binding.resolve_msg_type(msg_type)
        resolved_msg_type_id = self._resolve_msg_type_id(
            binding=binding, msg_type=resolved_msg_type
        )
        payload_format = self._format_registry.format_for_id(
            serialized_payload.format_id
        )
        self._ensure_type_format_supported(
            binding=binding,
            msg_type=resolved_msg_type,
            payload_format=payload_format,
        )
        payload = payload_format.decode(serialized_payload.payload_bytes)

        self._deliver(
            payload=payload,
            serialized_payload=serialized_payload,
            msg_topic=binding.msg_topic,
            msg_type=resolved_msg_type,
            msg_producer=binding.msg_producer,
            msg_topic_id=binding.msg_topic_id,
            msg_type_id=resolved_msg_type_id,
            msg_producer_id=binding.msg_producer_id,
            bus_operation=bus_operation,
            correlation_id=correlation_id,
            reply_to=reply_to,
        )

    def registrations_for(
        self, item: EmitterBinding | SubscriptionBinding | BusMessage
    ) -> tuple[RegistrationRecord, ...]:
        if isinstance(item, EmitterBinding):
            return self._emitter_registrations(item)
        if isinstance(item, SubscriptionBinding):
            return self._subscription_registrations(item)
        if isinstance(item, BusMessage):
            return self._message_registrations(item)
        item_type = type(item).__name__
        raise UnsupportedRegistrationSourceError(
            f"unsupported registration source: {item_type}"
        )

    def _emit(
        self,
        *,
        payload: Any,
        msg_format: PortableFormat[Any, Any],
        msg_format_id: PortableFormatID,
        msg_topic: str,
        msg_type: str,
        msg_producer: str,
        msg_topic_id: TopicID,
        msg_type_id: MessageTypeID,
        msg_producer_id: ProducerID,
        bus_operation: BusOperation,
        correlation_id: CorrelationID | None = None,
        reply_to: MessageID | None = None,
    ) -> None:
        serialized_payload = self._serialize_payload(
            payload=payload, msg_format=msg_format, msg_format_id=msg_format_id
        )
        self._deliver(
            payload=payload,
            serialized_payload=serialized_payload,
            msg_topic=msg_topic,
            msg_type=msg_type,
            msg_producer=msg_producer,
            msg_topic_id=msg_topic_id,
            msg_type_id=msg_type_id,
            msg_producer_id=msg_producer_id,
            bus_operation=bus_operation,
            correlation_id=correlation_id,
            reply_to=reply_to,
        )

    def _deliver(
        self,
        *,
        payload: Any,
        serialized_payload: SerializedPayload,
        msg_topic: str,
        msg_type: str,
        msg_producer: str,
        msg_topic_id: TopicID,
        msg_type_id: MessageTypeID,
        msg_producer_id: ProducerID,
        bus_operation: BusOperation,
        correlation_id: CorrelationID | None = None,
        reply_to: MessageID | None = None,
    ) -> None:
        message = self._build_message(
            payload=payload,
            serialized_payload=serialized_payload,
            msg_topic=msg_topic,
            msg_type=msg_type,
            msg_producer=msg_producer,
            msg_topic_id=msg_topic_id,
            msg_type_id=msg_type_id,
            msg_producer_id=msg_producer_id,
            bus_operation=bus_operation,
            correlation_id=correlation_id,
            reply_to=reply_to,
        )

        matching_receivers = self._matching_receivers(message)
        self._ensure_bootstrap_message_allowed(message)
        self._capture_controller.write_message_record(message.captured_view())
        for receiver in matching_receivers:
            receiver.deliver(message)

    def _ensure_bootstrap_message_allowed(self, message: BusMessage) -> None:
        if self._capture_controller.state is not CaptureState.BOOTSTRAPPING:
            return

        decision = self._bootstrap_policy.decide_message(message)
        if not decision.accepted:
            raise BootstrapMessageRejectedError(decision.reason)

    def _build_message(
        self,
        *,
        payload: Any,
        serialized_payload: SerializedPayload,
        msg_topic: str,
        msg_type: str,
        msg_producer: str,
        msg_topic_id: TopicID,
        msg_type_id: MessageTypeID,
        msg_producer_id: ProducerID,
        bus_operation: BusOperation,
        correlation_id: CorrelationID | None = None,
        reply_to: MessageID | None = None,
    ) -> BusMessage:
        bus_sequence = self._next_bus_sequence()
        topic_sequence = self._next_topic_sequence(msg_topic_id)
        message = BusMessage(
            payload=payload,
            serialized_payload=serialized_payload,
            msg_id=MessageID(bus_sequence),
            msg_topic=msg_topic,
            msg_type=msg_type,
            msg_producer=msg_producer,
            msg_topic_id=msg_topic_id,
            msg_type_id=msg_type_id,
            msg_producer_id=msg_producer_id,
            bus_operation=bus_operation,
            bus_sequence=bus_sequence,
            topic_sequence=topic_sequence,
            bus_received_at=monotonic_ns(),
            correlation_id=correlation_id,
            reply_to=reply_to,
        )
        return message

    def _serialize_payload(
        self,
        *,
        payload: Any,
        msg_format: PortableFormat[Any, Any],
        msg_format_id: PortableFormatID,
    ) -> SerializedPayload:
        try:
            data = msg_format.encode(payload)
        except (TypeError, ValueError) as e:
            raise PayloadSerializationError(
                "payload could not be serialized with message format "
                f"{msg_format.key.registration_key!r}"
            ) from e

        if not isinstance(data, bytes):
            raise PayloadSerializationError(
                "message format produced a non-bytes payload: "
                f"{msg_format.key.registration_key!r}"
            )

        payload = SerializedPayload(
            format_id=msg_format_id, payload_bytes=data
        )
        return payload

    def _capture_format_event(
        self, registration: PortableFormatRegistration | None
    ) -> None:
        if registration is not None:
            self._registrations.append(registration)
            self._capture_controller.write_format_registration(registration)

    def _capture_symbol_event(
        self, registration: MessageSymbolRegistration | None
    ) -> None:
        if registration is not None:
            self._registrations.append(registration)
            self._capture_controller.write_symbol_registration(registration)

    def _ensure_msg_type_id(self, msg_type: str) -> MessageTypeID:
        msg_type_id, registration = (
            self._symbol_registry.ensure_msg_type_id(msg_type)
        )
        self._capture_symbol_event(registration)
        return msg_type_id

    def _ensure_format_ids_for_policy(
        self,
        policy: TypeFormatPolicy,
        *,
        registrations: list[RegistrationRecord],
    ) -> dict[PortableFormatKey, PortableFormatID]:
        format_ids: dict[PortableFormatKey, PortableFormatID] = {}
        for support in policy.supported_type_formats.values():
            for payload_format in support.supported_formats:
                format_id = self._ensure_format_id(
                    payload_format, registrations=registrations
                )
                format_ids[payload_format.key] = format_id

        return format_ids

    def _ensure_format_id(
        self,
        payload_format: PortableFormat[Any, Any],
        *,
        registrations: list[RegistrationRecord] | None = None,
    ) -> PortableFormatID:
        format_id, registration = self._format_registry.ensure_format_id(
            payload_format
        )
        self._capture_format_event(registration)
        if registration is not None and registrations is not None:
            registrations.append(registration)

        return format_id

    def _ensure_type_format_supported(
        self,
        *,
        binding: EmitterBinding,
        msg_type: str,
        payload_format: PortableFormat[Any, Any],
    ) -> None:
        if binding.format_policy.supports(
            msg_type=msg_type, payload_format=payload_format
        ):
            return

        raise UnsupportedTypeFormatError(
            "message type and payload format are not listed for this emitter: "
            f"{msg_type!r}, {payload_format.key.registration_key!r}"
        )

    def _resolve_format_id(
        self,
        *,
        binding: EmitterBinding,
        payload_format: PortableFormat[Any, Any],
    ) -> PortableFormatID:
        format_id = binding.format_ids.get(payload_format.key)
        if format_id is not None:
            return format_id

        if not binding.allow_unlisted_type_formats:
            raise UnsupportedTypeFormatError(
                "payload format is not listed for this emitter: "
                f"{payload_format.key.registration_key!r}"
            )

        return self._ensure_format_id(payload_format)

    def _next_bus_sequence(self) -> int:
        sequence = self._bus_sequence
        self._bus_sequence += 1
        return sequence

    def _next_topic_sequence(self, msg_topic_id: TopicID) -> int:
        sequence = self._topic_sequences.get(msg_topic_id, 0)
        self._topic_sequences[msg_topic_id] = sequence + 1
        return sequence

    def _resolve_msg_type_id(
        self, *, binding: EmitterBinding, msg_type: str
    ) -> MessageTypeID:
        validate_msg_type(msg_type)

        if msg_type == binding.msg_type:
            return binding.msg_type_id

        msg_type_id = binding.additional_msg_type_ids.get(msg_type)
        if msg_type_id is not None:
            return msg_type_id

        if not binding.allow_unlisted_type_formats:
            raise UnlistedMessageTypeError(
                f"message type is not listed for this emitter: {msg_type!r}"
            )

        return self._ensure_msg_type_id(msg_type)

    def _matching_receivers(
        self, message: BusMessage
    ) -> list[BrokerDeliveryTarget]:
        receivers = [
            receiver
            for subscription, receiver in self._receivers
            if subscription.matches(message)
        ]
        return receivers

    def _emitter_registrations(
        self, binding: EmitterBinding
    ) -> tuple[RegistrationRecord, ...]:
        registrations: list[RegistrationRecord] = [
            self._message_registration_for(
                symbol_kind=MessageSymbolKind.TOPIC,
                symbol_id=binding.msg_topic_id,
            ),
            self._message_registration_for(
                symbol_kind=MessageSymbolKind.PRODUCER,
                symbol_id=binding.msg_producer_id,
            ),
            self._message_registration_for(
                symbol_kind=MessageSymbolKind.MSG_TYPE,
                symbol_id=binding.msg_type_id,
            ),
        ]
        for msg_type_id in binding.additional_msg_type_ids.values():
            msg_type_registration = self._message_registration_for(
                symbol_kind=MessageSymbolKind.MSG_TYPE,
                symbol_id=msg_type_id,
            )
            registrations.append(msg_type_registration)
        for format_id in binding.format_ids.values():
            format_registration = self._format_registration_for(format_id)
            registrations.append(format_registration)
        return tuple(registrations)

    def _subscription_registrations(
        self, binding: SubscriptionBinding
    ) -> tuple[RegistrationRecord, ...]:
        registrations: list[RegistrationRecord] = []

        for msg_topic_id in binding.msg_topic_id:
            topic_registration = self._message_registration_for(
                symbol_kind=MessageSymbolKind.TOPIC,
                symbol_id=msg_topic_id,
            )
            registrations.append(topic_registration)

        for msg_producer_id in binding.msg_producer_id:
            producer_registration = self._message_registration_for(
                symbol_kind=MessageSymbolKind.PRODUCER,
                symbol_id=msg_producer_id,
            )
            registrations.append(producer_registration)

        for msg_type_id in binding.msg_type_id:
            type_registration = self._message_registration_for(
                symbol_kind=MessageSymbolKind.MSG_TYPE, symbol_id=msg_type_id
            )
            registrations.append(type_registration)
        return tuple(registrations)

    def _message_registrations(
        self, message: BusMessage
    ) -> tuple[RegistrationRecord, ...]:
        registrations = (
            self._message_registration_for(
                symbol_kind=MessageSymbolKind.TOPIC,
                symbol_id=message.msg_topic_id,
            ),
            self._message_registration_for(
                symbol_kind=MessageSymbolKind.PRODUCER,
                symbol_id=message.msg_producer_id,
            ),
            self._message_registration_for(
                symbol_kind=MessageSymbolKind.MSG_TYPE,
                symbol_id=message.msg_type_id,
            ),
            self._format_registration_for(
                message.serialized_payload.format_id
            ),
        )
        return registrations

    def _message_registration_for(
        self,
        *,
        symbol_kind: MessageSymbolKind,
        symbol_id: TopicID | ProducerID | MessageTypeID,
    ) -> MessageSymbolRegistration:
        for registration in self._registrations:
            if not isinstance(registration, MessageSymbolRegistration):
                continue
            if registration.symbol_kind is not symbol_kind:
                continue
            if registration.symbol_id == symbol_id:
                return registration
        raise MissingRegistrationRecordError(
            f"missing message symbol registration for {symbol_id!r}"
        )

    def _format_registration_for(
        self, format_id: PortableFormatID
    ) -> PortableFormatRegistration:
        for registration in self._registrations:
            if not isinstance(registration, PortableFormatRegistration):
                continue
            if registration.format_id == format_id:
                return registration
        raise MissingRegistrationRecordError(
            f"missing portable format registration for {format_id!r}"
        )
