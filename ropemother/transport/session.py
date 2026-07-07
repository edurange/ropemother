#!/usr/bin/env python3
# ropemother/transport/session.py

"""Broker-side protocol session for one transport connection."""

from typing import Any

from ropemother.broker.directcore import BrokerDeliveryTarget, DirectBrokerCore
from ropemother.exceptions import MessageBusBaseException
from ropemother.format.formattable import (
    PortableFormat,
    PortableFormatTable,
    UnknownPortableFormatError,
)
from ropemother.message.records import (
    BusMessage,
    BusOperation,
    SerializedPayload,
)
from ropemother.message.selectors import SubscriptionTopicFilter
from ropemother.transport.connection import FrameChannel
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
)
from ropemother.transport.sessionstate import TransportSessionState

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-05T16:49:49+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


class BrokerTransportSession:
    """Broker-side protocol session for one transport connection."""
    _channel: FrameChannel
    _core: DirectBrokerCore
    _format_table: PortableFormatTable
    _state: TransportSessionState

    def __init__(
        self,
        *,
        channel: FrameChannel,
        core: DirectBrokerCore,
        format_table: PortableFormatTable,
    ) -> None:
        self._channel = channel
        self._core = core
        self._format_table = format_table
        self._state = TransportSessionState()

    def handle_next_frame(self) -> None:
        frame = self._channel.receive_frame()
        if isinstance(frame, RegisterEmitterFrame):
            self._handle_register_emitter_frame(frame)
        elif isinstance(frame, RegisterMessageTypeFrame):
            self._handle_register_msg_type_frame(frame)
        elif isinstance(frame, RegisterPayloadFormatFrame):
            self._handle_register_payload_format_frame(frame)
        elif isinstance(frame, SubscribeFrame):
            self._handle_subscribe_frame(frame)
        elif isinstance(frame, EmitFrame):
            self._handle_emit_frame(frame)
        else:
            frame_type = type(frame).__name__
            self._channel.send_frame(
                TransportErrorFrame(
                    error_code="unsupported_frame",
                    error_message=f"Unsupported session frame: {frame_type}",
                )
            )

    def _handle_register_emitter_frame(
        self, frame: RegisterEmitterFrame
    ) -> None:
        try:
            payload_format = self._format_table.from_key(frame.format_key)
            supported_type_formats = (
                self._supported_type_formats_from_frame(frame)
            )
        except UnknownPortableFormatError:
            key = frame.format_key.registration_key
            error_frame = TransportErrorFrame(
                error_code="unsupported_format",
                error_message=f"unsupported portable format: {key}",
            )
            self._channel.send_frame(error_frame)
            return

        binding, _ = self._core.bind_emitter(
            msg_topic=frame.msg_topic,
            msg_producer=frame.msg_producer,
            msg_type=frame.msg_type,
            additional_msg_types=frame.additional_msg_types,
            allow_unlisted_type_formats=frame.allow_unlisted_type_formats,
            payload_format=payload_format,
            supported_type_formats=supported_type_formats,
        )
        self._state.add_emitter_binding(binding)
        registrations = self._state.registrations_to_send(
            self._core.registrations_for(binding)
        )

        result_frame = RegisterEmitterResultFrame(
            msg_topic_id=binding.msg_topic_id,
            msg_producer_id=binding.msg_producer_id,
            msg_type_id=binding.msg_type_id,
            msg_format_id=binding.default_format_id,
            registrations=registrations,
        )
        self._channel.send_frame(result_frame)

    def _handle_register_msg_type_frame(
        self, frame: RegisterMessageTypeFrame
    ) -> None:
        try:
            msg_type_id, registrations = self._core.register_msg_type(
                frame.msg_type
            )
        except MessageBusBaseException as e:
            self._send_error_frame(e)
            return

        registrations_to_send = self._state.registrations_to_send(
            registrations
        )
        result_frame = RegisterMessageTypeResultFrame(
            msg_type_id=msg_type_id, registrations=registrations_to_send
        )
        self._channel.send_frame(result_frame)

    def _handle_register_payload_format_frame(
        self, frame: RegisterPayloadFormatFrame
    ) -> None:
        try:
            payload_format = self._format_table.from_key(frame.format_key)
        except UnknownPortableFormatError:
            key = frame.format_key.registration_key
            error_frame = TransportErrorFrame(
                error_code="unsupported_format",
                error_message=f"unsupported portable format: {key}",
            )
            self._channel.send_frame(error_frame)
            return

        format_id, registrations = self._core.register_payload_format(
            payload_format
        )
        registrations_to_send = self._state.registrations_to_send(
            registrations
        )
        result_frame = RegisterPayloadFormatResultFrame(
            format_id=format_id, registrations=registrations_to_send
        )
        self._channel.send_frame(result_frame)

    def _handle_subscribe_frame(self, frame: SubscribeFrame) -> None:
        msg_topic_filter = SubscriptionTopicFilter(frame.msg_topic)
        binding, _ = self._core.bind_subscription(
            msg_topic_filter=msg_topic_filter,
            msg_producer=frame.msg_producer,
            msg_type=frame.msg_type,
        )
        subscription_id = self._state.add_subscription_binding(binding)
        delivery_target = _TransportDeliveryTarget(
            session=self, subscription_id=subscription_id
        )
        self._core.add_receiver(
            subscription=binding.subscription, delivery_target=delivery_target
        )
        registrations = self._state.registrations_to_send(
            self._core.registrations_for(binding)
        )
        result_frame = SubscribeResultFrame(
            subscription_id=subscription_id, registrations=registrations
        )
        self._channel.send_frame(result_frame)

    def _handle_emit_frame(self, frame: EmitFrame) -> None:
        binding = self._state.emitter_binding_for_frame(frame)
        if binding is None:
            error_frame = TransportErrorFrame(
                error_code="unknown_emitter",
                error_message="emit frame did not match a registered emitter",
            )
            if frame.result_requested:
                self._channel.send_frame(error_frame)
            return

        msg_type = self._state.msg_type_for_frame(
            binding=binding, frame=frame
        )
        serialized_payload = SerializedPayload(
            format_id=frame.msg_format_id, payload_bytes=frame.payload_bytes
        )
        try:
            self._core.emit_serialized_from(
                binding=binding,
                serialized_payload=serialized_payload,
                msg_type=msg_type,
                bus_operation=frame.bus_operation,
                correlation_id=frame.correlation_id,
                reply_to=frame.reply_to,
            )
        except MessageBusBaseException as e:
            if frame.result_requested:
                self._send_error_frame(e)
            return

        if frame.result_requested:
            self._channel.send_frame(EmitResultFrame())

    def _send_error_frame(self, error: MessageBusBaseException) -> None:
        error_frame = TransportErrorFrame(
            error_code=type(error).__name__, error_message=str(error)
        )
        self._channel.send_frame(error_frame)

    def _send_delivery_frame(
        self,
        *,
        subscription_id: TransportSubscriptionID,
        message: BusMessage,
    ) -> None:
        registrations = self._state.registrations_to_send(
            self._core.registrations_for(message)
        )
        if registrations:
            self._channel.send_frame(
                RegistrationFrame(registrations=registrations)
            )
        serialized_payload = message.serialized_payload
        frame = DeliveryFrame(
            subscription_id=subscription_id,
            msg_topic_id=message.msg_topic_id,
            msg_producer_id=message.msg_producer_id,
            msg_type_id=message.msg_type_id,
            msg_format_id=serialized_payload.format_id,
            msg_id=message.msg_id,
            payload_bytes=serialized_payload.payload_bytes,
            bus_operation=message.bus_operation,
            correlation_id=message.correlation_id,
            reply_to=message.reply_to,
        )
        self._channel.send_frame(frame)


    def _supported_type_formats_from_frame(
        self, frame: RegisterEmitterFrame
    ) -> dict[str, tuple[PortableFormat[Any, Any], ...]]:
        supported_type_formats = {}
        for support in frame.supported_type_formats:
            formats = tuple(
                self._format_table.from_key(format_key)
                for format_key in support.format_keys
            )
            supported_type_formats[support.msg_type] = formats

        return supported_type_formats


class _TransportDeliveryTarget(BrokerDeliveryTarget):
    _session: BrokerTransportSession
    _subscription_id: TransportSubscriptionID

    def __init__(
        self,
        *,
        session: BrokerTransportSession,
        subscription_id: TransportSubscriptionID,
    ) -> None:
        self._session = session
        self._subscription_id = subscription_id

    def deliver(self, message: BusMessage) -> None:
        self._session._send_delivery_frame(
            subscription_id=self._subscription_id, message=message
        )
