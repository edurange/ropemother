#!/usr/bin/env python3
# ropemother/transport/asyncsession.py

"""Asynchronous broker-side protocol session for one transport connection."""

import asyncio

from ropemother.broker.directcore import BrokerDeliveryTarget, DirectBrokerCore
from ropemother.exceptions import MessageBusBaseException
from ropemother.format.formattable import UnknownPortableFormatError
from ropemother.format.portableformat import PortableFormat
from ropemother.message.records import BusMessage, SerializedPayload
from ropemother.message.selectors import SubscriptionTopicFilter
from ropemother.transport.asyncconnection import AsyncFrameChannel
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
__date__ = "2026-10-03T03:28:51+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev11"
__status__ = "Development"


_DELIVERY_QUEUE_CAPACITY = 64


class AsyncBrokerTransportSession:
    """Async broker-side protocol session for one transport connection."""
    _channel: AsyncFrameChannel
    _core: DirectBrokerCore
    _state: TransportSessionState
    _delivery_targets: list[BrokerDeliveryTarget]
    _delivery_queue: asyncio.Queue[
        tuple[TransportSubscriptionID, BusMessage, asyncio.Future[None]]
    ]
    _delivery_waiters: set[asyncio.Future[None]]
    _delivery_task: asyncio.Task[None] | None
    _delivery_stopped: bool
    _delivery_error: Exception | None

    def __init__(
        self, *, channel: AsyncFrameChannel, core: DirectBrokerCore
    ) -> None:
        self._channel = channel
        self._core = core
        self._state = TransportSessionState()
        self._delivery_targets = []
        self._delivery_queue = asyncio.Queue(maxsize=_DELIVERY_QUEUE_CAPACITY)
        self._delivery_waiters = set()
        self._delivery_task = None
        self._delivery_stopped = False
        self._delivery_error = None

    def close(self) -> None:
        self._delivery_stopped = True
        for delivery_target in self._delivery_targets:
            self._core.remove_receiver(delivery_target=delivery_target)
        self._delivery_targets.clear()

        for waiter in self._delivery_waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._delivery_waiters.clear()

        delivery_task = self._delivery_task
        if delivery_task is not None and not delivery_task.done():
            delivery_task.cancel()

    async def handle_next_frame(self) -> None:
        frame = await self._channel.receive_frame()
        if isinstance(frame, RegisterEmitterFrame):
            await self._handle_register_emitter_frame(frame)
        elif isinstance(frame, RegisterMessageTypeFrame):
            await self._handle_register_msg_type_frame(frame)
        elif isinstance(frame, RegisterPayloadFormatFrame):
            await self._handle_register_payload_format_frame(frame)
        elif isinstance(frame, SubscribeFrame):
            await self._handle_subscribe_frame(frame)
        elif isinstance(frame, EmitFrame):
            await self._handle_emit_frame(frame)
        else:
            frame_type = type(frame).__name__
            error_frame = TransportErrorFrame(
                error_code="unsupported_frame",
                error_message=f"Unsupported session frame: {frame_type}",
            )
            await self._channel.send_frame(error_frame)

    def _delivery_was_stopped(self) -> bool:
        return self._delivery_stopped

    def _delivery_failure(self) -> Exception | None:
        return self._delivery_error

    async def _handle_register_emitter_frame(
        self, frame: RegisterEmitterFrame
    ) -> None:
        try:
            format_registry = self._core.format_registry()
            payload_format = format_registry.from_key(frame.format_key)
            supported_type_formats = (
                self._supported_type_formats_from_frame(frame)
            )
        except UnknownPortableFormatError:
            key = frame.format_key.registration_key
            error_frame = TransportErrorFrame(
                error_code="unsupported_format",
                error_message=f"unsupported portable format: {key}",
            )
            await self._channel.send_frame(error_frame)
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
        await self._channel.send_frame(result_frame)

    async def _handle_register_msg_type_frame(
        self, frame: RegisterMessageTypeFrame
    ) -> None:
        try:
            msg_type_id, registrations = self._core.register_msg_type(
                frame.msg_type
            )
        except MessageBusBaseException as e:
            await self._send_error_frame(e)
            return

        registrations_to_send = self._state.registrations_to_send(
            registrations
        )
        result_frame = RegisterMessageTypeResultFrame(
            msg_type_id=msg_type_id, registrations=registrations_to_send
        )
        await self._channel.send_frame(result_frame)

    async def _handle_register_payload_format_frame(
        self, frame: RegisterPayloadFormatFrame
    ) -> None:
        try:
            format_registry = self._core.format_registry()
            payload_format = format_registry.from_key(frame.format_key)
        except UnknownPortableFormatError:
            registration_key = frame.format_key.registration_key
            error_message = f"unsupported portable format: {registration_key}"
            error_frame = TransportErrorFrame(
                error_code="unsupported_format", error_message=error_message
            )
            await self._channel.send_frame(error_frame)
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
        await self._channel.send_frame(result_frame)

    async def _handle_subscribe_frame(self, frame: SubscribeFrame) -> None:
        msg_topic_filter = SubscriptionTopicFilter(frame.msg_topic)
        binding, _ = self._core.bind_subscription(
            msg_topic_filter=msg_topic_filter,
            msg_producer=frame.msg_producer,
            msg_type=frame.msg_type,
        )
        subscription_id = self._state.add_subscription_binding(
            binding,
            request_reply_subscription=frame.request_reply_subscription,
        )
        delivery_target = _AsyncTransportDeliveryTarget(
            session=self, subscription_id=subscription_id
        )
        self._delivery_targets.append(delivery_target)
        self._core.add_receiver(
            subscription=binding.subscription, delivery_target=delivery_target
        )
        registrations = self._state.registrations_to_send(
            self._core.registrations_for(binding)
        )
        result_frame = SubscribeResultFrame(
            subscription_id=subscription_id, registrations=registrations
        )
        await self._channel.send_frame(result_frame)

    async def _handle_emit_frame(self, frame: EmitFrame) -> None:
        binding = self._state.emitter_binding_for_frame(frame)
        if binding is None:
            error_frame = TransportErrorFrame(
                error_code="unknown_emitter",
                error_message="emit frame did not match a registered emitter",
            )
            if frame.result_requested:
                await self._channel.send_frame(error_frame)
            return

        msg_type = self._state.msg_type_for_frame(binding=binding, frame=frame)
        serialized_payload = SerializedPayload(
            format_id=frame.msg_format_id, payload_bytes=frame.payload_bytes
        )
        if not self._state.request_reply_subscription_is_valid(frame):
            if frame.result_requested:
                error_message = "invalid request/reply subscription for emit"
                error_frame = TransportErrorFrame(
                    error_code="invalid_request_reply_subscription",
                    error_message=error_message,
                )
                await self._channel.send_frame(error_frame)
            return

        message_id_observer = self._state.begin_request_emit(frame)

        try:
            message_id = self._core.emit_serialized_from(
                binding=binding,
                serialized_payload=serialized_payload,
                msg_type=msg_type,
                bus_operation=frame.bus_operation,
                correlation_id=frame.correlation_id,
                reply_to=frame.reply_to,
                message_id_observer=message_id_observer,
            )
        except MessageBusBaseException as e:
            self._state.finish_request_emit(discard_owner=True)
            if frame.result_requested:
                await self._send_error_frame(e)
            return

        self._state.finish_request_emit()
        await self._wait_for_delivery_tasks()
        if frame.result_requested:
            result_frame = EmitResultFrame(msg_id=message_id)
            await self._channel.send_frame(result_frame)

    def _deliver(
        self, *, subscription_id: TransportSubscriptionID, message: BusMessage
    ) -> None:
        relevant = self._state.accept_delivery(
            subscription_id=subscription_id, message=message
        )
        if not relevant or self._delivery_stopped:
            return

        self._ensure_delivery_task()
        waiter = asyncio.get_running_loop().create_future()
        try:
            self._delivery_queue.put_nowait(
                (subscription_id, message, waiter)
            )
        except asyncio.QueueFull:
            self.close()
            self._channel.close()
            return

        self._delivery_waiters.add(waiter)

    def _ensure_delivery_task(self) -> None:
        if self._delivery_task is None:
            delivery_task = asyncio.create_task(self._run_deliveries())
            delivery_task.add_done_callback(self._delivery_task_finished)
            self._delivery_task = delivery_task

    def _delivery_task_finished(
        self, delivery_task: asyncio.Task[None]
    ) -> None:
        if delivery_task.cancelled():
            return
        error = delivery_task.exception()
        if error is None:
            return
        self._delivery_error = error
        self.close()
        self._channel.close()

    async def _run_deliveries(self) -> None:
        while not self._delivery_stopped:
            subscription_id, message, waiter = await self._delivery_queue.get()
            try:
                await self._send_delivery_frame(
                    subscription_id=subscription_id, message=message
                )
            finally:
                self._delivery_waiters.discard(waiter)
                if not waiter.done():
                    waiter.set_result(None)
                self._delivery_queue.task_done()

    async def _wait_for_delivery_tasks(self) -> None:
        waiters = tuple(self._delivery_waiters)
        for waiter in waiters:
            await waiter

    async def _send_error_frame(self, error: MessageBusBaseException) -> None:
        error_frame = TransportErrorFrame(
            error_code=type(error).__name__, error_message=str(error)
        )
        await self._channel.send_frame(error_frame)

    async def _send_delivery_frame(
        self, *, subscription_id: TransportSubscriptionID, message: BusMessage
    ) -> None:
        registrations = self._state.registrations_to_send(
            self._core.registrations_for(message)
        )
        if registrations:
            registration_frame = RegistrationFrame(registrations=registrations)
            await self._channel.send_frame(registration_frame)

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
        await self._channel.send_frame(frame)

    def _supported_type_formats_from_frame(
        self, frame: RegisterEmitterFrame
    ) -> dict[str, tuple[PortableFormat, ...]]:
        supported_type_formats = {}
        for support in frame.supported_type_formats:
            formats = tuple(
                self._core.format_registry().from_key(format_key)
                for format_key in support.format_keys
            )
            supported_type_formats[support.msg_type] = formats

        return supported_type_formats


class _AsyncTransportDeliveryTarget(BrokerDeliveryTarget):
    _session: AsyncBrokerTransportSession
    _subscription_id: TransportSubscriptionID

    def __init__(
        self,
        *,
        session: AsyncBrokerTransportSession,
        subscription_id: TransportSubscriptionID,
    ) -> None:
        self._session = session
        self._subscription_id = subscription_id

    def deliver(self, message: BusMessage) -> None:
        self._session._deliver(
            subscription_id=self._subscription_id, message=message
        )
