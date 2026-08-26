#!/usr/bin/env python3
# ropemother/broker/asyncdirect.py

"""Asynchronous, in-process broker and support classes, without transport."""

import asyncio
import collections.abc
import typing

from ropemother.bootstrap.buffer import BootstrapBufferLimits
from ropemother.bootstrap.policy import BootstrapPolicy
from ropemother.broker.asyncbase import AsyncMessageBus
from ropemother.broker.asyncendpoints import AsyncEmitter, AsyncReceiver
from ropemother.broker.directcore import (
    BrokerDeliveryTarget,
    CaptureMode,
    DirectBrokerCore,
    EmitterBinding,
)
from ropemother.broker.endpoints import (
    InvalidReceiverSelectionError,
    reply_metadata_for,
)
from ropemother.broker.subscription import Subscription
from ropemother.capture.sink import CaptureSink
from ropemother.capture.writer import CaptureRecordSource
from ropemother.format.portableformat import (
    PortableFormat,
    JSON_PORTABLE_FORMAT,
)
from ropemother.format.registry import PortableFormatRegistry
from ropemother.message.messageidentity import CorrelationID, MessageID
from ropemother.message.records import (
    BusMessage,
    BusOperation,
    ReceivedMessage,
)
from ropemother.message.selectors import (
    OptionalSymbolInput,
    SubscriptionTopicInput,
    SymbolCollectionInput,
    topic_filter_from_input,
)
from ropemother.message.typeformats import SupportedTypeFormatsInput
from ropemother.transport.asyncconnection import AsyncFrameChannel
from ropemother.transport.asyncsession import AsyncBrokerTransportSession

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-08-26T15:22:13+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev7"
__status__ = "Development"


class AsyncDirectMessageBus(AsyncMessageBus):
    """Async direct broker that routes messages to local receiver queues."""
    _core: DirectBrokerCore
    _receive_monitor: "_AsyncReceiveMonitor"

    def __init__(
        self,
        *,
        extra_formats: collections.abc.Iterable[PortableFormat] = (),
        capture_mode: CaptureMode = CaptureMode.CAPTURE_ENABLED,
        capture_sink: CaptureSink | None = None,
    ) -> None:
        bootstrap_enabled = (
            capture_mode.capture_enabled and capture_sink is None
        )
        self._core = DirectBrokerCore(
            capture_enabled=capture_mode.capture_enabled,
            bootstrap_enabled=bootstrap_enabled,
            capture_sink=capture_sink,
            extra_formats=extra_formats,
        )
        self._receive_monitor = _AsyncReceiveMonitor()

    def register_emitter(
        self,
        *,
        msg_topic: str,
        msg_producer: str,
        msg_type: str,
        additional_msg_types: SymbolCollectionInput = (),
        allow_unlisted_type_formats: bool = False,
        payload_format: PortableFormat = JSON_PORTABLE_FORMAT,
        supported_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> AsyncEmitter:
        binding, _ = self._core.bind_emitter(
            msg_topic=msg_topic,
            msg_producer=msg_producer,
            msg_type=msg_type,
            additional_msg_types=additional_msg_types,
            allow_unlisted_type_formats=allow_unlisted_type_formats,
            payload_format=payload_format,
            supported_type_formats=supported_type_formats,
        )
        return _AsyncBrokerEmitter(core=self._core, binding=binding)

    def subscribe(
        self,
        *,
        msg_topic: SubscriptionTopicInput,
        msg_producer: OptionalSymbolInput = None,
        msg_type: OptionalSymbolInput = None,
    ) -> AsyncReceiver:
        msg_topic_filter = topic_filter_from_input(msg_topic)
        binding, _ = self._core.bind_subscription(
            msg_topic_filter=msg_topic_filter,
            msg_producer=msg_producer,
            msg_type=msg_type,
        )
        receiver = _AsyncBrokerReceiver(self._receive_monitor)
        self._core.add_receiver(
            subscription=binding.subscription,
            delivery_target=receiver.delivery_target,
        )
        return receiver

    async def receive_from(
        self, *receivers: AsyncReceiver
    ) -> tuple[AsyncReceiver, ReceivedMessage]:
        selected_receivers = self._validate_receiver_selection(receivers)
        return await self._receive_monitor.receive_from(selected_receivers)

    def install_format(self, payload_format: PortableFormat) -> None:
        self._core.install_format(payload_format)

    def install_formats(
        self, payload_formats: collections.abc.Iterable[PortableFormat]
    ) -> None:
        self._core.install_formats(payload_formats)

    def set_capture_sink(self, capture_sink: CaptureSink) -> None:
        self._core.set_capture_sink(capture_sink)

    def capture_source(self) -> CaptureRecordSource | None:
        return self._core.capture_source()

    def create_transport_session(
        self, *, channel: AsyncFrameChannel
    ) -> AsyncBrokerTransportSession:
        return AsyncBrokerTransportSession(channel=channel, core=self._core)

    def _portable_format_table(self) -> PortableFormatRegistry:
        return self._core.format_registry()

    def _validate_receiver_selection(
        self, receivers: tuple[AsyncReceiver, ...]
    ) -> tuple["_AsyncBrokerReceiver", ...]:
        if not receivers:
            raise InvalidReceiverSelectionError(
                "receive_from requires at least one receiver"
            )

        selected_receivers = []
        for receiver in receivers:
            if (
                not isinstance(receiver, _AsyncBrokerReceiver)
                or receiver._receive_monitor is not self._receive_monitor
            ):
                raise InvalidReceiverSelectionError(
                    "receive_from requires receivers created by this bus"
                )
            selected_receivers.append(receiver)

        return tuple(selected_receivers)

    @classmethod
    def capture_bootstrap(
        cls,
        *,
        bootstrap_policy: BootstrapPolicy | None = None,
        bootstrap_limits: BootstrapBufferLimits | None = None,
    ) -> typing.Self:
        core = DirectBrokerCore(
            capture_enabled=True,
            bootstrap_enabled=True,
            bootstrap_policy=bootstrap_policy,
            bootstrap_limits=bootstrap_limits,
        )
        return cls._from_core(core)

    @classmethod
    def _from_core(cls, core: DirectBrokerCore) -> typing.Self:
        bus = cls.__new__(cls)
        bus._core = core
        bus._receive_monitor = _AsyncReceiveMonitor()
        return bus


class _AsyncBrokerEmitter(AsyncEmitter):
    _core: DirectBrokerCore
    _binding: EmitterBinding

    def __init__(
        self, *, core: DirectBrokerCore, binding: EmitterBinding
    ) -> None:
        self._core = core
        self._binding = binding

    async def emit(
        self,
        payload: typing.Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat | None = None,
    ) -> None:
        self._core.emit_from(
            binding=self._binding,
            payload=payload,
            msg_type=msg_type,
            payload_format=payload_format,
            bus_operation=BusOperation.PUBLISH,
        )

    async def emit_request(
        self,
        payload: typing.Any,
        *,
        correlation_id: CorrelationID,
        msg_type: str | None = None,
        payload_format: PortableFormat | None = None,
    ) -> MessageID:
        request_id = self._core.emit_from(
            binding=self._binding,
            payload=payload,
            msg_type=msg_type,
            payload_format=payload_format,
            bus_operation=BusOperation.REQUEST,
            correlation_id=correlation_id,
        )
        return request_id

    async def emit_reply(
        self,
        request: ReceivedMessage,
        payload: typing.Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat | None = None,
    ) -> None:
        correlation_id, reply_to = reply_metadata_for(request)
        self._core.emit_from(
            binding=self._binding,
            payload=payload,
            msg_type=msg_type,
            payload_format=payload_format,
            bus_operation=BusOperation.REPLY,
            correlation_id=correlation_id,
            reply_to=reply_to,
        )


class _AsyncBrokerReceiver(AsyncReceiver):
    _queue: asyncio.Queue[ReceivedMessage]
    _delivery_target: BrokerDeliveryTarget
    _receive_monitor: "_AsyncReceiveMonitor"

    def __init__(self, receive_monitor: "_AsyncReceiveMonitor") -> None:
        self._queue = asyncio.Queue()
        self._receive_monitor = receive_monitor
        self._delivery_target = _AsyncDeliveryTarget(
            self._queue, receive_monitor
        )

    @property
    def delivery_target(self) -> BrokerDeliveryTarget:
        return self._delivery_target

    async def _receive_batch(
        self, *, min_count: int, max_count: int | None = None
    ) -> list[ReceivedMessage]:
        messages = []
        while len(messages) < min_count:
            messages.append(await self._queue.get())

        while max_count is None or len(messages) < max_count:
            try:
                messages.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        return messages

    def _receive_batch_nowait(
        self, *, max_count: int | None
    ) -> list[ReceivedMessage]:
        messages = []
        while max_count is None or len(messages) < max_count:
            try:
                messages.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        return messages


class _AsyncDeliveryTarget(BrokerDeliveryTarget):
    _queue: asyncio.Queue[ReceivedMessage]
    _receive_monitor: "_AsyncReceiveMonitor"

    def __init__(
        self,
        queue: asyncio.Queue[ReceivedMessage],
        receive_monitor: "_AsyncReceiveMonitor",
    ) -> None:
        self._queue = queue
        self._receive_monitor = receive_monitor

    def deliver(self, message: BusMessage) -> None:
        self._queue.put_nowait(message.received_view())
        self._receive_monitor.notify_delivery()


class _AsyncReceiveMonitor:
    _event: asyncio.Event

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def notify_delivery(self) -> None:
        self._event.set()

    async def receive_from(
        self, receivers: tuple[_AsyncBrokerReceiver, ...]
    ) -> tuple[AsyncReceiver, ReceivedMessage]:
        received = self._take_available_from(receivers)
        while received is None:
            self._event.clear()
            received = self._take_available_from(receivers)
            if received is None:
                await self._event.wait()
                received = self._take_available_from(receivers)

        return received

    def _take_available_from(
        self, receivers: tuple[_AsyncBrokerReceiver, ...]
    ) -> tuple[AsyncReceiver, ReceivedMessage] | None:
        received = None
        for receiver in receivers:
            message = receiver.receive_nowait()
            if message is not None:
                received = (receiver, message)
                break

        return received
