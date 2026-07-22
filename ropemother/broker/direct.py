#!/usr/bin/env python3
# ropemother/broker/direct.py

"""Synchronous in-process broker and support classes, without transport."""

from collections.abc import Iterable
from queue import Empty, Queue
from typing import Any, Self

from ropemother.bootstrap.buffer import BootstrapBufferLimits
from ropemother.bootstrap.policy import BootstrapPolicy
from ropemother.broker.base import MessageBus
from ropemother.broker.directcore import (
    BrokerDeliveryTarget,
    CaptureMode,
    DirectBrokerCore,
    EmitterBinding,
)
from ropemother.broker.endpoints import Emitter, Receiver, reply_metadata_for
from ropemother.broker.subscription import Subscription
from ropemother.capture.sink import CaptureSink
from ropemother.capture.writer import CaptureRecordSource
from ropemother.client.request import (
    RequestClient,
    RequestService,
    Requester,
    Responder,
)
from ropemother.format.portableformat import (
    PortableFormat,
    JSON_PORTABLE_FORMAT,
)
from ropemother.format.registry import PortableFormatRegistry
from ropemother.message.messageidentity import CorrelationID
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
from ropemother.transport.connection import FrameChannel
from ropemother.transport.session import BrokerTransportSession

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-22T16:08:24+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class DirectMessageBus(MessageBus):
    """Direct broker that routes messages to local receiver queues."""
    _core: DirectBrokerCore

    def __init__(
        self,
        *,
        extra_formats: Iterable[PortableFormat[Any, Any]] = (),
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

    def register_emitter(
        self,
        *,
        msg_topic: str,
        msg_producer: str,
        msg_type: str,
        additional_msg_types: SymbolCollectionInput = (),
        allow_unlisted_type_formats: bool = False,
        payload_format: PortableFormat[Any, Any] = JSON_PORTABLE_FORMAT,
        supported_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> Emitter:
        binding, _ = self._core.bind_emitter(
            msg_topic=msg_topic,
            msg_producer=msg_producer,
            msg_type=msg_type,
            additional_msg_types=additional_msg_types,
            allow_unlisted_type_formats=allow_unlisted_type_formats,
            payload_format=payload_format,
            supported_type_formats=supported_type_formats,
        )
        return _BrokerEmitter(core=self._core, binding=binding)

    def subscribe(
        self,
        *,
        msg_topic: SubscriptionTopicInput,
        msg_producer: OptionalSymbolInput = None,
        msg_type: OptionalSymbolInput = None,
    ) -> Receiver:
        msg_topic_filter = topic_filter_from_input(msg_topic)
        binding, _ = self._core.bind_subscription(
            msg_topic_filter=msg_topic_filter,
            msg_producer=msg_producer,
            msg_type=msg_type,
        )
        receiver = _BrokerReceiver()
        self._core.add_receiver(
            subscription=binding.subscription,
            delivery_target=receiver.delivery_target,
        )
        return receiver

    def install_format(
        self, payload_format: PortableFormat[Any, Any]
    ) -> None:
        self._core.install_format(payload_format)

    def install_formats(
        self, payload_formats: Iterable[PortableFormat[Any, Any]]
    ) -> None:
        self._core.install_formats(payload_formats)

    def set_capture_sink(self, capture_sink: CaptureSink) -> None:
        self._core.set_capture_sink(capture_sink)

    def capture_source(self) -> CaptureRecordSource | None:
        return self._core.capture_source()

    def _portable_format_table(self) -> PortableFormatRegistry:
        return self._core.format_registry()

    def create_transport_session(
        self, *, channel: FrameChannel
    ) -> BrokerTransportSession:
        return BrokerTransportSession(channel=channel, core=self._core)

    @classmethod
    def capture_bootstrap(
        cls,
        *,
        bootstrap_policy: BootstrapPolicy | None = None,
        bootstrap_limits: BootstrapBufferLimits | None = None,
    ) -> Self:
        core = DirectBrokerCore(
            capture_enabled=True,
            bootstrap_enabled=True,
            bootstrap_policy=bootstrap_policy,
            bootstrap_limits=bootstrap_limits,
        )
        bus = cls._from_core(core)
        return bus

    @classmethod
    def _from_core(cls, core: DirectBrokerCore) -> Self:
        bus = cls.__new__(cls)
        bus._core = core
        return bus


class _BrokerEmitter(Emitter):
    _core: DirectBrokerCore
    _binding: EmitterBinding

    def __init__(
        self, *, core: DirectBrokerCore, binding: EmitterBinding
    ) -> None:
        self._core = core
        self._binding = binding

    def emit(
        self,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        self._core.emit_from(
            binding=self._binding,
            payload=payload,
            msg_type=msg_type,
            payload_format=payload_format,
            bus_operation=BusOperation.PUBLISH,
        )

    def emit_request(
        self,
        payload: Any,
        *,
        correlation_id: CorrelationID,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        self._core.emit_from(
            binding=self._binding,
            payload=payload,
            msg_type=msg_type,
            payload_format=payload_format,
            bus_operation=BusOperation.REQUEST,
            correlation_id=correlation_id,
        )

    def emit_reply(
        self,
        request: ReceivedMessage,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
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


class _BrokerReceiver(Receiver):
    _queue: Queue[ReceivedMessage]
    _delivery_target: BrokerDeliveryTarget

    def __init__(self) -> None:
        self._queue = Queue()
        self._delivery_target = _SyncDeliveryTarget(self._queue)

    @property
    def delivery_target(self) -> BrokerDeliveryTarget:
        return self._delivery_target

    def _receive_batch(
        self, *, min_count: int, max_count: int | None
    ) -> list[ReceivedMessage]:
        messages = []
        while len(messages) < min_count:
            messages.append(self._queue.get())
        while max_count is None or len(messages) < max_count:
            try:
                messages.append(self._queue.get_nowait())
            except Empty:
                break
        return messages


class _SyncDeliveryTarget(BrokerDeliveryTarget):
    _queue: Queue[ReceivedMessage]

    def __init__(self, queue: Queue[ReceivedMessage]) -> None:
        self._queue = queue

    def deliver(self, message: BusMessage) -> None:
        self._queue.put(message.received_view())
