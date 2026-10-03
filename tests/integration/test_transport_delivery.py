#!/usr/bin/env python3
# tests/integration/test_transport_delivery.py

"""Integration tests for synchronous transport delivery isolation."""

import contextlib
import socket
import threading
import time

import pytest

from ropemother.broker.direct import DirectMessageBus
from ropemother.broker.directcore import BrokerDeliveryTarget, DirectBrokerCore
from ropemother.broker.endpoints import Receiver
from ropemother.capture.memorysink import InMemoryCaptureSink
from ropemother.client.request import RequestClient
from ropemother.format.portableformat import (
    PortableFormat,
    PortableFormatKey,
    RAW_BYTES_PORTABLE_FORMAT,
)
from ropemother.message.records import (
    BusMessage,
    BusOperation,
    ReceivedMessage,
)
from ropemother.message.selectors import topic_filter_from_input
from ropemother.transport.client import (
    TransportClient,
    TransportPayloadDecodeError,
)
from ropemother.transport.codec import FrameParts
from ropemother.transport.connection import FrameChannel, FrameConnection
from ropemother.transport.session import BrokerTransportSession
from ropemother.transport.sessionrunner import BrokerTransportSessionRunner
from ropemother.util.serializer import (
    IDENTITY_BYTES_ADAPTER,
    IDENTITY_SERIALIZER,
)
from ropemother.transport.socketconnection import SocketFrameConnection

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-10-03T04:17:19+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev11"
__status__ = "Development"


TOPIC = "slow-subscriber"
PRODUCER = "producer"
MSG_TYPE = "payload"
SOCKET_BUFFER_SIZE = 1_024
DELIVERY_WAIT_SECONDS = 0.5
POLL_INTERVAL_SECONDS = 0.01
CLEANUP_WAIT_SECONDS = 1.0
OVERFLOW_MESSAGE_COUNT = 70

REQUEST_TOPIC = "request-routing.requests"
REPLY_TOPIC = "request-routing.replies"
REQUESTER_PRODUCER = "request-routing-client"
RESPONDER_PRODUCER = "request-routing-service"
REQUEST_MSG_TYPE = "request"
REPLY_MSG_TYPE = "reply"
ROUTING_REPLY_SIZE = 32_000

UNAVAILABLE_BYTES_FORMAT = PortableFormat(
    key=PortableFormatKey.from_str("unavailable-bytes"),
    adapter=IDENTITY_BYTES_ADAPTER,
    serializer=IDENTITY_SERIALIZER,
)


class _BlockableFrameConnection(FrameConnection):
    def __init__(self, connection: FrameConnection) -> None:
        self._connection = connection
        self._block_sends = threading.Event()
        self._send_started = threading.Event()
        self._release_send = threading.Event()
        self._closed = threading.Event()

    def block_sends(self) -> None:
        self._block_sends.set()

    def wait_until_send_started(self, timeout: float) -> bool:
        return self._send_started.wait(timeout)

    def release_sends(self) -> None:
        self._release_send.set()

    def is_closed(self) -> bool:
        return self._closed.is_set()

    def send_frame_parts(self, parts: FrameParts) -> None:
        if self._block_sends.is_set():
            self._send_started.set()
            self._release_send.wait()
        self._connection.send_frame_parts(parts)

    def receive_frame_parts(self) -> FrameParts:
        return self._connection.receive_frame_parts()

    def receive_frame_parts_nowait(self) -> FrameParts | None:
        return self._connection.receive_frame_parts_nowait()

    def close(self) -> None:
        self._closed.set()
        self._release_send.set()
        self._connection.close()


def _make_transport_client(
    bus: DirectMessageBus,
    *,
    constrain_buffers: bool = False,
    broker_closed: threading.Event | None = None,
) -> tuple[TransportClient, BrokerTransportSessionRunner]:
    client_socket, broker_socket = socket.socketpair()
    if constrain_buffers:
        client_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFFER_SIZE
        )
        broker_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUFFER_SIZE
        )

    client_connection = SocketFrameConnection(client_socket)
    broker_connection = SocketFrameConnection(broker_socket)
    client = TransportClient(channel=FrameChannel(client_connection))
    session = bus.create_transport_session(
        channel=FrameChannel(broker_connection)
    )

    def _close_broker_connection() -> None:
        broker_connection.close()
        if broker_closed is not None:
            broker_closed.set()

    runner = BrokerTransportSessionRunner(
        session=session, close_connection=_close_broker_connection
    )
    runner.start()
    return client, runner


def _make_blockable_transport_client(bus: DirectMessageBus) -> tuple[
    TransportClient,
    BrokerTransportSessionRunner,
    _BlockableFrameConnection,
]:
    client_socket, broker_socket = socket.socketpair()
    client_connection = SocketFrameConnection(client_socket)
    broker_connection = _BlockableFrameConnection(
        SocketFrameConnection(broker_socket)
    )
    client = TransportClient(channel=FrameChannel(client_connection))
    session = bus.create_transport_session(
        channel=FrameChannel(broker_connection)
    )
    runner = BrokerTransportSessionRunner(
        session=session, close_connection=broker_connection.close
    )
    runner.start()
    return client, runner, broker_connection


def _create_request_client(client: TransportClient) -> RequestClient:
    request_client = client.create_request_client(
        request_topic=REQUEST_TOPIC,
        reply_topic=REPLY_TOPIC,
        requester_producer=REQUESTER_PRODUCER,
        responder_producer=RESPONDER_PRODUCER,
        request_msg_type=REQUEST_MSG_TYPE,
        reply_msg_type=REPLY_MSG_TYPE,
        request_payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    return request_client


def _receive_until(
    receiver: Receiver, deadline: float
) -> ReceivedMessage | None:
    message = receiver.receive_nowait()
    while message is None and time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL_SECONDS)
        message = receiver.receive_nowait()
    return message


def _close_transport_client(
    client: TransportClient, runner: BrokerTransportSessionRunner
) -> None:
    try:
        client.close()
    finally:
        runner.join(CLEANUP_WAIT_SECONDS)
        assert not runner.is_alive()


def test_slow_transport_subscriber_does_not_block_other_subscribers() -> None:
    bus = DirectMessageBus(capture_sink=InMemoryCaptureSink())
    idle_client, idle_runner, idle_connection = (
        _make_blockable_transport_client(bus)
    )
    active_client, active_runner = _make_transport_client(bus)
    worker = None

    with contextlib.ExitStack() as stack:

        def _join_runner(runner: BrokerTransportSessionRunner) -> None:
            runner.join(CLEANUP_WAIT_SECONDS)
            assert not runner.is_alive()

        def _join_worker() -> None:
            if worker is not None:
                worker.join(CLEANUP_WAIT_SECONDS)
                assert not worker.is_alive()

        stack.callback(_join_runner, active_runner)
        stack.callback(_join_runner, idle_runner)
        stack.callback(active_client.close)
        stack.callback(idle_client.close)
        stack.callback(_join_worker)
        stack.callback(idle_connection.release_sends)

        idle_client.subscribe(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
        )
        active_receiver = active_client.subscribe(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
        )
        emitter = bus.register_emitter(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        idle_connection.block_sends()
        payload = b"delivered"
        emit_started = threading.Event()

        def emit_payload() -> None:
            emit_started.set()
            emitter.emit(payload)

        emit_thread = threading.Thread(target=emit_payload)
        emit_thread.start()
        worker = emit_thread
        assert emit_started.wait(DELIVERY_WAIT_SECONDS)
        assert idle_connection.wait_until_send_started(DELIVERY_WAIT_SECONDS)

        deadline = time.monotonic() + DELIVERY_WAIT_SECONDS
        received = _receive_until(active_receiver, deadline)

        assert received is not None
        assert received.payload == payload


def test_full_delivery_queue_disconnects_only_slow_subscriber() -> None:
    bus = DirectMessageBus(capture_sink=InMemoryCaptureSink())
    idle_client, idle_runner, idle_connection = (
        _make_blockable_transport_client(bus)
    )
    active_client, active_runner = _make_transport_client(bus)

    with contextlib.ExitStack() as stack:
        stack.callback(_close_transport_client, idle_client, idle_runner)
        stack.callback(_close_transport_client, active_client, active_runner)

        idle_client.subscribe(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
        )
        active_receiver = active_client.subscribe(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
        )
        emitter = bus.register_emitter(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        idle_connection.block_sends()

        first_payload = b"blocked"
        emitter.emit(first_payload)
        assert idle_connection.wait_until_send_started(DELIVERY_WAIT_SECONDS)
        deadline = time.monotonic() + DELIVERY_WAIT_SECONDS
        first_received = _receive_until(active_receiver, deadline)
        assert first_received is not None
        assert first_received.payload == first_payload

        for index in range(OVERFLOW_MESSAGE_COUNT):
            payload = str(index).encode()
            emitter.emit(payload)
            deadline = time.monotonic() + DELIVERY_WAIT_SECONDS
            received = _receive_until(active_receiver, deadline)
            assert received is not None
            assert received.payload == payload

        assert idle_connection.is_closed()

        final_payload = b"still-active"
        emitter.emit(final_payload)
        deadline = time.monotonic() + DELIVERY_WAIT_SECONDS
        final_received = _receive_until(active_receiver, deadline)
        assert final_received is not None
        assert final_received.payload == final_payload


def test_other_request_replies_do_not_disconnect_idle_request_client() -> None:
    bus = DirectMessageBus(capture_sink=InMemoryCaptureSink())
    active_transport, active_runner = _make_transport_client(bus)
    idle_closed = threading.Event()
    idle_transport, idle_runner = _make_transport_client(
        bus, constrain_buffers=True, broker_closed=idle_closed
    )
    service_transport, service_runner = _make_transport_client(bus)

    with contextlib.ExitStack() as stack:
        stack.callback(
            _close_transport_client, active_transport, active_runner
        )
        stack.callback(_close_transport_client, idle_transport, idle_runner)
        stack.callback(
            _close_transport_client, service_transport, service_runner
        )

        active_client = _create_request_client(active_transport)
        idle_client = _create_request_client(idle_transport)
        service = service_transport.create_request_service(
            request_topic=REQUEST_TOPIC,
            reply_topic=REPLY_TOPIC,
            requester_producer=REQUESTER_PRODUCER,
            responder_producer=RESPONDER_PRODUCER,
            request_msg_type=REQUEST_MSG_TYPE,
            reply_msg_type=REPLY_MSG_TYPE,
            reply_payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        reply_payload = b"x" * ROUTING_REPLY_SIZE

        for index in range(OVERFLOW_MESSAGE_COUNT):
            handle = active_client.send(str(index).encode())
            request = service.receive()
            request.reply(reply_payload)
            reply = active_client.receive(handle)
            assert reply.payload == reply_payload

        assert not idle_closed.wait(DELIVERY_WAIT_SECONDS)

        idle_handle = idle_client.send(b"idle-request")
        idle_request = service.receive()
        idle_request.reply(b"idle-reply")
        idle_reply = idle_client.receive(idle_handle)
        assert idle_reply.payload == b"idle-reply"


def test_ordinary_transport_subscription_receives_request_reply() -> None:
    bus = DirectMessageBus(capture_sink=InMemoryCaptureSink())
    requester_transport, requester_runner = _make_transport_client(bus)
    observer_transport, observer_runner = _make_transport_client(bus)
    service_transport, service_runner = _make_transport_client(bus)

    with contextlib.ExitStack() as stack:
        stack.callback(
            _close_transport_client, requester_transport, requester_runner
        )
        stack.callback(
            _close_transport_client, observer_transport, observer_runner
        )
        stack.callback(
            _close_transport_client, service_transport, service_runner
        )

        client = _create_request_client(requester_transport)
        observer = observer_transport.subscribe(
            msg_topic=REPLY_TOPIC,
            msg_producer=RESPONDER_PRODUCER,
            msg_type=REPLY_MSG_TYPE,
        )
        service = service_transport.create_request_service(
            request_topic=REQUEST_TOPIC,
            reply_topic=REPLY_TOPIC,
            requester_producer=REQUESTER_PRODUCER,
            responder_producer=RESPONDER_PRODUCER,
            request_msg_type=REQUEST_MSG_TYPE,
            reply_msg_type=REPLY_MSG_TYPE,
            reply_payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        reply_payload = b"observed-reply"

        handle = client.send(b"request")
        request = service.receive()
        request.reply(reply_payload)
        client_reply = client.receive(handle)
        deadline = time.monotonic() + DELIVERY_WAIT_SECONDS
        observed_reply = _receive_until(observer, deadline)

        assert observed_reply is not None
        assert observed_reply.payload == reply_payload
        assert observed_reply.msg_id == client_reply.msg_id


def test_request_reply_can_arrive_before_request_emit_returns() -> None:
    core = DirectBrokerCore(capture_sink=InMemoryCaptureSink())
    client_socket, broker_socket = socket.socketpair()
    client_connection = SocketFrameConnection(client_socket)
    broker_connection = SocketFrameConnection(broker_socket)
    requester_transport = TransportClient(
        channel=FrameChannel(client_connection)
    )
    session = BrokerTransportSession(
        channel=FrameChannel(broker_connection), core=core
    )
    requester_runner = BrokerTransportSessionRunner(
        session=session, close_connection=broker_connection.close
    )
    requester_runner.start()

    with contextlib.ExitStack() as stack:
        stack.callback(
            _close_transport_client, requester_transport, requester_runner
        )

        client = _create_request_client(requester_transport)
        reply_binding, _ = core.bind_emitter(
            msg_topic=REPLY_TOPIC,
            msg_producer=RESPONDER_PRODUCER,
            msg_type=REPLY_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        request_binding, _ = core.bind_subscription(
            msg_topic_filter=topic_filter_from_input(REQUEST_TOPIC),
            msg_producer=REQUESTER_PRODUCER,
            msg_type=REQUEST_MSG_TYPE,
        )
        reply_payload = b"immediate-reply"

        class _ImmediateReplyTarget(BrokerDeliveryTarget):
            def deliver(self, message: BusMessage) -> None:
                core.emit_from(
                    binding=reply_binding,
                    payload=reply_payload,
                    msg_type=None,
                    payload_format=None,
                    bus_operation=BusOperation.REPLY,
                    correlation_id=message.correlation_id,
                    reply_to=message.msg_id,
                )

        core.add_receiver(
            subscription=request_binding.subscription,
            delivery_target=_ImmediateReplyTarget(),
        )

        handle = client.send(b"request")
        reply = client.receive(handle)

        assert reply.payload == reply_payload


def test_transport_receiver_reports_missing_local_payload_decoder() -> None:
    bus = DirectMessageBus(
        extra_formats=(UNAVAILABLE_BYTES_FORMAT,),
        capture_sink=InMemoryCaptureSink(),
    )
    client, runner = _make_transport_client(bus)

    with contextlib.ExitStack() as stack:
        stack.callback(_close_transport_client, client, runner)

        receiver = client.subscribe(
            msg_topic=TOPIC, msg_producer=PRODUCER, msg_type=MSG_TYPE
        )
        emitter = bus.register_emitter(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
            payload_format=UNAVAILABLE_BYTES_FORMAT,
        )
        emitter.emit(b"payload")

        with pytest.raises(TransportPayloadDecodeError):
            receiver.receive()
