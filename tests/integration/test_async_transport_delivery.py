#!/usr/bin/env python3
# tests/integration/test_async_transport_delivery.py

"""Integration tests for asynchronous transport delivery isolation."""

import asyncio

from ropemother.broker.asyncdirect import AsyncDirectMessageBus
from ropemother.broker.directcore import BrokerDeliveryTarget, DirectBrokerCore
from ropemother.capture.memorysink import InMemoryCaptureSink
from ropemother.client.asyncrequest import (
    AsyncRequestClient,
    AsyncRequestService,
    AsyncServiceRequest,
)
from ropemother.client.request import RequestHandle
from ropemother.format.portableformat import RAW_BYTES_PORTABLE_FORMAT
from ropemother.message.records import BusMessage, BusOperation
from ropemother.message.selectors import topic_filter_from_input
from ropemother.transport.asyncclient import AsyncTransportClient
from ropemother.transport.asyncconnection import (
    AsyncFrameChannel,
    AsyncFrameConnection,
    AsyncMemoryFrameConnection,
)
from ropemother.transport.asyncsession import AsyncBrokerTransportSession
from ropemother.transport.codec import FrameParts

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-10-03T00:14:55+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev10"
__status__ = "Development"


TOPIC = "slow-subscriber"
PRODUCER = "producer"
MSG_TYPE = "payload"
DELIVERY_WAIT_SECONDS = 0.5
OVERFLOW_MESSAGE_COUNT = 70

REQUEST_TOPIC = "request-routing.requests"
REPLY_TOPIC = "request-routing.replies"
REQUESTER_PRODUCER = "request-routing-client"
RESPONDER_PRODUCER = "request-routing-service"
REQUEST_MSG_TYPE = "request"
REPLY_MSG_TYPE = "reply"


class _BlockableAsyncFrameConnection(AsyncFrameConnection):
    def __init__(self, connection: AsyncFrameConnection) -> None:
        self._connection = connection
        self._block_sends = False
        self._send_started = asyncio.Event()
        self._release_send = asyncio.Event()
        self._closed = asyncio.Event()

    def block_sends(self) -> None:
        self._block_sends = True

    async def wait_until_send_started(self) -> None:
        await asyncio.wait_for(
            self._send_started.wait(), timeout=DELIVERY_WAIT_SECONDS
        )

    def release_sends(self) -> None:
        self._release_send.set()

    def is_closed(self) -> bool:
        return self._closed.is_set()

    async def send_frame_parts(self, parts: FrameParts) -> None:
        if self._block_sends:
            self._send_started.set()
            await self._release_send.wait()
        await self._connection.send_frame_parts(parts)

    async def receive_frame_parts(self) -> FrameParts:
        return await self._connection.receive_frame_parts()

    def receive_frame_parts_nowait(self) -> FrameParts | None:
        return self._connection.receive_frame_parts_nowait()

    def close(self) -> None:
        self._closed.set()
        self._release_send.set()
        self._connection.close()


def _make_blockable_async_transport_client(
    bus: AsyncDirectMessageBus,
) -> tuple[
    AsyncTransportClient,
    AsyncBrokerTransportSession,
    _BlockableAsyncFrameConnection,
]:
    client_connection, broker_connection = (
        AsyncMemoryFrameConnection.make_pair()
    )
    blockable_connection = _BlockableAsyncFrameConnection(broker_connection)
    client = AsyncTransportClient(channel=AsyncFrameChannel(client_connection))
    session = bus.create_transport_session(
        channel=AsyncFrameChannel(blockable_connection)
    )
    return client, session, blockable_connection


def _make_async_transport_client(
    bus: AsyncDirectMessageBus,
) -> tuple[
    AsyncTransportClient,
    AsyncBrokerTransportSession,
    AsyncMemoryFrameConnection,
]:
    client_connection, broker_connection = (
        AsyncMemoryFrameConnection.make_pair()
    )
    client = AsyncTransportClient(channel=AsyncFrameChannel(client_connection))
    session = bus.create_transport_session(
        channel=AsyncFrameChannel(broker_connection)
    )
    return client, session, client_connection


async def _drive_operation[T](
    task: asyncio.Task[T], session: AsyncBrokerTransportSession
) -> T:
    await asyncio.sleep(0)
    while not task.done():
        await session.handle_next_frame()
        await asyncio.sleep(0)
    return await task


async def _create_request_client(
    client: AsyncTransportClient, session: AsyncBrokerTransportSession
) -> AsyncRequestClient:
    task = asyncio.create_task(
        client.create_request_client(
            request_topic=REQUEST_TOPIC,
            reply_topic=REPLY_TOPIC,
            requester_producer=REQUESTER_PRODUCER,
            responder_producer=RESPONDER_PRODUCER,
            request_msg_type=REQUEST_MSG_TYPE,
            reply_msg_type=REPLY_MSG_TYPE,
            request_payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
    )
    return await _drive_operation(task, session)


async def _create_request_service(
    client: AsyncTransportClient, session: AsyncBrokerTransportSession
) -> AsyncRequestService:
    task = asyncio.create_task(
        client.create_request_service(
            request_topic=REQUEST_TOPIC,
            reply_topic=REPLY_TOPIC,
            requester_producer=REQUESTER_PRODUCER,
            responder_producer=RESPONDER_PRODUCER,
            request_msg_type=REQUEST_MSG_TYPE,
            reply_msg_type=REPLY_MSG_TYPE,
            reply_payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
    )
    return await _drive_operation(task, session)


async def _send_request(
    client: AsyncRequestClient,
    session: AsyncBrokerTransportSession,
    payload: bytes,
) -> RequestHandle:
    task = asyncio.create_task(client.send(payload))
    return await _drive_operation(task, session)


async def _reply_to_request(
    request: AsyncServiceRequest,
    session: AsyncBrokerTransportSession,
    payload: bytes,
) -> None:
    task = asyncio.create_task(request.reply(payload))
    await _drive_operation(task, session)


def test_async_slow_subscriber_does_not_block_others() -> None:
    asyncio.run(_test_async_slow_subscriber_does_not_block_others())


async def _test_async_slow_subscriber_does_not_block_others() -> None:
    bus = AsyncDirectMessageBus(capture_sink=InMemoryCaptureSink())
    idle_transport, idle_session, idle_connection = (
        _make_blockable_async_transport_client(bus)
    )
    active_transport, active_session, _ = _make_async_transport_client(bus)

    idle_task = asyncio.create_task(
        idle_transport.subscribe(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
        )
    )
    await _drive_operation(idle_task, idle_session)
    active_task = asyncio.create_task(
        active_transport.subscribe(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
        )
    )
    active_receiver = await _drive_operation(active_task, active_session)
    emitter = bus.register_emitter(
        msg_topic=TOPIC,
        msg_producer=PRODUCER,
        msg_type=MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    idle_connection.block_sends()

    try:
        payload = b"delivered"
        await emitter.emit(payload)
        await idle_connection.wait_until_send_started()
        received = await asyncio.wait_for(
            active_receiver.receive(), timeout=DELIVERY_WAIT_SECONDS
        )

        assert received.payload == payload
    finally:
        idle_connection.release_sends()
        idle_transport.close()
        active_transport.close()
        idle_session.close()
        active_session.close()


def test_async_queue_overflow_isolates_slow_subscriber() -> None:
    asyncio.run(_test_async_queue_overflow_isolates_slow_subscriber())


async def _test_async_queue_overflow_isolates_slow_subscriber() -> None:
    bus = AsyncDirectMessageBus(capture_sink=InMemoryCaptureSink())
    idle_transport, idle_session, idle_connection = (
        _make_blockable_async_transport_client(bus)
    )
    active_transport, active_session, _ = _make_async_transport_client(bus)

    idle_task = asyncio.create_task(
        idle_transport.subscribe(
            msg_topic=TOPIC, msg_producer=PRODUCER, msg_type=MSG_TYPE
        )
    )
    await _drive_operation(idle_task, idle_session)
    active_task = asyncio.create_task(
        active_transport.subscribe(
            msg_topic=TOPIC, msg_producer=PRODUCER, msg_type=MSG_TYPE
        )
    )
    active_receiver = await _drive_operation(active_task, active_session)
    emitter = bus.register_emitter(
        msg_topic=TOPIC,
        msg_producer=PRODUCER,
        msg_type=MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    idle_connection.block_sends()

    try:
        first_payload = b"blocked"
        await emitter.emit(first_payload)
        await idle_connection.wait_until_send_started()
        first_received = await asyncio.wait_for(
            active_receiver.receive(), timeout=DELIVERY_WAIT_SECONDS
        )
        assert first_received.payload == first_payload

        for index in range(OVERFLOW_MESSAGE_COUNT):
            payload = str(index).encode()
            await emitter.emit(payload)
            received = await asyncio.wait_for(
                active_receiver.receive(), timeout=DELIVERY_WAIT_SECONDS
            )
            assert received.payload == payload

        assert idle_connection.is_closed()

        final_payload = b"still-active"
        await emitter.emit(final_payload)
        final_received = await asyncio.wait_for(
            active_receiver.receive(), timeout=DELIVERY_WAIT_SECONDS
        )
        assert final_received.payload == final_payload
    finally:
        idle_connection.release_sends()
        idle_transport.close()
        active_transport.close()
        idle_session.close()
        active_session.close()


def test_async_request_client_ignores_foreign_replies() -> None:
    asyncio.run(_test_async_request_client_ignores_foreign_replies())


async def _test_async_request_client_ignores_foreign_replies() -> None:
    bus = AsyncDirectMessageBus(capture_sink=InMemoryCaptureSink())
    active_transport, active_session, _ = _make_async_transport_client(bus)
    idle_transport, idle_session, idle_connection = (
        _make_async_transport_client(bus)
    )
    service_transport, service_session, _ = _make_async_transport_client(bus)

    active_client = await _create_request_client(
        active_transport, active_session
    )
    await _create_request_client(idle_transport, idle_session)
    service = await _create_request_service(service_transport, service_session)

    handle = await _send_request(active_client, active_session, b"request")
    request = await service.receive()
    await _reply_to_request(request, service_session, b"reply")
    reply = await active_client.receive(handle)
    await asyncio.sleep(0)

    assert reply.payload == b"reply"
    assert idle_connection.receive_frame_parts_nowait() is None


def test_async_ordinary_subscription_receives_request_reply() -> None:
    asyncio.run(_test_async_ordinary_subscription_receives_request_reply())


async def _test_async_ordinary_subscription_receives_request_reply() -> None:
    bus = AsyncDirectMessageBus(capture_sink=InMemoryCaptureSink())
    requester_transport, requester_session, _ = _make_async_transport_client(
        bus
    )
    observer_transport, observer_session, _ = _make_async_transport_client(bus)
    service_transport, service_session, _ = _make_async_transport_client(bus)

    client = await _create_request_client(
        requester_transport, requester_session
    )
    observer_task = asyncio.create_task(
        observer_transport.subscribe(
            msg_topic=REPLY_TOPIC,
            msg_producer=RESPONDER_PRODUCER,
            msg_type=REPLY_MSG_TYPE,
        )
    )
    observer = await _drive_operation(observer_task, observer_session)
    service = await _create_request_service(service_transport, service_session)

    handle = await _send_request(client, requester_session, b"request")
    request = await service.receive()
    await _reply_to_request(request, service_session, b"observed-reply")
    client_reply = await client.receive(handle)
    observed_reply = await observer.receive()

    assert observed_reply.payload == b"observed-reply"
    assert observed_reply.msg_id == client_reply.msg_id


def test_async_reply_can_arrive_before_request_emit_returns() -> None:
    asyncio.run(_test_async_reply_can_arrive_before_request_emit_returns())


async def _test_async_reply_can_arrive_before_request_emit_returns() -> None:
    core = DirectBrokerCore(capture_sink=InMemoryCaptureSink())
    client_connection, broker_connection = (
        AsyncMemoryFrameConnection.make_pair()
    )
    requester_transport = AsyncTransportClient(
        channel=AsyncFrameChannel(client_connection)
    )
    session = AsyncBrokerTransportSession(
        channel=AsyncFrameChannel(broker_connection), core=core
    )
    client = await _create_request_client(requester_transport, session)
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

    handle = await _send_request(client, session, b"request")
    reply = await client.receive(handle)

    assert reply.payload == reply_payload
