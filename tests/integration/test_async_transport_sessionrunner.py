#!/usr/bin/env python3
# tests/integration/test_async_transport_sessionrunner.py

"""Integration tests for asynchronous transport session lifecycle."""

import asyncio
import contextlib
import dataclasses

import pytest

from ropemother.broker.asyncdirect import AsyncDirectMessageBus
from ropemother.capture.memorysink import InMemoryCaptureSink
from ropemother.format.portableformat import RAW_BYTES_PORTABLE_FORMAT
from ropemother.transport.asyncclient import AsyncTransportClient
from ropemother.transport.asyncconnection import (
    AsyncFrameChannel,
    AsyncFrameConnection,
    AsyncMemoryFrameConnection,
)
from ropemother.transport.asyncsessionrunner import (
    AsyncBrokerTransportSessionRunner,
    AsyncTransportSessionFailedError,
)
from ropemother.transport.codec import FrameParts

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-10-03T03:04:49+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev10"
__status__ = "Development"


TOPIC = "runner-lifecycle"
PRODUCER = "producer"
MSG_TYPE = "payload"
DELIVERY_WAIT_SECONDS = 0.5
OVERFLOW_MESSAGE_COUNT = 70


class _DeliveryFailure(Exception):
    pass


class _ControllableAsyncFrameConnection(AsyncFrameConnection):
    def __init__(self, connection: AsyncFrameConnection) -> None:
        self._connection = connection
        self._closed = asyncio.Event()
        self._peer_closed = asyncio.Event()
        self._block_sends = False
        self._send_started = asyncio.Event()
        self._release_send = asyncio.Event()
        self._send_error: Exception | None = None

    def peer_close(self) -> None:
        self._peer_closed.set()

    def block_sends(self) -> None:
        self._block_sends = True

    def fail_sends(self, error: Exception) -> None:
        self._send_error = error

    async def wait_until_send_started(self) -> None:
        await asyncio.wait_for(
            self._send_started.wait(), timeout=DELIVERY_WAIT_SECONDS
        )

    async def wait_until_closed(self) -> None:
        await asyncio.wait_for(
            self._closed.wait(), timeout=DELIVERY_WAIT_SECONDS
        )

    def is_closed(self) -> bool:
        return self._closed.is_set()

    async def send_frame_parts(self, parts: FrameParts) -> None:
        if self._send_error is not None:
            raise self._send_error
        if self._block_sends:
            self._send_started.set()
            await self._release_send.wait()
        await self._connection.send_frame_parts(parts)

    async def receive_frame_parts(self) -> FrameParts:
        receive_task = asyncio.create_task(
            self._connection.receive_frame_parts()
        )
        close_task = asyncio.create_task(self._closed.wait())
        peer_close_task = asyncio.create_task(self._peer_closed.wait())
        tasks = (receive_task, close_task, peer_close_task)
        done, _ = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )
        try:
            if peer_close_task in done:
                raise EOFError
            if close_task in done:
                raise OSError("connection closed")
            return await receive_task
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            for task in tasks:
                if task is receive_task and receive_task in done:
                    continue
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    def receive_frame_parts_nowait(self) -> FrameParts | None:
        return self._connection.receive_frame_parts_nowait()

    def close(self) -> None:
        self._closed.set()
        self._release_send.set()
        self._connection.close()


@dataclasses.dataclass(frozen=True)
class _RunnerTransport:
    bus: AsyncDirectMessageBus
    client: AsyncTransportClient
    runner: AsyncBrokerTransportSessionRunner
    connection: _ControllableAsyncFrameConnection


def _make_runner_transport() -> _RunnerTransport:
    bus = AsyncDirectMessageBus(capture_sink=InMemoryCaptureSink())
    client_connection, broker_connection = (
        AsyncMemoryFrameConnection.make_pair()
    )
    broker_connection = _ControllableAsyncFrameConnection(broker_connection)
    client = AsyncTransportClient(channel=AsyncFrameChannel(client_connection))
    session = bus.create_transport_session(
        channel=AsyncFrameChannel(broker_connection)
    )
    runner = AsyncBrokerTransportSessionRunner(
        session=session, close_connection=broker_connection.close
    )
    runner.start()
    transport = _RunnerTransport(
        bus=bus, client=client, runner=runner, connection=broker_connection
    )
    return transport


async def _subscribe(client: AsyncTransportClient) -> None:
    await client.subscribe(
        msg_topic=TOPIC, msg_producer=PRODUCER, msg_type=MSG_TYPE
    )


def test_async_runner_closes_connection_after_peer_close() -> None:
    asyncio.run(_test_async_runner_closes_connection_after_peer_close())


async def _test_async_runner_closes_connection_after_peer_close() -> None:
    transport = _make_runner_transport()
    try:
        transport.connection.peer_close()
        await transport.runner.wait()
        assert transport.connection.is_closed()
    finally:
        transport.client.close()
        transport.runner.request_stop()


def test_async_runner_queue_overflow_is_endpoint_close() -> None:
    asyncio.run(_test_async_runner_queue_overflow_is_endpoint_close())


async def _test_async_runner_queue_overflow_is_endpoint_close() -> None:
    transport = _make_runner_transport()
    try:
        await _subscribe(transport.client)
        emitter = transport.bus.register_emitter(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        transport.connection.block_sends()

        await emitter.emit(b"blocked")
        await transport.connection.wait_until_send_started()
        for index in range(OVERFLOW_MESSAGE_COUNT):
            await emitter.emit(str(index).encode())
            await asyncio.sleep(0)

        await transport.connection.wait_until_closed()
        await transport.runner.wait()
    finally:
        transport.client.close()
        transport.runner.request_stop()


def test_async_runner_reports_delivery_worker_failure() -> None:
    asyncio.run(_test_async_runner_reports_delivery_worker_failure())


async def _test_async_runner_reports_delivery_worker_failure() -> None:
    transport = _make_runner_transport()
    try:
        await _subscribe(transport.client)
        emitter = transport.bus.register_emitter(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        transport.connection.fail_sends(_DeliveryFailure("failed to send"))

        await emitter.emit(b"trigger attempted delivery")
        await transport.connection.wait_until_closed()

        with pytest.raises(AsyncTransportSessionFailedError) as failure:
            await transport.runner.wait()
        assert isinstance(failure.value.__cause__, _DeliveryFailure)
    finally:
        transport.client.close()
        transport.runner.request_stop()
