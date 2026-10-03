#!/usr/bin/env python3
# tests/integration/test_transport_sessionrunner.py

"""Integration tests for synchronous transport session lifecycle."""

import dataclasses
import socket
import threading

import pytest

from ropemother.broker.direct import DirectMessageBus
from ropemother.capture.memorysink import InMemoryCaptureSink
from ropemother.format.portableformat import RAW_BYTES_PORTABLE_FORMAT
from ropemother.transport.client import TransportClient
from ropemother.transport.codec import FrameParts
from ropemother.transport.connection import FrameChannel, FrameConnection
from ropemother.transport.sessionrunner import (
    BrokerTransportSessionRunner,
    TransportSessionFailedError,
)
from ropemother.transport.socketconnection import SocketFrameConnection

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-10-03T04:14:45+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev10"
__status__ = "Development"


TOPIC = "runner-lifecycle"
PRODUCER = "producer"
MSG_TYPE = "payload"
RUNNER_WAIT_SECONDS = 1.0


class _DeliveryFailure(Exception):
    pass


class _ControllableFrameConnection(FrameConnection):
    def __init__(self, connection: FrameConnection) -> None:
        self._connection = connection
        self._closed = threading.Event()
        self._send_error: Exception | None = None

    def fail_sends(self, error: Exception) -> None:
        self._send_error = error

    def wait_until_closed(self) -> bool:
        return self._closed.wait(RUNNER_WAIT_SECONDS)

    def is_closed(self) -> bool:
        return self._closed.is_set()

    def send_frame_parts(self, parts: FrameParts) -> None:
        if self._send_error is not None:
            raise self._send_error
        self._connection.send_frame_parts(parts)

    def receive_frame_parts(self) -> FrameParts:
        return self._connection.receive_frame_parts()

    def receive_frame_parts_nowait(self) -> FrameParts | None:
        return self._connection.receive_frame_parts_nowait()

    def close(self) -> None:
        self._closed.set()
        self._connection.close()


@dataclasses.dataclass(frozen=True)
class _RunnerTransport:
    bus: DirectMessageBus
    client: TransportClient
    runner: BrokerTransportSessionRunner
    connection: _ControllableFrameConnection


def _make_runner_transport() -> _RunnerTransport:
    bus = DirectMessageBus(capture_sink=InMemoryCaptureSink())
    client_socket, broker_socket = socket.socketpair()
    client_connection = SocketFrameConnection(client_socket)
    broker_connection = _ControllableFrameConnection(
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
    transport = _RunnerTransport(
        bus=bus,
        client=client,
        runner=runner,
        connection=broker_connection,
    )
    return transport


def test_runner_closes_connection_after_peer_close() -> None:
    transport = _make_runner_transport()
    try:
        transport.client.close()
        transport.runner.join(RUNNER_WAIT_SECONDS)
        assert not transport.runner.is_alive()
        assert transport.connection.is_closed()
    finally:
        transport.runner.request_stop()


def test_runner_reports_delivery_worker_failure() -> None:
    transport = _make_runner_transport()
    try:
        transport.client.subscribe(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
        )
        emitter = transport.bus.register_emitter(
            msg_topic=TOPIC,
            msg_producer=PRODUCER,
            msg_type=MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        transport.connection.fail_sends(_DeliveryFailure("failed to send"))

        emitter.emit(b"trigger attempted delivery")
        assert transport.connection.wait_until_closed()

        with pytest.raises(TransportSessionFailedError) as failure:
            transport.runner.join(RUNNER_WAIT_SECONDS)
        assert isinstance(failure.value.__cause__, _DeliveryFailure)
    finally:
        transport.client.close()
        transport.runner.request_stop()
