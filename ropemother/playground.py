#!/usr/bin/env python3
# ropemother/playground.py

"""A preliminary demonstration/verification of module features."""

import asyncio
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from typing import Any

from ropemother.bootstrap.policy import (
    BootstrapMessageRejectedError,
    LifecycleMessageType,
)
from ropemother.broker.asyncdirect import AsyncDirectMessageBus
from ropemother.broker.direct import DirectMessageBus
from ropemother.broker.directcore import (
    BrokerDeliveryTarget,
    CaptureMode,
    DirectBrokerCore,
)
from ropemother.broker.dispatch import (
    run_async_receiver_batch,
    run_receiver_batch,
)
from ropemother.broker.endpoints import (
    UnlistedMessageTypeError,
    UnsupportedTypeFormatError,
)
from ropemother.capture.filehistory import JSONLinesCaptureHistory
from ropemother.capture.filesink import JSONLinesCaptureSink
from ropemother.capture.history import InMemoryCaptureHistory
from ropemother.capture.historyservice import (
    AsyncHistoryClient,
    AsyncHistoryService,
    HistoryClient,
    HistoryService,
)
from ropemother.capture.memorysink import InMemoryCaptureSink
from ropemother.capture.runtime import history_for
from ropemother.capture.sink import CaptureSink
from ropemother.capture.writer import RegistrationRecord
from ropemother.client.asyncrequest import (
    AsyncRequestClient,
    AsyncRequestService,
    AsyncRequester,
    AsyncResponder,
)
from ropemother.client.asyncendpointprovisioner import (
    ImmediateAsyncEndpointProvisioner,
)
from ropemother.client.procedure import (
    PROCEDURE_INVOCATION_JSON_FORMAT,
    ProcedureInvocation,
)
from ropemother.client.request import Requester, Responder
from ropemother.exceptions import (
    CaptureDisabledError,
    CaptureUnavailableError,
    MissingMessageTypeError,
    PayloadSerializationError,
)
from ropemother.format.defaults import default_portable_format_registry
from ropemother.format.formattable import PortableFormatTable
from ropemother.format.portableformat import (
    COMPOSITE_PORTABLE_FORMAT,
    JSON_PORTABLE_FORMAT,
    PortableFormat,
    PortableFormatKey,
    RAW_BYTES_PORTABLE_FORMAT,
)
from ropemother.format.registry import (
    PortableFormatID,
    PortableFormatRegistration,
    PortableFormatRegistry,
)
from ropemother.fixtures.scriptedinput import (
    ScriptedInputEmitter,
    ScriptedInputPlan,
)
from ropemother.message.messageidentity import CorrelationID
from ropemother.message.records import (
    BusMessage,
    BusOperation,
    CapturedMessage,
    ReceivedMessage,
    SerializedPayload,
)
from ropemother.message.selectors import topic_tree
from ropemother.message.symbols import (
    InvalidMessageSymbolError,
    MessageSymbolKind,
    MessageSymbolRegistration,
    MessageTypeID,
    ProducerID,
    ReservedMessageSymbolError,
    TopicID,
)
from ropemother.service.brokerhistory import (
    BrokerHistoryExtension,
    preconfigured_history_client,
)
from ropemother.service.connector import connect_transport_client
from ropemother.service.environment import (
    connect_async_message_bus,
    connect_client_from_bus_contact,
    bus_contact_variables,
    set_bus_contact_uri,
)
from ropemother.service.host import LocalMessageBusHost
from ropemother.service.asyncservice import AsyncMessageBusService
from ropemother.service.service import MessageBusService
from ropemother.service.socketlistener import (
    AsyncLocalBusServiceListener,
    LocalBusServiceListener,
)
from ropemother.transport.asyncclient import AsyncTransportClient
from ropemother.transport.asyncconnection import (
    AsyncFrameChannel,
    AsyncFrameConnection,
    AsyncMemoryFrameConnection,
)
from ropemother.transport.asyncsession import AsyncBrokerTransportSession
from ropemother.transport.client import TransportClient, TransportRequestError
from ropemother.transport.codec import decode_frame, encode_frame
from ropemother.transport.connection import (
    FrameChannel,
    FrameConnection,
    MemoryFrameConnection,
)
from ropemother.transport.frames import (
    DeliveryFrame,
    EmitFrame,
    EmitResultFrame,
    RegisterEmitterFrame,
    RegisterEmitterResultFrame,
    SubscribeFrame,
    SubscribeResultFrame,
    TransportSubscriptionID,
)
from ropemother.transport.session import BrokerTransportSession
from ropemother.transport.sessionrunner import BrokerTransportSessionRunner
from ropemother.transport.socketconnection import SocketFrameConnection
from ropemother.transport.zeromq.connection import ZMQFrameConnection
from ropemother.util.onelinejson import oneline_deserialize, oneline_serialize
from ropemother.util.serializer import IDENTITY_SERIALIZER, IdentityAdapter

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T18:58:53+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


DEMO_TOPIC = "topics.foo.bar"
DEMO_PRODUCER = "producer-baz"
DEMO_MSG_TYPE = "event-qux"
DEMO_ALT_MSG_TYPE = "event-quux"
DEMO_RESERVED_TOPIC = "bus.foo"
DEMO_LIFECYCLE_TOPIC = "lifecycle.foo"
DEMO_LIFECYCLE_TYPE = LifecycleMessageType.STARTED.value

DEMO_CUSTOM_BYTES_FORMAT = PortableFormat[bytes, bytes](
    key=PortableFormatKey.from_str("demo-custom-bytes"),
    adapter=IdentityAdapter[bytes](),
    serializer=IDENTITY_SERIALIZER,
)

CapturedRecord = (
    CapturedMessage
    | MessageSymbolRegistration
    | PortableFormatRegistration
)


def _record_type_names(records: list[CapturedRecord]) -> list[str]:
    return [type(record).__name__ for record in records]


def _registration_kinds(
    records: list[CapturedRecord]
) -> list[MessageSymbolKind]:
    kinds = [
        record.symbol_kind
        for record in records
        if isinstance(record, MessageSymbolRegistration)
    ]
    return kinds


def demo_basic_publish_subscribe() -> None:
    print("Demo: basic publish/subscribe delivery")
    # Parameterize classes under test later
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    # Make sure to support receiving multiple/all types in future interfaces
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    canonical_payload = "foo bar"

    emitter.emit(canonical_payload)
    received_message = receiver.receive()

    payload_success = received_message.payload == canonical_payload
    topic_success = received_message.msg_topic == DEMO_TOPIC
    producer_success = received_message.msg_producer == DEMO_PRODUCER
    type_success = received_message.msg_type == DEMO_MSG_TYPE
    success = (
        payload_success and topic_success and producer_success and type_success
    )

    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_message.payload" + eq_string + "canonical_payload")
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Message was delivered to a matching receiver")
    else:
        print("Message delivery did not match the expected receiver view")
    print("\n")


def demo_capture_order() -> None:
    print("Demo: symbol registrations precede compact message capture")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)

    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )

    emitter.emit({"foo": "bar"})
    receiver.receive()
    observed_record_types = _record_type_names(sink.records)
    expected_record_types = [
        "MessageSymbolRegistration",
        "MessageSymbolRegistration",
        "MessageSymbolRegistration",
        "PortableFormatRegistration",
        "CapturedMessage",
    ]
    observed_registration_kinds = _registration_kinds(sink.records)
    expected_registration_kinds = [
        MessageSymbolKind.TOPIC,
        MessageSymbolKind.PRODUCER,
        MessageSymbolKind.MSG_TYPE,
    ]
    print(f"{observed_record_types=}")
    print(f"{expected_record_types=}")
    print(f"{observed_registration_kinds=}")
    print(f"{expected_registration_kinds=}")

    order_success = observed_record_types == expected_record_types
    eq_string = "=="
    if not order_success:
        eq_string = "!="
    print(f"observed_record_types" + eq_string + "expected_record_types")
    print(f"({type(sink).__name__}): ", end="")
    if order_success:
        print("Sink observed expected message types")
    else:
        print("Sink did not receive expected message types")

    kind_success = observed_registration_kinds == expected_registration_kinds
    eq_string = "=="
    if not kind_success:
        eq_string = "!="
    print(
        "observed_registration_kinds"
        + eq_string
        + "expected_registration_kinds"
    )
    print(f"({type(sink).__name__}): ", end="")
    if kind_success:
        print("Sink observed expected symbol kinds")
    else:
        print("Sink did not receive expected symbol kinds")

    success = order_success and kind_success
    print(f"({type(sink).__name__}): ", end="")
    if success:
        print("Capture stream can interpret compact message symbols")
    else:
        print("Capture stream ordering or symbol kinds were unexpected")
    print("\n")


def demo_late_capture_sink_replays_registered_symbols() -> None:
    print("Demo: late capture sink receives existing registrations")
    bus = DirectMessageBus()
    bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)

    observed_record_types = _record_type_names(sink.records)
    expected_record_types = [
        "MessageSymbolRegistration",
        "MessageSymbolRegistration",
        "MessageSymbolRegistration",
        "PortableFormatRegistration",
    ]
    observed_registration_kinds = _registration_kinds(sink.records)
    expected_registration_kinds = [
        MessageSymbolKind.TOPIC,
        MessageSymbolKind.PRODUCER,
        MessageSymbolKind.MSG_TYPE,
    ]
    print(f"{observed_record_types=}")
    print(f"{expected_record_types=}")
    print(f"{observed_registration_kinds=}")
    print(f"{expected_registration_kinds=}")

    replay_success = observed_record_types == expected_record_types
    eq_string = "=="
    if not replay_success:
        eq_string = "!="
    print("observed_record_types" + eq_string + "expected_record_types")
    print(f"({type(bus).__name__}): ", end="")
    if replay_success:
        print("Late capture sink received existing symbol records")
    else:
        print("Late capture sink did not receive expected symbol records")

    kind_success = observed_registration_kinds == expected_registration_kinds
    eq_string = "=="
    if not kind_success:
        eq_string = "!="
    print(
        "observed_registration_kinds"
        + eq_string
        + "expected_registration_kinds"
    )
    print(f"({type(bus).__name__}): ", end="")
    if kind_success:
        print("Late capture sink received expected symbol kinds")
    else:
        print("Late capture sink did not receive expected symbol kinds")

    success = replay_success and kind_success
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Existing registrations were replayed into the capture sink")
    else:
        print("Existing registration replay was incomplete or unexpected")
    print("\n")


def demo_emit_time_message_type_override() -> None:
    print("Demo: emitter can publish with an explicit message type")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_ALT_MSG_TYPE,
    )
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        additional_msg_types=(DEMO_ALT_MSG_TYPE,),
    )

    emitter.emit({"foo": "bar"}, msg_type=DEMO_ALT_MSG_TYPE)
    received_message = receiver.receive()
    canonical_msg_type = DEMO_ALT_MSG_TYPE
    received_msg_type = received_message.msg_type
    captured_record_types = _record_type_names(sink.records)

    captured_messages = [
        (index, record)
        for index, record in enumerate(sink.records)
        if isinstance(record, CapturedMessage)
    ]
    captured_message_index = None
    captured_message = None
    if captured_messages:
        captured_message_index, captured_message = captured_messages[0]

    records_before_message = []
    if captured_message_index is not None:
        records_before_message = sink.records[:captured_message_index]

    msg_type_registrations = [
        record
        for record in records_before_message
        if isinstance(record, MessageSymbolRegistration)
        and record.symbol_kind is MessageSymbolKind.MSG_TYPE
        and record.symbol == DEMO_ALT_MSG_TYPE
    ]
    registered_msg_type_id = None
    if msg_type_registrations:
        registered_msg_type_id = msg_type_registrations[0].symbol_id

    captured_msg_type_id = None
    if captured_message is not None:
        captured_msg_type_id = captured_message.msg_type_id

    print(f"{canonical_msg_type=}")
    print(f"{received_msg_type=}")
    print(f"{captured_record_types=}")
    print(f"{captured_message_index=}")
    print(f"{registered_msg_type_id=}")
    print(f"{captured_msg_type_id=}")

    type_success = received_msg_type == canonical_msg_type
    eq_string = "=="
    if not type_success:
        eq_string = "!="
    print("received_msg_type " + eq_string + " canonical_msg_type")
    print(f"({type(receiver).__name__}): ", end="")
    if type_success:
        print("Receiver received the explicit message type")
    else:
        print("Receiver did not receive the explicit message type")

    registration_success = (
        registered_msg_type_id is not None
        and captured_msg_type_id == registered_msg_type_id
    )
    eq_string = "=="
    if not registration_success:
        eq_string = "!="
    print("captured_msg_type_id " + eq_string + " registered_msg_type_id")
    print(f"({type(sink).__name__}): ", end="")
    if registration_success:
        print("Sink captured explicit message type before compact message")
    else:
        print("Sink did not capture explicit message type before use")

    success = type_success and registration_success
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Emitter override resolved and captured correctly")
    else:
        print("Emitter override behavior was incomplete or unexpected")
    print("\n")


def demo_invalid_explicit_message_type_is_rejected() -> None:
    print("Demo: invalid explicit message type is rejected")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )

    try:
        emitter.emit({"corge": "grault"}, msg_type="")
        empty_type_rejected = False
    except InvalidMessageSymbolError:
        empty_type_rejected = True
    print(f"{empty_type_rejected=}")

    try:
        emitter.emit({"corge": "grault"}, msg_type="type.garply")
        dotted_type_rejected = False
    except InvalidMessageSymbolError:
        dotted_type_rejected = True
    print(f"{dotted_type_rejected=}")

    success = empty_type_rejected and dotted_type_rejected
    print(f"({type(emitter).__name__}.emit): ", end="")
    if success:
        print("Invalid explicit message types were rejected")
    else:
        print("Invalid explicit message types were accepted")
    print("\n")


def demo_reserved_topic_root_rejected() -> None:
    print("Demo: ordinary emitters cannot use reserved topic roots")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)

    try:
        bus.register_emitter(
            msg_topic=DEMO_RESERVED_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        reserved_root_rejected = False
    except ReservedMessageSymbolError:
        reserved_root_rejected = True
    print(f"{DEMO_RESERVED_TOPIC=}")
    print(f"{reserved_root_rejected=}")

    eq_string = "=="
    if not reserved_root_rejected:
        eq_string = "!="
    print("reserved_root_rejected" + eq_string + "True")
    print(f"({type(bus).__name__}): ", end="")
    if reserved_root_rejected:
        print("Ordinary emitter registration rejected reserved topic root")
    else:
        print("Ordinary emitter registration accepted reserved topic root")
    print("\n")


def demo_capture_sink_required_before_delivery() -> None:
    print("Demo: bootstrap rejects ordinary messages before capture sink")
    bus = DirectMessageBus()
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )

    try:
        emitter.emit({"corge": "grault"})
        ordinary_message_rejected = False
    except BootstrapMessageRejectedError:
        ordinary_message_rejected = True
    print(f"{ordinary_message_rejected=}")

    eq_string = "=="
    if not ordinary_message_rejected:
        eq_string = "!="
    print("ordinary_message_rejected" + eq_string + "True")
    print(f"({type(bus).__name__}): ", end="")
    if ordinary_message_rejected:
        print("Ordinary delivery was rejected while capture was bootstrapping")
    else:
        print("Ordinary delivery improperly began before capture was ready")
    print("\n")


def demo_failed_serialization_delivers_nothing() -> None:
    print("Demo: messages are not delivered when serialization fails")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    not_bytes = {"foo": "bar"}
    valid_bytes = b"baz bleep"

    record_count_before = len(sink.records)
    try:
        emitter.emit(not_bytes)
        serialization_rejected = False
    except PayloadSerializationError:
        serialization_rejected = True
    print(f"{serialization_rejected=}")

    record_count_after = len(sink.records)
    emitter.emit(valid_bytes)
    received_message = receiver.receive()

    no_capture_written = record_count_after == record_count_before
    print(f"{no_capture_written=}")
    eq_string = "=="
    if not no_capture_written:
        eq_string = "!="
    print("record_count_after" + eq_string + "record_count_before")

    no_bad_delivery = received_message.payload == valid_bytes
    print(f"{no_bad_delivery=}")
    eq_string = "=="
    if not no_bad_delivery:
        eq_string = "!="
    print("received_message.payload" + eq_string + "valid_bytes")

    print(f"({type(bus).__name__}): ", end="")
    if serialization_rejected and no_capture_written and no_bad_delivery:
        print("Failed serialization was rejected before capture and delivery")
    else:
        print("Failed serialization behavior was incomplete or unexpected")
    print("\n")


def demo_batch_receiver_handler() -> None:
    print("Demo: messages can be received in batches with handler")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    canonical_records = [
        {"foo": "bar"},
        {"baz": "qux"},
        {"quux": "corge"},
    ]
    num_records = len(canonical_records)
    observed_messages = []

    print(f"{canonical_records=}")
    for record in canonical_records:
        emitter.emit(record)
    run_receiver_batch(
        receiver,
        observed_messages.append,
        min_count=num_records,
        max_count=num_records,
    )
    observed_records = [message.payload for message in observed_messages]
    print(f"{observed_records=}")

    success = observed_records == canonical_records
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("observed_records " + eq_string + " canonical_records")
    print(f"({type(receiver).__name__}): ", end="")
    if success:
        print("Batch receiver helper preserved payload contents and order")
    else:
        print("Batch receiver failed to preserve payload contents/order")
    print("\n")


def demo_receive_nowait() -> None:
    print("Demo: receiver can check for a message without blocking")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    canonical_payload = "foo bar"

    empty_message = receiver.receive_nowait()
    print(f"{empty_message=}")

    print(f"{canonical_payload=}")
    emitter.emit(canonical_payload)
    observed_message = receiver.receive_nowait()
    observed_payload = None
    if observed_message is not None:
        observed_payload = observed_message.payload
    print(f"{observed_payload=}")

    empty_success = empty_message is None
    payload_success = observed_payload == canonical_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("observed_payload" + eq_string + "canonical_payload")
    print(f"({type(receiver).__name__}): ", end="")
    if empty_success and payload_success:
        print("Non-blocking receive reported absence and preserved payload")
    else:
        print("Non-blocking receive behavior was incomplete or unexpected")
    print("\n")


async def demo_async_receive_waits_for_message() -> None:
    print("Demo: coroutines advance while async receiver waits for message")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    canonical_payload = "foo bar"

    receive_task = asyncio.create_task(receiver.receive())
    await asyncio.sleep(0)

    observed_task_done_before_emit = receive_task.done()
    expected_task_done_before_emit = False
    observed_record_types_before_emit = _record_type_names(sink.records)
    expected_record_types_before_emit = [
        "MessageSymbolRegistration",
        "MessageSymbolRegistration",
        "MessageSymbolRegistration",
        "PortableFormatRegistration",
    ]

    print(f"{observed_task_done_before_emit=}")
    print(f"{expected_task_done_before_emit=}")
    print(f"{observed_record_types_before_emit=}")
    print(f"{expected_record_types_before_emit=}")

    await emitter.emit(canonical_payload)

    observed_record_types_after_emit = _record_type_names(sink.records)
    expected_record_types_after_emit = [
        "MessageSymbolRegistration",
        "MessageSymbolRegistration",
        "MessageSymbolRegistration",
        "PortableFormatRegistration",
        "CapturedMessage",
    ]
    received_message = await receive_task
    observed_payload = received_message.payload

    print(f"{observed_record_types_after_emit=}")
    print(f"{expected_record_types_after_emit=}")
    print(f"{observed_payload=}")
    print(f"{canonical_payload=}")

    task_success = (
        observed_task_done_before_emit == expected_task_done_before_emit
    )
    eq_string = "=="
    if not task_success:
        eq_string = "!="
    print(
        "observed_task_done_before_emit "
        + eq_string
        + " expected_task_done_before_emit"
    )

    before_emit_success = (
        observed_record_types_before_emit == expected_record_types_before_emit
    )
    eq_string = "=="
    if not before_emit_success:
        eq_string = "!="
    print(
        "observed_record_types_before_emit "
        + eq_string
        + " expected_record_types_before_emit"
    )

    after_emit_success = (
        observed_record_types_after_emit == expected_record_types_after_emit
    )
    eq_string = "=="
    if not after_emit_success:
        eq_string = "!="
    print(
        "observed_record_types_after_emit "
        + eq_string
        + " expected_record_types_after_emit"
    )

    payload_success = observed_payload == canonical_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("observed_payload" + eq_string + "canonical_payload")

    success = (
        task_success
        and before_emit_success
        and after_emit_success
        and payload_success
    )

    print(f"({type(receiver).__name__}): ", end="")
    if success:
        print("Receive task waited until captured message delivery")
    else:
        print("Async receive behavior was incomplete or unexpected")
    print("\n")


async def record_payload(
    observed_records: list[Any], message: ReceivedMessage
) -> None:
    observed_records.append(message.payload)


async def demo_async_batch_receiver_handler() -> None:
    print("Demo: async messages can be received in batches with handler")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    canonical_records = [
        {"foo": "bar"},
        {"baz": "qux"},
        {"quux": "corge"},
    ]
    canonical_count = len(canonical_records)
    observed_records = []
    handler = partial(record_payload, observed_records)

    for record in canonical_records:
        await emitter.emit(record)
    observed_count = await run_async_receiver_batch(
        receiver,
        handler,
        min_count=canonical_count,
        max_count=canonical_count,
    )
    print(f"{canonical_records=}")
    print(f"{observed_records=}")
    print(f"{canonical_count=}")
    print(f"{observed_count=}")

    records_success = observed_records == canonical_records
    eq_string = "=="
    if not records_success:
        eq_string = "!="
    print("observed_records " + eq_string + " canonical_records")

    count_success = observed_count == canonical_count
    eq_string = "=="
    if not count_success:
        eq_string = "!="
    print("observed_count " + eq_string + " canonical_count")

    print(f"({type(receiver).__name__}): ", end="")
    if records_success and count_success:
        print("Async batch receiver helper preserved records and count")
    else:
        print("Async batch receiver helper behavior was incomplete")
    print("\n")


def demo_register_emitter_frame_codec() -> None:
    print("Demo: register-emitter frame preserves setup request")
    sent_frame = RegisterEmitterFrame(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        format_key=RAW_BYTES_PORTABLE_FORMAT.key,
    )

    received_frame = decode_frame(encode_frame(sent_frame))
    canonical_request = (
        sent_frame.msg_topic,
        sent_frame.msg_producer,
        sent_frame.msg_type,
        sent_frame.format_key,
    )
    observed_request = (
        received_frame.msg_topic,
        received_frame.msg_producer,
        received_frame.msg_type,
        received_frame.format_key,
    )

    print(f"{canonical_request=}")
    print(f"{observed_request=}")

    success = observed_request == canonical_request
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("observed_request" + eq_string + "canonical_request")
    print(f"({RegisterEmitterFrame.__name__}): ", end="")
    if success:
        print("Readable setup request survived codec round-trip")
    else:
        print("Readable setup request did not survive codec round-trip")
    print("\n")


def demo_emit_frame_codec() -> None:
    print("Demo: emit frame preserves payload")
    canonical_bytes = b"foo bar baz"
    sent_frame = EmitFrame(
        msg_topic_id=TopicID(0),
        msg_producer_id=ProducerID(0),
        msg_type_id=MessageTypeID(0),
        msg_format_id=PortableFormatID(0),
        payload_bytes=canonical_bytes,
    )

    received_frame = decode_frame(encode_frame(sent_frame))
    observed_bytes = received_frame.payload_bytes
    print(f"{canonical_bytes=}")
    print(f"{observed_bytes=}")

    success = observed_bytes == canonical_bytes
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("observed_bytes" + eq_string + "canonical_bytes")
    print(f"({type(sent_frame).__name__}): ", end="")
    if success:
        print("Payload survived codec round-trip")
    else:
        print("Payload did not survive codec round-trip")
    print("\n")


def demo_broker_transport_session_registers_emitter() -> None:
    print("Demo: broker transport session registers emitter")
    endpoint_connection, broker_connection = MemoryFrameConnection.make_pair()
    endpoint_channel = FrameChannel(endpoint_connection)
    broker_channel = FrameChannel(broker_connection)
    session = BrokerTransportSession(
        channel=broker_channel,
        core=DirectBrokerCore(),
    )
    request_frame = RegisterEmitterFrame(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        format_key=RAW_BYTES_PORTABLE_FORMAT.key,
    )

    endpoint_channel.send_frame(request_frame)
    session.handle_next_frame()
    received_frame = endpoint_channel.receive_frame()
    canonical_frame_type = RegisterEmitterResultFrame.__name__
    received_frame_type = type(received_frame).__name__
    print(f"{canonical_frame_type=}")
    print(f"{received_frame_type=}")

    success = isinstance(received_frame, RegisterEmitterResultFrame)
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_frame_type " + eq_string + " canonical_frame_type")
    print(f"({type(session).__name__}): ", end="")
    if success:
        print("Emitter registration result frame received")
    else:
        print("Emitter registration result frame was not received")
    print("\n")


def demo_broker_transport_session_subscribes() -> None:
    print("Demo: broker transport session subscribes")
    endpoint_connection, broker_connection = MemoryFrameConnection.make_pair()
    endpoint_channel = FrameChannel(endpoint_connection)
    broker_channel = FrameChannel(broker_connection)
    session = BrokerTransportSession(
        channel=broker_channel,
        core=DirectBrokerCore(),
    )
    request_frame = SubscribeFrame(
        msg_topic=(topic_tree(DEMO_TOPIC),),
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )

    endpoint_channel.send_frame(request_frame)
    session.handle_next_frame()
    received_frame = endpoint_channel.receive_frame()
    canonical_frame_type = SubscribeResultFrame.__name__
    received_frame_type = type(received_frame).__name__
    # How can we obtain this without constructing our own copy?
    canonical_subscription_id = TransportSubscriptionID(0)
    received_subscription_id = None
    if isinstance(received_frame, SubscribeResultFrame):
        received_subscription_id = received_frame.subscription_id

    print(f"{canonical_frame_type=}")
    print(f"{received_frame_type=}")
    print(f"{canonical_subscription_id=}")
    print(f"{received_subscription_id=}")

    type_success = isinstance(received_frame, SubscribeResultFrame)
    eq_string = "=="
    if not type_success:
        eq_string = "!="
    print("received_frame_type " + eq_string + " canonical_frame_type")

    id_success = received_subscription_id == canonical_subscription_id
    eq_string = "=="
    if not id_success:
        eq_string = "!="
    print(
        "received_subscription_id " + eq_string + " canonical_subscription_id"
    )

    success = type_success and id_success
    print(f"({type(session).__name__}): ", end="")
    if success:
        print("Subscription result frame received")
    else:
        print("Subscription result frame was not received")
    print("\n")


# Needs to be rewritten - using the core directly is a bad example to readers
def _make_transport_session() -> tuple[FrameChannel, BrokerTransportSession]:
    endpoint_connection, broker_connection = MemoryFrameConnection.make_pair()
    endpoint_channel = FrameChannel(endpoint_connection)
    broker_channel = FrameChannel(broker_connection)
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    session = bus.create_transport_session(channel=broker_channel)
    return endpoint_channel, session


def demo_broker_transport_session_emits_payload() -> None:
    print("Demo: broker transport session accepts emitted payload")
    endpoint_channel, session = _make_transport_session()
    register_frame = RegisterEmitterFrame(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        format_key=RAW_BYTES_PORTABLE_FORMAT.key,
    )
    endpoint_channel.send_frame(register_frame)
    session.handle_next_frame()
    register_result = endpoint_channel.receive_frame()
    if not isinstance(register_result, RegisterEmitterResultFrame):
        raise TypeError("register emitter did not return a result frame")
    if register_result.msg_type_id is None:
        raise MissingMessageTypeError(
            "demo emitter registration did not produce a message type ID"
        )
    subscribe_frame = SubscribeFrame(
        msg_topic=(topic_tree(DEMO_TOPIC),),
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    endpoint_channel.send_frame(subscribe_frame)
    session.handle_next_frame()
    subscribe_result = endpoint_channel.receive_frame()
    if not isinstance(subscribe_result, SubscribeResultFrame):
        raise TypeError("subscribe did not return a result frame")
    canonical_bytes = b"foo bar baz"
    emit_frame = EmitFrame(
        msg_topic_id=register_result.msg_topic_id,
        msg_producer_id=register_result.msg_producer_id,
        msg_type_id=register_result.msg_type_id,
        msg_format_id=register_result.msg_format_id,
        payload_bytes=canonical_bytes,
    )
    endpoint_channel.send_frame(emit_frame)
    session.handle_next_frame()

    received_frame = endpoint_channel.receive_frame()
    received_bytes = None
    if isinstance(received_frame, DeliveryFrame):
        received_bytes = received_frame.payload_bytes
    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")

    success = received_bytes == canonical_bytes
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")
    print(f"({type(session).__name__}): ", end="")
    if success:
        print("Transport emit delivered payload to matching subscriber")
    else:
        print("Transport emit did not deliver payload to matching subscriber")
    print("\n")


def _handle_transport_session_frames(
    session: BrokerTransportSession, frame_count: int
) -> None:
    for _ in range(frame_count):
        session.handle_next_frame()


def _service_transport_session(
    session: BrokerTransportSession, frame_count: int
) -> Thread:
    worker = Thread(
        target=_handle_transport_session_frames, args=(session, frame_count)
    )
    worker.start()
    return worker


def _make_transport_endpoint(
    *,
    bus: DirectMessageBus,
    format_registry: PortableFormatTable,
    connections: tuple[FrameConnection, FrameConnection],
) -> tuple[TransportClient, BrokerTransportSession]:
    endpoint_connection, broker_connection = connections
    endpoint_channel = FrameChannel(endpoint_connection)
    broker_channel = FrameChannel(broker_connection)
    client = TransportClient(
        channel=endpoint_channel, extra_formats=format_registry.formats()
    )
    session = bus.create_transport_session(channel=broker_channel)
    return client, session


# Needs to be rewritten - using the core directly is a bad example to readers
def demo_transport_client_receives_from_another_endpoint() -> None:
    print("Demo: transport client receives from another endpoint")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    worker = _service_transport_session(producer_session, 1)
    emitter = producer_client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    worker.join()
    worker = _service_transport_session(subscriber_session, 1)
    receiver = subscriber_client.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    worker.join()

    canonical_bytes = b"foo bar baz"
    worker = _service_transport_session(producer_session, 1)
    emitter.emit(canonical_bytes)
    worker.join()
    received_message = receiver.receive()
    received_bytes = received_message.payload
    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    print(f"received_topic={received_message.msg_topic!r}")
    print(f"received_type={received_message.msg_type!r}")
    print(f"received_producer={received_message.msg_producer!r}")

    payload_success = received_bytes == canonical_bytes
    symbols_success = (
        received_message.msg_topic == DEMO_TOPIC
        and received_message.msg_type == DEMO_MSG_TYPE
        and received_message.msg_producer == DEMO_PRODUCER
    )
    success = payload_success and symbols_success

    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    print(f"({type(subscriber_client).__name__}): ", end="")
    if success:
        print("Endpoint receiver decoded payload and readable symbols")
    else:
        print("Endpoint receiver did not reconstruct the expected message")
    print("\n")


def demo_socket_transport_client_receives_from_another_endpoint() -> None:
    print("Demo: socket transport client receives from another endpoint")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=SocketFrameConnection.make_pair(),
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=SocketFrameConnection.make_pair(),
    )

    worker = _service_transport_session(producer_session, 1)
    emitter = producer_client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    worker.join()

    worker = _service_transport_session(subscriber_session, 1)
    receiver = subscriber_client.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    worker.join()

    canonical_bytes = b"foo bar baz"
    worker = _service_transport_session(producer_session, 1)
    emitter.emit(canonical_bytes)
    worker.join()

    received_message = receiver.receive()
    received_bytes = received_message.payload
    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    print(f"received_topic={received_message.msg_topic!r}")
    print(f"received_type={received_message.msg_type!r}")
    print(f"received_producer={received_message.msg_producer!r}")

    payload_success = received_bytes == canonical_bytes
    symbols_success = (
        received_message.msg_topic == DEMO_TOPIC
        and received_message.msg_type == DEMO_MSG_TYPE
        and received_message.msg_producer == DEMO_PRODUCER
    )
    success = payload_success and symbols_success

    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    print(f"({type(subscriber_client).__name__}): ", end="")
    if success:
        print("Socket endpoint receiver decoded payload and readable symbols")
    else:
        print(
            "Socket endpoint receiver did not reconstruct the expected message"
        )
    print("\n")


def demo_socket_transport_client_uses_session_runners() -> None:
    print("Demo: socket transport client uses session runners")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_connections = SocketFrameConnection.make_pair()
    subscriber_connections = SocketFrameConnection.make_pair()
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=producer_connections,
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=subscriber_connections,
    )

    producer_endpoint_connection, producer_broker_connection = (
        producer_connections
    )
    subscriber_endpoint_connection, subscriber_broker_connection = (
        subscriber_connections
    )
    producer_runner = BrokerTransportSessionRunner(
        session=producer_session,
        close_connection=producer_broker_connection.close,
    )
    subscriber_runner = BrokerTransportSessionRunner(
        session=subscriber_session,
        close_connection=subscriber_broker_connection.close,
    )
    producer_runner.start()
    subscriber_runner.start()

    emitter = producer_client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    receiver = subscriber_client.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )

    canonical_bytes = b"foo bar baz"
    emitter.emit(canonical_bytes)
    received_message = receiver.receive()
    received_bytes = received_message.payload
    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    print(f"received_topic={received_message.msg_topic!r}")
    print(f"received_type={received_message.msg_type!r}")
    print(f"received_producer={received_message.msg_producer!r}")

    payload_success = received_bytes == canonical_bytes
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    producer_runner.request_stop()
    subscriber_runner.request_stop()
    producer_runner.join()
    subscriber_runner.join()
    producer_endpoint_connection.close()
    subscriber_endpoint_connection.close()

    symbols_success = (
        received_message.msg_topic == DEMO_TOPIC
        and received_message.msg_type == DEMO_MSG_TYPE
        and received_message.msg_producer == DEMO_PRODUCER
    )
    success = payload_success and symbols_success
    print(f"({type(subscriber_client).__name__}): ", end="")
    if success:
        print(
            "Runner-backed socket receiver decoded payload and readable "
            "symbols"
        )
    else:
        print(
            "Runner-backed socket receiver did not reconstruct the expected "
            "message"
        )
    print("\n")


def demo_zmq_transport_client_uses_session_runners() -> None:
    print("Demo: ZMQ transport client uses session runners")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_connections = ZMQFrameConnection.make_pair(
        second_receive_timeout_ms=25
    )
    subscriber_connections = ZMQFrameConnection.make_pair(
        second_receive_timeout_ms=25
    )
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=producer_connections,
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=subscriber_connections,
    )
    producer_endpoint_connection, producer_broker_connection = (
        producer_connections
    )
    subscriber_endpoint_connection, subscriber_broker_connection = (
        subscriber_connections
    )
    producer_runner = BrokerTransportSessionRunner(
        session=producer_session,
        close_connection=producer_broker_connection.close,
    )
    subscriber_runner = BrokerTransportSessionRunner(
        session=subscriber_session,
        close_connection=subscriber_broker_connection.close,
    )
    producer_runner.start()
    subscriber_runner.start()
    emitter = producer_client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    receiver = subscriber_client.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )

    canonical_bytes = b"foo bar baz"
    emitter.emit(canonical_bytes)
    received_message = receiver.receive()
    received_bytes = received_message.payload
    received_topic = received_message.msg_topic
    received_type = received_message.msg_type
    received_producer = received_message.msg_producer

    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    print(f"received_topic={received_topic!r}")
    print(f"received_type={received_type!r}")
    print(f"received_producer={received_producer!r}")

    payload_success = received_bytes == canonical_bytes
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    producer_runner.request_stop()
    subscriber_runner.request_stop()
    producer_runner.join()
    subscriber_runner.join()
    producer_endpoint_connection.close()
    subscriber_endpoint_connection.close()

    symbols_success = (
        received_topic == DEMO_TOPIC
        and received_type == DEMO_MSG_TYPE
        and received_producer == DEMO_PRODUCER
    )
    success = payload_success and symbols_success
    print(f"({TransportClient.__name__}): ", end="")
    if success:
        print("ZMQ endpoint receiver decoded payload and readable symbols")
    else:
        print("ZMQ endpoint receiver did not reconstruct the expected message")
    print("\n")


def demo_zmq_transport_preserves_message_identity() -> None:
    print("Demo: ZMQ transport preserves message identity")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_connections = ZMQFrameConnection.make_pair(
        second_receive_timeout_ms=25
    )
    subscriber_connections = ZMQFrameConnection.make_pair(
        second_receive_timeout_ms=25
    )

    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=producer_connections,
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=subscriber_connections,
    )
    producer_endpoint_connection, producer_broker_connection = (
        producer_connections
    )
    subscriber_endpoint_connection, subscriber_broker_connection = (
        subscriber_connections
    )

    producer_runner = BrokerTransportSessionRunner(
        session=producer_session,
        close_connection=producer_broker_connection.close,
    )
    subscriber_runner = BrokerTransportSessionRunner(
        session=subscriber_session,
        close_connection=subscriber_broker_connection.close,
    )
    producer_runner.start()
    subscriber_runner.start()
    emitter = producer_client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    receiver = subscriber_client.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )

    emitter.emit(b"foo bar baz")
    received_message = receiver.receive()
    captured_message = None
    for record in sink.records:
        if isinstance(record, CapturedMessage):
            if captured_message is not None:
                raise RuntimeError("expected exactly one captured message")
            captured_message = record
    if captured_message is None:
        raise RuntimeError("demo expected one captured message")

    captured_msg_id = captured_message.msg_id
    received_msg_id = received_message.msg_id
    print(f"{captured_msg_id=}")
    print(f"{received_msg_id=}")
    success = received_msg_id == captured_msg_id
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_msg_id " + eq_string + " captured_msg_id")

    producer_runner.request_stop()
    subscriber_runner.request_stop()
    producer_runner.join()
    subscriber_runner.join()
    producer_endpoint_connection.close()
    subscriber_endpoint_connection.close()

    print(f"({type(subscriber_endpoint_connection).__name__}): ", end="")
    if success:
        print("Transport receiver preserved broker message identity")
    else:
        print("Transport receiver did not preserve broker message identity")
    print("\n")


def demo_direct_request_reply() -> None:
    print("Demo: direct request/reply")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer="requester-baz",
        msg_type="query-qux",
    )
    reply_receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer="responder-baz",
        msg_type="result-quux",
    )
    request_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer="requester-baz",
        msg_type="query-qux",
    )
    reply_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer="responder-baz",
        msg_type="result-quux",
    )
    canonical_request = {"source": "foo", "target": "bar"}
    canonical_reply = {"path": ["foo", "baz", "bar"]}
    correlation_id = CorrelationID(0)

    request_emitter.emit_request(
        canonical_request,
        correlation_id=correlation_id,
    )
    received_request = request_receiver.receive()
    reply_emitter.emit_reply(received_request, canonical_reply)
    received_reply = reply_receiver.receive()
    received_request_payload = received_request.payload

    print(f"{canonical_request=}")
    print(f"{received_request_payload=}")
    request_success = received_request_payload == canonical_request
    eq_string = "=="
    if not request_success:
        eq_string = "!="
    print("received_request_payload " + eq_string + " canonical_request")

    received_reply_payload = received_reply.payload
    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    request_msg_id = received_request.msg_id
    reply_to_msg_id = received_reply.reply_to
    reply_target_success = reply_to_msg_id == request_msg_id
    eq_string = "=="
    if not reply_target_success:
        eq_string = "!="
    print("reply_to_msg_id " + eq_string + " request_msg_id")

    request_correlation_id = received_request.correlation_id
    reply_correlation_id = received_reply.correlation_id
    correlation_success = reply_correlation_id == request_correlation_id
    eq_string = "=="
    if not correlation_success:
        eq_string = "!="
    print("reply_correlation_id " + eq_string + " request_correlation_id")

    request_operation = received_request.bus_operation
    reply_operation = received_reply.bus_operation
    operation_success = (
        request_operation == BusOperation.REQUEST
        and reply_operation == BusOperation.REPLY
    )
    success = (
        request_success
        and reply_success
        and reply_target_success
        and correlation_success
        and operation_success
    )
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Direct reply matched the received request")
    else:
        print("Direct request/reply behavior was incomplete or unexpected")
    print("\n")


def demo_client_request_reply() -> None:
    print("Demo: client request/reply helper")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-requester",
        msg_type="demo-query",
    )
    reply_receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-responder",
        msg_type="demo-result",
    )
    request_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-requester",
        msg_type="demo-query",
    )
    reply_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-responder",
        msg_type="demo-result",
    )
    requester = Requester(request_emitter, reply_receiver)
    responder = Responder(reply_emitter)
    canonical_request = {"foo": "bar"}
    canonical_reply = {"plugh": "xyzzy"}

    requester.request(canonical_request)
    received_request = request_receiver.receive()
    responder.reply(received_request, canonical_reply)
    received_reply = reply_receiver.receive()
    received_request_payload = received_request.payload

    print(f"{canonical_request=}")
    print(f"{received_request_payload=}")
    request_success = received_request_payload == canonical_request
    eq_string = "=="
    if not request_success:
        eq_string = "!="
    print("received_request_payload " + eq_string + " canonical_request")

    received_reply_payload = received_reply.payload
    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    request_operation = received_request.bus_operation
    reply_operation = received_reply.bus_operation
    operation_success = (
        request_operation == BusOperation.REQUEST
        and reply_operation == BusOperation.REPLY
    )
    reply_target_success = received_reply.reply_to == received_request.msg_id
    correlation_success = (
        received_reply.correlation_id == received_request.correlation_id
    )
    success = (
        request_success
        and reply_success
        and operation_success
        and reply_target_success
        and correlation_success
    )
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Client helper completed direct request/reply")
    else:
        print("Client helper request/reply behavior was incomplete")
    print("\n")


def demo_client_request_reply_handle_matching() -> None:
    print("Demo: client request/reply helper matches handles")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-requester",
        msg_type="demo-query",
    )
    reply_receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-responder",
        msg_type="demo-result",
    )
    request_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-requester",
        msg_type="demo-query",
    )
    reply_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-responder",
        msg_type="demo-result",
    )
    requester = Requester(request_emitter, reply_receiver)
    responder = Responder(reply_emitter)
    canonical_first_request = {"foo": "bar"}
    canonical_second_request = {"corge": "grault"}
    canonical_first_reply = {"baz": "quz"}
    canonical_second_reply = {"garply": "waldo"}
    first_handle = requester.request(canonical_first_request)
    second_handle = requester.request(canonical_second_request)
    received_first_request = request_receiver.receive()
    received_second_request = request_receiver.receive()
    responder.reply(received_second_request, canonical_second_reply)
    responder.reply(received_first_request, canonical_first_reply)
    first_reply = requester.receive_reply(first_handle)
    second_reply = requester.receive_reply(second_handle)
    received_first_reply_payload = first_reply.payload
    received_second_reply_payload = second_reply.payload

    print(f"{canonical_first_reply=}")
    print(f"{received_first_reply_payload=}")
    first_success = received_first_reply_payload == canonical_first_reply
    eq_string = "=="
    if not first_success:
        eq_string = "!="
    print(
        "received_first_reply_payload "
        + eq_string
        + " canonical_first_reply"
    )

    print(f"{canonical_second_reply=}")
    print(f"{received_second_reply_payload=}")
    second_success = received_second_reply_payload == canonical_second_reply
    eq_string = "=="
    if not second_success:
        eq_string = "!="
    print(
        "received_second_reply_payload "
        + eq_string
        + " canonical_second_reply"
    )

    first_metadata_success = (
        first_reply.bus_operation == BusOperation.REPLY
        and first_reply.reply_to == received_first_request.msg_id
        and first_reply.correlation_id == received_first_request.correlation_id
    )
    second_reply_id = second_reply.correlation_id
    received_second_reply_id = received_second_request.correlation_id
    second_metadata_success = (
        second_reply.bus_operation == BusOperation.REPLY
        and second_reply.reply_to == received_second_request.msg_id
        and second_reply_id == received_second_reply_id
    )
    success = (
        first_success
        and second_success
        and first_metadata_success
        and second_metadata_success
    )
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Client helper matched replies to request handles")
    else:
        print("Client helper handle matching was incomplete")
    print("\n")


def demo_direct_request_reply_facade() -> None:
    print("Demo: direct request/reply facade matches handles")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_name = "query-corge"
    reply_name = "result-grault"
    requester = bus.create_requester(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )
    responder = bus.create_responder(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )

    canonical_first_request = {"foo": "bar"}
    canonical_second_request = {"fred": "plugh"}
    canonical_first_reply = {"qux": "quux"}
    canonical_second_reply = {"garply": "waldo"}
    first_handle = requester.request(canonical_first_request)
    second_handle = requester.request(canonical_second_request)
    received_first_request = responder.receive()
    received_second_request = responder.receive()
    responder.reply(received_second_request, canonical_second_reply)
    responder.reply(received_first_request, canonical_first_reply)
    first_reply = requester.receive_reply(first_handle)
    second_reply = requester.receive_reply(second_handle)
    received_first_reply = first_reply.payload
    received_second_reply = second_reply.payload

    print(f"{canonical_first_reply=}")
    print(f"{received_first_reply=}")
    first_success = received_first_reply == canonical_first_reply
    eq_string = "=="
    if not first_success:
        eq_string = "!="
    print("received_first_reply " + eq_string + " canonical_first_reply")

    print(f"{canonical_second_reply=}")
    print(f"{received_second_reply=}")
    second_success = received_second_reply == canonical_second_reply
    eq_string = "=="
    if not second_success:
        eq_string = "!="
    print("received_second_reply " + eq_string + " canonical_second_reply")

    first_metadata_success = (
        first_reply.bus_operation == BusOperation.REPLY
        and first_reply.reply_to == received_first_request.msg_id
        and first_reply.correlation_id == received_first_request.correlation_id
    )
    second_correlation_id = second_reply.correlation_id
    received_second_correlation_id = received_second_request.correlation_id
    second_metadata_success = (
        second_reply.bus_operation == BusOperation.REPLY
        and second_reply.reply_to == received_second_request.msg_id
        and second_correlation_id == received_second_correlation_id
    )
    success = (
        first_success
        and second_success
        and first_metadata_success
        and second_metadata_success
    )
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Direct facade matched replies to request handles")
    else:
        print("Direct facade handle matching was incomplete")
    print("\n")


def demo_transport_request_reply_facade() -> None:
    print("Demo: transport request/reply facade matches handles")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(JSON_PORTABLE_FORMAT)
    requester_client, requester_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    responder_client, responder_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_name = "query-corge"
    reply_name = "result-grault"
    worker = _service_transport_session(requester_session, 2)
    requester = requester_client.create_requester(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )
    worker.join()
    worker = _service_transport_session(responder_session, 2)
    responder = responder_client.create_responder(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )
    worker.join()

    canonical_first_request = {"foo": "bar"}
    canonical_second_request = {"fred": "plugh"}
    canonical_first_reply = {"qux": "quux"}
    canonical_second_reply = {"garply": "waldo"}
    worker = _service_transport_session(requester_session, 2)
    first_handle = requester.request(canonical_first_request)
    second_handle = requester.request(canonical_second_request)
    worker.join()
    received_first_request = responder.receive()
    received_second_request = responder.receive()
    worker = _service_transport_session(responder_session, 2)
    responder.reply(received_second_request, canonical_second_reply)
    responder.reply(received_first_request, canonical_first_reply)
    worker.join()
    first_reply = requester.receive_reply(first_handle)
    second_reply = requester.receive_reply(second_handle)
    received_first_reply = first_reply.payload
    received_second_reply = second_reply.payload

    print(f"{canonical_first_reply=}")
    print(f"{received_first_reply=}")
    first_success = received_first_reply == canonical_first_reply
    eq_string = "=="
    if not first_success:
        eq_string = "!="
    print("received_first_reply " + eq_string + " canonical_first_reply")

    print(f"{canonical_second_reply=}")
    print(f"{received_second_reply=}")
    second_success = received_second_reply == canonical_second_reply
    eq_string = "=="
    if not second_success:
        eq_string = "!="
    print("received_second_reply " + eq_string + " canonical_second_reply")

    first_metadata_success = (
        first_reply.bus_operation == BusOperation.REPLY
        and first_reply.reply_to == received_first_request.msg_id
        and first_reply.correlation_id == received_first_request.correlation_id
    )
    second_reply_id = second_reply.correlation_id
    received_second_reply_id = received_second_request.correlation_id
    second_metadata_success = (
        second_reply.bus_operation == BusOperation.REPLY
        and second_reply.reply_to == received_second_request.msg_id
        and second_reply_id == received_second_reply_id
    )
    success = (
        first_success
        and second_success
        and first_metadata_success
        and second_metadata_success
    )
    print(f"({type(requester_client).__name__}): ", end="")
    if success:
        print("Transport facade matched replies to request handles")
    else:
        print("Transport facade handle matching was incomplete")
    print("\n")


def demo_request_client_service_facade() -> None:
    print("Demo: request client/service facade")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_name = "query-corge"
    reply_name = "result-grault"

    client = bus.create_request_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )
    service = bus.create_request_service(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )

    canonical_request = {"foo": "bar"}
    canonical_reply = {"baz": "qux"}

    handle = client.send(canonical_request)
    request = service.receive()
    received_request_payload = request.payload
    request.reply(canonical_reply)
    reply = client.receive(handle)
    received_reply_payload = reply.payload

    print(f"{canonical_request=}")
    print(f"{received_request_payload=}")
    request_success = received_request_payload == canonical_request
    eq_string = "=="
    if not request_success:
        eq_string = "!="
    print("received_request_payload " + eq_string + " canonical_request")

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    success = request_success and reply_success
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Request client/service facade completed request/reply")
    else:
        print("Request client/service facade was incomplete")
    print("\n")


def _unary_procedure_handler(payload: str) -> str:
    return "handled " + payload


def demo_procedure_facade() -> None:
    print("Demo: procedure facade")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_name = "query-corge"
    reply_name = "result-grault"
    client = bus.create_procedure_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )
    service = bus.create_procedure_service(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
        handler=_unary_procedure_handler,
    )

    canonical_request = "foo"
    canonical_reply = _unary_procedure_handler(canonical_request)
    worker = Thread(target=service.handle)
    worker.start()
    received_reply_payload = client(canonical_request)
    worker.join()

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Procedure facade completed request/reply")
    else:
        print("Procedure facade was incomplete")
    print("\n")


async def demo_async_direct_request_reply() -> None:
    print("Demo: async direct request/reply")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer="requester-baz",
        msg_type="query-qux",
    )
    reply_receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer="responder-baz",
        msg_type="result-quux",
    )
    request_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer="requester-baz",
        msg_type="query-qux",
    )
    reply_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer="responder-baz",
        msg_type="result-quux",
    )
    canonical_request = {"source": "foo", "target": "bar"}
    canonical_reply = {"path": ["foo", "baz", "bar"]}
    correlation_id = CorrelationID(0)

    await request_emitter.emit_request(
        canonical_request,
        correlation_id=correlation_id,
    )
    received_request = await request_receiver.receive()
    await reply_emitter.emit_reply(received_request, canonical_reply)
    received_reply = await reply_receiver.receive()
    received_request_payload = received_request.payload

    print(f"{canonical_request=}")
    print(f"{received_request_payload=}")
    request_success = received_request_payload == canonical_request
    eq_string = "=="
    if not request_success:
        eq_string = "!="
    print("received_request_payload " + eq_string + " canonical_request")

    received_reply_payload = received_reply.payload
    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    request_msg_id = received_request.msg_id
    reply_to_msg_id = received_reply.reply_to
    reply_target_success = reply_to_msg_id == request_msg_id
    eq_string = "=="
    if not reply_target_success:
        eq_string = "!="
    print("reply_to_msg_id " + eq_string + " request_msg_id")

    request_correlation_id = received_request.correlation_id
    reply_correlation_id = received_reply.correlation_id
    correlation_success = reply_correlation_id == request_correlation_id
    eq_string = "=="
    if not correlation_success:
        eq_string = "!="
    print("reply_correlation_id " + eq_string + " request_correlation_id")

    request_operation = received_request.bus_operation
    reply_operation = received_reply.bus_operation
    operation_success = (
        request_operation == BusOperation.REQUEST
        and reply_operation == BusOperation.REPLY
    )
    success = (
        request_success
        and reply_success
        and reply_target_success
        and correlation_success
        and operation_success
    )
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Async direct reply matched the received request")
    else:
        print("Async direct request/reply behavior was incomplete")
    print("\n")


async def demo_async_client_request_reply_handle_matching() -> None:
    print("Demo: async client request/reply helper matches handles")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-requester",
        msg_type="demo-query",
    )
    reply_receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-responder",
        msg_type="demo-result",
    )
    request_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-requester",
        msg_type="demo-query",
    )
    reply_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer="demo-responder",
        msg_type="demo-result",
    )
    requester = AsyncRequester(request_emitter, reply_receiver)
    responder = AsyncResponder(reply_emitter)
    canonical_first_request = {"foo": "bar"}
    canonical_second_request = {"corge": "grault"}
    canonical_first_reply = {"baz": "quz"}
    canonical_second_reply = {"garply": "waldo"}

    first_handle = await requester.request(canonical_first_request)
    second_handle = await requester.request(canonical_second_request)
    received_first_request = await request_receiver.receive()
    received_second_request = await request_receiver.receive()
    await responder.reply(received_second_request, canonical_second_reply)
    await responder.reply(received_first_request, canonical_first_reply)
    first_reply = await requester.receive_reply(first_handle)
    second_reply = await requester.receive_reply(second_handle)
    received_first_reply_payload = first_reply.payload
    received_second_reply_payload = second_reply.payload

    print(f"{canonical_first_reply=}")
    print(f"{received_first_reply_payload=}")
    first_success = received_first_reply_payload == canonical_first_reply
    eq_string = "=="
    if not first_success:
        eq_string = "!="
    print(
        "received_first_reply_payload " + eq_string + " canonical_first_reply"
    )

    print(f"{canonical_second_reply=}")
    print(f"{received_second_reply_payload=}")
    second_success = received_second_reply_payload == canonical_second_reply
    eq_string = "=="
    if not second_success:
        eq_string = "!="
    print(
        "received_second_reply_payload "
        + eq_string
        + " canonical_second_reply"
    )

    first_metadata_success = (
        first_reply.bus_operation == BusOperation.REPLY
        and first_reply.reply_to == received_first_request.msg_id
        and first_reply.correlation_id == received_first_request.correlation_id
    )
    second_reply_id = second_reply.correlation_id
    received_second_reply_id = received_second_request.correlation_id
    second_metadata_success = (
        second_reply.bus_operation == BusOperation.REPLY
        and second_reply.reply_to == received_second_request.msg_id
        and second_reply_id == received_second_reply_id
    )
    success = (
        first_success
        and second_success
        and first_metadata_success
        and second_metadata_success
    )
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Async client helper matched replies to request handles")
    else:
        print("Async client helper handle matching was incomplete")
    print("\n")


async def demo_async_request_client_service_facade() -> None:
    print("Demo: async request client/service facade")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_name = "query-corge"
    reply_name = "result-grault"
    client = bus.create_request_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )
    service = bus.create_request_service(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )

    canonical_request = {"foo": "bar"}
    canonical_reply = {"baz": "qux"}
    handle = await client.send(canonical_request)
    request = await service.receive()
    received_request_payload = request.payload
    await request.reply(canonical_reply)
    reply = await client.receive(handle)
    received_reply_payload = reply.payload

    print(f"{canonical_request=}")
    print(f"{received_request_payload=}")
    request_success = received_request_payload == canonical_request
    eq_string = "=="
    if not request_success:
        eq_string = "!="
    print("received_request_payload " + eq_string + " canonical_request")

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    success = request_success and reply_success
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Async request client/service facade completed request/reply")
    else:
        print("Async request client/service facade was incomplete")
    print("\n")


async def demo_async_procedure_facade() -> None:
    print("Demo: async procedure facade")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_name = "query-corge"
    reply_name = "result-grault"
    client = bus.create_procedure_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )
    service = bus.create_procedure_service(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
        handler=_unary_procedure_handler,
    )

    canonical_request = "foo"
    canonical_reply = "handled foo"
    service_task = asyncio.create_task(service.handle())
    received_reply_payload = await client(canonical_request)
    await service_task

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Async procedure facade completed request/reply")
    else:
        print("Async procedure facade was incomplete")
    print("\n")


def demo_request_service_receive_nowait() -> None:
    print("Demo: request service receive_nowait")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )
    service = bus.create_request_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )

    canonical_request = {"foo": "bar"}
    canonical_reply = {"baz": "qux"}
    empty_request = service.receive_nowait()
    handle = client.send(canonical_request)
    request = service.receive_nowait()
    received_request_payload = None
    if request is not None:
        received_request_payload = request.payload
        request.reply(canonical_reply)
    reply = client.receive(handle)
    received_reply_payload = reply.payload

    print(f"{empty_request=}")
    print(f"{canonical_request=}")
    print(f"{received_request_payload=}")
    request_success = received_request_payload == canonical_request
    eq_string = "=="
    if not request_success:
        eq_string = "!="
    print("received_request_payload " + eq_string + " canonical_request")

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    success = empty_request is None and request_success and reply_success
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("receive_nowait reported absence and then received a request")
    else:
        print("receive_nowait behavior was incomplete")
    print("\n")


def demo_request_service_receive_available() -> None:
    print("Demo: request service receive_available")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )
    service = bus.create_request_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )

    canonical_requests = [{"foo": "bar"}, {"baz": "qux"}]
    canonical_replies = [{"quux": "corge"}, {"grault": "garply"}]
    handles = []
    for payload in canonical_requests:
        handle = client.send(payload)
        handles.append(handle)
    requests = service.receive_available()
    received_request_payloads = []
    for request, reply_payload in zip(requests, canonical_replies):
        received_request_payloads.append(request.payload)
        request.reply(reply_payload)
    received_reply_payloads = []
    for handle in handles:
        reply = client.receive(handle)
        received_reply_payloads.append(reply.payload)

    print(f"{canonical_requests=}")
    print(f"{received_request_payloads=}")
    request_success = received_request_payloads == canonical_requests
    eq_string = "=="
    if not request_success:
        eq_string = "!="
    print("received_request_payloads " + eq_string + " canonical_requests")

    print(f"{canonical_replies=}")
    print(f"{received_reply_payloads=}")
    reply_success = received_reply_payloads == canonical_replies
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payloads " + eq_string + " canonical_replies")

    success = request_success and reply_success
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("receive_available drained queued requests")
    else:
        print("receive_available behavior was incomplete")
    print("\n")


def demo_request_service_receive_many() -> None:
    print("Demo: request service receive_many")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )
    service = bus.create_request_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )
    canonical_payloads = ["foo", "bar"]

    for payload in canonical_payloads:
        client.send(payload)
    requests = service.receive_many(2)
    received_payloads = []
    for request in requests:
        received_payloads.append(request.payload)

    print(f"{canonical_payloads=}")
    print(f"{received_payloads=}")
    success = received_payloads == canonical_payloads
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_payloads " + eq_string + " canonical_payloads")

    print(f"({type(service).__name__}): ", end="")
    if success:
        print("receive_many received up to the requested maximum")
    else:
        print("receive_many behavior was incomplete")
    print("\n")


def demo_procedure_service_handle_nowait() -> None:
    print("Demo: procedure service handle_nowait")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        request_payload_format=PROCEDURE_INVOCATION_JSON_FORMAT,
    )
    service = bus.create_procedure_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        handler=_unary_procedure_handler,
    )

    canonical_request = "foo"
    canonical_reply = _unary_procedure_handler(canonical_request)
    empty_result = service.handle_nowait()
    procedure_invocation = ProcedureInvocation.from_call(canonical_request)
    request_handle = client.send(procedure_invocation)
    service_result = service.handle_nowait()
    reply = client.receive(request_handle)
    received_reply_payload = reply.payload

    print(f"{empty_result=}")
    print(f"{service_result=}")
    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    success = (
        reply_success and empty_result is False and service_result is True
    )
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("handle_nowait returned status and sent a reply")
    else:
        print("handle_nowait behavior was incomplete")
    print("\n")


def demo_procedure_service_handle_available() -> None:
    print("Demo: procedure service handle_available")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        request_payload_format=PROCEDURE_INVOCATION_JSON_FORMAT,
    )
    service = bus.create_procedure_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        handler=_unary_procedure_handler,
    )

    canonical_payloads = ["foo", "bar"]
    canonical_replies = []
    request_handles = []
    for payload in canonical_payloads:
        canonical_replies.append(_unary_procedure_handler(payload))
        procedure_invocation = ProcedureInvocation.from_call(payload)
        request_handle = client.send(procedure_invocation)
        request_handles.append(request_handle)
    service_count = service.handle_available()
    received_reply_payloads = []
    for request_handle in request_handles:
        reply = client.receive(request_handle)
        received_reply_payloads.append(reply.payload)

    print(f"{service_count=}")
    print(f"{canonical_replies=}")
    print(f"{received_reply_payloads=}")
    reply_success = received_reply_payloads == canonical_replies
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payloads " + eq_string + " canonical_replies")

    success = reply_success and service_count == len(canonical_replies)
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("handle_available returned count and sent replies")
    else:
        print("handle_available behavior was incomplete")
    print("\n")


def demo_procedure_service_handle_many() -> None:
    print("Demo: procedure service handle_many")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        request_payload_format=PROCEDURE_INVOCATION_JSON_FORMAT,
    )
    service = bus.create_procedure_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        handler=_unary_procedure_handler,
    )

    canonical_payloads = ["foo", "bar"]
    canonical_replies = []
    request_handles = []
    for payload in canonical_payloads:
        canonical_replies.append(_unary_procedure_handler(payload))
        procedure_invocation = ProcedureInvocation.from_call(payload)
        request_handle = client.send(procedure_invocation)
        request_handles.append(request_handle)
    service_count = service.handle_many(2)
    received_reply_payloads = []
    for request_handle in request_handles:
        reply = client.receive(request_handle)
        received_reply_payloads.append(reply.payload)

    print(f"{service_count=}")
    print(f"{canonical_replies=}")
    print(f"{received_reply_payloads=}")
    reply_success = received_reply_payloads == canonical_replies
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payloads " + eq_string + " canonical_replies")

    success = reply_success and service_count == len(canonical_replies)
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("handle_many returned count and sent replies")
    else:
        print("handle_many behavior was incomplete")
    print("\n")


async def demo_async_request_service_receive_nowait() -> None:
    print("Demo: async request service receive_nowait")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )
    service = bus.create_request_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )

    canonical_request = "foo"
    canonical_reply = "bar"
    empty_request = service.receive_nowait()
    handle = await client.send(canonical_request)
    request = service.receive_nowait()
    received_request_payload = None
    if request is not None:
        received_request_payload = request.payload
        await request.reply(canonical_reply)
    reply = await client.receive(handle)
    received_reply_payload = reply.payload

    print(f"{empty_request=}")
    print(f"{canonical_request=}")
    print(f"{received_request_payload=}")
    request_success = received_request_payload == canonical_request
    eq_string = "=="
    if not request_success:
        eq_string = "!="
    print("received_request_payload " + eq_string + " canonical_request")

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    success = request_success and reply_success and empty_request is None
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("receive_nowait reported absence and then received a request")
    else:
        print("receive_nowait behavior was incomplete")
    print("\n")


async def demo_async_request_service_receive_available() -> None:
    print("Demo: async request service receive_available")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )
    service = bus.create_request_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )

    canonical_payloads = ["foo", "bar"]
    canonical_replies = ["baz", "qux"]
    handles = []
    for payload in canonical_payloads:
        handle = await client.send(payload)
        handles.append(handle)
    requests = service.receive_available()
    received_payloads = []
    for request, reply_payload in zip(requests, canonical_replies):
        received_payloads.append(request.payload)
        await request.reply(reply_payload)
    received_replies = []
    for handle in handles:
        reply = await client.receive(handle)
        received_replies.append(reply.payload)

    print(f"{canonical_payloads=}")
    print(f"{received_payloads=}")
    payload_success = received_payloads == canonical_payloads
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payloads " + eq_string + " canonical_payloads")

    print(f"{canonical_replies=}")
    print(f"{received_replies=}")
    reply_success = received_replies == canonical_replies
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_replies " + eq_string + " canonical_replies")

    success = payload_success and reply_success
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("receive_available received available requests")
    else:
        print("receive_available behavior was incomplete")
    print("\n")


async def demo_async_request_service_receive_many() -> None:
    print("Demo: async request service receive_many")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )
    service = bus.create_request_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
    )

    canonical_payloads = ["foo", "bar"]
    for payload in canonical_payloads:
        await client.send(payload)
    requests = await service.receive_many(2)
    received_payloads = []
    for request in requests:
        received_payloads.append(request.payload)

    print(f"{canonical_payloads=}")
    print(f"{received_payloads=}")
    success = received_payloads == canonical_payloads
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_payloads " + eq_string + " canonical_payloads")

    print(f"({type(service).__name__}): ", end="")
    if success:
        print("receive_many received multiple requests")
    else:
        print("receive_many behavior was incomplete")
    print("\n")


async def demo_async_procedure_service_handle_nowait() -> None:
    print("Demo: async procedure service handle_nowait")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        request_payload_format=PROCEDURE_INVOCATION_JSON_FORMAT,
    )
    service = bus.create_procedure_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        handler=_unary_procedure_handler,
    )

    canonical_request = "foo"
    canonical_reply = _unary_procedure_handler(canonical_request)
    empty_result = await service.handle_nowait()
    procedure_invocation = ProcedureInvocation.from_call(canonical_request)
    request_handle = await client.send(procedure_invocation)
    service_result = await service.handle_nowait()
    reply = await client.receive(request_handle)
    received_reply_payload = reply.payload

    print(f"{empty_result=}")
    print(f"{service_result=}")
    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    success = (
        reply_success and empty_result is False and service_result is True
    )
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("handle_nowait returned status and sent a reply")
    else:
        print("handle_nowait behavior was incomplete")
    print("\n")


async def demo_async_procedure_service_handle_available() -> None:
    print("Demo: async procedure service handle_available")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        request_payload_format=PROCEDURE_INVOCATION_JSON_FORMAT,
    )
    service = bus.create_procedure_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        handler=_unary_procedure_handler,
    )

    canonical_payloads = ["foo", "bar"]
    canonical_replies = []
    request_handles = []
    for payload in canonical_payloads:
        canonical_replies.append(_unary_procedure_handler(payload))
        procedure_invocation = ProcedureInvocation.from_call(payload)
        request_handle = await client.send(procedure_invocation)
        request_handles.append(request_handle)
    service_count = await service.handle_available()
    received_reply_payloads = []
    for request_handle in request_handles:
        reply = await client.receive(request_handle)
        received_reply_payloads.append(reply.payload)

    print(f"{service_count=}")
    print(f"{canonical_replies=}")
    print(f"{received_reply_payloads=}")
    reply_success = received_reply_payloads == canonical_replies
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payloads " + eq_string + " canonical_replies")

    success = reply_success and service_count == len(canonical_replies)
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("handle_available returned count and sent replies")
    else:
        print("handle_available behavior was incomplete")
    print("\n")


async def demo_async_procedure_service_handle_many() -> None:
    print("Demo: async procedure service handle_many")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    request_topic = "foo.bar"
    reply_topic = "foo.bar"
    requester_producer = "baz"
    responder_producer = "qux"
    request_msg_type = "quux"
    reply_msg_type = "corge"
    client = bus.create_request_client(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        request_payload_format=PROCEDURE_INVOCATION_JSON_FORMAT,
    )
    service = bus.create_procedure_service(
        request_topic=request_topic,
        reply_topic=reply_topic,
        requester_producer=requester_producer,
        responder_producer=responder_producer,
        request_msg_type=request_msg_type,
        reply_msg_type=reply_msg_type,
        handler=_unary_procedure_handler,
    )

    canonical_payloads = ["foo", "bar"]
    canonical_replies = []
    request_handles = []
    for payload in canonical_payloads:
        canonical_replies.append(_unary_procedure_handler(payload))
        procedure_invocation = ProcedureInvocation.from_call(payload)
        request_handle = await client.send(procedure_invocation)
        request_handles.append(request_handle)
    service_count = await service.handle_many(2)
    received_reply_payloads = []
    for request_handle in request_handles:
        reply = await client.receive(request_handle)
        received_reply_payloads.append(reply.payload)

    print(f"{service_count=}")
    print(f"{canonical_replies=}")
    print(f"{received_reply_payloads=}")
    reply_success = received_reply_payloads == canonical_replies
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payloads " + eq_string + " canonical_replies")

    success = reply_success and service_count == len(canonical_replies)
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("handle_many returned count and sent replies")
    else:
        print("handle_many behavior was incomplete")
    print("\n")


def demo_transport_procedure_facade() -> None:
    print("Demo: transport procedure facade")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(
        JSON_PORTABLE_FORMAT, PROCEDURE_INVOCATION_JSON_FORMAT
    )
    requester_client, requester_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    responder_client, responder_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_name = "query-corge"
    reply_name = "result-grault"
    worker = _service_transport_session(requester_session, 2)
    client = requester_client.create_procedure_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )
    worker.join()
    worker = _service_transport_session(responder_session, 2)
    service = responder_client.create_procedure_service(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
        handler=_unary_procedure_handler,
    )
    worker.join()

    canonical_request = "foo"
    canonical_reply = _unary_procedure_handler(canonical_request)
    service_worker = Thread(target=service.handle)
    requester_worker = _service_transport_session(requester_session, 1)
    responder_worker = _service_transport_session(responder_session, 1)
    service_worker.start()
    received_reply_payload = client(canonical_request)
    service_worker.join()
    requester_worker.join()
    responder_worker.join()

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    print(f"({type(requester_client).__name__}): ", end="")
    if success:
        print("Transport procedure facade completed request/reply")
    else:
        print("Transport procedure facade was incomplete")
    print("\n")


def demo_socket_transport_procedure_facade() -> None:
    print("Demo: socket transport procedure facade")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(
        JSON_PORTABLE_FORMAT, PROCEDURE_INVOCATION_JSON_FORMAT
    )
    requester_connections = SocketFrameConnection.make_pair()
    responder_connections = SocketFrameConnection.make_pair()
    requester_client, requester_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=requester_connections,
    )
    responder_client, responder_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=responder_connections,
    )
    requester_endpoint_connection, requester_broker_connection = (
        requester_connections
    )
    responder_endpoint_connection, responder_broker_connection = (
        responder_connections
    )
    requester_runner = BrokerTransportSessionRunner(
        session=requester_session,
        close_connection=requester_broker_connection.close,
    )
    responder_runner = BrokerTransportSessionRunner(
        session=responder_session,
        close_connection=responder_broker_connection.close,
    )
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_name = "query-corge"
    reply_name = "result-grault"
    requester_runner.start()
    responder_runner.start()
    client = requester_client.create_procedure_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )
    service = responder_client.create_procedure_service(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
        handler=_unary_procedure_handler,
    )

    canonical_request = "foo"
    canonical_reply = _unary_procedure_handler(canonical_request)
    service_worker = Thread(target=service.handle)
    service_worker.start()
    received_reply_payload = client(canonical_request)
    service_worker.join()

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    requester_runner.request_stop()
    responder_runner.request_stop()
    requester_runner.join()
    responder_runner.join()
    requester_endpoint_connection.close()
    responder_endpoint_connection.close()

    print(f"({type(requester_endpoint_connection).__name__}): ", end="")
    if success:
        print("Socket transport procedure facade completed request/reply")
    else:
        print("Socket transport procedure facade was incomplete")
    print("\n")


def demo_transport_failed_serialization_delivers_nothing() -> None:
    print("Demo: transport messages are not sent when serialization fails")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    worker = _service_transport_session(subscriber_session, 1)
    receiver = subscriber_client.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    worker.join()
    worker = _service_transport_session(producer_session, 1)
    emitter = producer_client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    worker.join()

    incompatible_payload = {"foo": "bar"}
    canonical_error_type = PayloadSerializationError.__name__
    canonical_empty_message = None
    record_count_before = len(sink.records)
    try:
        emitter.emit(incompatible_payload)
        caught_error_type = None
    except PayloadSerializationError as error:
        caught_error_type = type(error).__name__
    record_count_after = len(sink.records)
    received_empty_message = receiver.receive_nowait()
    canonical_bytes = b"baz bleep"
    worker = _service_transport_session(producer_session, 1)
    emitter.emit(canonical_bytes)
    worker.join()
    received_message = receiver.receive()
    received_bytes = received_message.payload

    print(f"{canonical_error_type=}")
    print(f"{caught_error_type=}")
    error_success = caught_error_type == canonical_error_type
    eq_string = "=="
    if not error_success:
        eq_string = "!="
    print("caught_error_type " + eq_string + " canonical_error_type")

    print(f"{record_count_before=}")
    print(f"{record_count_after=}")
    capture_success = record_count_after == record_count_before
    eq_string = "=="
    if not capture_success:
        eq_string = "!="
    print("record_count_after " + eq_string + " record_count_before")

    print(f"{canonical_empty_message=}")
    print(f"{received_empty_message=}")
    queue_success = received_empty_message == canonical_empty_message
    eq_string = "=="
    if not queue_success:
        eq_string = "!="
    print("received_empty_message " + eq_string + " canonical_empty_message")

    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    payload_success = received_bytes == canonical_bytes
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    success = (
        error_success and capture_success and queue_success and payload_success
    )
    print(f"({type(producer_client).__name__}): ", end="")
    if success:
        print("Transport serialization failed before capture and delivery")
    else:
        print("Transport serialization failure behavior was incomplete")
    print("\n")


def demo_transport_only_mode_delivers_without_capture() -> None:
    print("Demo: transport-only bus delivers without capture")
    bus = DirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )

    canonical_payload = {"foo": "bar"}
    emitter.emit(canonical_payload)
    received_message = receiver.receive()
    received_payload = received_message.payload
    sink = InMemoryCaptureSink()
    canonical_sink_result = CaptureDisabledError.__name__
    received_sink_result = "accepted"
    try:
        bus.set_capture_sink(sink)
    except CaptureDisabledError as e:
        received_sink_result = type(e).__name__

    print(f"{canonical_payload=}")
    print(f"{received_payload=}")
    print(f"{canonical_sink_result=}")
    print(f"{received_sink_result=}")
    delivery_success = received_payload == canonical_payload
    eq_string = "=="
    if not delivery_success:
        eq_string = "!="
    print("received_payload " + eq_string + " canonical_payload")

    sink_success = received_sink_result == canonical_sink_result
    eq_string = "=="
    if not sink_success:
        eq_string = "!="
    print("received_sink_result " + eq_string + " canonical_sink_result")

    success = delivery_success and sink_success
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Transport-only mode delivered while leaving capture disabled")
    else:
        print("Transport-only mode did not match capture/delivery policy")
    print("\n")


def demo_transport_only_client_routes_without_capture() -> None:
    print("Demo: transport-only bus routes transport client messages")
    bus = DirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    worker = _service_transport_session(producer_session, 1)
    emitter = producer_client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    worker.join()
    worker = _service_transport_session(subscriber_session, 1)
    receiver = subscriber_client.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    worker.join()

    canonical_bytes = b"foo bar baz"
    worker = _service_transport_session(producer_session, 1)
    emitter.emit(canonical_bytes)
    worker.join()
    received_message = receiver.receive()
    received_bytes = received_message.payload
    canonical_symbols = (DEMO_TOPIC, DEMO_MSG_TYPE, DEMO_PRODUCER)
    received_symbols = (
        received_message.msg_topic,
        received_message.msg_type,
        received_message.msg_producer,
    )

    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    print(f"{canonical_symbols=}")
    print(f"{received_symbols=}")
    payload_success = received_bytes == canonical_bytes
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    symbols_success = received_symbols == canonical_symbols
    eq_string = "=="
    if not symbols_success:
        eq_string = "!="
    print("received_symbols " + eq_string + " canonical_symbols")

    success = payload_success and symbols_success
    print(f"({type(subscriber_client).__name__}): ", end="")
    if success:
        print("Transport-only broker routed transport client message")
    else:
        print("Transport-only broker did not route the expected message")
    print("\n")


async def demo_async_frame_channel_waits_for_frame() -> None:
    print("Demo: async frame channel waits for frame")
    endpoint_connection, broker_connection = (
        AsyncMemoryFrameConnection.make_pair()
    )
    endpoint_channel = AsyncFrameChannel(endpoint_connection)
    broker_channel = AsyncFrameChannel(broker_connection)

    canonical_bytes = b"foo bar baz"
    sent_frame = EmitFrame(
        msg_topic_id=TopicID(0),
        msg_producer_id=ProducerID(0),
        msg_type_id=MessageTypeID(0),
        msg_format_id=PortableFormatID(0),
        payload_bytes=canonical_bytes,
    )
    receive_task = asyncio.create_task(broker_channel.receive_frame())
    await asyncio.sleep(0)
    canonical_task_done_before_send = False
    received_task_done_before_send = receive_task.done()
    await endpoint_channel.send_frame(sent_frame)
    received_frame = await receive_task
    received_bytes = received_frame.payload_bytes

    print(f"{canonical_task_done_before_send=}")
    print(f"{received_task_done_before_send=}")
    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    wait_success = (
        received_task_done_before_send == canonical_task_done_before_send
    )
    eq_string = "=="
    if not wait_success:
        eq_string = "!="
    print(
        "received_task_done_before_send "
        + eq_string
        + " canonical_task_done_before_send"
    )

    payload_success = received_bytes == canonical_bytes
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    success = wait_success and payload_success
    print(f"({type(broker_channel).__name__}): ", end="")
    if success:
        print("Async channel waited and then received the encoded frame")
    else:
        print("Async channel behavior was incomplete or unexpected")
    print("\n")


async def demo_async_broker_transport_session_registers_emitter() -> None:
    print("Demo: async broker transport session registers emitter")
    endpoint_connection, broker_connection = (
        AsyncMemoryFrameConnection.make_pair()
    )
    endpoint_channel = AsyncFrameChannel(endpoint_connection)
    broker_channel = AsyncFrameChannel(broker_connection)
    session = AsyncBrokerTransportSession(
        channel=broker_channel,
        core=DirectBrokerCore(),
    )
    request_frame = RegisterEmitterFrame(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        format_key=RAW_BYTES_PORTABLE_FORMAT.key,
    )

    await endpoint_channel.send_frame(request_frame)
    await session.handle_next_frame()
    received_frame = await endpoint_channel.receive_frame()
    canonical_frame_type = RegisterEmitterResultFrame.__name__
    received_frame_type = type(received_frame).__name__

    print(f"{canonical_frame_type=}")
    print(f"{received_frame_type=}")
    success = isinstance(received_frame, RegisterEmitterResultFrame)
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_frame_type " + eq_string + " canonical_frame_type")

    print(f"({type(session).__name__}): ", end="")
    if success:
        print("Async transport session handled emitter registration")
    else:
        print("Async transport session did not return expected result")
    print("\n")


async def demo_async_broker_transport_session_subscribes() -> None:
    print("Demo: async broker transport session subscribes")
    endpoint_connection, broker_connection = (
        AsyncMemoryFrameConnection.make_pair()
    )
    endpoint_channel = AsyncFrameChannel(endpoint_connection)
    broker_channel = AsyncFrameChannel(broker_connection)
    session = AsyncBrokerTransportSession(
        channel=broker_channel,
        core=DirectBrokerCore(),
    )
    request_frame = SubscribeFrame(
        msg_topic=(topic_tree(DEMO_TOPIC),),
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )

    await endpoint_channel.send_frame(request_frame)
    await session.handle_next_frame()
    received_frame = await endpoint_channel.receive_frame()
    canonical_frame_type = SubscribeResultFrame.__name__
    received_frame_type = type(received_frame).__name__
    canonical_subscription_id = TransportSubscriptionID(0)
    received_subscription_id = None
    if isinstance(received_frame, SubscribeResultFrame):
        received_subscription_id = received_frame.subscription_id

    print(f"{canonical_frame_type=}")
    print(f"{received_frame_type=}")
    print(f"{canonical_subscription_id=}")
    print(f"{received_subscription_id=}")
    type_success = isinstance(received_frame, SubscribeResultFrame)
    eq_string = "=="
    if not type_success:
        eq_string = "!="
    print("received_frame_type " + eq_string + " canonical_frame_type")

    id_success = received_subscription_id == canonical_subscription_id
    eq_string = "=="
    if not id_success:
        eq_string = "!="
    print(
        "received_subscription_id " + eq_string + " canonical_subscription_id"
    )

    success = type_success and id_success
    print(f"({type(session).__name__}): ", end="")
    if success:
        print("Async transport session handled subscription")
    else:
        print("Async transport session did not return expected subscription")
    print("\n")


async def demo_async_broker_transport_session_emits_payload() -> None:
    print("Demo: async broker transport session accepts emitted payload")
    endpoint_connection, broker_connection = (
        AsyncMemoryFrameConnection.make_pair()
    )
    endpoint_channel = AsyncFrameChannel(endpoint_connection)
    broker_channel = AsyncFrameChannel(broker_connection)
    session = AsyncBrokerTransportSession(
        channel=broker_channel,
        core=DirectBrokerCore(capture_enabled=False),
    )
    register_frame = RegisterEmitterFrame(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        format_key=RAW_BYTES_PORTABLE_FORMAT.key,
    )
    await endpoint_channel.send_frame(register_frame)
    await session.handle_next_frame()
    register_result = await endpoint_channel.receive_frame()
    if not isinstance(register_result, RegisterEmitterResultFrame):
        raise TypeError("register emitter did not return a result frame")
    if register_result.msg_type_id is None:
        raise MissingMessageTypeError(
            "demo emitter registration did not produce a message type ID"
        )
    subscribe_frame = SubscribeFrame(
        msg_topic=(topic_tree(DEMO_TOPIC),),
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    await endpoint_channel.send_frame(subscribe_frame)
    await session.handle_next_frame()
    subscribe_result = await endpoint_channel.receive_frame()
    if not isinstance(subscribe_result, SubscribeResultFrame):
        raise TypeError("subscribe did not return a result frame")

    canonical_bytes = b"foo bar baz"
    emit_frame = EmitFrame(
        msg_topic_id=register_result.msg_topic_id,
        msg_producer_id=register_result.msg_producer_id,
        msg_type_id=register_result.msg_type_id,
        msg_format_id=register_result.msg_format_id,
        payload_bytes=canonical_bytes,
    )
    await endpoint_channel.send_frame(emit_frame)
    await session.handle_next_frame()
    received_frame = await endpoint_channel.receive_frame()
    received_bytes = None
    if isinstance(received_frame, DeliveryFrame):
        received_bytes = received_frame.payload_bytes

    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    success = received_bytes == canonical_bytes
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    print(f"({type(session).__name__}): ", end="")
    if success:
        print("Async transport emit delivered payload")
    else:
        print("Async transport emit did not deliver expected payload")
    print("\n")


async def demo_async_transport_client_receives_payload() -> None:
    print("Demo: async transport client receives payload")
    endpoint_connection, broker_connection = (
        AsyncMemoryFrameConnection.make_pair()
    )
    endpoint_channel = AsyncFrameChannel(endpoint_connection)
    broker_channel = AsyncFrameChannel(broker_connection)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    client = AsyncTransportClient(
        channel=endpoint_channel, extra_formats=format_registry.formats()
    )
    session = AsyncBrokerTransportSession(
        channel=broker_channel,
        core=DirectBrokerCore(capture_enabled=False),
    )

    registration_task = asyncio.create_task(
        client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
    )
    await session.handle_next_frame()
    emitter = await registration_task
    subscription_task = asyncio.create_task(
        client.subscribe(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
    )
    await session.handle_next_frame()
    receiver = await subscription_task

    canonical_bytes = b"foo bar baz"
    emit_task = asyncio.create_task(emitter.emit(canonical_bytes))
    await session.handle_next_frame()
    await emit_task
    received_message = await receiver.receive()
    received_bytes = received_message.payload
    canonical_symbols = (DEMO_TOPIC, DEMO_MSG_TYPE, DEMO_PRODUCER)
    received_symbols = (
        received_message.msg_topic,
        received_message.msg_type,
        received_message.msg_producer,
    )

    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    print(f"{canonical_symbols=}")
    print(f"{received_symbols=}")
    payload_success = received_bytes == canonical_bytes
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    symbols_success = received_symbols == canonical_symbols
    eq_string = "=="
    if not symbols_success:
        eq_string = "!="
    print("received_symbols " + eq_string + " canonical_symbols")

    success = payload_success and symbols_success
    print(f"({type(client).__name__}): ", end="")
    if success:
        print("Async transport client decoded payload and readable symbols")
    else:
        print("Async transport client did not reconstruct expected message")
    print("\n")


def _make_async_transport_endpoint(
    *,
    bus: AsyncDirectMessageBus,
    format_registry: PortableFormatTable,
    connections: tuple[AsyncFrameConnection, AsyncFrameConnection],
) -> tuple[AsyncTransportClient, AsyncBrokerTransportSession]:
    endpoint_connection, broker_connection = connections
    endpoint_channel = AsyncFrameChannel(endpoint_connection)
    broker_channel = AsyncFrameChannel(broker_connection)
    client = AsyncTransportClient(
        channel=endpoint_channel, extra_formats=format_registry.formats()
    )
    session = bus.create_transport_session(channel=broker_channel)
    return client, session


async def demo_async_transport_client_receives_from_another_endpoint() -> None:
    print("Demo: async transport client receives from another endpoint")
    bus = AsyncDirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )
    subscriber_client, subscriber_session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )

    registration_task = asyncio.create_task(
        producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
    )
    await producer_session.handle_next_frame()
    emitter = await registration_task
    subscription_task = asyncio.create_task(
        subscriber_client.subscribe(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
    )
    await subscriber_session.handle_next_frame()
    receiver = await subscription_task
    canonical_bytes = b"foo bar baz"
    emit_task = asyncio.create_task(emitter.emit(canonical_bytes))
    await producer_session.handle_next_frame()
    await emit_task
    received_message = await receiver.receive()
    received_bytes = received_message.payload
    canonical_symbols = (DEMO_TOPIC, DEMO_MSG_TYPE, DEMO_PRODUCER)
    received_symbols = (
        received_message.msg_topic,
        received_message.msg_type,
        received_message.msg_producer,
    )

    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    print(f"{canonical_symbols=}")
    print(f"{received_symbols=}")
    payload_success = received_bytes == canonical_bytes
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    symbols_success = received_symbols == canonical_symbols
    eq_string = "=="
    if not symbols_success:
        eq_string = "!="
    print("received_symbols " + eq_string + " canonical_symbols")

    success = payload_success and symbols_success
    print(f"({type(subscriber_client).__name__}): ", end="")
    if success:
        print("Async endpoint receiver decoded payload and readable symbols")
    else:
        print("Async endpoint receiver did not reconstruct expected message")
    print("\n")


async def demo_async_transport_request_client_service_facade() -> None:
    print("Demo: async transport request client/service facade")
    bus = AsyncDirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
    format_registry = PortableFormatRegistry(JSON_PORTABLE_FORMAT)
    requester_client, requester_session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )
    responder_client, responder_session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_name = "query-corge"
    reply_name = "result-grault"
    client_task = asyncio.create_task(
        requester_client.create_request_client(
            request_topic=DEMO_TOPIC,
            reply_topic=DEMO_TOPIC,
            requester_producer=requester_name,
            responder_producer=responder_name,
            request_msg_type=request_name,
            reply_msg_type=reply_name,
        )
    )
    await requester_session.handle_next_frame()
    await requester_session.handle_next_frame()
    client = await client_task
    service_task = asyncio.create_task(
        responder_client.create_request_service(
            request_topic=DEMO_TOPIC,
            reply_topic=DEMO_TOPIC,
            requester_producer=requester_name,
            responder_producer=responder_name,
            request_msg_type=request_name,
            reply_msg_type=reply_name,
        )
    )
    await responder_session.handle_next_frame()
    await responder_session.handle_next_frame()
    service = await service_task

    canonical_request = {"foo": "bar"}
    canonical_reply = {"baz": "qux"}
    send_task = asyncio.create_task(client.send(canonical_request))
    await requester_session.handle_next_frame()
    handle = await send_task
    request = await service.receive()
    received_request_payload = request.payload
    reply_task = asyncio.create_task(request.reply(canonical_reply))
    await responder_session.handle_next_frame()
    await reply_task
    reply = await client.receive(handle)
    received_reply_payload = reply.payload

    print(f"{canonical_request=}")
    print(f"{received_request_payload=}")
    request_success = received_request_payload == canonical_request
    eq_string = "=="
    if not request_success:
        eq_string = "!="
    print("received_request_payload " + eq_string + " canonical_request")

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    success = request_success and reply_success
    print(f"({type(requester_client).__name__}): ", end="")
    if success:
        print("Async transport request facade completed request/reply")
    else:
        print("Async transport request facade was incomplete")
    print("\n")


async def demo_async_transport_procedure_facade() -> None:
    print("Demo: async transport procedure facade")
    bus = AsyncDirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
    format_registry = PortableFormatRegistry(
        JSON_PORTABLE_FORMAT, PROCEDURE_INVOCATION_JSON_FORMAT
    )
    requester_client, requester_session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )
    responder_client, responder_session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_name = "query-corge"
    reply_name = "result-grault"
    client_task = asyncio.create_task(
        requester_client.create_procedure_client(
            request_topic=DEMO_TOPIC,
            reply_topic=DEMO_TOPIC,
            requester_producer=requester_name,
            responder_producer=responder_name,
            request_msg_type=request_name,
            reply_msg_type=reply_name,
        )
    )
    await requester_session.handle_next_frame()
    await requester_session.handle_next_frame()
    client = await client_task
    service_task = asyncio.create_task(
        responder_client.create_procedure_service(
            request_topic=DEMO_TOPIC,
            reply_topic=DEMO_TOPIC,
            requester_producer=requester_name,
            responder_producer=responder_name,
            request_msg_type=request_name,
            reply_msg_type=reply_name,
            handler=_unary_procedure_handler,
        )
    )
    await responder_session.handle_next_frame()
    await responder_session.handle_next_frame()
    service = await service_task

    canonical_request = "foo"
    canonical_reply = _unary_procedure_handler(canonical_request)
    service_task = asyncio.create_task(service.handle())
    call_task = asyncio.create_task(client(canonical_request))
    await requester_session.handle_next_frame()
    await responder_session.handle_next_frame()
    received_reply_payload = await call_task
    await service_task

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    print(f"({type(requester_client).__name__}): ", end="")
    if success:
        print("Async transport procedure facade completed request/reply")
    else:
        print("Async transport procedure facade was incomplete")
    print("\n")


async def demo_immediate_async_endpoint_provisioner() -> None:
    print("Demo: immediate async endpoint provisioner")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    provisioner = ImmediateAsyncEndpointProvisioner(bus)
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_name = "query-corge"
    reply_name = "result-grault"
    client = await provisioner.create_request_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )
    service = await provisioner.create_request_service(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=request_name,
        reply_msg_type=reply_name,
    )

    canonical_request = {"foo": "bar"}
    canonical_reply = {"baz": "qux"}
    handle = await client.send(canonical_request)
    request = await service.receive()
    received_request_payload = request.payload
    await request.reply(canonical_reply)
    reply = await client.receive(handle)
    received_reply_payload = reply.payload

    print(f"{canonical_request=}")
    print(f"{received_request_payload=}")
    request_success = received_request_payload == canonical_request
    eq_string = "=="
    if not request_success:
        eq_string = "!="
    print("received_request_payload " + eq_string + " canonical_request")

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    success = request_success and reply_success
    print(f"({type(provisioner).__name__}): ", end="")
    if success:
        print("Immediate provisioner completed request/reply")
    else:
        print("Immediate provisioner request/reply incomplete")
    print("\n")


def demo_message_bus_service_routes_between_clients() -> None:
    print("Demo: message bus service routes between clients")
    with TemporaryDirectory() as runtime_dir:
        socket_path = Path(runtime_dir) / "ropemother-service-demo.sock"
        bus = DirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
        listener = LocalBusServiceListener.from_socket_path(socket_path)
        format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
        service = MessageBusService.from_listener(bus=bus, listener=listener)
        service_thread = Thread(target=service.serve_forever)
        service_thread.start()
        descriptor = service.connection_descriptor()
        producer_client = connect_transport_client(
            descriptor=descriptor,
            extra_formats=format_registry.formats(),
        )
        subscriber_client = connect_transport_client(
            descriptor=descriptor,
            extra_formats=format_registry.formats(),
        )
        emitter = producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        receiver = subscriber_client.subscribe(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )

        canonical_bytes = b"foo bar baz"
        emitter.emit(canonical_bytes)
        received_message = receiver.receive()
        received_bytes = received_message.payload
        producer_client.close()
        subscriber_client.close()
        service.request_stop()
        service_thread.join()

    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    payload_success = received_bytes == canonical_bytes
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    print(f"received_topic={received_message.msg_topic!r}")
    print(f"received_type={received_message.msg_type!r}")
    print(f"received_producer={received_message.msg_producer!r}")
    symbols_success = (
        received_message.msg_topic == DEMO_TOPIC
        and received_message.msg_type == DEMO_MSG_TYPE
        and received_message.msg_producer == DEMO_PRODUCER
    )
    success = payload_success and symbols_success
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("Service-backed clients routed payload and readable symbols")
    else:
        print("Service-backed clients failed to reconstruct expected message")
    print("\n")


def _captured_message_count(records: list[CapturedRecord]) -> int:
    messages = [
        record for record in records if isinstance(record, CapturedMessage)
    ]
    return len(messages)


def demo_capture_bootstrap_lifecycle_facade() -> None:
    print("Demo: capture bootstrap lifecycle facade")
    bus = DirectMessageBus.capture_bootstrap()
    lifecycle = bus.create_lifecycle_publisher(msg_producer=DEMO_PRODUCER)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    sink = InMemoryCaptureSink()
    startup_payload = {"service": "foo"}
    message_payload = {"foo": "bar"}

    lifecycle.started(startup_payload)
    try:
        emitter.emit(message_payload)
        ordinary_rejected = False
    except BootstrapMessageRejectedError:
        ordinary_rejected = True
    bus.set_capture_sink(sink)
    bootstrap_count = _captured_message_count(sink.records)
    emitter.emit(message_payload)
    received_payload = receiver.receive().payload
    active_count = _captured_message_count(sink.records)

    print(f"{ordinary_rejected=}")
    print(f"{bootstrap_count=}")
    print(f"{message_payload=}")
    print(f"{received_payload=}")
    print(f"{active_count=}")
    rejection_success = ordinary_rejected
    capture_success = bootstrap_count == 1 and active_count == 2

    payload_success = received_payload == message_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " message_payload")

    success = rejection_success and capture_success and payload_success
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Bootstrap capture opened after lifecycle startup")
    else:
        print("Bootstrap capture behavior was incomplete")
    print("\n")


async def demo_async_capture_bootstrap_lifecycle_facade() -> None:
    print("Demo: async capture bootstrap lifecycle facade")
    bus = AsyncDirectMessageBus.capture_bootstrap()
    lifecycle = bus.create_lifecycle_publisher(msg_producer=DEMO_PRODUCER)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    sink = InMemoryCaptureSink()
    startup_payload = {"service": "foo"}
    message_payload = {"foo": "bar"}

    await lifecycle.started(startup_payload)
    try:
        await emitter.emit(message_payload)
        ordinary_rejected = False
    except BootstrapMessageRejectedError:
        ordinary_rejected = True
    bus.set_capture_sink(sink)
    bootstrap_count = _captured_message_count(sink.records)
    await emitter.emit(message_payload)
    received_payload = (await receiver.receive()).payload
    active_count = _captured_message_count(sink.records)

    print(f"{ordinary_rejected=}")
    print(f"{bootstrap_count=}")
    print(f"{message_payload=}")
    print(f"{received_payload=}")
    print(f"{active_count=}")
    rejection_success = ordinary_rejected
    capture_success = bootstrap_count == 1 and active_count == 2

    payload_success = received_payload == message_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " message_payload")

    success = rejection_success and capture_success and payload_success
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Async bootstrap capture opened after lifecycle startup")
    else:
        print("Async bootstrap capture behavior was incomplete")
    print("\n")


def demo_broker_transport_session_emit_acknowledgement() -> None:
    print("Demo: broker transport session acknowledges emit")
    endpoint_channel, session = _make_transport_session()

    register_frame = RegisterEmitterFrame(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        format_key=RAW_BYTES_PORTABLE_FORMAT.key,
    )
    endpoint_channel.send_frame(register_frame)
    session.handle_next_frame()
    register_result = endpoint_channel.receive_frame()
    if not isinstance(register_result, RegisterEmitterResultFrame):
        raise TypeError("register emitter did not return a result frame")
    if register_result.msg_type_id is None:
        raise MissingMessageTypeError(
            "demo emitter registration did not produce a message type ID"
        )
    subscribe_frame = SubscribeFrame(
        msg_topic=(topic_tree(DEMO_TOPIC),),
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    endpoint_channel.send_frame(subscribe_frame)
    session.handle_next_frame()
    subscribe_result = endpoint_channel.receive_frame()
    if not isinstance(subscribe_result, SubscribeResultFrame):
        raise TypeError("subscribe did not return a result frame")
    emit_frame = EmitFrame(
        msg_topic_id=register_result.msg_topic_id,
        msg_producer_id=register_result.msg_producer_id,
        msg_type_id=register_result.msg_type_id,
        msg_format_id=register_result.msg_format_id,
        payload_bytes=b"foo bar baz",
    )
    endpoint_channel.send_frame(emit_frame)
    session.handle_next_frame()
    delivery_frame = endpoint_channel.receive_frame()
    result_frame = endpoint_channel.receive_frame()
    expected_frame_types = [DeliveryFrame.__name__, EmitResultFrame.__name__]
    observed_frame_types = [
        type(delivery_frame).__name__,
        type(result_frame).__name__,
    ]

    print(f"{expected_frame_types=}")
    print(f"{observed_frame_types=}")
    success = observed_frame_types == expected_frame_types
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("observed_frame_types " + eq_string + " expected_frame_types")
    print(f"({type(session).__name__}): ", end="")
    if success:
        print("Transport emit produced delivery and acknowledgement frames")
    else:
        print("Transport emit acknowledgement behavior was unexpected")
    print("\n")


async def demo_async_broker_transport_session_emit_acknowledgement() -> None:
    print("Demo: async broker transport session acknowledges emit")
    endpoint_connection, broker_connection = (
        AsyncMemoryFrameConnection.make_pair()
    )
    endpoint_channel = AsyncFrameChannel(endpoint_connection)
    broker_channel = AsyncFrameChannel(broker_connection)
    session = AsyncBrokerTransportSession(
        channel=broker_channel,
        core=DirectBrokerCore(capture_enabled=False),
    )

    register_frame = RegisterEmitterFrame(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        format_key=RAW_BYTES_PORTABLE_FORMAT.key,
    )
    await endpoint_channel.send_frame(register_frame)
    await session.handle_next_frame()
    register_result = await endpoint_channel.receive_frame()
    if not isinstance(register_result, RegisterEmitterResultFrame):
        raise TypeError("register emitter did not return a result frame")
    if register_result.msg_type_id is None:
        raise MissingMessageTypeError(
            "demo emitter registration did not produce a message type ID"
        )
    subscribe_frame = SubscribeFrame(
        msg_topic=(topic_tree(DEMO_TOPIC),),
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    await endpoint_channel.send_frame(subscribe_frame)
    await session.handle_next_frame()
    subscribe_result = await endpoint_channel.receive_frame()
    if not isinstance(subscribe_result, SubscribeResultFrame):
        raise TypeError("subscribe did not return a result frame")
    emit_frame = EmitFrame(
        msg_topic_id=register_result.msg_topic_id,
        msg_producer_id=register_result.msg_producer_id,
        msg_type_id=register_result.msg_type_id,
        msg_format_id=register_result.msg_format_id,
        payload_bytes=b"foo bar baz",
    )
    await endpoint_channel.send_frame(emit_frame)
    await session.handle_next_frame()
    delivery_frame = await endpoint_channel.receive_frame()
    result_frame = await endpoint_channel.receive_frame()
    expected_frame_types = (DeliveryFrame.__name__, EmitResultFrame.__name__)
    observed_frame_types = (
        type(delivery_frame).__name__, type(result_frame).__name__
    )

    print(f"{expected_frame_types=}")
    print(f"{observed_frame_types=}")
    success = observed_frame_types == expected_frame_types
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("observed_frame_types " + eq_string + " expected_frame_types")

    print(f"({type(session).__name__}): ", end="")
    if success:
        print("Async transport emit produced delivery/acknowledgement frames")
    else:
        print("Async transport emit acknowledgement behavior was unexpected")
    print("\n")


def demo_transport_emit_reports_rejection() -> None:
    print("Demo: transport verified emit reports rejection")
    bus = DirectMessageBus.capture_bootstrap()
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    client, session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    worker = _service_transport_session(session, 1)
    emitter = client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    worker.join()

    expected_error_code = BootstrapMessageRejectedError.__name__
    worker = _service_transport_session(session, 1)
    try:
        emitter.emit(b"foo bar baz")
        received_error_code = None
    except TransportRequestError as error:
        received_error_code = error.error_code
    worker.join()

    print(f"{expected_error_code=}")
    print(f"{received_error_code=}")
    success = received_error_code == expected_error_code
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_error_code " + eq_string + " expected_error_code")

    print(f"({type(emitter).__name__}): ", end="")
    if success:
        print("Transport emit reported broker rejection")
    else:
        print("Transport emit did not report expected rejection")
    print("\n")


async def demo_async_transport_emit_reports_rejection() -> None:
    print("Demo: async transport verified emit reports rejection")
    bus = AsyncDirectMessageBus.capture_bootstrap()
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    client, session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )
    registration_task = asyncio.create_task(
        client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
    )
    await session.handle_next_frame()
    emitter = await registration_task

    expected_error_code = BootstrapMessageRejectedError.__name__
    emit_task = asyncio.create_task(emitter.emit(b"foo bar baz"))
    await session.handle_next_frame()
    try:
        await emit_task
        received_error_code = None
    except TransportRequestError as error:
        received_error_code = error.error_code

    print(f"{expected_error_code=}")
    print(f"{received_error_code=}")
    success = received_error_code == expected_error_code
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_error_code " + eq_string + " expected_error_code")

    print(f"({type(emitter).__name__}): ", end="")
    if success:
        print("Async transport emit reported broker rejection")
    else:
        print("Async transport emit did not report expected rejection")
    print("\n")


def demo_message_bus_service_capture_bootstrap() -> None:
    print("Demo: message bus service capture bootstrap")
    with TemporaryDirectory() as runtime_dir:
        socket_path = Path(runtime_dir) / "ropemother-bootstrap-demo.sock"
        listener = LocalBusServiceListener.from_socket_path(socket_path)
        format_registry = PortableFormatRegistry(
            JSON_PORTABLE_FORMAT, RAW_BYTES_PORTABLE_FORMAT
        )
        service = MessageBusService.capture_bootstrap(listener=listener)
        service_thread = Thread(target=service.serve_forever)
        service_thread.start()
        descriptor = service.connection_descriptor()
        lifecycle_client = connect_transport_client(
            descriptor=descriptor,
            extra_formats=format_registry.formats(),
        )
        producer_client = connect_transport_client(
            descriptor=descriptor,
            extra_formats=format_registry.formats(),
        )
        subscriber_client = connect_transport_client(
            descriptor=descriptor,
            extra_formats=format_registry.formats(),
        )
        lifecycle = lifecycle_client.create_lifecycle_publisher(
            msg_producer=DEMO_PRODUCER
        )
        emitter = producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        receiver = subscriber_client.subscribe(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        startup_payload = {"service": "foo"}
        message_payload = b"foo bar baz"
        lifecycle.started(startup_payload)

        expected_error_code = BootstrapMessageRejectedError.__name__
        try:
            emitter.emit(message_payload)
            received_error_code = None
        except TransportRequestError as error:
            received_error_code = error.error_code

        sink = InMemoryCaptureSink()
        service.set_capture_sink(sink)
        bootstrap_count = _captured_message_count(sink.records)
        emitter.emit(message_payload)
        received_payload = receiver.receive().payload
        active_count = _captured_message_count(sink.records)

        lifecycle_client.close()
        producer_client.close()
        subscriber_client.close()
        service.request_stop()
        service_thread.join()

    print(f"{expected_error_code=}")
    print(f"{received_error_code=}")
    rejection_success = received_error_code == expected_error_code
    eq_string = "=="
    if not rejection_success:
        eq_string = "!="
    print("received_error_code " + eq_string + " expected_error_code")

    print(f"{bootstrap_count=}")
    print(f"{message_payload=}")
    print(f"{received_payload=}")
    payload_success = received_payload == message_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " message_payload")
    print(f"{active_count=}")

    capture_success = bootstrap_count == 1 and active_count == 2
    success = rejection_success and payload_success and capture_success
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("Service bootstrap opened after lifecycle startup")
    else:
        print("Service bootstrap behavior was incomplete")
    print("\n")


def demo_message_bus_service_contact_variable() -> None:
    print("Demo: clients connect using bus contact variable")
    with TemporaryDirectory() as runtime_dir:
        socket_path = Path(runtime_dir) / "ropemother-env-demo.sock"
        bus = DirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
        listener = LocalBusServiceListener.from_socket_path(socket_path)
        format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
        service = MessageBusService.from_listener(bus=bus, listener=listener)
        service_thread = Thread(target=service.serve_forever)
        service_thread.start()
        variables = {}
        descriptor = service.connection_descriptor()
        set_bus_contact_uri(descriptor, variables=variables)
        producer_client = connect_client_from_bus_contact(
            variables=variables,
            extra_formats=format_registry.formats(),
        )
        subscriber_client = connect_client_from_bus_contact(
            variables=variables,
            extra_formats=format_registry.formats(),
        )
        emitter = producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        receiver = subscriber_client.subscribe(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        canonical_bytes = b"foo bar baz"
        emitter.emit(canonical_bytes)
        received_bytes = receiver.receive().payload

        producer_client.close()
        subscriber_client.close()
        service.request_stop()
        service_thread.join()

    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    success = received_bytes == canonical_bytes
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    print(f"({type(service).__name__}): ", end="")
    if success:
        print("Bus contact environment variable connected clients")
    else:
        print("Bus contact environment variable did not route payload")
    print("\n")


def demo_message_bus_service_contact_handoff() -> None:
    print("Demo: bus contact handoff preserves source variables")
    with TemporaryDirectory() as runtime_dir:
        socket_path = Path(runtime_dir) / "ropemother-env-vars-demo.sock"
        bus = DirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
        listener = LocalBusServiceListener.from_socket_path(socket_path)
        format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
        service = MessageBusService.from_listener(bus=bus, listener=listener)
        service_thread = Thread(target=service.serve_forever)
        service_thread.start()
        source_variables = {"FOO": "bar"}
        descriptor = service.connection_descriptor()
        contact_variables = bus_contact_variables(
            descriptor, variables=source_variables
        )
        producer_client = connect_client_from_bus_contact(
            variables=contact_variables,
            extra_formats=format_registry.formats(),
        )
        subscriber_client = connect_client_from_bus_contact(
            variables=contact_variables,
            extra_formats=format_registry.formats(),
        )
        emitter = producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        receiver = subscriber_client.subscribe(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        canonical_bytes = b"foo bar baz"
        emitter.emit(canonical_bytes)
        received_bytes = receiver.receive().payload

        producer_client.close()
        subscriber_client.close()
        service.request_stop()
        service_thread.join()

    print(f"{source_variables=}")
    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    payload_success = received_bytes == canonical_bytes
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    source_success = source_variables == {"FOO": "bar"}
    success = payload_success and source_success
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("Bus service contact handoff preserved source variables")
    else:
        print("Bus service contact handoff behavior was incomplete")
    print("\n")


def demo_message_bus_service_contact_helper() -> None:
    print("Demo: service prepares a valid bus contact handoff value")
    with TemporaryDirectory() as runtime_dir:
        socket_path = (
            Path(runtime_dir) / "ropemother-service-env-vars-demo.sock"
        )
        bus = DirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
        listener = LocalBusServiceListener.from_socket_path(socket_path)
        format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
        service = MessageBusService.from_listener(bus=bus, listener=listener)
        service_thread = Thread(target=service.serve_forever)
        service_thread.start()
        source_variables = {"FOO": "bar"}
        contact_variables = service.bus_contact_variables(
            variables=source_variables
        )
        producer_client = connect_client_from_bus_contact(
            variables=contact_variables,
            extra_formats=format_registry.formats(),
        )
        subscriber_client = connect_client_from_bus_contact(
            variables=contact_variables,
            extra_formats=format_registry.formats(),
        )
        emitter = producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        receiver = subscriber_client.subscribe(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        canonical_bytes = b"foo bar baz"
        emitter.emit(canonical_bytes)
        received_bytes = receiver.receive().payload

        producer_client.close()
        subscriber_client.close()
        service.request_stop()
        service_thread.join()

    print(f"{source_variables=}")
    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    payload_success = received_bytes == canonical_bytes
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    source_success = source_variables == {"FOO": "bar"}
    success = payload_success and source_success
    print(f"({type(service).__name__}): ", end="")
    if success:
        print("Service prepared bus contact variable values")
    else:
        print("Service bus contact handoff was incomplete")
    print("\n")


def _read_capture_records(path: Path) -> list[dict[str, Any]]:
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line:
            records.append(oneline_deserialize(line))
    return records


def demo_message_bus_service_file_capture_bootstrap() -> None:
    print("Demo: message bus service file capture bootstrap")
    with TemporaryDirectory() as runtime_dir:
        runtime_path = Path(runtime_dir)
        socket_path = runtime_path / "ropemother-file-bootstrap-demo.sock"
        capture_path = runtime_path / "capture.jsonl"
        listener = LocalBusServiceListener.from_socket_path(socket_path)
        format_registry = PortableFormatRegistry(
            JSON_PORTABLE_FORMAT, RAW_BYTES_PORTABLE_FORMAT
        )
        service = MessageBusService.capture_bootstrap(listener=listener)
        service_thread = Thread(target=service.serve_forever)
        service_thread.start()
        descriptor = service.connection_descriptor()
        lifecycle_client = connect_transport_client(
            descriptor=descriptor,
            extra_formats=format_registry.formats(),
        )
        producer_client = connect_transport_client(
            descriptor=descriptor,
            extra_formats=format_registry.formats(),
        )
        subscriber_client = connect_transport_client(
            descriptor=descriptor,
            extra_formats=format_registry.formats(),
        )
        lifecycle = lifecycle_client.create_lifecycle_publisher(
            msg_producer=DEMO_PRODUCER
        )
        emitter = producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        receiver = subscriber_client.subscribe(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )

        startup_payload = {"service": "foo"}
        canonical_bytes = b"foo bar baz"
        lifecycle.started(startup_payload)
        expected_error_code = BootstrapMessageRejectedError.__name__
        try:
            emitter.emit(canonical_bytes)
            received_error_code = None
        except TransportRequestError as error:
            received_error_code = error.error_code
        sink = JSONLinesCaptureSink(capture_path, append=False)
        service.set_capture_sink(sink)
        emitter.emit(canonical_bytes)
        received_bytes = receiver.receive().payload
        file_records = _read_capture_records(capture_path)

        lifecycle_client.close()
        producer_client.close()
        subscriber_client.close()
        service.request_stop()
        service_thread.join()

    observed_record_types = []
    captured_record_count = 0
    for record in file_records:
        record_type = record["record_type"]
        if record_type not in observed_record_types:
            observed_record_types.append(record_type)
        if record_type == "CapturedMessage":
            captured_record_count += 1

    expected_record_types = [
        "MessageSymbolRegistration",
        "PortableFormatRegistration",
        "CapturedMessage",
    ]
    expected_captured_record_count = 2

    print(f"{expected_error_code=}")
    print(f"{received_error_code=}")
    rejection_success = received_error_code == expected_error_code
    eq_string = "=="
    if not rejection_success:
        eq_string = "!="
    print("received_error_code " + eq_string + " expected_error_code")

    print(f"{canonical_bytes=}")
    print(f"{received_bytes=}")
    payload_success = received_bytes == canonical_bytes
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_bytes " + eq_string + " canonical_bytes")

    print(f"{expected_record_types=}")
    print(f"{observed_record_types=}")
    record_success = observed_record_types == expected_record_types
    eq_string = "=="
    if not record_success:
        eq_string = "!="
    print("observed_record_types " + eq_string + " expected_record_types")

    print(f"{expected_captured_record_count=}")
    print(f"{captured_record_count=}")
    count_success = captured_record_count == expected_captured_record_count
    eq_string = "=="
    if not count_success:
        eq_string = "!="
    print(
        "captured_record_count "
        + eq_string
        + " expected_captured_record_count"
    )

    success = (
        rejection_success
        and payload_success
        and record_success
        and count_success
    )
    print(f"({type(sink).__name__}): ", end="")
    if success:
        print("Service bootstrap captured to a JSON Lines file")
    else:
        print("Service bootstrap file capture was incomplete")
    print("\n")


def demo_topic_tree_selector() -> None:
    print("Demo: topic tree selector receives base and subtopics")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    canonical_topics = [DEMO_TOPIC, DEMO_TOPIC + ".quux"]
    canonical_selector = topic_tree(canonical_topics[0])
    canonical_payloads = [{"foo": "bar"}, {"baz": "qux"}]

    receiver = bus.subscribe(
        msg_topic=canonical_selector,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitters = []
    for topic in canonical_topics:
        emitter = bus.register_emitter(
            msg_topic=topic,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        emitters.append(emitter)
    for index, emitter in enumerate(emitters):
        emitter.emit(canonical_payloads[index])
    received_messages = []
    for _ in canonical_payloads:
        received_messages.append(receiver.receive())
    received_topics = [message.msg_topic for message in received_messages]
    received_payloads = [message.payload for message in received_messages]

    print(f"{canonical_topics=}")
    print(f"{received_topics=}")
    topic_success = received_topics == canonical_topics
    eq_string = "=="
    if not topic_success:
        eq_string = "!="
    print("received_topics " + eq_string + " canonical_topics")

    print(f"{canonical_payloads=}")
    print(f"{received_payloads=}")
    payload_success = received_payloads == canonical_payloads
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payloads " + eq_string + " canonical_payloads")

    success = topic_success and payload_success
    print(f"({type(receiver).__name__}): ", end="")
    if success:
        print("Topic tree subscription received base and subtopic messages")
    else:
        print("Topic tree subscription behavior was incomplete")
    print("\n")


def demo_topic_selector_collection() -> None:
    print("Demo: topic selector collection receives exact and tree topics")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    canonical_topics = [
        DEMO_TOPIC, DEMO_TOPIC + ".qux", DEMO_TOPIC + ".qux.quux"
    ]
    canonical_selector = (canonical_topics[0], topic_tree(canonical_topics[1]))
    canonical_payloads = [{"foo": "bar"}, {"baz": "qux"}, {"quux": "corge"}]
    receiver = bus.subscribe(
        msg_topic=canonical_selector,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    emitters = []
    for topic in canonical_topics:
        emitter = bus.register_emitter(
            msg_topic=topic, msg_producer=DEMO_PRODUCER, msg_type=DEMO_MSG_TYPE
        )
        emitters.append(emitter)

    for index, emitter in enumerate(emitters):
        emitter.emit(canonical_payloads[index])
    received_messages = []
    for _ in canonical_payloads:
        received_messages.append(receiver.receive())
    received_topics = [message.msg_topic for message in received_messages]
    received_payloads = [message.payload for message in received_messages]

    print(f"{canonical_topics=}")
    print(f"{received_topics=}")
    topic_success = received_topics == canonical_topics
    eq_string = "=="
    if not topic_success:
        eq_string = "!="
    print("received_topics " + eq_string + " canonical_topics")

    print(f"{canonical_payloads=}")
    print(f"{received_payloads=}")
    payload_success = received_payloads == canonical_payloads
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payloads " + eq_string + " canonical_payloads")

    success = topic_success and payload_success
    print(f"({type(receiver).__name__}): ", end="")
    if success:
        print("Topic selector collection received exact and tree messages")
    else:
        print("Topic selector collection behavior was incomplete")
    print("\n")


def demo_transport_client_topic_selector_collection() -> None:
    print("Demo: transport client receives exact and tree topics")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    canonical_topics = [
        DEMO_TOPIC,
        DEMO_TOPIC + ".qux",
        DEMO_TOPIC + ".qux.quux",
    ]
    canonical_selector = (canonical_topics[0], topic_tree(canonical_topics[1]))
    canonical_payloads = [b"foo bar", b"baz qux", b"quux corge"]

    emitters = []
    for topic in canonical_topics:
        worker = _service_transport_session(producer_session, 1)
        emitter = producer_client.register_emitter(
            msg_topic=topic,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        worker.join()
        emitters.append(emitter)
    worker = _service_transport_session(subscriber_session, 1)
    receiver = subscriber_client.subscribe(
        msg_topic=canonical_selector,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    worker.join()

    for index, emitter in enumerate(emitters):
        worker = _service_transport_session(producer_session, 1)
        emitter.emit(canonical_payloads[index])
        worker.join()
    received_messages = []
    for _ in canonical_payloads:
        received_messages.append(receiver.receive())
    received_topics = [message.msg_topic for message in received_messages]
    received_payloads = [message.payload for message in received_messages]

    print(f"{canonical_topics=}")
    print(f"{received_topics=}")
    topic_success = received_topics == canonical_topics
    eq_string = "=="
    if not topic_success:
        eq_string = "!="
    print("received_topics " + eq_string + " canonical_topics")

    print(f"{canonical_payloads=}")
    print(f"{received_payloads=}")
    payload_success = received_payloads == canonical_payloads
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payloads " + eq_string + " canonical_payloads")

    success = topic_success and payload_success
    print(f"({type(subscriber_client).__name__}): ", end="")
    if success:
        print("Transport receiver accepted exact and tree topics")
    else:
        print("Transport topic selector collection behavior was incomplete")
    print("\n")


def demo_producer_selector_collection() -> None:
    print("Demo: producer selector collection receives multiple producers")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    canonical_producers = [DEMO_PRODUCER, DEMO_PRODUCER + "-quux"]
    canonical_payloads = [{"foo": "bar"}, {"baz": "qux"}]
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=canonical_producers,
        msg_type=DEMO_MSG_TYPE,
    )
    emitters = []
    for producer in canonical_producers:
        emitter = bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=producer,
            msg_type=DEMO_MSG_TYPE,
        )
        emitters.append(emitter)

    for index, emitter in enumerate(emitters):
        emitter.emit(canonical_payloads[index])
    received_messages = []
    for _ in canonical_payloads:
        received_messages.append(receiver.receive())
    received_producers = [
        message.msg_producer for message in received_messages
    ]
    received_payloads = [message.payload for message in received_messages]

    print(f"{canonical_producers=}")
    print(f"{received_producers=}")
    producer_success = received_producers == canonical_producers
    eq_string = "=="
    if not producer_success:
        eq_string = "!="
    print("received_producers " + eq_string + " canonical_producers")

    print(f"{canonical_payloads=}")
    print(f"{received_payloads=}")
    payload_success = received_payloads == canonical_payloads
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payloads " + eq_string + " canonical_payloads")

    success = producer_success and payload_success
    print(f"({type(receiver).__name__}): ", end="")
    if success:
        print("Producer selector collection received both producers")
    else:
        print("Producer selector collection behavior was incomplete")
    print("\n")


def demo_transport_client_producer_selector_collection() -> None:
    print("Demo: transport client receives multiple producers")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    canonical_producers = [DEMO_PRODUCER, DEMO_PRODUCER + "-quux"]
    canonical_payloads = [b"foo bar", b"baz qux"]

    worker = _service_transport_session(subscriber_session, 1)
    receiver = subscriber_client.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=canonical_producers,
        msg_type=DEMO_MSG_TYPE,
    )
    worker.join()
    emitters = []
    for producer in canonical_producers:
        worker = _service_transport_session(producer_session, 1)
        emitter = producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=producer,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        worker.join()
        emitters.append(emitter)

    for index, emitter in enumerate(emitters):
        worker = _service_transport_session(producer_session, 1)
        emitter.emit(canonical_payloads[index])
        worker.join()
    received_messages = []
    for _ in canonical_payloads:
        received_messages.append(receiver.receive())
    received_producers = [
        message.msg_producer for message in received_messages
    ]
    received_payloads = [message.payload for message in received_messages]

    print(f"{canonical_producers=}")
    print(f"{received_producers=}")
    producer_success = received_producers == canonical_producers
    eq_string = "=="
    if not producer_success:
        eq_string = "!="
    print("received_producers " + eq_string + " canonical_producers")

    print(f"{canonical_payloads=}")
    print(f"{received_payloads=}")
    payload_success = received_payloads == canonical_payloads
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payloads " + eq_string + " canonical_payloads")

    success = producer_success and payload_success
    print(f"({type(subscriber_client).__name__}): ", end="")
    if success:
        print("Transport receiver accepted both producers")
    else:
        print("Transport producer selector collection behavior was incomplete")
    print("\n")


def demo_any_producer_subscription() -> None:
    print("Demo: producer filter omitted receives any producer")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    canonical_producers = [DEMO_PRODUCER, DEMO_PRODUCER + "-quux"]
    canonical_payloads = [{"foo": "bar"}, {"baz": "qux"}]
    receiver = bus.subscribe(msg_topic=DEMO_TOPIC, msg_type=DEMO_MSG_TYPE)
    emitters = []
    for producer in canonical_producers:
        emitter = bus.register_emitter(
            msg_topic=DEMO_TOPIC, msg_producer=producer, msg_type=DEMO_MSG_TYPE
        )
        emitters.append(emitter)

    for index, emitter in enumerate(emitters):
        emitter.emit(canonical_payloads[index])
    received_messages = []
    for _ in canonical_payloads:
        received_messages.append(receiver.receive())
    received_producers = [
        message.msg_producer for message in received_messages
    ]
    received_payloads = [message.payload for message in received_messages]

    print(f"{canonical_producers=}")
    print(f"{received_producers=}")
    producer_success = received_producers == canonical_producers
    eq_string = "=="
    if not producer_success:
        eq_string = "!="
    print("received_producers " + eq_string + " canonical_producers")

    print(f"{canonical_payloads=}")
    print(f"{received_payloads=}")
    payload_success = received_payloads == canonical_payloads
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payloads " + eq_string + " canonical_payloads")

    success = producer_success and payload_success
    print(f"({type(receiver).__name__}): ", end="")
    if success:
        print("Omitted producer filter received both producers")
    else:
        print("Omitted producer filter behavior was incomplete")
    print("\n")


def demo_unlisted_type_format_allowed() -> None:
    print("Demo: emitter may allow unlisted message types")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_ALT_MSG_TYPE,
    )
    rejecting_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    accepting_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        allow_unlisted_type_formats=True,
    )
    rejected_payload = {"baz": "qux"}
    canonical_payload = {"foo": "bar"}

    try:
        rejecting_emitter.emit(
            rejected_payload, msg_type=DEMO_ALT_MSG_TYPE
        )
        default_emitter_rejected = False
    except UnlistedMessageTypeError:
        default_emitter_rejected = True
    accepting_emitter.emit(
        canonical_payload, msg_type=DEMO_ALT_MSG_TYPE
    )
    received_message = receiver.receive()
    canonical_msg_type = DEMO_ALT_MSG_TYPE
    received_msg_type = received_message.msg_type
    received_payload = received_message.payload

    print(f"{default_emitter_rejected=}")
    print(f"{canonical_msg_type=}")
    print(f"{received_msg_type=}")
    type_success = received_msg_type == canonical_msg_type
    eq_string = "=="
    if not type_success:
        eq_string = "!="
    print("received_msg_type " + eq_string + " canonical_msg_type")

    print(f"{canonical_payload=}")
    print(f"{received_payload=}")
    payload_success = received_payload == canonical_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " canonical_payload")

    success = default_emitter_rejected and type_success and payload_success
    print(f"({type(accepting_emitter).__name__}): ", end="")
    if success:
        print("Emitter allowed an unlisted explicit message type")
    else:
        print("Unlisted message type policy behavior was incomplete")
    print("\n")


def demo_transport_client_additional_message_type() -> None:
    print("Demo: transport client emits declared additional message type")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    canonical_msg_type = DEMO_ALT_MSG_TYPE
    canonical_payload = b"foo bar"

    worker = _service_transport_session(producer_session, 1)
    emitter = producer_client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        additional_msg_types=(canonical_msg_type,),
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    worker.join()
    worker = _service_transport_session(subscriber_session, 1)
    receiver = subscriber_client.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=canonical_msg_type,
    )
    worker.join()
    worker = _service_transport_session(producer_session, 1)
    emitter.emit(canonical_payload, msg_type=canonical_msg_type)
    worker.join()
    received_message = receiver.receive()
    received_msg_type = received_message.msg_type
    received_payload = received_message.payload

    print(f"{canonical_msg_type=}")
    print(f"{received_msg_type=}")
    type_success = received_msg_type == canonical_msg_type
    eq_string = "=="
    if not type_success:
        eq_string = "!="
    print("received_msg_type " + eq_string + " canonical_msg_type")

    print(f"{canonical_payload=}")
    print(f"{received_payload=}")
    payload_success = received_payload == canonical_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " canonical_payload")

    success = type_success and payload_success
    print(f"({type(producer_client).__name__}): ", end="")
    if success:
        print("Transport emitter used declared additional message type")
    else:
        print("Transport additional message type behavior was incomplete")
    print("\n")


def demo_transport_client_unlisted_message_type_allowed() -> None:
    print("Demo: transport client may allow unlisted message type")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    canonical_msg_type = DEMO_ALT_MSG_TYPE
    canonical_payload = b"foo bar"

    worker = _service_transport_session(producer_session, 1)
    emitter = producer_client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        allow_unlisted_type_formats=True,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    worker.join()
    worker = _service_transport_session(subscriber_session, 1)
    receiver = subscriber_client.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=canonical_msg_type,
    )
    worker.join()

    worker = _service_transport_session(producer_session, 2)
    emitter.emit(canonical_payload, msg_type=canonical_msg_type)
    worker.join()
    received_message = receiver.receive()
    received_msg_type = received_message.msg_type
    received_payload = received_message.payload

    print(f"{canonical_msg_type=}")
    print(f"{received_msg_type=}")
    type_success = received_msg_type == canonical_msg_type
    eq_string = "=="
    if not type_success:
        eq_string = "!="
    print("received_msg_type " + eq_string + " canonical_msg_type")

    print(f"{canonical_payload=}")
    print(f"{received_payload=}")
    payload_success = received_payload == canonical_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " canonical_payload")

    success = type_success and payload_success
    print(f"({type(producer_client).__name__}): ", end="")
    if success:
        print("Transport emitter allowed unlisted message type")
    else:
        print("Transport unlisted message type behavior was incomplete")
    print("\n")


async def demo_async_transport_client_additional_message_type() -> None:
    print("Demo: async transport client emits declared additional types")
    bus = AsyncDirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )
    subscriber_client, subscriber_session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )
    canonical_msg_type = DEMO_ALT_MSG_TYPE
    canonical_payload = b"foo bar"

    registration_task = asyncio.create_task(
        producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            additional_msg_types=(canonical_msg_type,),
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
    )
    await producer_session.handle_next_frame()
    emitter = await registration_task
    subscription_task = asyncio.create_task(
        subscriber_client.subscribe(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=canonical_msg_type,
        )
    )
    await subscriber_session.handle_next_frame()
    receiver = await subscription_task
    emit_task = asyncio.create_task(
        emitter.emit(canonical_payload, msg_type=canonical_msg_type)
    )
    await producer_session.handle_next_frame()
    await emit_task
    received_message = await receiver.receive()
    received_msg_type = received_message.msg_type
    received_payload = received_message.payload

    print(f"{canonical_msg_type=}")
    print(f"{received_msg_type=}")
    type_success = received_msg_type == canonical_msg_type
    eq_string = "=="
    if not type_success:
        eq_string = "!="
    print("received_msg_type " + eq_string + " canonical_msg_type")

    print(f"{canonical_payload=}")
    print(f"{received_payload=}")
    payload_success = received_payload == canonical_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " canonical_payload")

    success = type_success and payload_success
    print(f"({type(subscriber_client).__name__}): ", end="")
    if success:
        print("Async transport emitter used declared additional message type")
    else:
        print("Async transport additional message type behavior incomplete")
    print("\n")


async def demo_async_transport_client_unlisted_message_type_allowed() -> None:
    print("Demo: async transport client may allow unlisted message type")
    bus = AsyncDirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )
    subscriber_client, subscriber_session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )
    canonical_msg_type = DEMO_ALT_MSG_TYPE
    canonical_payload = b"foo bar"

    registration_task = asyncio.create_task(
        producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            allow_unlisted_type_formats=True,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
    )
    await producer_session.handle_next_frame()
    emitter = await registration_task
    subscription_task = asyncio.create_task(
        subscriber_client.subscribe(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=canonical_msg_type,
        )
    )
    await subscriber_session.handle_next_frame()
    receiver = await subscription_task

    emit_task = asyncio.create_task(
        emitter.emit(canonical_payload, msg_type=canonical_msg_type)
    )
    await producer_session.handle_next_frame()
    await producer_session.handle_next_frame()
    await emit_task
    received_message = await receiver.receive()
    received_msg_type = received_message.msg_type
    received_payload = received_message.payload

    print(f"{canonical_msg_type=}")
    print(f"{received_msg_type=}")
    type_success = received_msg_type == canonical_msg_type
    eq_string = "=="
    if not type_success:
        eq_string = "!="
    print("received_msg_type " + eq_string + " canonical_msg_type")

    print(f"{canonical_payload=}")
    print(f"{received_payload=}")
    payload_success = received_payload == canonical_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " canonical_payload")

    success = type_success and payload_success
    print(f"({type(producer_client).__name__}): ", end="")
    if success:
        print("Async transport emitter allowed unlisted message type")
    else:
        print("Async transport unlisted message type behavior was incomplete")
    print("\n")


def demo_transport_client_unlisted_message_type_rejected() -> None:
    print("Demo: transport client rejects unlisted message type by default")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=MemoryFrameConnection.make_pair(),
    )
    canonical_rejected = True

    worker = _service_transport_session(producer_session, 1)
    emitter = producer_client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )
    worker.join()

    try:
        emitter.emit(b"foo bar", msg_type=DEMO_ALT_MSG_TYPE)
        unlisted_type_rejected = False
    except UnlistedMessageTypeError:
        unlisted_type_rejected = True

    print(f"{canonical_rejected=}")
    print(f"{unlisted_type_rejected=}")
    eq_string = "=="
    if unlisted_type_rejected != canonical_rejected:
        eq_string = "!="
    print("unlisted_type_rejected " + eq_string + " canonical_rejected")

    if unlisted_type_rejected:
        print("Transport emitter rejected unlisted message type by default")
    else:
        print("Transport unlisted message type rejection was incomplete")
    print("\n")


async def demo_async_transport_client_unlisted_message_type_rejected() -> None:
    print("Demo: async transport client rejects unlisted type by default")
    bus = AsyncDirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
    format_registry = PortableFormatRegistry(RAW_BYTES_PORTABLE_FORMAT)
    producer_client, producer_session = _make_async_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=AsyncMemoryFrameConnection.make_pair(),
    )
    canonical_rejected = True

    registration_task = asyncio.create_task(
        producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
    )
    await producer_session.handle_next_frame()
    emitter = await registration_task

    try:
        await emitter.emit(b"foo bar", msg_type=DEMO_ALT_MSG_TYPE)
        unlisted_type_rejected = False
    except UnlistedMessageTypeError:
        unlisted_type_rejected = True

    print(f"{canonical_rejected=}")
    print(f"{unlisted_type_rejected=}")
    eq_string = "=="
    if unlisted_type_rejected != canonical_rejected:
        eq_string = "!="
    print("unlisted_type_rejected " + eq_string + " canonical_rejected")

    if unlisted_type_rejected:
        print("Async transport emitter rejected unlisted type by default")
    else:
        print("Async transport unlisted message type rejection was incomplete")
    print("\n")


def demo_local_message_bus_host_routes_between_clients() -> None:
    print("Demo: local message bus host routes between clients")
    with LocalMessageBusHost(capture_sink=InMemoryCaptureSink()) as host:
        generator = host.client("graph-generator")
        analyzer = host.client("graph-analyzer")
        graph_emitter = generator.register_emitter(
            msg_topic="demo.graph",
            msg_producer="graph-generator",
            msg_type="graph-json",
            payload_format=JSON_PORTABLE_FORMAT,
        )
        graph_receiver = analyzer.subscribe(
            msg_topic="demo.graph",
            msg_producer="graph-generator",
            msg_type="graph-json",
        )
        summary_emitter = generator.register_emitter(
            msg_topic="demo.graph.summary",
            msg_producer="graph-generator",
            msg_type="graph-summary-bytes",
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )
        summary_receiver = analyzer.subscribe(
            msg_topic="demo.graph.summary",
            msg_producer="graph-generator",
            msg_type="graph-summary-bytes",
        )

        canonical_graph = {
            "nodes": ["foo", "bar"], "edges": [["foo", "bar"]]
        }
        canonical_summary = b"foo -> bar"
        graph_emitter.emit(canonical_graph)
        summary_emitter.emit(canonical_summary)
        received_graph = graph_receiver.receive().payload
        received_summary = summary_receiver.receive().payload

    print(f"{canonical_graph=}")
    print(f"{received_graph=}")
    graph_success = received_graph == canonical_graph
    eq_string = "=="
    if not graph_success:
        eq_string = "!="
    print("received_graph " + eq_string + " canonical_graph")

    print(f"{canonical_summary=}")
    print(f"{received_summary=}")
    summary_success = received_summary == canonical_summary
    eq_string = "=="
    if not summary_success:
        eq_string = "!="
    print("received_summary " + eq_string + " canonical_summary")

    success = graph_success and summary_success
    print(f"({LocalMessageBusHost.__name__}): ", end="")
    if success:
        print("Host-backed clients exchanged JSON and raw byte payloads")
    else:
        print("Host-backed client routing was incomplete")
    print("\n")


def demo_transport_client_supported_type_formats() -> None:
    print("Demo: transport client supports declared type-format pairs")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    canonical_json_format = JSON_PORTABLE_FORMAT
    canonical_bytes_format = RAW_BYTES_PORTABLE_FORMAT
    canonical_formats = [canonical_json_format, canonical_bytes_format]
    supported_type_formats = {
        DEMO_MSG_TYPE: canonical_formats,
        DEMO_ALT_MSG_TYPE: canonical_json_format,
    }
    format_registry = PortableFormatRegistry(*canonical_formats)
    producer_connections = MemoryFrameConnection.make_pair()
    subscriber_connections = MemoryFrameConnection.make_pair()
    producer_client, producer_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=producer_connections,
    )
    subscriber_client, subscriber_session = _make_transport_endpoint(
        bus=bus,
        format_registry=format_registry,
        connections=subscriber_connections,
    )

    canonical_json_payload = {"foo": "bar"}
    canonical_bytes_payload = b"baz"
    canonical_alt_payload = {"qux": "quux"}
    canonical_msg_types = [
        DEMO_MSG_TYPE, DEMO_MSG_TYPE, DEMO_ALT_MSG_TYPE
    ]
    canonical_payloads = [
        canonical_json_payload, canonical_bytes_payload, canonical_alt_payload
    ]

    worker = _service_transport_session(producer_session, 1)
    emitter = producer_client.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=canonical_json_format,
        supported_type_formats=supported_type_formats,
    )
    worker.join()
    worker = _service_transport_session(subscriber_session, 1)
    receiver = subscriber_client.subscribe(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
    )
    worker.join()

    try:
        emitter.emit(
            b"rejected",
            msg_type=DEMO_ALT_MSG_TYPE,
            payload_format=canonical_bytes_format,
        )
        unsupported_format_rejected = False
    except UnsupportedTypeFormatError:
        unsupported_format_rejected = True
    worker = _service_transport_session(producer_session, 1)
    emitter.emit(canonical_json_payload)
    worker.join()
    worker = _service_transport_session(producer_session, 1)
    emitter.emit(
        canonical_bytes_payload,
        payload_format=canonical_bytes_format,
    )
    worker.join()
    worker = _service_transport_session(producer_session, 1)
    emitter.emit(canonical_alt_payload, msg_type=DEMO_ALT_MSG_TYPE)
    worker.join()
    received_messages = receiver.receive_batch(min_count=3, max_count=3)
    received_msg_types = [message.msg_type for message in received_messages]
    received_payloads = [message.payload for message in received_messages]

    print(f"{unsupported_format_rejected=}")
    print(f"{canonical_msg_types=}")
    print(f"{received_msg_types=}")
    type_success = received_msg_types == canonical_msg_types
    eq_string = "=="
    if not type_success:
        eq_string = "!="
    print("received_msg_types " + eq_string + " canonical_msg_types")

    print(f"{canonical_payloads=}")
    print(f"{received_payloads=}")
    payload_success = received_payloads == canonical_payloads
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payloads " + eq_string + " canonical_payloads")

    success = unsupported_format_rejected and type_success and payload_success
    print(f"({type(producer_client).__name__}): ", end="")
    if success:
        print("Transport emitter honored declared type-format support")
    else:
        print("Transport type-format support behavior was incomplete")
    print("\n")


def demo_request_client_service_type_format_support() -> None:
    print("Demo: request client/service declared type-format support")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_type_formats = {
        DEMO_MSG_TYPE: [JSON_PORTABLE_FORMAT, RAW_BYTES_PORTABLE_FORMAT],
    }
    reply_type_formats = {
        DEMO_ALT_MSG_TYPE: [JSON_PORTABLE_FORMAT, RAW_BYTES_PORTABLE_FORMAT],
    }
    client = bus.create_request_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=DEMO_MSG_TYPE,
        reply_msg_type=DEMO_ALT_MSG_TYPE,
        request_payload_format=JSON_PORTABLE_FORMAT,
        request_type_formats=request_type_formats,
    )
    service = bus.create_request_service(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=DEMO_MSG_TYPE,
        reply_msg_type=DEMO_ALT_MSG_TYPE,
        reply_payload_format=JSON_PORTABLE_FORMAT,
        reply_type_formats=reply_type_formats,
    )

    try:
        client.send(
            {"foo": "bar"},
            payload_format=COMPOSITE_PORTABLE_FORMAT,
        )
        unsupported_request_rejected = False
    except UnsupportedTypeFormatError:
        unsupported_request_rejected = True
    canonical_request = b"foo bar"
    canonical_reply = b"baz qux"
    handle = client.send(
        canonical_request, payload_format=RAW_BYTES_PORTABLE_FORMAT
    )
    request = service.receive()
    received_request_payload = request.payload
    try:
        request.reply(
            {"baz": "qux"},
            payload_format=COMPOSITE_PORTABLE_FORMAT,
        )
        unsupported_reply_rejected = False
    except UnsupportedTypeFormatError:
        unsupported_reply_rejected = True
    request.reply(canonical_reply, payload_format=RAW_BYTES_PORTABLE_FORMAT)
    reply = client.receive(handle)
    received_reply_payload = reply.payload

    print(f"{unsupported_request_rejected=}")
    print(f"{unsupported_reply_rejected=}")
    print(f"{canonical_request=}")
    print(f"{received_request_payload=}")
    request_success = received_request_payload == canonical_request
    eq_string = "=="
    if not request_success:
        eq_string = "!="
    print("received_request_payload " + eq_string + " canonical_request")

    print(f"{canonical_reply=}")
    print(f"{received_reply_payload=}")
    reply_success = received_reply_payload == canonical_reply
    eq_string = "=="
    if not reply_success:
        eq_string = "!="
    print("received_reply_payload " + eq_string + " canonical_reply")

    success = (
        unsupported_request_rejected
        and unsupported_reply_rejected
        and request_success
        and reply_success
    )
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Request helpers honored declared type-format support")
    else:
        print("Request helper type-format behavior was incomplete")
    print("\n")


def demo_in_memory_capture_history_selects_messages() -> None:
    print("Demo: in-memory capture history selects messages")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    history = InMemoryCaptureHistory(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC, msg_producer=DEMO_PRODUCER
    )
    primary_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    secondary_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_ALT_MSG_TYPE,
    )

    primary_emitter.emit({"foo": "bar"})
    secondary_emitter.emit({"baz": "qux"})
    primary_message = receiver.receive()
    secondary_message = receiver.receive()
    page = history.select(
        msg_topic=secondary_message.msg_topic,
        msg_type=secondary_message.msg_type,
        msg_producer=secondary_message.msg_producer,
    )
    received_entry = None
    if len(page.entries) == 1:
        received_entry = page.entries[0]
    received_msg_id = None
    received_payload = None
    if received_entry is not None:
        received_msg_id = received_entry.msg_id
        received_payload = received_entry.payload

    print(f"{primary_message.msg_type=}")
    print(f"{secondary_message.msg_type=}")
    print(f"{secondary_message.msg_id=}")
    print(f"{received_msg_id=}")
    id_success = received_msg_id == secondary_message.msg_id
    eq_string = "=="
    if not id_success:
        eq_string = "!="
    print("received_msg_id " + eq_string + " secondary_message.msg_id")

    print(f"{secondary_message.payload=}")
    print(f"{received_payload=}")
    payload_success = received_payload == secondary_message.payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " secondary_message.payload")

    success = id_success and payload_success
    print(f"({type(history).__name__}): ", end="")
    if success:
        print("History selected the delivered message and payload")
    else:
        print("History selection did not match the delivered message")
    print("\n")


def demo_history_query_service_selects_messages() -> None:
    print("Demo: history query service selects messages")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    history = InMemoryCaptureHistory(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC, msg_producer=DEMO_PRODUCER
    )
    primary_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    secondary_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_ALT_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )

    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_client = bus.create_request_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=DEMO_MSG_TYPE,
        reply_msg_type=DEMO_ALT_MSG_TYPE,
        request_payload_format=JSON_PORTABLE_FORMAT,
        request_type_formats={DEMO_MSG_TYPE: JSON_PORTABLE_FORMAT},
    )
    request_service = bus.create_request_service(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=DEMO_MSG_TYPE,
        reply_msg_type=DEMO_ALT_MSG_TYPE,
        reply_payload_format=COMPOSITE_PORTABLE_FORMAT,
        reply_type_formats={DEMO_ALT_MSG_TYPE: COMPOSITE_PORTABLE_FORMAT},
    )
    history_client = HistoryClient(request_client)
    history_service = HistoryService(history, request_service)
    primary_emitter.emit({"foo": "bar"})
    secondary_emitter.emit(b"baz qux")
    primary_message = receiver.receive()
    secondary_message = receiver.receive()
    handle = history_client.send(
        msg_topic=secondary_message.msg_topic,
        msg_type=secondary_message.msg_type,
        msg_producer=secondary_message.msg_producer,
    )
    history_service.handle()
    page = history_client.receive(handle)
    received_entry = None
    if len(page.entries) == 1:
        received_entry = page.entries[0]
    received_msg_id = None
    received_payload = None
    if received_entry is not None:
        received_msg_id = received_entry.msg_id
        received_payload = received_entry.payload

    print(f"{primary_message.msg_type=}")
    print(f"{secondary_message.msg_type=}")
    print(f"{secondary_message.msg_id=}")
    print(f"{received_msg_id=}")
    id_success = received_msg_id == secondary_message.msg_id
    eq_string = "=="
    if not id_success:
        eq_string = "!="
    print("received_msg_id " + eq_string + " secondary_message.msg_id")

    print(f"{secondary_message.payload=}")
    print(f"{received_payload=}")
    payload_success = received_payload == secondary_message.payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " secondary_message.payload")

    success = id_success and payload_success
    print(f"({type(history_service).__name__}): ", end="")
    if success:
        print("History service returned the selected message and payload")
    else:
        print("History service result did not match the selected message")
    print("\n")


async def demo_async_history_query_service_selects_messages() -> None:
    print("Demo: async history query service selects messages")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    history = InMemoryCaptureHistory(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC, msg_producer=DEMO_PRODUCER
    )
    primary_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    secondary_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_ALT_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )

    requester_name = "requester-foo"
    responder_name = "responder-bar"
    request_client = bus.create_request_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=DEMO_MSG_TYPE,
        reply_msg_type=DEMO_ALT_MSG_TYPE,
        request_payload_format=JSON_PORTABLE_FORMAT,
        request_type_formats={DEMO_MSG_TYPE: JSON_PORTABLE_FORMAT},
    )
    request_service = bus.create_request_service(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=DEMO_MSG_TYPE,
        reply_msg_type=DEMO_ALT_MSG_TYPE,
        reply_payload_format=COMPOSITE_PORTABLE_FORMAT,
        reply_type_formats={DEMO_ALT_MSG_TYPE: COMPOSITE_PORTABLE_FORMAT},
    )
    history_client = AsyncHistoryClient(request_client)
    history_service = AsyncHistoryService(history, request_service)
    await primary_emitter.emit({"foo": "bar"})
    await secondary_emitter.emit(b"baz qux")
    primary_message = await receiver.receive()
    secondary_message = await receiver.receive()
    handle = await history_client.send(
        msg_topic=secondary_message.msg_topic,
        msg_type=secondary_message.msg_type,
        msg_producer=secondary_message.msg_producer,
    )
    await history_service.handle()
    page = await history_client.receive(handle)
    received_entry = None
    if len(page.entries) == 1:
        received_entry = page.entries[0]
    received_msg_id = None
    received_payload = None
    if received_entry is not None:
        received_msg_id = received_entry.msg_id
        received_payload = received_entry.payload

    print(f"{primary_message.msg_type=}")
    print(f"{secondary_message.msg_type=}")
    print(f"{secondary_message.msg_id=}")
    print(f"{received_msg_id=}")
    id_success = received_msg_id == secondary_message.msg_id
    eq_string = "=="
    if not id_success:
        eq_string = "!="
    print("received_msg_id " + eq_string + " secondary_message.msg_id")

    print(f"{secondary_message.payload=}")
    print(f"{received_payload=}")
    payload_success = received_payload == secondary_message.payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " secondary_message.payload")

    success = id_success and payload_success
    print(f"({type(history_service).__name__}): ", end="")
    if success:
        print("Async history service returned selected message and payload")
    else:
        print("Async history service result did not match selected message")
    print("\n")


def demo_scripted_input_emits_file_events() -> None:
    print("Demo: scripted input emits file events")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC, msg_producer=DEMO_PRODUCER
    )

    file_records = [
        {
            "at": 0.0,
            "msg_topic": DEMO_TOPIC,
            "msg_type": DEMO_MSG_TYPE,
            "msg_producer": DEMO_PRODUCER,
            "payload": {"foo": "bar"},
        },
        {
            "at": 0.25,
            "msg_topic": DEMO_TOPIC,
            "msg_type": DEMO_ALT_MSG_TYPE,
            "msg_producer": DEMO_PRODUCER,
            "payload_format": "raw-bytes",
            "payload_text": "baz qux",
        },
    ]
    canonical_plan = [
        (0.0, DEMO_MSG_TYPE, {"foo": "bar"}),
        (0.25, DEMO_ALT_MSG_TYPE, b"baz qux"),
    ]
    canonical_messages = [
        (DEMO_MSG_TYPE, {"foo": "bar"}),
        (DEMO_ALT_MSG_TYPE, b"baz qux"),
    ]
    file_lines = []
    for record in file_records:
        file_lines.append(oneline_serialize(record))

    with TemporaryDirectory() as temp_dir:
        input_path = Path(temp_dir) / "input.jsonl"
        input_path.write_text("\n".join(file_lines), encoding="utf-8")
        input_plan = ScriptedInputPlan.from_jsonl(input_path)
    recovered_plan = []
    for event in input_plan.events:
        item = (event.at, event.msg_type, event.payload)
        recovered_plan.append(item)

    input_fixture = ScriptedInputEmitter(bus, input_plan)
    input_fixture.emit_all()
    received_messages = []
    for _ in input_plan.events:
        message = receiver.receive()
        item = (message.msg_type, message.payload)
        received_messages.append(item)

    print(f"{canonical_plan=}")
    print(f"{recovered_plan=}")
    plan_success = recovered_plan == canonical_plan
    eq_string = "=="
    if not plan_success:
        eq_string = "!="
    print("recovered_plan " + eq_string + " canonical_plan")

    print(f"{canonical_messages=}")
    print(f"{received_messages=}")
    message_success = received_messages == canonical_messages
    eq_string = "=="
    if not message_success:
        eq_string = "!="
    print("received_messages " + eq_string + " canonical_messages")

    success = plan_success and message_success
    print(f"({type(input_fixture).__name__}): ", end="")
    if success:
        print("Input fixture recovered and emitted file events")
    else:
        print("Input fixture did not recover and emit file events")
    print("\n")


def demo_jsonl_capture_history_selects_messages() -> None:
    print("Demo: JSONL capture history selects messages")
    with TemporaryDirectory() as runtime_dir:
        capture_path = Path(runtime_dir) / "capture.jsonl"

        bus = DirectMessageBus()
        sink = JSONLinesCaptureSink(capture_path, append=False)
        bus.set_capture_sink(sink)
        history = JSONLinesCaptureHistory(capture_path)

        primary_emitter = bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        secondary_emitter = bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_ALT_MSG_TYPE,
        )

        primary_payload = {"foo": "bar"}
        secondary_payload = {"baz": "qux"}
        primary_emitter.emit(primary_payload)
        secondary_emitter.emit(secondary_payload)

        page = history.select(
            msg_topic=DEMO_TOPIC,
            msg_type=DEMO_ALT_MSG_TYPE,
            msg_producer=DEMO_PRODUCER,
        )

    canonical_entries = [(DEMO_ALT_MSG_TYPE, secondary_payload)]
    recovered_entries = []
    for entry in page.entries:
        recovered_entry = (entry.msg_type, entry.payload)
        recovered_entries.append(recovered_entry)

    print(f"{canonical_entries=}")
    print(f"{recovered_entries=}")
    success = recovered_entries == canonical_entries
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("recovered_entries " + eq_string + " canonical_entries")

    print(f"({type(history).__name__}): ", end="")
    if success:
        print("JSONL history returned the selected message")
    else:
        print("JSONL history did not return the selected message")
    print("\n")


def demo_jsonl_history_query_service_selects_messages() -> None:
    print("Demo: JSONL history query service selects messages")
    with TemporaryDirectory() as runtime_dir:
        capture_path = Path(runtime_dir) / "capture.jsonl"

        bus = DirectMessageBus()
        sink = JSONLinesCaptureSink(capture_path, append=False)
        bus.set_capture_sink(sink)
        history = JSONLinesCaptureHistory(capture_path)

        primary_emitter = bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        secondary_emitter = bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_ALT_MSG_TYPE,
        )

        requester_name = "requester-foo"
        responder_name = "responder-bar"
        request_client = bus.create_request_client(
            request_topic=DEMO_TOPIC,
            reply_topic=DEMO_TOPIC,
            requester_producer=requester_name,
            responder_producer=responder_name,
            request_msg_type=DEMO_MSG_TYPE,
            reply_msg_type=DEMO_ALT_MSG_TYPE,
            request_payload_format=JSON_PORTABLE_FORMAT,
            request_type_formats={DEMO_MSG_TYPE: JSON_PORTABLE_FORMAT},
        )
        request_service = bus.create_request_service(
            request_topic=DEMO_TOPIC,
            reply_topic=DEMO_TOPIC,
            requester_producer=requester_name,
            responder_producer=responder_name,
            request_msg_type=DEMO_MSG_TYPE,
            reply_msg_type=DEMO_ALT_MSG_TYPE,
            reply_payload_format=COMPOSITE_PORTABLE_FORMAT,
            reply_type_formats={
                DEMO_ALT_MSG_TYPE: COMPOSITE_PORTABLE_FORMAT
            },
        )
        history_client = HistoryClient(request_client)
        history_service = HistoryService(history, request_service)
        primary_payload = {"foo": "bar"}
        secondary_payload = {"baz": "qux"}
        primary_emitter.emit(primary_payload)
        secondary_emitter.emit(secondary_payload)

        handle = history_client.send(
            msg_topic=DEMO_TOPIC,
            msg_type=DEMO_ALT_MSG_TYPE,
            msg_producer=DEMO_PRODUCER,
        )
        history_service.handle()
        page = history_client.receive(handle)

    canonical_entries = [(DEMO_ALT_MSG_TYPE, secondary_payload)]
    recovered_entries = []
    for entry in page.entries:
        recovered_entry = (entry.msg_type, entry.payload)
        recovered_entries.append(recovered_entry)

    print(f"{canonical_entries=}")
    print(f"{recovered_entries=}")
    success = recovered_entries == canonical_entries
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("recovered_entries " + eq_string + " canonical_entries")

    print(f"({type(history_service).__name__}): ", end="")
    if success:
        print("JSONL history service returned the selected message")
    else:
        print("JSONL history service did not return the selected message")
    print("\n")


def demo_jsonl_capture_history_reconstructs_payload_formats() -> None:
    print("Demo: JSONL capture history reconstructs payload formats")
    with TemporaryDirectory() as runtime_dir:
        capture_path = Path(runtime_dir) / "capture.jsonl"

        bus = DirectMessageBus()
        sink = JSONLinesCaptureSink(capture_path, append=False)
        bus.set_capture_sink(sink)
        history = JSONLinesCaptureHistory(capture_path)

        json_emitter = bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        bytes_emitter = bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_ALT_MSG_TYPE,
            payload_format=RAW_BYTES_PORTABLE_FORMAT,
        )

        canonical_json_payload = {"foo": "bar"}
        canonical_bytes_payload = b"baz qux"

        json_emitter.emit(canonical_json_payload)
        bytes_emitter.emit(canonical_bytes_payload)

        page = history.select(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
        )

    recovered_json_payload = None
    recovered_bytes_payload = None
    for entry in page.entries:
        if entry.msg_type == DEMO_MSG_TYPE:
            recovered_json_payload = entry.payload
        elif entry.msg_type == DEMO_ALT_MSG_TYPE:
            recovered_bytes_payload = entry.payload

    print(f"{canonical_json_payload=}")
    print(f"{recovered_json_payload=}")
    json_success = recovered_json_payload == canonical_json_payload
    eq_string = "=="
    if not json_success:
        eq_string = "!="
    print("recovered_json_payload " + eq_string + " canonical_json_payload")

    print(f"{canonical_bytes_payload=}")
    print(f"{recovered_bytes_payload=}")
    bytes_success = recovered_bytes_payload == canonical_bytes_payload
    eq_string = "=="
    if not bytes_success:
        eq_string = "!="
    print("recovered_bytes_payload " + eq_string + " canonical_bytes_payload")

    success = json_success and bytes_success
    print(f"({type(history).__name__}): ", end="")
    if success:
        print("JSONL history reconstructed payloads using registered formats")
    else:
        print("JSONL history did not reconstruct the registered payloads")
    print("\n")


async def demo_async_jsonl_history_query_service_selects_messages() -> None:
    print("Demo: async JSONL history query service selects messages")
    with TemporaryDirectory() as runtime_dir:
        capture_path = Path(runtime_dir) / "capture.jsonl"

        bus = AsyncDirectMessageBus()
        sink = JSONLinesCaptureSink(capture_path, append=False)
        bus.set_capture_sink(sink)
        history = JSONLinesCaptureHistory(capture_path)

        primary_emitter = bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        secondary_emitter = bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_ALT_MSG_TYPE,
        )

        requester_name = "requester-foo"
        responder_name = "responder-bar"
        request_client = bus.create_request_client(
            request_topic=DEMO_TOPIC,
            reply_topic=DEMO_TOPIC,
            requester_producer=requester_name,
            responder_producer=responder_name,
            request_msg_type=DEMO_MSG_TYPE,
            reply_msg_type=DEMO_ALT_MSG_TYPE,
            request_payload_format=JSON_PORTABLE_FORMAT,
            request_type_formats={DEMO_MSG_TYPE: JSON_PORTABLE_FORMAT},
        )
        request_service = bus.create_request_service(
            request_topic=DEMO_TOPIC,
            reply_topic=DEMO_TOPIC,
            requester_producer=requester_name,
            responder_producer=responder_name,
            request_msg_type=DEMO_MSG_TYPE,
            reply_msg_type=DEMO_ALT_MSG_TYPE,
            reply_payload_format=COMPOSITE_PORTABLE_FORMAT,
            reply_type_formats={
                DEMO_ALT_MSG_TYPE: COMPOSITE_PORTABLE_FORMAT
            },
        )
        history_client = AsyncHistoryClient(request_client)
        history_service = AsyncHistoryService(history, request_service)
        primary_payload = {"foo": "bar"}
        canonical_payload = {"baz": "qux"}
        await primary_emitter.emit(primary_payload)
        await secondary_emitter.emit(canonical_payload)

        service_task = asyncio.create_task(history_service.handle())
        page = await history_client.select(
            msg_topic=DEMO_TOPIC,
            msg_type=DEMO_ALT_MSG_TYPE,
            msg_producer=DEMO_PRODUCER,
        )
        await service_task

    recovered_payload = None
    recovered_msg_type = None
    if len(page.entries) == 1:
        entry = page.entries[0]
        recovered_payload = entry.payload
        recovered_msg_type = entry.msg_type

    print(f"{DEMO_ALT_MSG_TYPE=}")
    print(f"{recovered_msg_type=}")
    type_success = recovered_msg_type == DEMO_ALT_MSG_TYPE
    eq_string = "=="
    if not type_success:
        eq_string = "!="
    print("recovered_msg_type " + eq_string + " DEMO_ALT_MSG_TYPE")

    print(f"{canonical_payload=}")
    print(f"{recovered_payload=}")
    payload_success = recovered_payload == canonical_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("recovered_payload " + eq_string + " canonical_payload")

    success = type_success and payload_success
    print(f"({type(history_service).__name__}): ", end="")
    if success:
        print("Async JSONL history service handled one background request")
    else:
        print("Async JSONL history service did not return selected payload")
    print("\n")


def demo_history_client_service_facade() -> None:
    print("Demo: history client/service facade")
    bus = DirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    history = InMemoryCaptureHistory(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC, msg_producer=DEMO_PRODUCER
    )
    primary_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    secondary_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_ALT_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )

    requester_name = "requester-foo"
    responder_name = "responder-bar"
    client = bus.create_history_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=DEMO_MSG_TYPE,
        reply_msg_type=DEMO_ALT_MSG_TYPE,
        request_payload_format=JSON_PORTABLE_FORMAT,
        request_type_formats={DEMO_MSG_TYPE: JSON_PORTABLE_FORMAT},
    )
    service = bus.create_history_service(
        history=history,
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=DEMO_MSG_TYPE,
        reply_msg_type=DEMO_ALT_MSG_TYPE,
        reply_payload_format=COMPOSITE_PORTABLE_FORMAT,
        reply_type_formats={DEMO_ALT_MSG_TYPE: COMPOSITE_PORTABLE_FORMAT},
    )
    primary_emitter.emit({"foo": "bar"})
    canonical_payload = b"baz qux"
    secondary_emitter.emit(canonical_payload)
    receiver.receive()
    canonical_message = receiver.receive()

    handle = client.send(
        msg_topic=canonical_message.msg_topic,
        msg_type=canonical_message.msg_type,
        msg_producer=canonical_message.msg_producer,
    )
    service.handle()
    page = client.receive(handle)
    canonical_msg_id = canonical_message.msg_id
    canonical_payload = canonical_message.payload
    received_msg_id = None
    received_payload = None
    if len(page.entries) == 1:
        received_entry = page.entries[0]
        received_msg_id = received_entry.msg_id
        received_payload = received_entry.payload

    print(f"{canonical_msg_id=}")
    print(f"{received_msg_id=}")
    id_success = received_msg_id == canonical_msg_id
    eq_string = "=="
    if not id_success:
        eq_string = "!="
    print("received_msg_id " + eq_string + " canonical_msg_id")

    print(f"{canonical_payload=}")
    print(f"{received_payload=}")
    payload_success = received_payload == canonical_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " canonical_payload")

    success = id_success and payload_success
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("History client/service facade selected captured message")
    else:
        print("History client/service facade did not select captured message")
    print("\n")


async def demo_async_history_client_service_facade() -> None:
    print("Demo: async history client/service facade")
    bus = AsyncDirectMessageBus()
    sink = InMemoryCaptureSink()
    bus.set_capture_sink(sink)
    history = InMemoryCaptureHistory(sink)
    receiver = bus.subscribe(
        msg_topic=DEMO_TOPIC, msg_producer=DEMO_PRODUCER
    )
    primary_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
    )
    secondary_emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_ALT_MSG_TYPE,
        payload_format=RAW_BYTES_PORTABLE_FORMAT,
    )

    requester_name = "requester-foo"
    responder_name = "responder-bar"
    client = bus.create_history_client(
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=DEMO_MSG_TYPE,
        reply_msg_type=DEMO_ALT_MSG_TYPE,
        request_payload_format=JSON_PORTABLE_FORMAT,
        request_type_formats={DEMO_MSG_TYPE: JSON_PORTABLE_FORMAT},
    )
    service = bus.create_history_service(
        history=history,
        request_topic=DEMO_TOPIC,
        reply_topic=DEMO_TOPIC,
        requester_producer=requester_name,
        responder_producer=responder_name,
        request_msg_type=DEMO_MSG_TYPE,
        reply_msg_type=DEMO_ALT_MSG_TYPE,
        reply_payload_format=COMPOSITE_PORTABLE_FORMAT,
        reply_type_formats={DEMO_ALT_MSG_TYPE: COMPOSITE_PORTABLE_FORMAT},
    )
    await primary_emitter.emit({"foo": "bar"})
    await secondary_emitter.emit(b"baz qux")
    await receiver.receive()
    canonical_message = await receiver.receive()

    handle = await client.send(
        msg_topic=canonical_message.msg_topic,
        msg_type=canonical_message.msg_type,
        msg_producer=canonical_message.msg_producer,
    )
    await service.handle()
    page = await client.receive(handle)
    canonical_msg_id = canonical_message.msg_id
    canonical_payload = canonical_message.payload
    received_msg_id = None
    received_payload = None
    if len(page.entries) == 1:
        received_entry = page.entries[0]
        received_msg_id = received_entry.msg_id
        received_payload = received_entry.payload

    print(f"{canonical_msg_id=}")
    print(f"{received_msg_id=}")
    id_success = received_msg_id == canonical_msg_id
    eq_string = "=="
    if not id_success:
        eq_string = "!="
    print("received_msg_id " + eq_string + " canonical_msg_id")

    print(f"{canonical_payload=}")
    print(f"{received_payload=}")
    payload_success = received_payload == canonical_payload
    eq_string = "=="
    if not payload_success:
        eq_string = "!="
    print("received_payload " + eq_string + " canonical_payload")

    success = id_success and payload_success
    print(f"({type(bus).__name__}): ", end="")
    if success:
        print("Async history facade selected captured message")
    else:
        print("Async history facade did not select captured message")
    print("\n")


def demo_register_emitter_rejects_invalid_message_names() -> None:
    print("Demo: register_emitter rejects invalid message names")
    bus = DirectMessageBus(capture_mode=CaptureMode.TRANSPORT_ONLY)
    expected_accepted_names = [
        (DEMO_TOPIC, DEMO_PRODUCER, DEMO_MSG_TYPE),
    ]
    received_accepted_names: list[tuple[str, str, str]] = []

    try:
        bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        received_accepted_names.append(
            (DEMO_TOPIC, DEMO_PRODUCER, DEMO_MSG_TYPE)
        )
    except InvalidMessageSymbolError:
        pass

    try:
        bus.register_emitter(
            msg_topic="foo topic.events",
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        received_accepted_names.append(
            ("foo topic.events", DEMO_PRODUCER, DEMO_MSG_TYPE)
        )
    except InvalidMessageSymbolError:
        pass

    try:
        bus.register_emitter(
            msg_topic="foo-topic..events",
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        received_accepted_names.append(
            ("foo-topic..events", DEMO_PRODUCER, DEMO_MSG_TYPE)
        )
    except InvalidMessageSymbolError:
        pass

    try:
        bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer="producer/corge",
            msg_type=DEMO_MSG_TYPE,
        )
        received_accepted_names.append(
            (DEMO_TOPIC, "producer/corge", DEMO_MSG_TYPE)
        )
    except InvalidMessageSymbolError:
        pass

    try:
        bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type="type.garply",
        )
        received_accepted_names.append(
            (DEMO_TOPIC, DEMO_PRODUCER, "type.garply")
        )
    except InvalidMessageSymbolError:
        pass

    print(f"{expected_accepted_names=}")
    print(f"{received_accepted_names=}")
    success = received_accepted_names == expected_accepted_names
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("received_accepted_names " + eq_string + " expected_accepted_names")

    print(f"({type(bus).__name__}.register_emitter): ", end="")
    if success:
        print("Only valid message names were accepted")
    else:
        print("Invalid message names were accepted")
    print("\n")


async def demo_async_socket_service_history_facade() -> None:
    print("Demo: async socket service history facade")
    with TemporaryDirectory() as runtime_dir:
        runtime_path = Path(runtime_dir)
        socket_path = runtime_path / "ropemother-async-history-demo.sock"
        capture_path = runtime_path / "ropemother-async-history-demo.jsonl"
        format_registry = default_portable_format_registry()
        bus = AsyncDirectMessageBus()
        sink = JSONLinesCaptureSink(capture_path, append=False)
        bus.set_capture_sink(sink)
        listener = AsyncLocalBusServiceListener.from_socket_path(socket_path)
        service = AsyncMessageBusService.from_listener(
            bus=bus, listener=listener
        )
        history = JSONLinesCaptureHistory(
            capture_path, extra_formats=format_registry.formats()
        )
        history_service = bus.create_history_service(
            history=history,
            request_topic="history.requests",
            reply_topic="history.replies",
            requester_producer="history-client",
            responder_producer="history-service",
            request_msg_type="history-request",
            reply_msg_type="history-reply",
        )
        service_task = asyncio.create_task(service.serve_forever())
        history_task = asyncio.create_task(history_service.handle())
        await asyncio.sleep(0)
        producer = await connect_async_message_bus(
            descriptor=service.connection_descriptor(),
            extra_formats=format_registry.formats(),
        )
        requester = await connect_async_message_bus(
            descriptor=service.connection_descriptor(),
            extra_formats=format_registry.formats(),
        )

        history_client = await requester.create_history_client(
            request_topic="history.requests",
            reply_topic="history.replies",
            requester_producer="history-client",
            responder_producer="history-service",
            request_msg_type="history-request",
            reply_msg_type="history-reply",
        )
        emitter = await producer.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        canonical_payload = {"foo": "bar"}
        await emitter.emit(canonical_payload)
        page = await history_client.select(
            msg_topic=DEMO_TOPIC, msg_type=DEMO_MSG_TYPE
        )
        recovered_payload = None
        if len(page.entries) == 1:
            recovered_payload = page.entries[0].payload
        await history_task
        producer.close()
        requester.close()
        service.request_stop()
        await service_task

        print(f"{canonical_payload=}")
        print(f"{recovered_payload=}")
        success = recovered_payload == canonical_payload
        eq_string = "=="
        if not success:
            eq_string = "!="
        print("recovered_payload " + eq_string + " canonical_payload")

        print(f"({type(service).__name__}): ", end="")
        if success:
            print("Async socket service exposed same-process history facade")
        else:
            print("Async socket service did not recover the expected payload")
        print("\n")


def demo_local_message_bus_host_broker_history() -> None:
    print("Demo: local message bus host preconfigured broker history")
    with TemporaryDirectory() as runtime_dir:
        runtime_path = Path(runtime_dir)
        capture_path = runtime_path / "capture.jsonl"
        format_registry = default_portable_format_registry()
        sink = JSONLinesCaptureSink(capture_path, append=False)
        history = JSONLinesCaptureHistory(
            capture_path, extra_formats=format_registry.formats()
        )

        host = LocalMessageBusHost(
            runtime_directory=runtime_path,
            capture_sink=sink,
            broker_extensions=[BrokerHistoryExtension(history)],
            extra_formats=format_registry.formats(),
        )
        host.start()

        producer_client = host.client("producer")
        history_requester_client = host.client("history-client")
        history_client = preconfigured_history_client(history_requester_client)
        emitter = producer_client.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
        )
        canonical_payload = {"foo": "bar"}
        emitter.emit(canonical_payload)
        page = history_client.select(
            msg_topic=DEMO_TOPIC,
            msg_type=DEMO_MSG_TYPE,
        )

        host.close()
        recovered_payload = None
        if len(page.entries) == 1:
            recovered_payload = page.entries[0].payload

    print(f"{canonical_payload=}")
    print(f"{recovered_payload=}")
    success = recovered_payload == canonical_payload
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("recovered_payload " + eq_string + " canonical_payload")

    print(f"({LocalMessageBusHost.__name__}): ", end="")
    if success:
        print("Preconfigured history selected captured service messages")
    else:
        print("Preconfigured history did not recover the expected payload")
    print("\n")


def demo_history_for_shares_live_format_registry() -> None:
    print("Demo: history_for shares live format registry")
    bus = DirectMessageBus(capture_sink=InMemoryCaptureSink())
    history = history_for(bus)
    emitter = bus.register_emitter(
        msg_topic=DEMO_TOPIC,
        msg_producer=DEMO_PRODUCER,
        msg_type=DEMO_MSG_TYPE,
        payload_format=DEMO_CUSTOM_BYTES_FORMAT,
    )
    canonical_payload = b"foo bar"
    emitter.emit(canonical_payload)

    page = history.select(
        msg_topic=DEMO_TOPIC,
        msg_type=DEMO_MSG_TYPE,
        msg_producer=DEMO_PRODUCER,
    )
    recovered_payload = None
    if len(page.entries) == 1:
        recovered_payload = page.entries[0].payload

    print(f"{canonical_payload=}")
    print(f"{recovered_payload=}")
    success = recovered_payload == canonical_payload
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("recovered_payload " + eq_string + " canonical_payload")

    print(f"({history_for.__name__}): ", end="")
    if success:
        print("Live history used a later registered payload format")
    else:
        print("Live history did not recover the custom payload")
    print("\n")


def demo_jsonl_capture_history_uses_extra_formats() -> None:
    print("Demo: JSONL capture history uses extra formats")
    with TemporaryDirectory() as runtime_dir:
        capture_path = Path(runtime_dir) / "capture.jsonl"
        sink = JSONLinesCaptureSink(capture_path, append=False)
        bus = DirectMessageBus(capture_sink=sink)
        emitter = bus.register_emitter(
            msg_topic=DEMO_TOPIC,
            msg_producer=DEMO_PRODUCER,
            msg_type=DEMO_MSG_TYPE,
            payload_format=DEMO_CUSTOM_BYTES_FORMAT,
        )
        canonical_payload = b"baz qux"
        emitter.emit(canonical_payload)
        history = JSONLinesCaptureHistory(
            capture_path, extra_formats=(DEMO_CUSTOM_BYTES_FORMAT,)
        )
        page = history.select(
            msg_topic=DEMO_TOPIC,
            msg_type=DEMO_MSG_TYPE,
            msg_producer=DEMO_PRODUCER,
        )

    recovered_payload = None
    if len(page.entries) == 1:
        recovered_payload = page.entries[0].payload

    print(f"{canonical_payload=}")
    print(f"{recovered_payload=}")
    success = recovered_payload == canonical_payload
    eq_string = "=="
    if not success:
        eq_string = "!="
    print("recovered_payload " + eq_string + " canonical_payload")

    print(f"({JSONLinesCaptureHistory.__name__}): ", end="")
    if success:
        print("Offline history decoded a custom extra format")
    else:
        print("Offline history did not recover the custom payload")
    print("\n")


def main() -> None:
    demo_basic_publish_subscribe()
    demo_capture_order()
    demo_late_capture_sink_replays_registered_symbols()
    demo_emit_time_message_type_override()
    demo_invalid_explicit_message_type_is_rejected()
    demo_reserved_topic_root_rejected()
    demo_capture_sink_required_before_delivery()
    demo_failed_serialization_delivers_nothing()
    demo_batch_receiver_handler()
    demo_receive_nowait()
    asyncio.run(demo_async_receive_waits_for_message())
    asyncio.run(demo_async_batch_receiver_handler())
    demo_register_emitter_frame_codec()
    demo_emit_frame_codec()
    demo_broker_transport_session_registers_emitter()
    demo_broker_transport_session_subscribes()
    demo_broker_transport_session_emits_payload()
    demo_transport_client_receives_from_another_endpoint()
    demo_socket_transport_client_receives_from_another_endpoint()
    demo_socket_transport_client_uses_session_runners()
    demo_zmq_transport_client_uses_session_runners()
    demo_zmq_transport_preserves_message_identity()
    demo_direct_request_reply()
    demo_client_request_reply()
    demo_client_request_reply_handle_matching()
    demo_direct_request_reply_facade()
    demo_transport_request_reply_facade()
    demo_request_client_service_facade()
    demo_procedure_facade()
    asyncio.run(demo_async_direct_request_reply())
    asyncio.run(demo_async_client_request_reply_handle_matching())
    asyncio.run(demo_async_request_client_service_facade())
    asyncio.run(demo_async_procedure_facade())
    demo_request_service_receive_nowait()
    demo_request_service_receive_available()
    demo_request_service_receive_many()
    demo_procedure_service_handle_nowait()
    demo_procedure_service_handle_available()
    demo_procedure_service_handle_many()
    asyncio.run(demo_async_request_service_receive_nowait())
    asyncio.run(demo_async_request_service_receive_available())
    asyncio.run(demo_async_request_service_receive_many())
    asyncio.run(demo_async_procedure_service_handle_nowait())
    asyncio.run(demo_async_procedure_service_handle_available())
    asyncio.run(demo_async_procedure_service_handle_many())
    demo_transport_procedure_facade()
    demo_socket_transport_procedure_facade()
    demo_transport_failed_serialization_delivers_nothing()
    demo_transport_only_mode_delivers_without_capture()
    demo_transport_only_client_routes_without_capture()
    asyncio.run(demo_async_frame_channel_waits_for_frame())
    asyncio.run(demo_async_broker_transport_session_registers_emitter())
    asyncio.run(demo_async_broker_transport_session_subscribes())
    asyncio.run(demo_async_broker_transport_session_emits_payload())
    asyncio.run(demo_async_transport_client_receives_payload())
    asyncio.run(demo_async_transport_client_receives_from_another_endpoint())
    asyncio.run(demo_async_transport_request_client_service_facade())
    asyncio.run(demo_async_transport_procedure_facade())
    asyncio.run(demo_immediate_async_endpoint_provisioner())
    demo_capture_bootstrap_lifecycle_facade()
    asyncio.run(demo_async_capture_bootstrap_lifecycle_facade())
    demo_broker_transport_session_emit_acknowledgement()
    demo_transport_emit_reports_rejection()
    asyncio.run(demo_async_transport_emit_reports_rejection())
    demo_message_bus_service_capture_bootstrap()
    demo_message_bus_service_contact_variable()
    demo_message_bus_service_contact_handoff()
    demo_message_bus_service_contact_helper()
    demo_message_bus_service_file_capture_bootstrap()
    demo_topic_tree_selector()
    demo_topic_selector_collection()
    demo_transport_client_topic_selector_collection()
    demo_producer_selector_collection()
    demo_transport_client_producer_selector_collection()
    demo_any_producer_subscription()
    demo_unlisted_type_format_allowed()
    demo_transport_client_additional_message_type()
    demo_transport_client_unlisted_message_type_allowed()
    asyncio.run(demo_async_transport_client_additional_message_type())
    asyncio.run(demo_async_transport_client_unlisted_message_type_allowed())
    demo_transport_client_unlisted_message_type_rejected()
    asyncio.run(demo_async_transport_client_unlisted_message_type_rejected())
    demo_local_message_bus_host_routes_between_clients()
    demo_transport_client_supported_type_formats()
    demo_request_client_service_type_format_support()
    demo_in_memory_capture_history_selects_messages()
    demo_history_query_service_selects_messages()
    asyncio.run(demo_async_history_query_service_selects_messages())
    demo_scripted_input_emits_file_events()
    demo_jsonl_capture_history_selects_messages()
    demo_jsonl_history_query_service_selects_messages()
    demo_jsonl_capture_history_reconstructs_payload_formats()
    asyncio.run(demo_async_jsonl_history_query_service_selects_messages())
    demo_history_client_service_facade()
    asyncio.run(demo_async_history_client_service_facade())
    demo_register_emitter_rejects_invalid_message_names()
    asyncio.run(demo_async_socket_service_history_facade())
    demo_local_message_bus_host_broker_history()
    demo_history_for_shares_live_format_registry()
    demo_jsonl_capture_history_uses_extra_formats()


if __name__ == "__main__":
    main()
