# ropemother

This is a developer release of `ropemother`, a Python package for building small message-oriented systems. It provides a low-configuration direct broker, readable topic/type/producer names, capture support, portable payload formats, and request/reply helpers for simple local services.

The direct broker is useful for local development, teaching, demos, and early integration work. It is not intended to be the final transport story for every deployment. The package is organized so code can begin against project-owned message concepts while leaving room for later transport adapters and stronger persistence infrastructure.

This preview does not claim to provide distributed consensus, production broker deployment, complete replay orchestration, or archive-level storage integration. It focuses on the local message model, capture behavior, request/reply helpers, and readable examples that can be used while the broader service architecture is still taking shape.

## Using the source checkout

Clone the repository into a workspace directory:

```sh
git clone https://github.com/edurange/ropemother.git
cd ropemother
```

Run preview commands from the repository root:

```sh
python -m ropemother.playground
```

Use the same working-directory setup for small scripts kept in the checkout:

```sh
python my_example.py
```

If an example also imports a companion source tree such as `intarsia`, keep the repositories next to each other and add the companion tree to `PYTHONPATH`:

```text
workspace/
    ropemother/
    intarsia/
```

```sh
PYTHONPATH="$PWD:../intarsia" python my_example.py
```

The freestanding broker uses the same source-checkout setup. Its startup and client commands are shown later in this README.

## Publish and subscribe

The direct broker routes a message from an emitter to every receiver whose subscription matches that message.

This example keeps the emitter and receivers in one Python session so you can run it locally. In a larger application, the producer and subscribers may belong to different components.

```python
from ropemother import DirectMessageBus, InMemoryCaptureSink

bus = DirectMessageBus(capture_sink=InMemoryCaptureSink())

emitter = bus.register_emitter(
    msg_topic="foo-topic.events",
    msg_producer="producer-corge",
    msg_type="type-garply",
)

first_receiver = bus.subscribe(
    msg_topic="foo-topic.events",
    msg_producer="producer-corge",
    msg_type="type-garply",
)

second_receiver = bus.subscribe(
    msg_topic="foo-topic.events",
    msg_producer="producer-corge",
    msg_type="type-garply",
)

canonical_payload = "hello from producer"

emitter.emit(canonical_payload)

first_message = first_receiver.receive()
second_message = second_receiver.receive()

print(first_message.payload)
print(second_message.payload)
```

Expected output:

```text
hello from producer
hello from producer
```

`producer-corge` identifies the component that produced the message. It does not identify either receiver.

Both receivers subscribed to messages from `producer-corge` on `foo-topic.events` with message type `type-garply`, so both receive the emitted payload.

## A small request/reply service

A service can be ordinary application code behind a message boundary. In this example, the service receives a string request, applies Python’s built-in `str.upper`, and sends the result back to the client.

`ropemother` provides the request/reply structure: the client sends a request message, the service handles it, and the client receives the reply. `str.upper` is a stand-in here for some application functionality hosted at a messaging endpoint, possibly a non-local one.

This is the first async example. The async code is here because the service must be waiting for a request while the client sends one and waits for the reply.

This example puts the client and service in the same Python session so you can run both sides locally. In a larger application, the client side and service side would usually belong to different components. The important point is that the client does not call `str.upper` directly; it sends a request message across a message boundary.

```python
import asyncio

from ropemother import AsyncDirectMessageBus, InMemoryCaptureSink

bus = AsyncDirectMessageBus(capture_sink=InMemoryCaptureSink())

# Client-side request endpoint
client = bus.create_procedure_client(
    request_topic="foo-topic.requests",
    reply_topic="foo-topic.replies",
    requester_producer="producer-corge",
    responder_producer="producer-grault",
    request_msg_type="type-garply",
    reply_msg_type="type-waldo",
)

# Service-side request handler
service = bus.create_procedure_service(
    request_topic="foo-topic.requests",
    reply_topic="foo-topic.replies",
    requester_producer="producer-corge",
    responder_producer="producer-grault",
    request_msg_type="type-garply",
    reply_msg_type="type-waldo",
    handler=str.upper,
)


async def run_one_request_reply_exchange() -> str:
    service_task = asyncio.create_task(service.handle())

    received_payload = await client("hello")

    await service_task

    return received_payload


received_payload = asyncio.run(run_one_request_reply_exchange())
print(received_payload)
```

Expected output:

```text
HELLO
```

`foo-topic.requests` is where the service receives requests. `foo-topic.replies` is where the client receives replies. `producer-corge` identifies the requesting component, and `producer-grault` identifies the responding component. `type-garply` identifies the request message type, and `type-waldo` identifies the reply message type.

`run_one_request_reply_exchange()` exists because the service handler and client call need to share one event loop. The service task waits for one request while the client sends `"hello"` and waits for the reply payload. The final `await service_task` lets the one-request service handler finish before the example exits.

A procedure client is callable. Calling the client uses ordinary Python function arguments and returns the reply payload. `call(...)` provides the same payload-returning operation with an explicit method name. `call_reply(...)` returns the full reply message rather than just the procedure result, which should only be necessary for advanced applications.

In the local example, both sides are visible. In application code, these responsibilities often separate.

The service side owns the handler:

```python
service = bus.create_procedure_service(
    request_topic="foo-topic.requests",
    reply_topic="foo-topic.replies",
    requester_producer="producer-corge",
    responder_producer="producer-grault",
    request_msg_type="type-garply",
    reply_msg_type="type-waldo",
    handler=str.upper,
)
```

The client side owns the request:

```python
client = bus.create_procedure_client(
    request_topic="foo-topic.requests",
    reply_topic="foo-topic.replies",
    requester_producer="producer-corge",
    responder_producer="producer-grault",
    request_msg_type="type-garply",
    reply_msg_type="type-waldo",
)

received = await client("hello")

# Use call_reply(...) when application code needs the reply message metadata.
reply = await client.call_reply("hello")
received_again = reply.payload
```

## Running a freestanding broker

The earlier examples create an in-process direct broker:

```python
from ropemother import DirectMessageBus, InMemoryCaptureSink

bus = DirectMessageBus(capture_sink=InMemoryCaptureSink())
```

For application-scale work, it will often be useful to run the bus as a freestanding broker and connect client processes to it.

Start the broker in one terminal:

```sh
python -m ropemother.service
```

The broker prints both the explicit broker URI and an environment-variable form:

```text
Message bus broker is running
broker URI: ropemother+unix:///...
environment: ROPEMOTHER_CONNECTION_DESCRIPTOR=ropemother+unix:///...
Press Ctrl-C to stop
```

For teaching examples, copy the printed broker URI directly into the client code. Each client process uses that URI to find the broker.

### Subscriber process

Save this as `subscriber.py`, then run it in a second terminal. It waits for one message.

```python
from ropemother import connect_message_bus

broker_uri = "ropemother+unix:///..."
bus = connect_message_bus(broker_uri)

receiver = bus.subscribe(
    msg_topic="foo-topic.events",
    msg_producer="producer-corge",
    msg_type="type-garply",
)

message = receiver.receive()
print(message.payload)

bus.close()
```

Run it while the broker is still running:

```sh
python subscriber.py
```

### Producer process

Save this as `producer.py`, then run it in a third terminal.

```python
from ropemother import connect_message_bus

broker_uri = "ropemother+unix:///..."
bus = connect_message_bus(broker_uri)

emitter = bus.register_emitter(
    msg_topic="foo-topic.events",
    msg_producer="producer-corge",
    msg_type="type-garply",
)

emitter.emit("hello from producer")

bus.close()
```

Run it with the same broker URI in the script:

```sh
python producer.py
```

As a deployment or convenience option, a process may also read the broker URI from the environment variable `ROPEMOTHER_CONNECTION_DESCRIPTOR` when no URI is provided:

```python
from ropemother import connect_message_bus

bus = connect_message_bus()
```

Use that form when the environment is responsible for providing the connection descriptor.

The subscriber prints:

```text
hello from producer
```

With `DirectMessageBus`, the broker object is created inside the Python process. With `connect_message_bus()`, the Python process connects to the freestanding broker. The endpoint vocabulary stays the same: register an emitter, subscribe a receiver, emit a message, and receive a message.

## Capture, logging, and history

Capture records the symbol registrations and messages that pass through the broker. It is the normal posture for `ropemother`, because later inspection, replay-oriented tools, and history queries only make sense when the run has preserved an interpretable message log.

For small local examples, the capture sink can be attached when the bus is constructed:

```python
bus = DirectMessageBus(capture_sink=InMemoryCaptureSink())
```

Managed applications may create the bus before the final capture sink is ready:

```python
bus = DirectMessageBus()

# Startup and registration work may happen here.

bus.set_capture_sink(capture_sink)
```

This second form is intended for service-style startup, where the bus and its capture sink may have different readiness lifecycles. While capture is enabled but no sink is attached, the bus may perform limited bootstrap work, but ordinary emitted messages are rejected rather than delivered without capture.

For the freestanding broker, the default command starts a local broker with capture enabled and writes captured records to `.ropemother/capture.jsonl`:

```sh
python -m ropemother.service
```

Use `--history` to start the broker's built-in history service, so application code can query captured message history:

```sh
python -m ropemother.service --history
```

Use `--capture-path` to choose a different JSON Lines capture file.

Use `--transport-only` only when you explicitly want a no-capture broker for local transport experiments. Transport-only mode routes messages without capture, history, or replay guarantees.

History is the application-facing way to ask about prior messages. Most application code should not parse the capture JSON Lines file directly.

Captured payloads are intended to be portable. A value that can be handed across an in-process receiver queue is not automatically a good persistent or cross-runtime payload. The preview includes basic JSON and raw-byte formats for adoption, prototyping, capture, and public-boundary interoperability; projects can add dedicated formats for their own message families later. Stable project interfaces should usually prefer narrow message contracts over treating generic JSON structures as the internal data model.

## Query broker history from application code

Start a freestanding broker and enable its built-in history service:

```sh
python -m ropemother.service --history
```

The broker prints a URI. Copy that URI into a client script.

Save this as `history_query.py`:

```python
from ropemother import connect_message_bus
from ropemother.service import preconfigured_history_client

broker_uri = "ropemother+unix:///..."
bus = connect_message_bus(broker_uri)

emitter = bus.register_emitter(
    msg_topic="foo-topic.events",
    msg_producer="producer-corge",
    msg_type="type-plugh",
)

history_client = preconfigured_history_client(bus)

emitter.emit("captured event")

page = history_client.select(
    msg_topic="foo-topic.events",
    msg_producer="producer-corge",
    msg_type="type-plugh",
)

print(page.entries[0].payload)

bus.close()
```

Run it with the broker's history service running:

```sh
python history_query.py
```

Expected output:

```text
captured event
```

The application code does not open the capture file and does not create the history service. It connects to the broker and sends a history request through the broker's built-in history service contract.

`preconfigured_history_client(...)` is the local/default wiring helper for that built-in broker history profile. It supplies the fixed request topic, reply topic, producer names, message types, and payload formats used by `python -m ropemother.service --history`.

That helper does not create history by magic, and it is not the general custom-service API. It is a shortcut over `create_history_client(...)` for this one preconfigured service contract. Code that uses a custom history service should call `create_history_client(...)` directly with explicit topics, producers, message types, and payload formats. A custom service should be constructed on the service side with matching `create_history_service(...)` parameters.

## Executable demos

The `playground.py` file contains executable demonstrations of more `ropemother` behavior.

The playground is intentionally more verbose than the README examples. It prints intermediate values, compares sent and received messages, and shows several feature combinations in one place. Use it when you want to see working examples beyond the short copy-paste sections above.

The playground is a preview-era teaching and validation file. Over time, smaller examples and formal tests should replace some of its responsibilities.

## Preview status

`ropemother` is a preview package. The current iteration focuses on the local message model, direct broker behavior, capture, request/reply helpers, history queries, and a freestanding broker process for local development.

The preview does not claim to provide a production distributed broker, consensus ordering, complete replay orchestration, or final transport infrastructure.

The direct broker is useful for local development and early integration work. Code that uses `register_emitter(...)`, `subscribe(...)`, `emit(...)`, `receive()`, request/reply clients, and history clients should remain close to the intended public workflow as stronger transport and persistence pieces are added.
