# Ropemother

`ropemother` is a Python package for building small message-oriented systems. It provides publish-subscribe messaging, request/reply helpers, capture and history support, portable payload formats, an in-process direct broker, and a freestanding broker for communication between local processes.

The current developer release is intended for teaching, research software, local development, and early integration work.

## Installation

`ropemother` requires Python 3.13 or newer.

Install the current developer release from PyPI:

```sh
python -m pip install --pre ropemother
```

The base package has no required third-party runtime dependencies. The exploratory ZeroMQ transport is available as an optional extra:

```sh
python -m pip install --pre "ropemother[zmq]"
```

## Publish and subscribe

The examples below introduce the main public messaging interfaces with small programs.

A direct message bus can route a message from one emitter to every receiver whose subscription matches that message.

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

emitter.emit("hello from producer")

print(first_receiver.receive().payload)
print(second_receiver.receive().payload)
```

Expected output:

```text
hello from producer
hello from producer
```

`msg_topic`, `msg_producer`, and `msg_type` describe the messages that an endpoint emits or receives. In the example, both receivers select messages produced by `producer-corge` on `foo-topic.events` with message type `type-garply`, so both receive the same broadcast message.

The direct broker is useful when the participating components can share one Python process. The same endpoint vocabulary is also used with the freestanding broker described below.

## Running a freestanding broker

A broker coordinates message delivery. The freestanding broker lets separate local processes connect to the same message bus.

Start a local broker in one terminal:

```sh
python -m ropemother.service
```

The broker prints its connection descriptor and an environment-variable form that client processes can use:

```text
Message bus broker is running
broker URI: ropemother+unix:///...
environment: ROPEMOTHER_CONNECTION_DESCRIPTOR=ropemother+unix:///...
Press Ctrl-C to stop
```

The connection descriptor identifies the broker endpoint that client processes use to find the same message bus.

A client can connect using the printed descriptor explicitly:

```python
from ropemother import connect_message_bus

bus = connect_message_bus("ropemother+unix:///...")
```

If `ROPEMOTHER_CONNECTION_DESCRIPTOR` is already set in the environment, the descriptor can be omitted:

```python
from ropemother import connect_message_bus

bus = connect_message_bus()
```

Once connected, client code elsewhere uses the same `register_emitter(...)`, `subscribe(...)`, `emit(...)`, and `receive()` operations as a direct bus.

For example, a subscriber process can wait for one message:

```python
from ropemother import connect_message_bus

bus = connect_message_bus()
receiver = bus.subscribe(
    msg_topic="foo-topic.events",
    msg_producer="producer-corge",
    msg_type="type-garply",
)

message = receiver.receive()
print(message.payload)

bus.close()
```

A separate producer process can publish the message:

```python
from ropemother import connect_message_bus

bus = connect_message_bus()
emitter = bus.register_emitter(
    msg_topic="foo-topic.events",
    msg_producer="producer-corge",
    msg_type="type-garply",
)

emitter.emit("hello from producer")
bus.close()
```

The broker allows the producer, subscriber, and other services to have independent process lifetimes while preserving the same application-facing message model.

## Request and reply

A request/reply service places an application operation behind a message boundary. The client sends a request and waits for the correlated reply instead of calling the service implementation directly.

The following local example uses `str.upper` as the service operation:

```python
import asyncio

from ropemother import AsyncDirectMessageBus, InMemoryCaptureSink

bus = AsyncDirectMessageBus(capture_sink=InMemoryCaptureSink())

client = bus.create_procedure_client(
    request_topic="foo-topic.requests",
    reply_topic="foo-topic.replies",
    requester_producer="producer-corge",
    responder_producer="producer-grault",
    request_msg_type="type-garply",
    reply_msg_type="type-waldo",
)

service = bus.create_procedure_service(
    request_topic="foo-topic.requests",
    reply_topic="foo-topic.replies",
    requester_producer="producer-corge",
    responder_producer="producer-grault",
    request_msg_type="type-garply",
    reply_msg_type="type-waldo",
    handler=str.upper,
)


async def run_one_request() -> str:
    service_task = asyncio.create_task(service.handle())
    result = await client("hello")
    await service_task
    return result


print(asyncio.run(run_one_request()))
```

Expected output:

```text
HELLO
```

A procedure client is callable and returns the reply payload. `client.call(...)` provides the same payload-returning operation with an explicit method name, while `client.call_reply(...)` returns the full reply message when application code also needs its message metadata.

## Capture and history

Capture records messages and the information needed to interpret them later.

Small in-process applications can supply a capture sink when constructing a direct bus:

```python
from ropemother import DirectMessageBus, InMemoryCaptureSink

bus = DirectMessageBus(capture_sink=InMemoryCaptureSink())
```

The freestanding broker enables capture by default and writes JSON Lines records to `.ropemother/capture.jsonl`:

```sh
python -m ropemother.service
```

Use `--capture-path` to choose another capture file. Use `--transport-only` only when a local transport experiment explicitly does not need capture, history, or replay guarantees.

History is the application-facing way to query captured messages. Start the broker with its built-in history service:

```sh
python -m ropemother.service --history
```

Then create the preconfigured client for that service:

```python
from ropemother import connect_message_bus
from ropemother.service import preconfigured_history_client

bus = connect_message_bus()

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

Expected output:

```text
captured event
```

Application code queries the history service through the bus rather than opening the capture file directly. `preconfigured_history_client(...)` supplies the fixed contract used by the broker's built-in history service. Applications that define a different history service can use the explicit history client and service constructors instead.

## Portable payloads

Portable formats serialize payloads for capture and transport. `ropemother` includes JSON and raw-byte formats and supports project-defined formats for other application data.

## Using the source checkout

Clone the repository and install its development dependencies into an environment chosen for development work:

```sh
git clone https://github.com/edurange/ropemother.git
cd ropemother
python -m pip install -e ".[dev]"
```

The source repository is available at:

<https://github.com/edurange/ropemother>

Issues are tracked at:

<https://github.com/edurange/ropemother/issues>

See `CONTRIBUTING.md` before preparing a substantial change.

### Executable development demos

`ropemother/playground.py` contains chronological executable demonstrations used for development, inspection, and smoke checking:

```sh
python -m ropemother.playground
```

The playground is intentionally more verbose than the short examples in this README and is mainly useful for development and inspection.

## Supplementary exercises

Exercises and tutorial materials are available at:

<https://github.com/edurange/ropemother-exercises>

## Development status

`ropemother` is a developer release. The current implementation covers the local message model, direct and freestanding broker operation, portable payload formats, capture, history queries, and request/reply helpers.

Interfaces may still change during the developer-release series.

## License

Ropemother is released under the MIT License. See `LICENSE` for details.
