#!/usr/bin/env python3
# ropemother/client/asyncrequest.py

"""Asynchronous request/reply client and service helpers."""

from collections.abc import Awaitable, Callable
from typing import Any

from ropemother.broker.asyncendpoints import AsyncEmitter, AsyncReceiver
from ropemother.client.procedure import (
    ProcedureInvocation,
    ensure_procedure_invocation,
)
from ropemother.client.request import (
    CompletedServiceRequestError,
    InvalidRequestOptionError,
    RequestClientLimits,
    RequestHandle,
    UnexpectedRequestMessageError,
    UnboundResponderError,
    _ReplyBuffer,
    _RequestLifecycleTable,
)
from ropemother.client.requestoptions import RequestOption, SAME_MSG_TYPE
from ropemother.format.portableformat import PortableFormat
from ropemother.message.messageidentity import CorrelationID
from ropemother.message.records import BusOperation, ReceivedMessage

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-06T06:22:21+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


class AsyncRequester:
    """Async helper that sends requests and receives replies."""
    _emitter: AsyncEmitter
    _reply_receiver: AsyncReceiver
    _next_correlation_value: int
    _limits: RequestClientLimits
    _reply_buffer: _ReplyBuffer

    def __init__(
        self,
        emitter: AsyncEmitter,
        reply_receiver: AsyncReceiver,
        limits: RequestClientLimits | None = None,
    ) -> None:
        self._emitter = emitter
        self._reply_receiver = reply_receiver
        self._next_correlation_value = 0
        self._limits = RequestClientLimits()
        if limits is not None:
            self._limits = limits
        self._reply_buffer = _ReplyBuffer(self._limits)

    async def request(
        self,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> RequestHandle:
        correlation_id = CorrelationID(self._next_correlation_value)
        self._next_correlation_value += 1
        await self._emitter.emit_request(
            payload,
            correlation_id=correlation_id,
            msg_type=msg_type,
            payload_format=payload_format,
        )
        return RequestHandle(correlation_id)

    async def receive_reply(self, request: RequestHandle) -> ReceivedMessage:
        reply = self._reply_buffer.take(request)
        while reply is None:
            message = await self._reply_receiver.receive()
            self._reply_buffer.validate(message)
            if self._reply_buffer.matches(request, message):
                reply = message
            else:
                self._reply_buffer.hold(message)
        return reply


class AsyncResponder:
    """Async helper that receives requests and sends replies."""
    _emitter: AsyncEmitter
    _request_receiver: AsyncReceiver | None

    def __init__(
        self,
        emitter: AsyncEmitter,
        request_receiver: AsyncReceiver | None = None,
    ) -> None:
        self._emitter = emitter
        self._request_receiver = request_receiver

    async def receive(self) -> ReceivedMessage:
        if self._request_receiver is None:
            raise UnboundResponderError(
                "responder does not have a request receiver"
            )

        message = await self._request_receiver.receive()
        if message.bus_operation != BusOperation.REQUEST:
            raise UnexpectedRequestMessageError(
                "responder received a message that is not a request"
            )

        return message

    def receive_nowait(self) -> ReceivedMessage | None:
        if self._request_receiver is None:
            raise UnboundResponderError(
                "responder does not have a request receiver"
            )

        message = self._request_receiver.receive_nowait()
        if message is not None:
            if message.bus_operation != BusOperation.REQUEST:
                raise UnexpectedRequestMessageError(
                    "responder received a message that is not a request"
                )

        return message

    def receive_available(self) -> list[ReceivedMessage]:
        if self._request_receiver is None:
            raise UnboundResponderError(
                "responder does not have a request receiver"
            )

        messages = self._request_receiver.receive_available()
        for message in messages:
            if message.bus_operation != BusOperation.REQUEST:
                raise UnexpectedRequestMessageError(
                    "responder received a message that is not a request"
                )

        return messages

    async def receive_many(self, max_count: int) -> list[ReceivedMessage]:
        if self._request_receiver is None:
            raise UnboundResponderError(
                "responder does not have a request receiver"
            )

        messages = await self._request_receiver.receive_many(max_count)
        for message in messages:
            if message.bus_operation != BusOperation.REQUEST:
                raise UnexpectedRequestMessageError(
                    "responder received a message that is not a request"
                )

        return messages

    async def reply(
        self,
        request: ReceivedMessage,
        payload: Any,
        *,
        msg_type: str | RequestOption | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        reply_msg_type = msg_type
        if msg_type is SAME_MSG_TYPE:
            reply_msg_type = request.msg_type
        elif isinstance(msg_type, str) or msg_type is None:
            reply_msg_type = msg_type
        else:
            raise InvalidRequestOptionError("unknown request option")

        await self._emitter.emit_reply(
            request,
            payload,
            msg_type=reply_msg_type,
            payload_format=payload_format,
        )


class AsyncServiceRequest:
    """Async received request that can send one reply."""
    _responder: AsyncResponder
    _message: ReceivedMessage
    _replied: bool

    def __init__(
        self, responder: AsyncResponder, message: ReceivedMessage
    ) -> None:
        self._responder = responder
        self._message = message
        self._replied = False

    @property
    def payload(self) -> Any:
        return self._message.payload

    async def reply(
        self,
        payload: Any,
        *,
        msg_type: str | RequestOption | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        if self._replied:
            raise CompletedServiceRequestError(
                "service request has already received a reply"
            )

        await self._responder.reply(
            self._message,
            payload,
            msg_type=msg_type,
            payload_format=payload_format,
        )
        self._replied = True


class AsyncRequestService:
    """Async service endpoint for receiving requests and sending replies."""
    _responder: AsyncResponder

    def __init__(self, responder: AsyncResponder) -> None:
        self._responder = responder

    async def receive(self) -> AsyncServiceRequest:
        message = await self._responder.receive()
        request = AsyncServiceRequest(self._responder, message)
        return request

    def receive_nowait(self) -> AsyncServiceRequest | None:
        message = self._responder.receive_nowait()
        request = None
        if message is not None:
            request = AsyncServiceRequest(self._responder, message)
        return request

    def receive_available(self) -> list[AsyncServiceRequest]:
        messages = self._responder.receive_available()
        requests = []
        for message in messages:
            request = AsyncServiceRequest(self._responder, message)
            requests.append(request)
        return requests

    async def receive_many(self, max_count: int) -> list[AsyncServiceRequest]:
        messages = await self._responder.receive_many(max_count)
        requests = []
        for message in messages:
            request = AsyncServiceRequest(self._responder, message)
            requests.append(request)
        return requests


class AsyncRequestClient:
    """Async client endpoint for sending requests and receiving replies."""
    _requester: AsyncRequester
    _limits: RequestClientLimits
    _requests: _RequestLifecycleTable

    def __init__(
        self,
        requester: AsyncRequester,
        limits: RequestClientLimits | None = None,
    ) -> None:
        self._requester = requester
        if limits is None:
            self._limits = RequestClientLimits()
        else:
            self._limits = limits
        self._requests = _RequestLifecycleTable(self._limits)

    async def send(
        self,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> RequestHandle:
        self._requests.ensure_can_start()
        handle = await self._requester.request(
            payload, msg_type=msg_type, payload_format=payload_format
        )
        self._requests.add_pending(handle)
        return handle

    async def receive(self, handle: RequestHandle) -> ReceivedMessage:
        self._requests.validate_pending(handle)
        reply = await self._requester.receive_reply(handle)
        self._requests.complete(handle)
        return reply

    async def call(
        self,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> ReceivedMessage:
        handle = await self.send(
            payload, msg_type=msg_type, payload_format=payload_format
        )
        return await self.receive(handle)


AsyncProcedureHandler = Callable[..., Awaitable[Any] | Any]


class AsyncProcedureClient:
    """Async client wrapper for request/reply procedure calls."""
    _client: AsyncRequestClient

    def __init__(self, client: AsyncRequestClient) -> None:
        self._client = client

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.call(*args, **kwargs)

    async def call(self, *args: Any, **kwargs: Any) -> Any:
        reply = await self.call_reply(*args, **kwargs)
        return reply.payload

    async def call_reply(
        self, *args: Any, **kwargs: Any
    ) -> ReceivedMessage:
        invocation = ProcedureInvocation.from_call(*args, **kwargs)
        return await self._client.call(invocation)


class AsyncProcedureService:
    """Async service wrapper that handles procedure-style requests."""
    _service: AsyncRequestService
    _handler: AsyncProcedureHandler

    def __init__(
        self, service: AsyncRequestService, handler: AsyncProcedureHandler
    ) -> None:
        self._service = service
        self._handler = handler

    async def handle(self) -> None:
        request = await self._service.receive()
        await self._handle_request(request)

    async def handle_nowait(self) -> bool:
        request = self._service.receive_nowait()
        handled = False
        if request is not None:
            await self._handle_request(request)
            handled = True
        return handled

    async def handle_available(self) -> int:
        requests = self._service.receive_available()
        for request in requests:
            await self._handle_request(request)
        return len(requests)

    async def handle_many(self, max_count: int) -> int:
        requests = await self._service.receive_many(max_count)
        for request in requests:
            await self._handle_request(request)
        return len(requests)

    async def _handle_request(self, request: AsyncServiceRequest) -> None:
        invocation = ensure_procedure_invocation(request.payload)
        keyword_arguments = invocation.keyword_argument_dict()
        reply = self._handler(
            *invocation.positional_arguments, **keyword_arguments
        )
        reply_payload = await _resolve_procedure_reply(reply)
        await request.reply(reply_payload)


async def _resolve_procedure_reply(reply: Any | Awaitable[Any]) -> Any:
    reply_payload = reply
    if isinstance(reply, Awaitable):
        reply_payload = await reply
    return reply_payload
