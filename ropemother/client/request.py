#!/usr/bin/env python3
# ropemother/client/request.py

"""Synchronous request/reply client and service helpers."""

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ropemother.broker.endpoints import Emitter, ReceiveEndpoint, Receiver
from ropemother.client.procedure import (
    ProcedureInvocation,
    ensure_procedure_invocation,
)
from ropemother.client.requestoptions import RequestOption, SAME_MSG_TYPE
from ropemother.exceptions import MessageBusBaseException
from ropemother.format.portableformat import PortableFormat
from ropemother.message.messageidentity import CorrelationID
from ropemother.message.records import BusOperation, ReceivedMessage

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-06T06:15:49+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


class ClientRequestError(MessageBusBaseException):
    """Base exception for request/reply client errors."""
    pass


class InvalidRequestOptionError(ValueError, ClientRequestError):
    """Raised when request/reply options are invalid."""
    pass


class UnboundResponderError(RuntimeError, ClientRequestError):
    """Raised when a responder is used without a reply binding."""
    pass


class UnexpectedRequestMessageError(ValueError, ClientRequestError):
    """Raised when a received message is not a valid request."""
    pass


class UnexpectedReplyMessageError(ValueError, ClientRequestError):
    """Raised when a received message is not a valid reply."""
    pass


class UnknownRequestHandleError(ValueError, ClientRequestError):
    """Raised when a request handle is not managed by this client."""
    pass


class CompletedRequestHandleError(RuntimeError, ClientRequestError):
    """Raised when a completed request handle is reused."""
    pass


class CompletedServiceRequestError(RuntimeError, ClientRequestError):
    """Raised when a service request receives more than one reply."""
    pass


class DuplicateReplyError(RuntimeError, ClientRequestError):
    """Raised when multiple replies arrive for one request."""
    pass


class RequestClientCapacityError(RuntimeError, ClientRequestError):
    """Raised when a request client has too many pending requests."""
    pass


class ReplyBufferCapacityError(RuntimeError, ClientRequestError):
    """Raised when unmatched replies exceed the reply buffer capacity."""
    pass


@dataclass(frozen=True)
class RequestClientLimits:
    """Capacity limits for a request/reply client."""
    max_pending: int = 1024
    max_ready_replies: int = 1024
    max_retired: int = 1024


@dataclass(frozen=True)
class RequestHandle:
    """Opaque handle for receiving a pending request reply."""
    _correlation_id: CorrelationID


@dataclass(frozen=True)
class _PendingRequest:
    handle: RequestHandle
    correlation_id: CorrelationID


class _RetiredCorrelationCache:
    _max_size: int
    _order: deque[CorrelationID]
    _members: set[CorrelationID]

    def __init__(self, max_size: int) -> None:
        self._max_size = max_size
        self._order = deque()
        self._members = set()

    def add(self, correlation_id: CorrelationID) -> None:
        if self._max_size == 0:
            return

        if correlation_id in self._members:
            return

        self._order.append(correlation_id)
        self._members.add(correlation_id)

        while len(self._order) > self._max_size:
            evicted = self._order.popleft()
            self._members.remove(evicted)

    def contains(self, correlation_id: CorrelationID) -> bool:
        return correlation_id in self._members


class _ReplyBuffer:
    _limits: RequestClientLimits
    _held_replies: dict[CorrelationID, ReceivedMessage]

    def __init__(self, limits: RequestClientLimits) -> None:
        self._limits = limits
        self._held_replies = {}

    def take(self, request: RequestHandle) -> ReceivedMessage | None:
        correlation_id = request._correlation_id
        return self._held_replies.pop(correlation_id, None)

    def hold(self, message: ReceivedMessage) -> None:
        correlation_id = message.correlation_id
        if correlation_id is None:
            raise UnexpectedReplyMessageError(
                "requester received a reply without a correlation ID"
            )

        if correlation_id in self._held_replies:
            raise DuplicateReplyError(
                "requester received multiple replies for one request"
            )

        if len(self._held_replies) >= self._limits.max_ready_replies:
            raise ReplyBufferCapacityError(
                "requester reply buffer capacity was exceeded"
            )

        self._held_replies[correlation_id] = message

    def matches(
        self, request: RequestHandle, message: ReceivedMessage
    ) -> bool:
        reply_matches = (
            message.bus_operation == BusOperation.REPLY
            and message.correlation_id == request._correlation_id
        )
        return reply_matches

    def validate(self, message: ReceivedMessage) -> None:
        if message.bus_operation != BusOperation.REPLY:
            raise UnexpectedReplyMessageError(
                "requester received a message that is not a reply"
            )
        if message.correlation_id is None:
            raise UnexpectedReplyMessageError(
                "requester received a reply without a correlation ID"
            )


class _RequestLifecycleTable:
    _pending: dict[CorrelationID, _PendingRequest]
    _retired: _RetiredCorrelationCache
    _limits: RequestClientLimits

    def __init__(self, limits: RequestClientLimits) -> None:
        self._pending = {}
        self._limits = limits
        self._retired = _RetiredCorrelationCache(limits.max_retired)

    def ensure_can_start(self) -> None:
        if len(self._pending) >= self._limits.max_pending:
            raise RequestClientCapacityError(
                "request client pending capacity was exceeded"
            )

    def add_pending(self, handle: RequestHandle) -> None:
        correlation_id = handle._correlation_id
        self._pending[correlation_id] = _PendingRequest(
            handle=handle, correlation_id=correlation_id
        )

    def validate_pending(self, handle: RequestHandle) -> None:
        correlation_id = handle._correlation_id
        if correlation_id in self._pending:
            return

        if self._retired.contains(correlation_id):
            raise CompletedRequestHandleError(
                "request handle has already received a reply"
            )

        raise UnknownRequestHandleError(
            "request handle is not managed by this request client"
        )

    def complete(self, handle: RequestHandle) -> None:
        self.validate_pending(handle)
        correlation_id = handle._correlation_id
        del self._pending[correlation_id]
        self._retired.add(correlation_id)


class Requester:
    """Low-level helper that sends requests and receives replies."""
    _emitter: Emitter
    _reply_receiver: ReceiveEndpoint
    _next_correlation_value: int
    _limits: RequestClientLimits
    _reply_buffer: _ReplyBuffer

    def __init__(
        self,
        emitter: Emitter,
        reply_receiver: ReceiveEndpoint,
        limits: RequestClientLimits | None = None,
    ) -> None:
        self._emitter = emitter
        self._reply_receiver = reply_receiver
        self._next_correlation_value = 0
        self._limits = RequestClientLimits()
        if limits is not None:
            self._limits = limits
        self._reply_buffer = _ReplyBuffer(self._limits)

    def request(
        self,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> RequestHandle:
        correlation_id = CorrelationID(self._next_correlation_value)
        self._next_correlation_value += 1
        self._emitter.emit_request(
            payload,
            correlation_id=correlation_id,
            msg_type=msg_type,
            payload_format=payload_format,
        )
        return RequestHandle(correlation_id)

    def receive_reply(self, request: RequestHandle) -> ReceivedMessage:
        reply = self._reply_buffer.take(request)
        while reply is None:
            message = self._reply_receiver.receive()
            self._reply_buffer.validate(message)
            if self._reply_buffer.matches(request, message):
                reply = message
            else:
                self._reply_buffer.hold(message)
        return reply


class Responder:
    """Low-level helper that receives requests and sends replies."""
    _emitter: Emitter
    _request_receiver: Receiver | None

    def __init__(
        self,
        emitter: Emitter,
        request_receiver: Receiver | None = None,
    ) -> None:
        self._emitter = emitter
        self._request_receiver = request_receiver

    def receive(self) -> ReceivedMessage:
        if self._request_receiver is None:
            raise UnboundResponderError(
                "responder does not have a request receiver"
            )

        message = self._request_receiver.receive()
        self._validate_request_message(message)
        return message

    def receive_nowait(self) -> ReceivedMessage | None:
        if self._request_receiver is None:
            raise UnboundResponderError(
                "responder does not have a request receiver"
            )

        message = self._request_receiver.receive_nowait()
        if message is not None:
            self._validate_request_message(message)
        return message

    def receive_available(self) -> list[ReceivedMessage]:
        if self._request_receiver is None:
            raise UnboundResponderError(
                "responder does not have a request receiver"
            )

        messages = self._request_receiver.receive_available()
        for message in messages:
            self._validate_request_message(message)
        return messages

    def receive_many(self, max_count: int) -> list[ReceivedMessage]:
        if self._request_receiver is None:
            raise UnboundResponderError(
                "responder does not have a request receiver"
            )

        messages = self._request_receiver.receive_many(max_count)
        for message in messages:
            self._validate_request_message(message)
        return messages

    def reply(
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
        self._emitter.emit_reply(
            request,
            payload,
            msg_type=reply_msg_type,
            payload_format=payload_format,
        )

    def _validate_request_message(self, message: ReceivedMessage) -> None:
        if message.bus_operation != BusOperation.REQUEST:
            raise UnexpectedRequestMessageError(
                "responder received a message that is not a request"
            )


class ServiceRequest:
    """Received request that can send one reply."""
    _responder: Responder
    _message: ReceivedMessage
    _replied: bool

    def __init__(self, responder: Responder, message: ReceivedMessage) -> None:
        self._responder = responder
        self._message = message
        self._replied = False

    @property
    def payload(self) -> Any:
        return self._message.payload

    def reply(
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

        self._responder.reply(
            self._message,
            payload,
            msg_type=msg_type,
            payload_format=payload_format,
        )
        self._replied = True


class RequestService:
    """Service endpoint for receiving requests and sending replies."""
    _responder: Responder

    def __init__(self, responder: Responder) -> None:
        self._responder = responder

    def receive(self) -> ServiceRequest:
        message = self._responder.receive()
        return ServiceRequest(self._responder, message)

    def receive_nowait(self) -> ServiceRequest | None:
        message = self._responder.receive_nowait()
        request = None
        if message is not None:
            request = ServiceRequest(self._responder, message)

        return request

    def receive_available(self) -> list[ServiceRequest]:
        messages = self._responder.receive_available()
        requests = []
        for message in messages:
            request = ServiceRequest(self._responder, message)
            requests.append(request)

        return requests


    def receive_many(self, max_count: int) -> list[ServiceRequest]:
        messages = self._responder.receive_many(max_count)
        requests = []
        for message in messages:
            request = ServiceRequest(self._responder, message)
            requests.append(request)
        return requests


class RequestClient:
    """Client endpoint for sending requests and receiving replies."""
    _requester: Requester
    _limits: RequestClientLimits
    _requests: _RequestLifecycleTable

    def __init__(
        self, requester: Requester, limits: RequestClientLimits | None = None
    ) -> None:
        self._requester = requester
        if limits is None:
            self._limits = RequestClientLimits()
        else:
            self._limits = limits
        self._requests = _RequestLifecycleTable(self._limits)

    def send(
        self,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> RequestHandle:
        self._requests.ensure_can_start()
        handle = self._requester.request(
            payload, msg_type=msg_type, payload_format=payload_format
        )
        self._requests.add_pending(handle)
        return handle

    def receive(self, handle: RequestHandle) -> ReceivedMessage:
        self._requests.validate_pending(handle)
        reply = self._requester.receive_reply(handle)
        self._requests.complete(handle)
        return reply

    def call(
        self,
        payload: Any,
        *,
        msg_type: str | None = None,
        payload_format: PortableFormat[Any, Any] | None = None,
    ) -> ReceivedMessage:
        handle = self.send(
            payload, msg_type=msg_type, payload_format=payload_format
        )
        return self.receive(handle)


ProcedureHandler = Callable[..., Any]


class ProcedureClient:
    """Client wrapper for request/reply procedure calls."""
    _client: RequestClient

    def __init__(self, client: RequestClient) -> None:
        self._client = client

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.call(*args, **kwargs)

    def call(self, *args: Any, **kwargs: Any) -> Any:
        reply = self.call_reply(*args, **kwargs)
        return reply.payload

    def call_reply(self, *args: Any, **kwargs: Any) -> ReceivedMessage:
        invocation = ProcedureInvocation.from_call(*args, **kwargs)
        return self._client.call(invocation)


class ProcedureService:
    """Service wrapper that handles procedure-style requests."""
    _service: RequestService
    _handler: ProcedureHandler

    def __init__(
        self, service: RequestService, handler: ProcedureHandler
    ) -> None:
        self._service = service
        self._handler = handler

    def handle(self) -> None:
        request = self._service.receive()
        self._handle_request(request)

    def handle_nowait(self) -> bool:
        request = self._service.receive_nowait()
        handled = False
        if request is not None:
            self._handle_request(request)
            handled = True
        return handled

    def handle_available(self) -> int:
        requests = self._service.receive_available()
        for request in requests:
            self._handle_request(request)
        return len(requests)

    def handle_many(self, max_count: int) -> int:
        requests = self._service.receive_many(max_count)
        for request in requests:
            self._handle_request(request)
        return len(requests)

    def _handle_request(self, request: ServiceRequest) -> None:
        invocation = ensure_procedure_invocation(request.payload)
        keyword_arguments = invocation.keyword_argument_dict()
        reply_payload = self._handler(
            *invocation.positional_arguments, **keyword_arguments
        )
        request.reply(reply_payload)
