#!/usr/bin/env python3
# ropemother/client/asyncendpointfactory.py

"""Asynchronous factory helpers for bus endpoints and clients."""

from typing import Any

from ropemother.bootstrap.policy import (
    DEFAULT_LIFECYCLE_TOPIC_ROOT,
    LifecycleMessageType,
)
from ropemother.broker.asyncendpoints import AsyncEmitter, AsyncReceiver
from ropemother.capture.history import MessageHistory
from ropemother.capture.historyservice import (
    AsyncHistoryClient,
    AsyncHistoryService,
)
from ropemother.client.asyncrequest import (
    AsyncProcedureClient,
    AsyncProcedureHandler,
    AsyncProcedureService,
    AsyncRequestClient,
    AsyncRequestService,
    AsyncRequester,
    AsyncResponder,
)
from ropemother.client.endpointfactorybase import EndpointFactoryBase
from ropemother.client.lifecycle import AsyncLifecyclePublisher
from ropemother.client.request import RequestClientLimits
from ropemother.format.portableformat import (
    JSON_PORTABLE_FORMAT,
    PortableFormat,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-06T03:26:38+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


class AsyncMessageEndpointFactory(
    EndpointFactoryBase[
        AsyncEmitter,
        AsyncReceiver,
        AsyncRequester,
        AsyncResponder,
        AsyncRequestClient,
        AsyncRequestService,
        AsyncHistoryClient,
        AsyncHistoryService,
        AsyncProcedureClient,
        AsyncProcedureService,
        AsyncProcedureHandler,
    ],
):
    """Async factory for bus endpoints and request/reply clients."""

    def create_lifecycle_publisher(
        self,
        *,
        msg_producer: str,
        msg_topic: str = DEFAULT_LIFECYCLE_TOPIC_ROOT,
    ) -> AsyncLifecyclePublisher:
        emitters = {}
        for msg_type in LifecycleMessageType:
            emitters[msg_type] = self.register_emitter(
                msg_topic=msg_topic,
                msg_producer=msg_producer,
                msg_type=msg_type.value,
            )
        return AsyncLifecyclePublisher(emitters)

    def _make_requester(
        self,
        request_emitter: AsyncEmitter,
        reply_receiver: AsyncReceiver,
        request_limits: RequestClientLimits | None,
    ) -> AsyncRequester:
        requester = AsyncRequester(
            request_emitter, reply_receiver, limits=request_limits
        )
        return requester

    def _make_responder(
        self, reply_emitter: AsyncEmitter, request_receiver: AsyncReceiver
    ) -> AsyncResponder:
        return AsyncResponder(reply_emitter, request_receiver)

    def _make_request_client(
        self,
        requester: AsyncRequester,
        request_limits: RequestClientLimits | None,
    ) -> AsyncRequestClient:
        return AsyncRequestClient(requester, limits=request_limits)

    def _make_request_service(
        self, responder: AsyncResponder
    ) -> AsyncRequestService:
        return AsyncRequestService(responder)

    def _make_history_client(
        self,
        request_client: AsyncRequestClient,
        selection_format: PortableFormat[Any, Any],
        page_format: PortableFormat[Any, Any],
    ) -> AsyncHistoryClient:
        history_client = AsyncHistoryClient(
            request_client,
            selection_format=selection_format,
            page_format=page_format,
        )
        return history_client

    def _make_history_service(
        self,
        history: MessageHistory,
        request_service: AsyncRequestService,
        page_format: PortableFormat[Any, Any],
    ) -> AsyncHistoryService:
        history_service = AsyncHistoryService(
            history, request_service, page_format=page_format
        )
        return history_service

    def _make_procedure_client(
        self, request_client: AsyncRequestClient
    ) -> AsyncProcedureClient:
        return AsyncProcedureClient(request_client)

    def _make_procedure_service(
        self,
        request_service: AsyncRequestService,
        handler: AsyncProcedureHandler,
    ) -> AsyncProcedureService:
        return AsyncProcedureService(request_service, handler)
