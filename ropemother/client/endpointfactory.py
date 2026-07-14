#!/usr/bin/env python3
# ropemother/client/endpointfactory.py

"""Synchronous factory helpers for bus endpoints and request/reply clients."""

from typing import Any

from ropemother.bootstrap.policy import (
    DEFAULT_LIFECYCLE_TOPIC_ROOT,
    LifecycleMessageType,
)
from ropemother.broker.endpoints import Emitter, ReceiveEndpoint
from ropemother.capture.history import MessageHistory
from ropemother.capture.historyservice import HistoryClient, HistoryService
from ropemother.client.endpointfactorybase import EndpointFactoryBase
from ropemother.client.lifecycle import LifecyclePublisher
from ropemother.client.request import (
    ProcedureClient,
    ProcedureHandler,
    ProcedureService,
    RequestClient,
    RequestClientLimits,
    RequestService,
    Requester,
    Responder,
)
from ropemother.format.portableformat import (
    JSON_PORTABLE_FORMAT,
    PortableFormat,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-06T03:25:28+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class MessageEndpointFactory(
    EndpointFactoryBase[
        Emitter,
        ReceiveEndpoint,
        Requester,
        Responder,
        RequestClient,
        RequestService,
        HistoryClient,
        HistoryService,
        ProcedureClient,
        ProcedureService,
        ProcedureHandler,
    ],
):
    """Synchronous factory for bus endpoints and request/reply clients."""

    def create_lifecycle_publisher(
        self,
        *,
        msg_producer: str,
        msg_topic: str = DEFAULT_LIFECYCLE_TOPIC_ROOT,
    ) -> LifecyclePublisher:
        emitters = {}
        for msg_type in LifecycleMessageType:
            emitters[msg_type] = self.register_emitter(
                msg_topic=msg_topic,
                msg_producer=msg_producer,
                msg_type=msg_type.value,
            )
        return LifecyclePublisher(emitters)

    def _make_requester(
        self,
        request_emitter: Emitter,
        reply_receiver: ReceiveEndpoint,
        request_limits: RequestClientLimits | None,
    ) -> Requester:
        requester = Requester(
            request_emitter,
            reply_receiver,
            limits=request_limits,
        )
        return requester

    def _make_responder(
        self, reply_emitter: Emitter, request_receiver: ReceiveEndpoint
    ) -> Responder:
        return Responder(reply_emitter, request_receiver)

    def _make_request_client(
        self, requester: Requester, request_limits: RequestClientLimits | None
    ) -> RequestClient:
        return RequestClient(requester, limits=request_limits)

    def _make_request_service(
        self, responder: Responder
    ) -> RequestService:
        return RequestService(responder)

    def _make_history_client(
        self,
        request_client: RequestClient,
        selection_format: PortableFormat[Any, Any],
        page_format: PortableFormat[Any, Any],
    ) -> HistoryClient:
        history_client = HistoryClient(
            request_client,
            selection_format=selection_format,
            page_format=page_format,
        )
        return history_client

    def _make_history_service(
        self,
        history: MessageHistory,
        request_service: RequestService,
        page_format: PortableFormat[Any, Any],
    ) -> HistoryService:
        history_service = HistoryService(
            history, request_service, page_format=page_format
        )
        return history_service

    def _make_procedure_client(
        self, request_client: RequestClient
    ) -> ProcedureClient:
        return ProcedureClient(request_client)

    def _make_procedure_service(
        self, request_service: RequestService, handler: ProcedureHandler
    ) -> ProcedureService:
        return ProcedureService(request_service, handler)
