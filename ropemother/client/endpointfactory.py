#!/usr/bin/env python3
# ropemother/client/endpointfactory.py

"""Synchronous factory helpers for bus endpoints and request/reply clients."""

import abc

from ropemother.bootstrap.policy import (
    DEFAULT_LIFECYCLE_TOPIC_ROOT,
    LifecycleMessageType,
)
from ropemother.broker.endpoints import Emitter, ReceiveEndpoint, Receiver
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
from ropemother.message.records import ReceivedMessage

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-08-26T16:24:27+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev7"
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

    @abc.abstractmethod
    def receive_from(
        self, *receivers: Receiver
    ) -> tuple[Receiver, ReceivedMessage]:
        ...

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
        selection_format: PortableFormat,
        page_format: PortableFormat,
    ) -> HistoryClient:
        format_table = self._portable_format_table()
        history_client = HistoryClient._from_format_registry(
            request_client,
            format_registry=format_table,
            selection_format=selection_format,
            page_format=page_format,
        )
        return history_client

    def _make_history_service(
        self,
        history: MessageHistory,
        request_service: RequestService,
        page_format: PortableFormat,
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
