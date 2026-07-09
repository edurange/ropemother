#!/usr/bin/env python3
# ropemother/client/endpointfactorybase.py

"""Shared endpoint factory support for client-facing helpers."""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from ropemother.capture.history import MessageHistory
from ropemother.client.procedure import PROCEDURE_INVOCATION_JSON_FORMAT
from ropemother.client.request import RequestClientLimits
from ropemother.format.portableformat import (
    COMPOSITE_PORTABLE_FORMAT,
    JSON_PORTABLE_FORMAT,
    PortableFormat,
)
from ropemother.message.selectors import (
    OptionalSymbolInput,
    SubscriptionTopicInput,
    SymbolCollectionInput,
)
from ropemother.message.typeformats import SupportedTypeFormatsInput

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-06T06:27:14+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


EmitterT = TypeVar("EmitterT")
ReceiverT = TypeVar("ReceiverT")
RequesterT = TypeVar("RequesterT")
ResponderT = TypeVar("ResponderT")
RequestClientT = TypeVar("RequestClientT")
RequestServiceT = TypeVar("RequestServiceT")
HistoryClientT = TypeVar("HistoryClientT")
HistoryServiceT = TypeVar("HistoryServiceT")
ProcedureClientT = TypeVar("ProcedureClientT")
ProcedureServiceT = TypeVar("ProcedureServiceT")
ProcedureHandlerT = TypeVar("ProcedureHandlerT")


class EndpointFactoryBase(
    ABC,
    Generic[
        EmitterT,
        ReceiverT,
        RequesterT,
        ResponderT,
        RequestClientT,
        RequestServiceT,
        HistoryClientT,
        HistoryServiceT,
        ProcedureClientT,
        ProcedureServiceT,
        ProcedureHandlerT,
    ],
):
    """Shared factory logic for endpoint and request/reply helpers."""
    @abstractmethod
    def register_emitter(
        self,
        *,
        msg_topic: str,
        msg_producer: str,
        msg_type: str,
        additional_msg_types: SymbolCollectionInput = (),
        allow_unlisted_type_formats: bool = False,
        payload_format: PortableFormat[Any, Any] = JSON_PORTABLE_FORMAT,
        supported_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> EmitterT:
        ...

    @abstractmethod
    def subscribe(
        self,
        *,
        msg_topic: SubscriptionTopicInput,
        msg_producer: OptionalSymbolInput = None,
        msg_type: OptionalSymbolInput = None,
    ) -> ReceiverT:
        ...

    @abstractmethod
    def _make_requester(
        self,
        request_emitter: EmitterT,
        reply_receiver: ReceiverT,
        request_limits: RequestClientLimits | None,
    ) -> RequesterT:
        ...

    @abstractmethod
    def _make_responder(
        self,
        reply_emitter: EmitterT,
        request_receiver: ReceiverT,
    ) -> ResponderT:
        ...

    @abstractmethod
    def _make_request_client(
        self,
        requester: RequesterT,
        request_limits: RequestClientLimits | None,
    ) -> RequestClientT:
        ...

    @abstractmethod
    def _make_request_service(
        self,
        responder: ResponderT,
    ) -> RequestServiceT:
        ...

    @abstractmethod
    def _make_history_client(
        self,
        request_client: RequestClientT,
        selection_format: PortableFormat[Any, Any],
        page_format: PortableFormat[Any, Any],
    ) -> HistoryClientT:
        ...

    @abstractmethod
    def _make_history_service(
        self,
        history: MessageHistory,
        request_service: RequestServiceT,
        page_format: PortableFormat[Any, Any]
    ) -> HistoryServiceT:
        ...

    @abstractmethod
    def _make_procedure_client(
        self, request_client: RequestClientT
    ) -> ProcedureClientT:
        ...

    @abstractmethod
    def _make_procedure_service(
        self, request_service: RequestServiceT, handler: ProcedureHandlerT
    ) -> ProcedureServiceT:
        ...

    def create_requester(
        self,
        *,
        request_topic: str,
        reply_topic: OptionalSymbolInput,
        requester_producer: str,
        responder_producer: str,
        request_msg_type: str,
        reply_msg_type: str,
        request_payload_format: PortableFormat[Any, Any] = (
            JSON_PORTABLE_FORMAT
        ),
        request_limits: RequestClientLimits | None = None,
        request_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> RequesterT:
        request_emitter = self.register_emitter(
            msg_topic=request_topic,
            msg_producer=requester_producer,
            msg_type=request_msg_type,
            payload_format=request_payload_format,
            supported_type_formats=request_type_formats,
        )
        reply_receiver = self.subscribe(
            msg_topic=reply_topic,
            msg_producer=responder_producer,
            msg_type=reply_msg_type,
        )
        requester = self._make_requester(
            request_emitter, reply_receiver, request_limits
        )
        return requester

    def create_responder(
        self,
        *,
        request_topic: OptionalSymbolInput,
        reply_topic: str,
        requester_producer: str,
        responder_producer: str,
        request_msg_type: str,
        reply_msg_type: str,
        reply_payload_format: PortableFormat[Any, Any] = JSON_PORTABLE_FORMAT,
        reply_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> ResponderT:
        request_receiver = self.subscribe(
            msg_topic=request_topic,
            msg_producer=requester_producer,
            msg_type=request_msg_type,
        )
        reply_emitter = self.register_emitter(
            msg_topic=reply_topic,
            msg_producer=responder_producer,
            msg_type=reply_msg_type,
            payload_format=reply_payload_format,
            supported_type_formats=reply_type_formats,
        )
        return self._make_responder(reply_emitter, request_receiver)

    def create_request_client(
        self,
        *,
        request_topic: str,
        reply_topic: OptionalSymbolInput,
        requester_producer: str,
        responder_producer: str,
        request_msg_type: str,
        reply_msg_type: str,
        request_payload_format: PortableFormat[Any, Any] = (
            JSON_PORTABLE_FORMAT
        ),
        request_limits: RequestClientLimits | None = None,
        request_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> RequestClientT:
        requester = self.create_requester(
            request_topic=request_topic,
            reply_topic=reply_topic,
            requester_producer=requester_producer,
            responder_producer=responder_producer,
            request_msg_type=request_msg_type,
            reply_msg_type=reply_msg_type,
            request_payload_format=request_payload_format,
            request_limits=request_limits,
            request_type_formats=request_type_formats,
        )
        return self._make_request_client(requester, request_limits)

    def create_request_service(
        self,
        *,
        request_topic: OptionalSymbolInput,
        reply_topic: str,
        requester_producer: str,
        responder_producer: str,
        request_msg_type: str,
        reply_msg_type: str,
        reply_payload_format: PortableFormat[Any, Any] = JSON_PORTABLE_FORMAT,
        reply_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> RequestServiceT:
        responder = self.create_responder(
            request_topic=request_topic,
            reply_topic=reply_topic,
            requester_producer=requester_producer,
            responder_producer=responder_producer,
            request_msg_type=request_msg_type,
            reply_msg_type=reply_msg_type,
            reply_payload_format=reply_payload_format,
            reply_type_formats=reply_type_formats,
        )
        return self._make_request_service(responder)

    def create_history_client(
        self,
        *,
        request_topic: str,
        reply_topic: OptionalSymbolInput,
        requester_producer: str,
        responder_producer: str,
        request_msg_type: str,
        reply_msg_type: str,
        request_payload_format: PortableFormat[Any, Any] = (
            JSON_PORTABLE_FORMAT
        ),
        reply_payload_format: PortableFormat[Any, Any] = (
            COMPOSITE_PORTABLE_FORMAT
        ),
        request_limits: RequestClientLimits | None = None,
        request_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> HistoryClientT:
        request_client = self.create_request_client(
            request_topic=request_topic,
            reply_topic=reply_topic,
            requester_producer=requester_producer,
            responder_producer=responder_producer,
            request_msg_type=request_msg_type,
            reply_msg_type=reply_msg_type,
            request_payload_format=request_payload_format,
            request_limits=request_limits,
            request_type_formats=request_type_formats,
        )
        history_client = self._make_history_client(
            request_client, request_payload_format, reply_payload_format
        )
        return history_client

    def create_history_service(
        self,
        *,
        history: MessageHistory,
        request_topic: OptionalSymbolInput,
        reply_topic: str,
        requester_producer: str,
        responder_producer: str,
        request_msg_type: str,
        reply_msg_type: str,
        reply_payload_format: PortableFormat[Any, Any] = (
            COMPOSITE_PORTABLE_FORMAT
        ),
        reply_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> HistoryServiceT:
        request_service = self.create_request_service(
            request_topic=request_topic,
            reply_topic=reply_topic,
            requester_producer=requester_producer,
            responder_producer=responder_producer,
            request_msg_type=request_msg_type,
            reply_msg_type=reply_msg_type,
            reply_payload_format=reply_payload_format,
            reply_type_formats=reply_type_formats,
        )
        history_service = self._make_history_service(
            history, request_service, reply_payload_format
        )
        return history_service

    def create_procedure_client(
        self,
        *,
        request_topic: str,
        reply_topic: OptionalSymbolInput,
        requester_producer: str,
        responder_producer: str,
        request_msg_type: str,
        reply_msg_type: str,
        procedure_invocation_format: PortableFormat[Any, Any] = (
            PROCEDURE_INVOCATION_JSON_FORMAT
        ),
        request_limits: RequestClientLimits | None = None,
        request_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> ProcedureClientT:
        request_client = self.create_request_client(
            request_topic=request_topic,
            reply_topic=reply_topic,
            requester_producer=requester_producer,
            responder_producer=responder_producer,
            request_msg_type=request_msg_type,
            reply_msg_type=reply_msg_type,
            request_payload_format=procedure_invocation_format,
            request_limits=request_limits,
            request_type_formats=request_type_formats,
        )
        return self._make_procedure_client(request_client)

    def create_procedure_service(
        self,
        *,
        request_topic: OptionalSymbolInput,
        reply_topic: str,
        requester_producer: str,
        responder_producer: str,
        request_msg_type: str,
        reply_msg_type: str,
        handler: ProcedureHandlerT,
        reply_payload_format: PortableFormat[Any, Any] = JSON_PORTABLE_FORMAT,
        reply_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> ProcedureServiceT:
        request_service = self.create_request_service(
            request_topic=request_topic,
            reply_topic=reply_topic,
            requester_producer=requester_producer,
            responder_producer=responder_producer,
            request_msg_type=request_msg_type,
            reply_msg_type=reply_msg_type,
            reply_payload_format=reply_payload_format,
            reply_type_formats=reply_type_formats,
        )
        return self._make_procedure_service(request_service, handler)
