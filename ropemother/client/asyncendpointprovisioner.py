#!/usr/bin/env python3
# ropemother/client/asyncendpointprovisioner.py

"""Awaitable endpoint provisioning for asynchronous endpoint surfaces."""

from abc import ABC, abstractmethod
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
from ropemother.client.asyncendpointfactory import AsyncMessageEndpointFactory
from ropemother.client.asyncrequest import (
    AsyncProcedureClient,
    AsyncProcedureHandler,
    AsyncProcedureService,
    AsyncRequestClient,
    AsyncRequestService,
    AsyncRequester,
    AsyncResponder,
)
from ropemother.client.procedure import PROCEDURE_INVOCATION_JSON_FORMAT
from ropemother.client.lifecycle import AsyncLifecyclePublisher
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
__date__ = "2026-07-06T06:28:43+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


class AsyncEndpointProvisioner(ABC):
    """ABC for provisioning async bus endpoints and clients."""

    @abstractmethod
    async def register_emitter(
        self,
        *,
        msg_topic: str,
        msg_producer: str,
        msg_type: str,
        additional_msg_types: SymbolCollectionInput = (),
        allow_unlisted_type_formats: bool = False,
        payload_format: PortableFormat[Any, Any] = JSON_PORTABLE_FORMAT,
        supported_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> AsyncEmitter:
        ...

    @abstractmethod
    async def subscribe(
        self,
        *,
        msg_topic: SubscriptionTopicInput,
        msg_producer: OptionalSymbolInput = None,
        msg_type: OptionalSymbolInput = None,
    ) -> AsyncReceiver:
        ...

    async def create_lifecycle_publisher(
        self,
        *,
        msg_producer: str,
        msg_topic: str = DEFAULT_LIFECYCLE_TOPIC_ROOT,
    ) -> AsyncLifecyclePublisher:
        emitters = {}
        for msg_type in LifecycleMessageType:
            emitters[msg_type] = await self.register_emitter(
                msg_topic=msg_topic,
                msg_producer=msg_producer,
                msg_type=msg_type.value,
            )
        return AsyncLifecyclePublisher(emitters)

    async def create_requester(
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
    ) -> AsyncRequester:
        request_emitter = await self.register_emitter(
            msg_topic=request_topic,
            msg_producer=requester_producer,
            msg_type=request_msg_type,
            payload_format=request_payload_format,
            supported_type_formats=request_type_formats,
        )
        reply_receiver = await self.subscribe(
            msg_topic=reply_topic,
            msg_producer=responder_producer,
            msg_type=reply_msg_type,
        )
        return AsyncRequester(
            request_emitter, reply_receiver, limits=request_limits
        )

    async def create_responder(
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
    ) -> AsyncResponder:
        request_receiver = await self.subscribe(
            msg_topic=request_topic,
            msg_producer=requester_producer,
            msg_type=request_msg_type,
        )
        reply_emitter = await self.register_emitter(
            msg_topic=reply_topic,
            msg_producer=responder_producer,
            msg_type=reply_msg_type,
            payload_format=reply_payload_format,
            supported_type_formats=reply_type_formats,
        )
        return AsyncResponder(reply_emitter, request_receiver)

    async def create_request_client(
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
    ) -> AsyncRequestClient:
        requester = await self.create_requester(
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
        return AsyncRequestClient(requester, limits=request_limits)

    async def create_request_service(
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
    ) -> AsyncRequestService:
        responder = await self.create_responder(
            request_topic=request_topic,
            reply_topic=reply_topic,
            requester_producer=requester_producer,
            responder_producer=responder_producer,
            request_msg_type=request_msg_type,
            reply_msg_type=reply_msg_type,
            reply_payload_format=reply_payload_format,
            reply_type_formats=reply_type_formats,
        )
        return AsyncRequestService(responder)

    async def create_history_client(
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
    ) -> AsyncHistoryClient:
        request_client = await self.create_request_client(
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
        history_client = AsyncHistoryClient(
            request_client,
            selection_format=request_payload_format,
            page_format=reply_payload_format,
        )
        return history_client

    async def create_history_service(
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
    ) -> AsyncHistoryService:
        request_service = await self.create_request_service(
            request_topic=request_topic,
            reply_topic=reply_topic,
            requester_producer=requester_producer,
            responder_producer=responder_producer,
            request_msg_type=request_msg_type,
            reply_msg_type=reply_msg_type,
            reply_payload_format=reply_payload_format,
            reply_type_formats=reply_type_formats,
        )
        history_service = AsyncHistoryService(
            history, request_service, page_format=reply_payload_format
        )
        return history_service

    async def create_procedure_client(
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
    ) -> AsyncProcedureClient:
        request_client = await self.create_request_client(
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
        return AsyncProcedureClient(request_client)

    async def create_procedure_service(
        self,
        *,
        request_topic: OptionalSymbolInput,
        reply_topic: str,
        requester_producer: str,
        responder_producer: str,
        request_msg_type: str,
        reply_msg_type: str,
        handler: AsyncProcedureHandler,
        reply_payload_format: PortableFormat[Any, Any] = JSON_PORTABLE_FORMAT,
        reply_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> AsyncProcedureService:
        request_service = await self.create_request_service(
            request_topic=request_topic,
            reply_topic=reply_topic,
            requester_producer=requester_producer,
            responder_producer=responder_producer,
            request_msg_type=request_msg_type,
            reply_msg_type=reply_msg_type,
            reply_payload_format=reply_payload_format,
            reply_type_formats=reply_type_formats,
        )
        return AsyncProcedureService(request_service, handler)


class ImmediateAsyncEndpointProvisioner(AsyncEndpointProvisioner):
    """Provisioner that returns preconstructed async endpoints."""
    _factory: AsyncMessageEndpointFactory

    def __init__(self, factory: AsyncMessageEndpointFactory) -> None:
        self._factory = factory

    async def register_emitter(
        self,
        *,
        msg_topic: str,
        msg_producer: str,
        msg_type: str,
        additional_msg_types: SymbolCollectionInput = (),
        allow_unlisted_type_formats: bool = False,
        payload_format: PortableFormat[Any, Any] = JSON_PORTABLE_FORMAT,
        supported_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> AsyncEmitter:
        emitter = self._factory.register_emitter(
            msg_topic=msg_topic,
            msg_producer=msg_producer,
            msg_type=msg_type,
            additional_msg_types=additional_msg_types,
            allow_unlisted_type_formats=allow_unlisted_type_formats,
            payload_format=payload_format,
            supported_type_formats=supported_type_formats,
        )
        return emitter

    async def subscribe(
        self,
        *,
        msg_topic: SubscriptionTopicInput,
        msg_producer: OptionalSymbolInput = None,
        msg_type: OptionalSymbolInput = None,
    ) -> AsyncReceiver:
        receiver = self._factory.subscribe(
            msg_topic=msg_topic, msg_producer=msg_producer, msg_type=msg_type
        )
        return receiver
