#!/usr/bin/env python3
# ropemother/broker/asyncbase.py

"""Asynchronous abstract interface for message bus implementation."""

from abc import abstractmethod
from typing import Any

from ropemother.broker.asyncendpoints import AsyncEmitter, AsyncReceiver
from ropemother.capture.sink import CaptureSink
from ropemother.client.asyncendpointfactory import AsyncMessageEndpointFactory
from ropemother.format.portableformat import (
    PortableFormat,
    JSON_PORTABLE_FORMAT,
)
from ropemother.message.selectors import (
    OptionalSymbolInput,
    SubscriptionTopicInput,
    SymbolCollectionInput,
)
from ropemother.message.typeformats import SupportedTypeFormatsInput

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-05T16:37:40+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


class AsyncMessageBus(AsyncMessageEndpointFactory):
    """Abstract broker interface for async message bus implementations."""
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
    ) -> AsyncEmitter:
        ...

    @abstractmethod
    def subscribe(
        self,
        *,
        msg_topic: SubscriptionTopicInput,
        msg_producer: OptionalSymbolInput = None,
        msg_type: OptionalSymbolInput = None,
    ) -> AsyncReceiver:
        ...

    @abstractmethod
    def set_capture_sink(self, capture_sink: CaptureSink) -> None:
        ...
