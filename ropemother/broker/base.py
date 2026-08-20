#!/usr/bin/env python3
# ropemother/broker/base.py

"""Abstract interface for message bus implementation."""

import abc

from ropemother.broker.endpoints import Emitter, Receiver
from ropemother.capture.sink import CaptureSink
from ropemother.client.endpointfactory import MessageEndpointFactory
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
__date__ = "2026-08-20T17:38:54+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev7"
__status__ = "Development"


class MessageBus(MessageEndpointFactory):
    """Abstract broker interface for local message bus implementations."""

    @abc.abstractmethod
    def register_emitter(
        self,
        *,
        msg_topic: str,
        msg_producer: str,
        msg_type: str,
        additional_msg_types: SymbolCollectionInput = (),
        allow_unlisted_type_formats: bool = False,
        payload_format: PortableFormat = JSON_PORTABLE_FORMAT,
        supported_type_formats: SupportedTypeFormatsInput | None = None,
    ) -> Emitter:
        ...

    @abc.abstractmethod
    def subscribe(
        self,
        *,
        msg_topic: SubscriptionTopicInput,
        msg_producer: OptionalSymbolInput = None,
        msg_type: OptionalSymbolInput = None,
    ) -> Receiver:
        ...

    @abc.abstractmethod
    def set_capture_sink(self, capture_sink: CaptureSink) -> None:
        ...
