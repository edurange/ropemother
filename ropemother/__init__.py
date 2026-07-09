#!/usr/bin/env python3
# ropemother/__init__.py

"""A module for publish-subscribe broadcast messaging."""

from importlib import import_module
from typing import Any

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-05T15:40:19+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


_EXPORTS = {
    "AsyncDirectMessageBus": "ropemother.broker.asyncdirect",
    "CAPTURE_PORTABLE_FORMAT": "ropemother.format.portableformat",
    "CaptureMode": "ropemother.broker.directcore",
    "DirectMessageBus": "ropemother.broker.direct",
    "InMemoryCaptureSink": "ropemother.capture.memorysink",
    "JSONLinesCaptureSink": "ropemother.capture.filesink",
    "JSON_PORTABLE_FORMAT": "ropemother.format.portableformat",
    "MessageBusBaseException": "ropemother.exceptions",
    "PayloadSerializationError": "ropemother.exceptions",
    "PortableFormat": "ropemother.format.portableformat",
    "RAW_BYTES_PORTABLE_FORMAT": "ropemother.format.portableformat",
    "ReceivedMessage": "ropemother.message.records",
    "connect_async_message_bus": "ropemother.service.environment",
    "connect_message_bus": "ropemother.service.environment",
    "exact_topic": "ropemother.message.selectors",
    "topic_tree": "ropemother.message.selectors",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(name)

    module = import_module(_EXPORTS[name])
    value = getattr(module, name)
    globals()[name] = value
    return value
