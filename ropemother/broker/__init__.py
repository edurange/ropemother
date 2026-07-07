#!/usr/bin/env python3
# ropemother/broker/__init__.py

"""Supports and implements runtime publish-subscribe services."""

from importlib import import_module
from typing import Any

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-06-30T18:59:10+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


_EXPORTS = {
    "AsyncDirectMessageBus": "ropemother.broker.asyncdirect",
    "AsyncEmitter": "ropemother.broker.asyncendpoints",
    "AsyncReceiver": "ropemother.broker.asyncendpoints",
    "CaptureMode": "ropemother.broker.directcore",
    "DirectMessageBus": "ropemother.broker.direct",
    "Emitter": "ropemother.broker.endpoints",
    "Receiver": "ropemother.broker.endpoints",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(name)

    module = import_module(_EXPORTS[name])
    value = getattr(module, name)
    globals()[name] = value
    return value
