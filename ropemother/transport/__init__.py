#!/usr/bin/env python3
# ropemother/transport/__init__.py

"""Transport frame and codec support for external message bus adapters."""

from importlib import import_module
from typing import Any

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T04:58:43+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


_EXPORTS = {
    "AsyncBrokerTransportSessionRunner": (
        "ropemother.transport.asyncsessionrunner"
    ),
    "AsyncFrameChannel": "ropemother.transport.asyncconnection",
    "AsyncFrameConnection": "ropemother.transport.asyncconnection",
    "AsyncMemoryFrameConnection": "ropemother.transport.asyncconnection",
    "AsyncSocketFrameConnection": "ropemother.transport.asyncsocketconnection",
    "AsyncTransportClient": "ropemother.transport.asyncclient",
    "FrameChannel": "ropemother.transport.connection",
    "FrameConnection": "ropemother.transport.connection",
    "MemoryFrameConnection": "ropemother.transport.connection",
    "SocketFrameConnection": "ropemother.transport.socketconnection",
    "SocketFrameConnectionError": "ropemother.transport.socketconnection",
    "TransportClient": "ropemother.transport.client",
    "TransportClientError": "ropemother.transport.client",
    "TransportPayloadDecodeError": "ropemother.transport.client",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(name)

    module = import_module(_EXPORTS[name])
    value = getattr(module, name)
    globals()[name] = value
    return value
