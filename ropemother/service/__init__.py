#!/usr/bin/env python3
# ropemother/service/__init__.py

"""Programmatic service helpers for freestanding message bus processes."""

from importlib import import_module
from typing import Any

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T17:41:46+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


_EXPORTS = {
    "AsyncLocalBusServiceListener": "ropemother.service.socketlistener",
    "AsyncMessageBusService": "ropemother.service.asyncservice",
    "AsyncMessageBusServiceError": "ropemother.service.asyncservice",
    "BUS_CONTACT_URI_VARIABLE": "ropemother.service.environment",
    "BusContactEnvironmentError": "ropemother.service.environment",
    "ConnectionDescriptor": "ropemother.service.descriptor",
    "LocalMessageBusHost": "ropemother.service.host",
    "LocalSocketBusResource": "ropemother.service.resource",
    "MessageBusHost": "ropemother.service.host",
    "MessageBusService": "ropemother.service.service",
    "MessageBusHostError": "ropemother.service.host",
    "MessageBusServiceError": "ropemother.service.service",
    "MissingBusContactEnvironmentError": "ropemother.service.environment",
    "ServiceConnectionFailedError": "ropemother.service.connector",
    "ServiceConnectorError": "ropemother.service.connector",
    "bus_contact_descriptor": "ropemother.service.environment",
    "bus_contact_variables": "ropemother.service.environment",
    "connect_async_client_from_bus_contact": "ropemother.service.environment",
    "connect_async_message_bus": "ropemother.service.environment",
    "connect_client_from_bus_contact": "ropemother.service.environment",
    "connect_message_bus": "ropemother.service.environment",
    "preconfigured_history_client": "ropemother.service.brokerhistory",
    "serve_local_message_bus": "ropemother.service.broker",
    "set_bus_contact_uri": "ropemother.service.environment",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(name)

    module = import_module(_EXPORTS[name])
    value = getattr(module, name)
    globals()[name] = value
    return value
