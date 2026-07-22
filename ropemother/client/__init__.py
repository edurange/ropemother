#!/usr/bin/env python3
# ropemother/client/__init__.py

"""Client-facing endpoint factory and request/reply helpers."""

from importlib import import_module
from typing import Any

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-06T06:31:36+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


_EXPORTS = {
    "AsyncMessageEndpointFactory": "ropemother.client.asyncendpointfactory",
    "AsyncProcedureClient": "ropemother.client.asyncrequest",
    "AsyncProcedureService": "ropemother.client.asyncrequest",
    "AsyncRequestClient": "ropemother.client.asyncrequest",
    "AsyncRequestService": "ropemother.client.asyncrequest",
    "ClientRequestError": "ropemother.client.request",
    "ImmediateAsyncEndpointProvisioner": (
        "ropemother.client.asyncendpointprovisioner"
    ),
    "InvalidProcedureInvocationError": "ropemother.client.procedure",
    "InvalidProcedureInvocationTypeError": "ropemother.client.procedure",
    "MessageEndpointFactory": "ropemother.client.endpointfactory",
    "PROCEDURE_INVOCATION_JSON_FORMAT": "ropemother.client.procedure",
    "ProcedureClient": "ropemother.client.request",
    "ProcedureError": "ropemother.client.procedure",
    "ProcedureInvocation": "ropemother.client.procedure",
    "ProcedureInvocationJSONAdapter": "ropemother.client.procedure",
    "ProcedureService": "ropemother.client.request",
    "RequestClient": "ropemother.client.request",
    "RequestClientLimits": "ropemother.client.request",
    "RequestHandle": "ropemother.client.request",
    "RequestService": "ropemother.client.request",
    "SAME_MSG_TYPE": "ropemother.client.requestoptions",
    "ServiceRequest": "ropemother.client.request",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(name)

    module = import_module(_EXPORTS[name])
    value = getattr(module, name)
    globals()[name] = value
    return value
