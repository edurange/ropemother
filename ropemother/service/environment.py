#!/usr/bin/env python3
# ropemother/service/environment.py

"""Environment-variable handoff for message bus contact details."""

from collections.abc import Iterable, Mapping, MutableMapping
import os
from typing import Any

from ropemother.exceptions import MessageBusBaseException
from ropemother.format.portableformat import PortableFormat
from ropemother.service.connector import (
    connect_async_transport_client,
    connect_transport_client,
)
from ropemother.service.descriptor import ConnectionDescriptor
from ropemother.transport.asyncclient import AsyncTransportClient
from ropemother.transport.client import TransportClient

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T17:41:15+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


BUS_CONTACT_URI_VARIABLE = "ROPEMOTHER_CONNECTION_DESCRIPTOR"


class BusContactEnvironmentError(MessageBusBaseException):
    """Base exception for bus contact environment errors."""
    pass


class MissingBusContactEnvironmentError(KeyError, BusContactEnvironmentError):
    """Raised when no bus contact URI is available."""
    pass


def set_bus_contact_uri(
    descriptor: ConnectionDescriptor,
    *,
    variables: MutableMapping[str, str] | None = None,
    name: str = BUS_CONTACT_URI_VARIABLE,
) -> None:
    """Store a bus contact URI in an environment mapping."""
    target = variables
    if target is None:
        target = os.environ

    target[name] = descriptor.to_uri()


def bus_contact_variables(
    descriptor: ConnectionDescriptor,
    *,
    variables: Mapping[str, str] | None = None,
    name: str = BUS_CONTACT_URI_VARIABLE,
) -> dict[str, str]:
    """Return environment variables with a bus contact URI added."""
    source = variables
    if source is None:
        source = os.environ

    target = dict(source)
    set_bus_contact_uri(descriptor, variables=target, name=name)
    return target


def bus_contact_descriptor(
    *,
    variables: Mapping[str, str] | None = None,
    name: str = BUS_CONTACT_URI_VARIABLE,
) -> ConnectionDescriptor:
    """Read a bus connection descriptor from environment variables."""
    source = variables
    if source is None:
        source = os.environ

    try:
        descriptor_uri = source[name]
    except KeyError as e:
        raise MissingBusContactEnvironmentError(
            "message bus connection descriptor is not available"
        ) from e

    return ConnectionDescriptor.parse(descriptor_uri)


def connect_client_from_bus_contact(
    *,
    extra_formats: Iterable[PortableFormat[Any, Any]] = (),
    variables: Mapping[str, str] | None = None,
    name: str = BUS_CONTACT_URI_VARIABLE,
) -> TransportClient:
    """Connect to the bus named by environment variables."""
    descriptor = bus_contact_descriptor(variables=variables, name=name)
    client = connect_transport_client(
        descriptor=descriptor, extra_formats=extra_formats
    )
    return client


def connect_message_bus(
    descriptor: ConnectionDescriptor | str | None = None,
    *,
    extra_formats: Iterable[PortableFormat[Any, Any]] = (),
    variables: Mapping[str, str] | None = None,
    name: str = BUS_CONTACT_URI_VARIABLE,
) -> TransportClient:
    """Connect to a message bus from a descriptor or environment."""
    if descriptor is None:
        connection_descriptor = bus_contact_descriptor(
            variables=variables, name=name
        )
    elif isinstance(descriptor, str):
        connection_descriptor = ConnectionDescriptor.parse(descriptor)
    else:
        connection_descriptor = descriptor

    client = connect_transport_client(
        descriptor=connection_descriptor, extra_formats=extra_formats
    )
    return client


async def connect_async_client_from_bus_contact(
    *,
    extra_formats: Iterable[PortableFormat[Any, Any]] = (),
    variables: Mapping[str, str] | None = None,
    name: str = BUS_CONTACT_URI_VARIABLE,
) -> AsyncTransportClient:
    """Connect asynchronously to the bus named by environment variables."""
    descriptor = bus_contact_descriptor(variables=variables, name=name)
    client = await connect_async_transport_client(
        descriptor=descriptor, extra_formats=extra_formats
    )
    return client


async def connect_async_message_bus(
    descriptor: ConnectionDescriptor | str | None = None,
    *,
    extra_formats: Iterable[PortableFormat[Any, Any]] = (),
    variables: Mapping[str, str] | None = None,
    name: str = BUS_CONTACT_URI_VARIABLE,
) -> AsyncTransportClient:
    """Connect asynchronously to a message bus using a URI descriptor."""
    if descriptor is None:
        connection_descriptor = bus_contact_descriptor(
            variables=variables, name=name
        )
    elif isinstance(descriptor, str):
        connection_descriptor = ConnectionDescriptor.parse(descriptor)
    else:
        connection_descriptor = descriptor

    client = await connect_async_transport_client(
        descriptor=connection_descriptor, extra_formats=extra_formats
    )
    return client
