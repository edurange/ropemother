#!/usr/bin/env python3
# ropemother/service/resource.py

"""Server-side resource values for freestanding message bus services."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from ropemother.exceptions import MessageBusBaseException
from ropemother.service.descriptor import ConnectionDescriptor

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T16:28:42+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


class BusResourceError(MessageBusBaseException):
    """Base exception for message bus service resource errors."""
    pass


class InvalidBusResourceError(ValueError, BusResourceError):
    """Raised when a message bus service resource is invalid."""
    pass


class BusResource(ABC):
    """Server-side resource that can describe its client connection."""
    @abstractmethod
    def connection_descriptor(self) -> ConnectionDescriptor:
        ...


@dataclass(frozen=True, kw_only=True)
class LocalSocketBusResource(BusResource):
    """Server-side resource for a local Unix socket message bus."""
    socket_path: Path

    def __post_init__(self) -> None:
        socket_path = Path(self.socket_path)
        if not socket_path.is_absolute():
            raise InvalidBusResourceError(
                "local socket bus resources require an absolute path"
            )

        object.__setattr__(self, "socket_path", socket_path)

    @classmethod
    def from_path(cls, socket_path: Path | str) -> Self:
        return cls(socket_path=Path(socket_path))

    def connection_descriptor(self) -> ConnectionDescriptor:
        return ConnectionDescriptor.for_unix_socket(self.socket_path)
