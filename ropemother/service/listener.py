#!/usr/bin/env python3
# ropemother/service/listener.py

"""Listener contracts for freestanding message bus services."""

from abc import ABC, abstractmethod

from ropemother.service.descriptor import ConnectionDescriptor
from ropemother.transport.asyncconnection import AsyncFrameConnection
from ropemother.transport.connection import FrameConnection

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-04T22:47:42+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


class FrameConnectionListener(ABC):
    """Abstract listener that accepts frame connections for a service."""
    @abstractmethod
    def connection_descriptor(self) -> ConnectionDescriptor:
        pass

    @abstractmethod
    def accept(self) -> FrameConnection:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class AsyncFrameConnectionListener(ABC):
    """Abstract listener that accepts async frame connections."""
    @abstractmethod
    def connection_descriptor(self) -> ConnectionDescriptor:
        pass

    @abstractmethod
    async def accept(self) -> AsyncFrameConnection:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
