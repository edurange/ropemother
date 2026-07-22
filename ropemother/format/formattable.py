#!/usr/bin/env python3
# ropemother/format/formattable.py

"""Local lookup support for portable payload formats."""

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any

from ropemother.exceptions import MessageBusBaseException
from ropemother.format.portableformat import PortableFormat, PortableFormatKey

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-22T16:04:47+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class PortableFormatTableError(MessageBusBaseException):
    """Base exception for portable format table errors."""
    pass


class UnknownPortableFormatError(PortableFormatTableError):
    """Raised when a portable format is not registered."""
    pass


class ConflictingPortableFormatError(ValueError, PortableFormatTableError):
    """Raised when a portable format key conflicts with an existing entry."""
    pass


class PortableFormatTable(ABC):
    """Lookup table for portable payload formats by durable key."""

    @abstractmethod
    def from_key(self, key: PortableFormatKey) -> PortableFormat[Any, Any]:
        ...
