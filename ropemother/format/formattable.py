#!/usr/bin/env python3
# ropemother/format/formattable.py

"""Local lookup support for portable payload formats."""

import abc

from ropemother.exceptions import MessageBusBaseException
from ropemother.format.portableformat import PortableFormat, PortableFormatKey

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-08-20T17:41:31+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev7"
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


class PortableFormatTable(abc.ABC):
    """Lookup table for portable payload formats by durable key."""

    @abc.abstractmethod
    def from_key(self, key: PortableFormatKey) -> PortableFormat:
        ...
