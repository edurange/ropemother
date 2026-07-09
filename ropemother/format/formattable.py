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
__date__ = "2026-07-09T04:42:39+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
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
    def from_key(
        self, key: PortableFormatKey
    ) -> PortableFormat[Any, Any]:
        ...


class StaticPortableFormatTable(PortableFormatTable):
    """Read-only portable format table built from known formats."""
    _formats: dict[PortableFormatKey, PortableFormat[Any, Any]]

    def __init__(self, *formats: PortableFormat[Any, Any]) -> None:
        self._formats = {}
        for portable_format in formats:
            self._formats[portable_format.key] = portable_format

    def from_key(
        self, key: PortableFormatKey
    ) -> PortableFormat[Any, Any]:
        try:
            portable_format = self._formats[key]
        except KeyError as e:
            raise UnknownPortableFormatError(
                f"unknown portable format: {key.registration_key}"
            ) from e
        return portable_format


class LocalPortableFormatTable(PortableFormatTable):
    """Mutable local portable format table for service configuration."""
    _formats: dict[PortableFormatKey, PortableFormat[Any, Any]]

    def __init__(
        self, formats: Iterable[PortableFormat[Any, Any]] = ()
    ) -> None:
        self._formats = {}
        self.add_formats(formats)

    def add_formats(self, formats: Iterable[PortableFormat[Any, Any]]) -> None:
        for portable_format in formats:
            self.add_format(portable_format)

    def add_format(self, portable_format: PortableFormat[Any, Any]) -> None:
        existing_format = self._formats.get(portable_format.key)
        if existing_format is not None and existing_format != portable_format:
            raise ConflictingPortableFormatError(
                "conflicting portable format for key: "
                + portable_format.key.registration_key
            )

        self._formats[portable_format.key] = portable_format

    def has_format_key(self, key: PortableFormatKey) -> bool:
        return key in self._formats

    def format_keys(self) -> tuple[PortableFormatKey, ...]:
        return tuple(self._formats)

    def formats(self) -> tuple[PortableFormat[Any, Any], ...]:
        return tuple(self._formats.values())

    def from_key(self, key: PortableFormatKey) -> PortableFormat[Any, Any]:
        try:
            portable_format = self._formats[key]
        except KeyError as e:
            raise UnknownPortableFormatError(
                f"unknown portable format: {key.registration_key}"
            ) from e

        return portable_format