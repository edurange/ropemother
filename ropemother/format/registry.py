#!/usr/bin/env python3
# ropemother/format/registry.py

"""Compact IDs and registration helpers for payload formats."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from ropemother.exceptions import MessageBusBaseException
from ropemother.format.portableformat import PortableFormat, PortableFormatKey
from ropemother.format.formattable import (
    ConflictingPortableFormatError,
    PortableFormatTable,
    UnknownPortableFormatError,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T04:45:32+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


class FormatRegistryError(MessageBusBaseException):
    """Base exception for portable format registry errors."""
    pass


class InvalidFormatIDError(ValueError, FormatRegistryError):
    """Raised when a portable format ID value is invalid."""
    pass


class InvalidFormatIDTypeError(TypeError, FormatRegistryError):
    """Raised when a portable format ID has the wrong type."""
    pass


class UnsupportedRegistrationTypeError(TypeError, FormatRegistryError):
    """Raised when a format registration has an unsupported shape."""
    pass


@dataclass(frozen=True, slots=True)
class PortableFormatID:
    """Compact captured identifier for a portable payload format."""
    value: int

    def __post_init__(self) -> None:
        if type(self.value) is bool:
            raise InvalidFormatIDTypeError(
                "portable format IDs must not be bool values"
            )
        if not isinstance(self.value, int):
            raise InvalidFormatIDTypeError(
                f"portable format IDs must be integers: got {self.value!r}"
            )
        if self.value < 0:
            raise InvalidFormatIDError(
                f"portable format IDs must be non-negative: got {self.value}"
            )


@dataclass(frozen=True, kw_only=True, slots=True)
class PortableFormatRegistration:
    """Captured binding from a compact format ID to a format key."""
    format_id: PortableFormatID
    key: PortableFormatKey


class PortableFormatRegistry(PortableFormatTable):
    """Registry that assigns compact IDs to portable payload formats."""
    _format_ids: dict[PortableFormatKey, PortableFormatID]
    _formats: dict[PortableFormatID, PortableFormat[Any, Any]]
    _registrations: list[PortableFormatRegistration]
    _local_formats: dict[PortableFormatKey, PortableFormat[Any, Any]]

    def __init__(self) -> None:
        self._format_ids = {}
        self._formats = {}
        self._registrations = []
        self._local_formats = {}

    def install_formats(
        self, formats: Iterable[PortableFormat[Any, Any]]
    ) -> None:
        for portable_format in formats:
            self.install_format(portable_format)

    def install_format(
        self, portable_format: PortableFormat[Any, Any]
    ) -> None:
        existing_format = self._local_formats.get(portable_format.key)
        if existing_format is not None:
            self._ensure_compatible_format(existing_format, portable_format)
        else:
            self._local_formats[portable_format.key] = portable_format

    def ensure_format_id(
        self, portable_format: PortableFormat[Any, Any]
    ) -> tuple[PortableFormatID, PortableFormatRegistration | None]:
        format_key = portable_format.key
        self.install_format(portable_format)
        if format_key in self._format_ids:
            format_id = self._format_ids[format_key]
            existing_format = self._local_formats[format_key]
            self._ensure_compatible_format(existing_format, portable_format)
            registration = None
        else:
            format_id = PortableFormatID(len(self._format_ids))
            self._format_ids[format_key] = format_id
            self._formats[format_id] = self._local_formats[format_key]
            registration = PortableFormatRegistration(
                format_id=format_id, key=format_key
            )
            self._registrations.append(registration)
        return format_id, registration

    def has_format_key(self, key: PortableFormatKey) -> bool:
        return key in self._local_formats

    def format_keys(self) -> tuple[PortableFormatKey, ...]:
        return tuple(self._local_formats)

    def formats(self) -> tuple[PortableFormat[Any, Any], ...]:
        return tuple(self._local_formats.values())

    def registrations(self) -> tuple[PortableFormatRegistration, ...]:
        return tuple(self._registrations)

    def format_for_id(
        self, format_id: PortableFormatID
    ) -> PortableFormat[Any, Any]:
        return self._formats[format_id]

    def from_key(
        self, key: PortableFormatKey
    ) -> PortableFormat[Any, Any]:
        try:
            portable_format = self._local_formats[key]
        except KeyError as e:
            raise UnknownPortableFormatError(
                f"unknown portable format: {key.registration_key}"
            ) from e
        return portable_format

    def _ensure_compatible_format(
        self,
        existing_format: PortableFormat[Any, Any],
        portable_format: PortableFormat[Any, Any],
    ) -> None:
        if existing_format != portable_format:
            raise ConflictingPortableFormatError(
                "conflicting portable format for key: "
                + portable_format.key.registration_key
            )
