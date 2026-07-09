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
__date__ = "2026-07-09T07:15:10+00:00"
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


class UnknownPortableFormatIDError(LookupError, FormatRegistryError):
    """Raised when a compact format ID is not registered locally."""
    pass


class ConflictingPortableFormatRegistrationError(
    ValueError, FormatRegistryError
):
    """Raised when a compact format ID conflicts with known state."""
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
    _format_keys: dict[PortableFormatID, PortableFormatKey]
    _registrations: list[PortableFormatRegistration]
    _local_formats: dict[PortableFormatKey, PortableFormat[Any, Any]]

    def __init__(
        self, *formats: PortableFormat[Any, Any]
    ) -> None:
        self._format_ids = {}
        self._format_keys = {}
        self._registrations = []
        self._local_formats = {}
        self.install_formats(formats)

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
            registration = PortableFormatRegistration(
                format_id=format_id, key=format_key
            )
            self.record_format_registration(registration)
        return format_id, registration

    def has_format_key(self, key: PortableFormatKey) -> bool:
        return key in self._local_formats

    def format_keys(self) -> tuple[PortableFormatKey, ...]:
        return tuple(self._local_formats)

    def formats(self) -> tuple[PortableFormat[Any, Any], ...]:
        return tuple(self._local_formats.values())

    def registrations(self) -> tuple[PortableFormatRegistration, ...]:
        return tuple(self._registrations)

    def record_format_registration(
        self, registration: PortableFormatRegistration
    ) -> None:
        existing_key = self._format_keys.get(registration.format_id)
        if existing_key is not None and existing_key != registration.key:
            raise ConflictingPortableFormatRegistrationError(
                "portable format ID conflicts with an existing key: "
                f"{registration.format_id!r}"
            )

        existing_id = self._format_ids.get(registration.key)
        if existing_id is not None and existing_id != registration.format_id:
            raise ConflictingPortableFormatRegistrationError(
                "portable format key conflicts with an existing ID: "
                f"{registration.key.registration_key!r}"
            )

        if existing_key is None:
            self._format_keys[registration.format_id] = registration.key
            self._format_ids[registration.key] = registration.format_id
            self._registrations.append(registration)

    def format_key_for_id(
        self, format_id: PortableFormatID
    ) -> PortableFormatKey:
        try:
            format_key = self._format_keys[format_id]
        except KeyError as error:
            raise UnknownPortableFormatIDError(
                f"unknown portable format ID: {format_id!r}"
            ) from error
        return format_key

    def find_format_id_for_key(
        self, format_key: PortableFormatKey
    ) -> PortableFormatID | None:
        return self._format_ids.get(format_key)

    def format_for_id(
        self, format_id: PortableFormatID
    ) -> PortableFormat[Any, Any]:
        format_key = self.format_key_for_id(format_id)
        return self.from_key(format_key)

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
