#!/usr/bin/env python3
# ropemother/message/typeformats.py

"""Message type to payload format support helpers."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeAlias

from ropemother.exceptions import MessageBusBaseException
from ropemother.format.portableformat import PortableFormat
from ropemother.message.selectors import (
    SymbolCollectionInput,
    normalize_symbol_collection_input,
)
from ropemother.message.symbols import validate_msg_type

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-07T01:06:41+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


SupportedFormatInput: TypeAlias = (
    PortableFormat[Any, Any]
    | tuple[PortableFormat[Any, Any], ...]
    | list[PortableFormat[Any, Any]]
)
SupportedTypeFormatsInput: TypeAlias = Mapping[str, SupportedFormatInput]


class TypeFormatSupportError(MessageBusBaseException):
    """Base exception for message type format support errors."""
    pass


class InvalidTypeFormatSupportError(ValueError, TypeFormatSupportError):
    """Raised when message type format support is invalid."""
    pass


class InvalidTypeFormatSupportTypeError(TypeError, TypeFormatSupportError):
    """Raised when message type format support has the wrong shape."""
    pass


@dataclass(frozen=True, kw_only=True)
class TypeFormatSupport:
    """Portable payload formats supported by one message type."""
    default_format: PortableFormat[Any, Any]
    supported_formats: tuple[PortableFormat[Any, Any], ...]

    def supports(self, payload_format: PortableFormat[Any, Any]) -> bool:
        for supported_format in self.supported_formats:
            if supported_format.key == payload_format.key:
                return True
        return False

    def with_format(
        self, payload_format: PortableFormat[Any, Any]
    ) -> "TypeFormatSupport":
        if self.supports(payload_format):
            return self

        supported_formats = self.supported_formats + (payload_format,)
        support = TypeFormatSupport(
            default_format=self.default_format,
            supported_formats=supported_formats,
        )
        return support


@dataclass(frozen=True, kw_only=True)
class TypeFormatPolicy:
    """Policy for resolving message types and portable payload formats."""
    default_msg_type: str
    default_payload_format: PortableFormat[Any, Any]
    supported_type_formats: dict[str, TypeFormatSupport]
    allow_unlisted_type_formats: bool

    def resolve_msg_type(self, msg_type: str | None) -> str:
        if msg_type is None:
            resolved_msg_type = self.default_msg_type
        else:
            resolved_msg_type = msg_type

        validate_msg_type(resolved_msg_type)
        return resolved_msg_type

    def resolve_payload_format(
        self,
        *,
        msg_type: str,
        payload_format: PortableFormat[Any, Any] | None,
    ) -> PortableFormat[Any, Any]:
        if payload_format is not None:
            return payload_format

        support = self.supported_type_formats.get(msg_type)
        if support is not None:
            return support.default_format

        return self.default_payload_format

    def supports(
        self, *, msg_type: str, payload_format: PortableFormat[Any, Any]
    ) -> bool:
        support = self.supported_type_formats.get(msg_type)

        if support is None:
            return self.allow_unlisted_type_formats

        if self.allow_unlisted_type_formats:
            return True

        return support.supports(payload_format)


def normalize_type_format_policy(
    *,
    msg_type: str,
    payload_format: PortableFormat[Any, Any],
    additional_msg_types: SymbolCollectionInput = (),
    supported_type_formats: SupportedTypeFormatsInput | None = None,
    allow_unlisted_type_formats: bool = False,
) -> TypeFormatPolicy:
    """Build a validated message type to payload format policy."""
    validate_msg_type(msg_type)
    supported = _support_map_from_input(supported_type_formats)
    additional_types = normalize_symbol_collection_input(
        additional_msg_types, argument_name="additional_msg_types"
    )
    _ensure_supported_format(
        supported, msg_type=msg_type, payload_format=payload_format
    )

    for additional_msg_type in additional_types:
        _ensure_supported_format(
            supported,
            msg_type=additional_msg_type,
            payload_format=payload_format,
        )

    policy = TypeFormatPolicy(
        default_msg_type=msg_type,
        default_payload_format=payload_format,
        supported_type_formats=supported,
        allow_unlisted_type_formats=allow_unlisted_type_formats,
    )
    return policy


def _support_map_from_input(
    supported_type_formats: SupportedTypeFormatsInput | None,
) -> dict[str, TypeFormatSupport]:
    supported: dict[str, TypeFormatSupport] = {}
    if supported_type_formats is None:
        return supported

    if not isinstance(supported_type_formats, Mapping):
        value_type = type(supported_type_formats).__name__
        raise InvalidTypeFormatSupportTypeError(
            "supported_type_formats must be a mapping, got " f"{value_type}"
        )

    for msg_type, formats in supported_type_formats.items():
        validate_msg_type(msg_type)
        support = _support_from_format_input(formats)
        supported[msg_type] = support

    return supported


def _support_from_format_input(
    formats: SupportedFormatInput,
) -> TypeFormatSupport:
    supported_formats = _format_tuple_from_input(formats)
    default_format = supported_formats[0]
    support = TypeFormatSupport(
        default_format=default_format, supported_formats=supported_formats
    )
    return support


def _format_tuple_from_input(
    formats: SupportedFormatInput,
) -> tuple[PortableFormat[Any, Any], ...]:
    if isinstance(formats, PortableFormat):
        return (formats,)

    if not isinstance(formats, tuple | list):
        value_type = type(formats).__name__
        raise InvalidTypeFormatSupportTypeError(
            "supported_type_formats values must be portable formats or "
            f"ordered format collections, got {value_type}"
        )

    if not formats:
        raise InvalidTypeFormatSupportError(
            "supported_type_formats values cannot be empty collections"
        )

    supported_formats = []
    seen_keys = set()
    for payload_format in formats:
        if not isinstance(payload_format, PortableFormat):
            value_type = type(payload_format).__name__
            raise InvalidTypeFormatSupportTypeError(
                "supported_type_formats entries must be portable formats, "
                f"got {value_type}"
            )
        if payload_format.key in seen_keys:
            raise InvalidTypeFormatSupportError(
                "supported_type_formats entries cannot duplicate format keys"
            )
        seen_keys.add(payload_format.key)
        supported_formats.append(payload_format)

    return tuple(supported_formats)


def _ensure_supported_format(
    supported: dict[str, TypeFormatSupport],
    *,
    msg_type: str,
    payload_format: PortableFormat[Any, Any],
) -> None:
    validate_msg_type(msg_type)
    support = supported.get(msg_type)
    if support is None:
        support = TypeFormatSupport(
            default_format=payload_format,
            supported_formats=(payload_format,),
        )
    else:
        support = support.with_format(payload_format)
    supported[msg_type] = support
