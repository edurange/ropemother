#!/usr/bin/env python3
# ropemother/format/portableformat.py

"""Portable format definitions for message bus payload representation."""

from dataclasses import dataclass
from typing import Final, TypeVar

from ropemother.exceptions import MessageBusBaseException
from ropemother.util.compositeblobserializer import (
    CompositeRecord,
    COMPOSITE_BLOB_SERIALIZER,
)
from ropemother.util.onelinejson import JSONL_SERIALIZER, JSONValue
from ropemother.util.serializer import (
    IDENTITY_BYTES_ADAPTER,
    IDENTITY_SERIALIZER,
    IdentityAdapter,
    Serializer,
    TypeAdapter,
)
from ropemother.util.symbol import (
    Symbol,
    is_ascii_alphanumeric,
    is_simple_symbol_character,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-14T15:36:43+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


DomainT = TypeVar("DomainT")
PortablePayloadT = TypeVar("PortablePayloadT")


class PortableFormatError(MessageBusBaseException):
    """Base exception for portable payload format errors."""
    pass


class InvalidFormatSymbolError(ValueError, PortableFormatError):
    """Raised when a portable format symbol is invalid."""
    pass


class InvalidFormatSymbolTypeError(TypeError, PortableFormatError):
    """Raised when a portable format symbol has the wrong type."""
    pass


class InvalidFormatVersionError(ValueError, PortableFormatError):
    """Raised when a portable format version is invalid."""
    pass


class InvalidFormatVersionTypeError(TypeError, PortableFormatError):
    """Raised when a portable format version has the wrong type."""
    pass


@dataclass(frozen=True, slots=True)
class PortableFormatSymbol(Symbol):
    """Symbol identifying a portable payload format."""
    def __post_init__(self) -> None:
        _validate_portable_format_symbol(self.value)


@dataclass(frozen=True, kw_only=True, slots=True)
class PortableFormatKey:
    """Registration key for a portable payload format."""
    symbol: PortableFormatSymbol
    version: str | None = None

    def __post_init__(self) -> None:
        if self.version is not None:
            _validate_portable_format_version(self.version)

    @property
    def registration_key(self) -> str:
        if self.version is None:
            key = self.symbol.value
        else:
            key = f"{self.symbol.value}:{self.version}"
        return key

    @classmethod
    def from_str(
        cls, symbol: str, *, version: str | None = None
    ) -> "PortableFormatKey":
        return cls(symbol=PortableFormatSymbol(symbol), version=version)


@dataclass(frozen=True, kw_only=True)
class PortableFormat[DomainT, PortablePayloadT]:
    """Named policy for adapting and serializing message payloads."""
    key: PortableFormatKey
    adapter: TypeAdapter[DomainT, PortablePayloadT]
    serializer: Serializer[PortablePayloadT]

    def encode(self, value: DomainT) -> bytes:
        payload = self.adapter.encode(value)
        data = self.serializer.encode(payload)
        return data

    def decode(self, data: bytes) -> DomainT:
        payload = self.serializer.decode(data)
        value = self.adapter.decode(payload)
        return value


def _validate_portable_format_symbol(symbol: str) -> None:
    if not isinstance(symbol, str):
        raise InvalidFormatSymbolTypeError(
            f"portable format symbol value must be a string: got {symbol!r}"
        )

    if symbol == "":
        raise InvalidFormatSymbolError(
            "portable format symbol must not be empty"
        )

    first = symbol[0]
    if not is_ascii_alphanumeric(first):
        raise InvalidFormatSymbolError(
            "portable format symbol must start with an ASCII letter or digit"
        )

    for char in symbol[1:]:
        if not is_simple_symbol_character(char):
            raise InvalidFormatSymbolError(
                "portable format symbol may contain only ASCII letters, "
                "digits, hyphen, or underscore"
            )


def _validate_portable_format_version(version: str) -> None:
    if not isinstance(version, str):
        raise InvalidFormatVersionTypeError(
            f"portable format version must be a string: got {version!r}"
        )

    if version == "":
        raise InvalidFormatVersionError(
            "portable format version must not be empty"
        )

    for char in version:
        if not is_simple_symbol_character(char):
            raise InvalidFormatVersionError(
                "portable format version may contain only ASCII letters, "
                "digits, hyphen, or underscore"
            )


RAW_BYTES_FORMAT_KEY: Final = PortableFormatKey.from_str("raw-bytes")
JSON_FORMAT_KEY: Final = PortableFormatKey.from_str("json")
COMPOSITE_FORMAT_KEY: Final = PortableFormatKey.from_str("composite-json")

JSON_VALUE_ADAPTER: Final = IdentityAdapter[JSONValue]()
COMPOSITE_RECORD_ADAPTER: Final = IdentityAdapter[CompositeRecord]()

RAW_BYTES_PORTABLE_FORMAT: Final = PortableFormat(
    key=RAW_BYTES_FORMAT_KEY,
    adapter=IDENTITY_BYTES_ADAPTER,
    serializer=IDENTITY_SERIALIZER,
)

JSON_PORTABLE_FORMAT: Final = PortableFormat(
    key=JSON_FORMAT_KEY,
    adapter=JSON_VALUE_ADAPTER,
    serializer=JSONL_SERIALIZER,
)

COMPOSITE_PORTABLE_FORMAT: Final = PortableFormat(
    key=COMPOSITE_FORMAT_KEY,
    adapter=COMPOSITE_RECORD_ADAPTER,
    serializer=COMPOSITE_BLOB_SERIALIZER,
)
