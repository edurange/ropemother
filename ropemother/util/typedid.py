#!/usr/bin/env python3
# ropemother/util/typedid.py

"""Shared base class for typed non-negative integer IDs."""

from typing import override

from ropemother.exceptions import MessageBusBaseException

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T20:20:03+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


class TypedIDError(MessageBusBaseException):
    """Base exception for typed identifier errors."""
    pass


class InvalidTypedIDError(ValueError, TypedIDError):
    """Raised when a typed identifier value is invalid."""
    pass


class InvalidTypedIDTypeError(TypeError, TypedIDError):
    """Raised when a typed identifier has the wrong type."""
    pass


class TypedID(int):
    """Typed non-negative integer identifier base class."""

    def __new__(cls, value: int) -> "TypedID":
        if type(value) is bool:
            raise InvalidTypedIDTypeError(
                f"{cls.__name__} must not be a bool value"
            )
        if isinstance(value, TypedID) and type(value) is not cls:
            raise InvalidTypedIDTypeError(
                f"expected {cls.__name__}, got {type(value).__name__}"
            )
        if not isinstance(value, int):
            raise InvalidTypedIDTypeError(
                f"{cls.__name__} must be an integer: got {value!r}"
            )
        if value < 0:
            raise InvalidTypedIDError(
                f"{cls.__name__} must be non-negative: got {value}"
            )
        return int.__new__(cls, value)

    @override
    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return False
        return int(self) == int(other)

    @override
    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash((type(self), int(self)))