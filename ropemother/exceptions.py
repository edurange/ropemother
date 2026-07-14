#!/usr/bin/env python3
# ropemother/exceptions.py

"""Common exceptions raised by message bus classes."""

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T05:58:05+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


__all__ = [
    "CaptureDisabledError",
    "CaptureError",
    "CaptureUnavailableError",
    "InvalidReceiveCountError",
    "MessageBusBaseException",
    "MissingMessageTypeError",
    "PayloadSerializationError",
]


class MessageBusBaseException(Exception):
    """Base exception for message bus errors."""
    pass


class CaptureError(MessageBusBaseException):
    """Base exception for capture-related bus errors."""
    pass


class CaptureUnavailableError(RuntimeError, CaptureError):
    """Raised when capture is expected but unavailable."""
    pass


class CaptureDisabledError(RuntimeError, CaptureError):
    """Raised when capture-only behavior is used without capture."""
    pass


# Could this have a more specific in-module parent?
class PayloadSerializationError(ValueError, MessageBusBaseException):
    """Raised when a payload cannot be serialized for bus use."""
    pass


# Same question - can this be more specific?
class MissingMessageTypeError(ValueError, MessageBusBaseException):
    """Raised when an emit operation lacks a message type."""
    pass


# Clean these up - these are all very niche and don't belong as direct
# inheritors of the module-wide base
class InvalidReceiveCountError(ValueError, MessageBusBaseException):
    """Raised when a receive batch count is invalid."""
    pass
