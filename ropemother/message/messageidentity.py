#!/usr/bin/env python3
# ropemother/message/messageidentity.py

"""Typed IDs for message records and request/reply correlation."""

from ropemother.util.typedid import TypedID

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T16:54:45+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class MessageID(TypedID):
    """Typed identifier for a bus message."""
    pass


class CorrelationID(TypedID):
    """Typed identifier for a correlated message exchange."""
    pass
