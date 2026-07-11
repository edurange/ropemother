#!/usr/bin/env python3
# ropemother/util/__init__.py

"""Shared serialization and encoding utilities."""

from ropemother.util.compositeblobserializer import (
    CompositeBlobSerializer,
    CompositeRecord,
    CompositeValue,
)
from ropemother.util.onelinejson import (
    JSONL_SERIALIZER,
    JSONPrimitive,
    JSONRecord,
    JSONValue,
    oneline_deserialize,
    oneline_serialize,
)
from ropemother.util.serializer import (
    COMPOSITE_BLOB_SERIALIZER,
    IDENTITY_BYTES_ADAPTER,
    IDENTITY_SERIALIZER,
    IdentityAdapter,
    JSONL_SERIALIZER,
    SerializationError,
    Serializer,
    TypeAdapter,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-11T02:00:41+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"

__all__ = [
    "COMPOSITE_BLOB_SERIALIZER",
    "CompositeBlobSerializer",
    "CompositeRecord",
    "CompositeValue",
    "IDENTITY_BYTES_ADAPTER",
    "IDENTITY_SERIALIZER",
    "IdentityAdapter",
    "JSONL_SERIALIZER",
    "JSONPrimitive",
    "JSONRecord",
    "JSONValue",
    "SerializationError",
    "Serializer",
    "TypeAdapter",
    "oneline_deserialize",
    "oneline_serialize",
]
