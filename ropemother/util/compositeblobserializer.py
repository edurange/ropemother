#!/usr/bin/env python3
# ropemother/util/compositeblobserializer.py

"""A Serializer strategy using JSONL headers with binary attachments."""

from collections.abc import Sequence
from typing import cast, Final, TypeAlias

from ropemother.util.lengthprefixed import (
    pack_prefixed_sequence,
    unpack_prefixed_sequence,
)
from ropemother.util.onelinejson import (
    JSONPrimitive,
    JSONRecord,
    JSONValue,
    oneline_serialize,
    oneline_deserialize,
)
from ropemother.util.serializer import Serializer, SerializationError

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T04:00:59+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


CompositeValue: TypeAlias = (
    JSONPrimitive
    | bytes
    | list["CompositeValue"]
    | dict[str, "CompositeValue"]
)

CompositeRecord: TypeAlias = dict[str, CompositeValue]


# I'm a little concerned that type annotations might get lost across Serializer
# implementations like this
class CompositeBlobSerializer(Serializer[CompositeRecord]):
    """Serializer for JSON-compatible records with attached byte blobs."""
    _encoding: str

    def __init__(self, encoding: str = "utf-8") -> None:
        self._encoding: str = encoding

    def encode(self, value: CompositeRecord) -> bytes:
        blobs: list[bytes] = []

        structural_record = deconstruct(value, blobs)
        # This also seems off - does it really need to be an instance method?
        # Feels like this should be more functional
        json_str = oneline_serialize(cast(JSONRecord, structural_record))
        json_bytes = json_str.encode(self._encoding)
        # Are the length prefixes sufficient structure, or do we need to
        # distinguish between a JSON header (to represent the composite format)
        # and a JSON content envelope?

        return pack_prefixed_sequence([json_bytes, *blobs])

    def decode(self, data: bytes) -> CompositeRecord:
        segments = unpack_prefixed_sequence(data)
        if not segments:
            raise SerializationError("empty record encountered")

        header_bytes = segments[0]
        blobs = segments[1:]
        structural_record = oneline_deserialize(
            header_bytes.decode(self._encoding)
        )

        return cast(CompositeRecord, reconstruct(structural_record, blobs))


COMPOSITE_BLOB_SERIALIZER: Final[Serializer[CompositeRecord]] = (
    CompositeBlobSerializer()
)


def deconstruct(value: CompositeValue, blobs: list[bytes]) -> JSONValue:
    if isinstance(value, bytes):
        ref_str = f"attached:{len(blobs)}"
        blobs.append(value)
        return {"$ref": ref_str}

    if isinstance(value, dict):
        return {k: deconstruct(v, blobs) for k, v in value.items()}

    if isinstance(value, list):
        return [deconstruct(v, blobs) for v in value]

    return value


def reconstruct(value: JSONValue, blobs: Sequence[bytes]) -> CompositeValue:
    # Can we tighten up this type annotation later?
    if isinstance(value, dict):
        if "$ref" in value and len(value) == 1:
            ref_path = cast(str, value["$ref"])
            if ref_path.startswith("attached:"):
                blob_index = int(ref_path.split(":")[1])
                return blobs[blob_index]
        return {k: reconstruct(v, blobs) for k, v in value.items()}

    if isinstance(value, list):
        return [reconstruct(v, blobs) for v in value]

    return value
