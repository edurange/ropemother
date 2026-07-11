#!/usr/bin/env python3
# ropemother/util/onelinejson.py

"""Serializer for single-line JSON records."""

import json
from typing import Final, TypeAlias

from ropemother.util.serializer import Serializer

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-11T02:25:18+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"

JSONPrimitive: TypeAlias = str | int | float | bool | None

JSONValue: TypeAlias = (
    JSONPrimitive
    | dict[str, "JSONValue"]
    | list["JSONValue"]
)

JSONRecord: TypeAlias = dict[str, JSONValue]


def oneline_serialize(data: JSONValue) -> str:
    """Serialize a JSON value as one newline-terminated record."""
    json_string = json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return json_string


def oneline_deserialize(data: str) -> JSONValue:
    """Deserialize one newline-terminated JSON record."""
    return json.loads(data)


class JSONLSerializer(Serializer[JSONRecord]):
    """Serializer for single-line JSON values."""
    _encoding: str

    def __init__(self, encoding: str = "utf-8") -> None:
        self._encoding: str = encoding

    def encode(self, record: JSONRecord) -> bytes:
        if not isinstance(record, dict):
            raise SerializationError(
                "single-line JSON serializer requires a JSON record"
            )

        json_string = oneline_serialize(record)
        return json_string.encode(self._encoding)

    def decode(self, data: bytes) -> JSONRecord:
        json_string = data.decode(self._encoding)
        value = oneline_deserialize(json_string)

        if not isinstance(value, dict):
            raise SerializationError(
                "single-line JSON payload must contain a JSON record"
            )

        return value


JSONL_SERIALIZER: Final[Serializer[JSONRecord]] = JSONLSerializer()
