#!/usr/bin/env python3
# ropemother/util/onelinejson.py

"""An single-line-JSON serialization discipline."""

import json
from typing import TypeAlias

from ropemother.util.serializer import Serializer

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T20:05:44+00:00"
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


def oneline_serialize(data: JSONValue) -> bytes:
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


class JSONLSerializer(Serializer[JSONValue]):
    """Serializer for single-line JSON values."""
    _encoding: str

    def __init__(self, encoding: str = "utf-8") -> None:
        self._encoding: str = encoding

    def encode(self, record: JSONRecord) -> bytes:
        json_string = oneline_serialize(record)
        return json_string.encode(self._encoding)

    def decode(self, data: bytes) -> JSONRecord:
        json_string = data.decode(self._encoding)
        return oneline_deserialize(json_string)
