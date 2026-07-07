#!/usr/bin/env python3
# ropemother/client/procedure.py

"""Procedure invocation payloads and formats."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from ropemother.exceptions import MessageBusBaseException
from ropemother.format.portableformat import PortableFormat, PortableFormatKey
from ropemother.util.onelinejson import JSONRecord, JSONValue, JSONLSerializer
from ropemother.util.serializer import TypeAdapter

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-06T06:08:38+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


class ProcedureError(MessageBusBaseException):
    """Base exception for procedure client and service errors."""
    pass


class InvalidProcedureInvocationError(ValueError, ProcedureError):
    """Raised when procedure invocation data is invalid."""
    pass


class InvalidProcedureInvocationTypeError(TypeError, ProcedureError):
    """Raised when procedure invocation data has the wrong type."""
    pass


@dataclass(frozen=True, kw_only=True, slots=True)
class ProcedureInvocation:
    """Message payload representing one procedure invocation."""
    positional_arguments: tuple[Any, ...] = ()
    keyword_arguments: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        positional_arguments = tuple(self.positional_arguments)
        keyword_arguments = dict(self.keyword_arguments)

        for name in keyword_arguments:
            if not isinstance(name, str):
                raise InvalidProcedureInvocationError(
                    "procedure keyword argument names must be strings"
                )

        protected_keyword_arguments = MappingProxyType(keyword_arguments)
        object.__setattr__(
            self, "positional_arguments", positional_arguments
        )
        object.__setattr__(
            self, "keyword_arguments", protected_keyword_arguments
        )

    @classmethod
    def from_call(
        cls, *args: Any, **kwargs: Any
    ) -> "ProcedureInvocation":
        return cls(positional_arguments=args, keyword_arguments=kwargs)

    def keyword_argument_dict(self) -> dict[str, Any]:
        return dict(self.keyword_arguments)


class ProcedureInvocationJSONAdapter(
    TypeAdapter[ProcedureInvocation, JSONRecord]
):
    """Adapter between procedure invocations and JSON records."""

    def encode(self, value: ProcedureInvocation) -> JSONRecord:
        invocation = ensure_procedure_invocation(value)
        record: JSONRecord = {
            "positional_arguments": list(invocation.positional_arguments),
            "keyword_arguments": dict(invocation.keyword_arguments),
        }
        return record

    def decode(self, data: JSONRecord) -> ProcedureInvocation:
        record = _ensure_json_record(data)
        positional_arguments = _read_positional_arguments(record)
        keyword_arguments = _read_keyword_arguments(record)
        invocation = ProcedureInvocation(
            positional_arguments=positional_arguments,
            keyword_arguments=keyword_arguments,
        )
        return invocation


def ensure_procedure_invocation(value: Any) -> ProcedureInvocation:
    if not isinstance(value, ProcedureInvocation):
        raise InvalidProcedureInvocationTypeError(
            "procedure request payload must be a ProcedureInvocation"
        )

    return value


def _ensure_json_record(value: Any) -> JSONRecord:
    if not isinstance(value, dict):
        raise InvalidProcedureInvocationTypeError(
            "procedure invocation JSON data must be a record"
        )

    return value


def _read_positional_arguments(record: JSONRecord) -> tuple[Any, ...]:
    value = record.get("positional_arguments")
    if not isinstance(value, list):
        raise InvalidProcedureInvocationTypeError(
            "procedure positional arguments must be a JSON list"
        )

    return tuple(value)


def _read_keyword_arguments(record: JSONRecord) -> dict[str, Any]:
    value = record.get("keyword_arguments")
    if not isinstance(value, dict):
        raise InvalidProcedureInvocationTypeError(
            "procedure keyword arguments must be a JSON record"
        )

    keyword_arguments: dict[str, Any] = {}
    for name, argument in value.items():
        if not isinstance(name, str):
            raise InvalidProcedureInvocationError(
                "procedure keyword argument names must be strings"
            )
        keyword_arguments[name] = argument

    return keyword_arguments


PROCEDURE_INVOCATION_JSON_FORMAT = PortableFormat[
    ProcedureInvocation,
    JSONValue,
](
    key=PortableFormatKey.from_str("procedure-invocation-json"),
    adapter=ProcedureInvocationJSONAdapter(),
    serializer=JSONLSerializer(),
)
