#!/usr/bin/env python3
# ropemother/fixtures/scriptedinput.py

"""File-backed scripted message input fixtures."""

import asyncio
import base64
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Any, Iterable

from ropemother.broker.asyncbase import AsyncMessageBus
from ropemother.broker.asyncendpoints import AsyncEmitter
from ropemother.broker.base import MessageBus
from ropemother.broker.endpoints import Emitter
from ropemother.exceptions import MessageBusBaseException
from ropemother.format.defaults import default_portable_format_registry
from ropemother.format.formattable import PortableFormatTable
from ropemother.format.portableformat import (
    COMPOSITE_PORTABLE_FORMAT,
    JSON_PORTABLE_FORMAT,
    RAW_BYTES_PORTABLE_FORMAT,
    PortableFormat,
    PortableFormatKey,
)
from ropemother.util.compositeblobserializer import CompositeValue
from ropemother.util.onelinejson import (
    JSONRecord,
    JSONValue,
    oneline_deserialize,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T17:26:24+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


DEFAULT_SCRIPTED_INPUT_FORMAT = JSON_PORTABLE_FORMAT


class ScriptedInputError(MessageBusBaseException):
    """Base exception for scripted input fixture errors."""
    pass


class UnsupportedScriptedInputPayloadError(TypeError, ScriptedInputError):
    """Raised when a scripted input payload shape is unsupported."""
    pass


class InvalidScriptedInputRecordError(ValueError, ScriptedInputError):
    """Raised when a scripted input record is invalid."""
    pass


@dataclass(frozen=True, kw_only=True)
class ScriptedInputEvent:
    """One scheduled message event in a scripted input plan."""
    at: float
    msg_topic: str
    msg_type: str
    msg_producer: str
    payload: Any
    payload_format: PortableFormat[Any, Any]


@dataclass(frozen=True, kw_only=True)
class ScriptedInputPlan:
    """Ordered scripted events for fixture-driven message emission."""
    events: tuple[ScriptedInputEvent, ...]

    @classmethod
    def from_jsonl(
        cls,
        path: str | Path,
        *,
        format_registry: PortableFormatTable | None = None,
    ) -> "ScriptedInputPlan":
        source_path = Path(path)
        text = source_path.read_text(encoding="utf-8")
        return cls.from_jsonl_text(text, format_registry=format_registry)

    @classmethod
    def from_jsonl_text(
        cls,
        text: str,
        *,
        format_registry: PortableFormatTable | None = None,
    ) -> "ScriptedInputPlan":
        if format_registry is None:
            format_registry = default_portable_format_registry()

        events: list[ScriptedInputEvent] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            stripped_line = line.strip()
            if stripped_line == "":
                continue
            record = _record_from_line(stripped_line, line_number)
            event = _event_from_record(
                record,
                line_number=line_number,
                format_registry=format_registry,
            )
            events.append(event)
        return cls(events=tuple(events))

    @classmethod
    def from_records(
        cls,
        records: Iterable[JSONRecord],
        *,
        format_registry: PortableFormatTable | None = None,
    ) -> "ScriptedInputPlan":
        if format_registry is None:
            format_registry = default_portable_format_registry()

        events: list[ScriptedInputEvent] = []
        for index, record in enumerate(records):
            event = _event_from_record(
                record, line_number=index + 1, format_registry=format_registry
            )
            events.append(event)
        return cls(events=tuple(events))


@dataclass(frozen=True, kw_only=True)
class _EmitterKey:
    msg_topic: str
    msg_type: str
    msg_producer: str
    payload_format_key: PortableFormatKey


class ScriptedInputEmitter:
    """Fixture that emits a scripted input plan on a sync bus."""
    _bus: MessageBus
    _plan: ScriptedInputPlan
    _emitters: dict[_EmitterKey, Emitter]

    def __init__(self, bus: MessageBus, plan: ScriptedInputPlan) -> None:
        self._bus = bus
        self._plan = plan
        self._emitters = {}

    @property
    def plan(self) -> ScriptedInputPlan:
        return self._plan

    def emit_all(self) -> int:
        count = 0
        for event in self._plan.events:
            self.emit_event(event)
            count += 1
        return count

    def emit_realtime(self) -> int:
        count = 0
        last_time = 0.0
        for event in self._plan.events:
            delay = event.at - last_time
            if delay > 0:
                sleep(delay)
            self.emit_event(event)
            last_time = event.at
            count += 1
        return count

    def emit_event(self, event: ScriptedInputEvent) -> None:
        emitter = self._emitter_for(event)
        emitter.emit(event.payload, payload_format=event.payload_format)

    def _emitter_for(self, event: ScriptedInputEvent) -> Emitter:
        key = _emitter_key_for(event)
        emitter = self._emitters.get(key)
        if emitter is None:
            emitter = self._bus.register_emitter(
                msg_topic=event.msg_topic,
                msg_producer=event.msg_producer,
                msg_type=event.msg_type,
                payload_format=event.payload_format,
            )
            self._emitters[key] = emitter
        return emitter


class AsyncScriptedInputEmitter:
    """Fixture that emits a scripted input plan on an async bus."""
    _bus: AsyncMessageBus
    _plan: ScriptedInputPlan
    _emitters: dict[_EmitterKey, AsyncEmitter]

    def __init__(
        self, bus: AsyncMessageBus, plan: ScriptedInputPlan
    ) -> None:
        self._bus = bus
        self._plan = plan
        self._emitters = {}

    @property
    def plan(self) -> ScriptedInputPlan:
        return self._plan

    async def emit_all(self) -> int:
        count = 0
        for event in self._plan.events:
            await self.emit_event(event)
            count += 1
        return count

    async def emit_realtime(self) -> int:
        count = 0
        last_time = 0.0
        for event in self._plan.events:
            delay = event.at - last_time
            if delay > 0:
                await asyncio.sleep(delay)
            await self.emit_event(event)
            last_time = event.at
            count += 1
        return count

    async def emit_event(self, event: ScriptedInputEvent) -> None:
        emitter = self._emitter_for(event)
        await emitter.emit(event.payload, payload_format=event.payload_format)

    def _emitter_for(self, event: ScriptedInputEvent) -> AsyncEmitter:
        key = _emitter_key_for(event)
        emitter = self._emitters.get(key)
        if emitter is None:
            emitter = self._bus.register_emitter(
                msg_topic=event.msg_topic,
                msg_producer=event.msg_producer,
                msg_type=event.msg_type,
                payload_format=event.payload_format,
            )
            self._emitters[key] = emitter
        return emitter


def _emitter_key_for(event: ScriptedInputEvent) -> _EmitterKey:
    key = _EmitterKey(
        msg_topic=event.msg_topic,
        msg_type=event.msg_type,
        msg_producer=event.msg_producer,
        payload_format_key=event.payload_format.key,
    )
    return key


def _record_from_line(line: str, line_number: int) -> JSONRecord:
    try:
        record = oneline_deserialize(line)
    except ValueError as e:
        raise InvalidScriptedInputRecordError(
            f"invalid JSON on scripted input line {line_number}"
        ) from e
    if not isinstance(record, dict):
        raise InvalidScriptedInputRecordError(
            f"scripted input line {line_number} must contain a record"
        )
    return record


def _event_from_record(
    record: JSONRecord,
    *,
    line_number: int,
    format_registry: PortableFormatTable,
) -> ScriptedInputEvent:
    format_key = _format_key_from_record(record)
    payload_format = format_registry.from_key(format_key)
    event = ScriptedInputEvent(
        at=_optional_float(record, "at", line_number=line_number),
        msg_topic=_required_str(record, "msg_topic", line_number),
        msg_type=_required_str(record, "msg_type", line_number),
        msg_producer=_required_str(record, "msg_producer", line_number),
        payload=_payload_from_record(record, payload_format=payload_format),
        payload_format=payload_format,
    )
    return event


def _format_key_from_record(record: JSONRecord) -> PortableFormatKey:
    value = record.get("payload_format")
    if value is None:
        key = DEFAULT_SCRIPTED_INPUT_FORMAT.key
    elif isinstance(value, str):
        key = _format_key_from_string(value)
    else:
        raise InvalidScriptedInputRecordError(
            "scripted input field 'payload_format' must be a string"
        )
    return key


def _format_key_from_string(value: str) -> PortableFormatKey:
    if ":" in value:
        symbol, version = value.split(":", 1)
        key = PortableFormatKey.from_str(symbol, version=version)
    else:
        key = PortableFormatKey.from_str(value)
    return key


def _payload_from_record(
    record: JSONRecord,
    *,
    payload_format: PortableFormat[Any, Any],
) -> Any:
    if "payload_base64" in record:
        payload = _bytes_from_base64(record["payload_base64"])
    elif "payload_text" in record:
        payload = _payload_text_value(
            record["payload_text"], payload_format=payload_format
        )
    else:
        payload = _portable_payload_value(record.get("payload"))
        if payload_format.key == RAW_BYTES_PORTABLE_FORMAT.key:
            payload = _raw_bytes_payload(payload)
    return payload


def _payload_text_value(
    value: JSONValue,
    *,
    payload_format: PortableFormat[Any, Any],
) -> str | bytes:
    if not isinstance(value, str):
        raise InvalidScriptedInputRecordError(
            "scripted input field 'payload_text' must be a string"
        )
    if payload_format.key == RAW_BYTES_PORTABLE_FORMAT.key:
        payload = value.encode("utf-8")
    else:
        payload = value
    return payload


def _bytes_from_base64(value: JSONValue) -> bytes:
    if not isinstance(value, str):
        raise InvalidScriptedInputRecordError(
            "scripted input field 'payload_base64' must be a string"
        )
    try:
        payload = base64.b64decode(value, validate=True)
    except ValueError as e:
        raise InvalidScriptedInputRecordError(
            "scripted input field 'payload_base64' is not valid base64"
        ) from e
    return payload


def _raw_bytes_payload(value: CompositeValue) -> bytes:
    if isinstance(value, bytes):
        payload = value
    elif isinstance(value, str):
        payload = value.encode("utf-8")
    else:
        raise UnsupportedScriptedInputPayloadError(
            "raw-bytes scripted input payload must be bytes or text"
        )
    return payload


def _portable_payload_value(value: JSONValue) -> CompositeValue:
    if value is None:
        return None
    if type(value) in (str, int, float, bool):
        return value
    if isinstance(value, list):
        result = []
        for item in value:
            result.append(_portable_payload_value(item))
        return result
    if isinstance(value, dict):
        bytes_value = _bytes_marker_value(value)
        if bytes_value is not None:
            return bytes_value
        result = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise UnsupportedScriptedInputPayloadError(
                    "scripted input payload record keys must be strings"
                )
            result[key] = _portable_payload_value(item)
        return result
    raise UnsupportedScriptedInputPayloadError(
        "scripted input payload is not portable: " + repr(value)
    )


def _bytes_marker_value(record: dict[str, JSONValue]) -> bytes | None:
    value = None
    if set(record) == {"$bytes_base64"}:
        value = _bytes_from_base64(record["$bytes_base64"])
    return value


def _required_str(
    record: JSONRecord, key: str, line_number: int
) -> str:
    value = record.get(key)
    if not isinstance(value, str):
        raise InvalidScriptedInputRecordError(
            f"scripted input line {line_number} field {key!r} must be a string"
        )
    return value


def _optional_float(
    record: JSONRecord, key: str, *, line_number: int
) -> float:
    value = record.get(key, 0.0)
    if type(value) not in (int, float):
        raise InvalidScriptedInputRecordError(
            f"scripted input line {line_number} field {key!r} must be numeric"
        )
    result = float(value)
    if result < 0:
        raise InvalidScriptedInputRecordError(
            "scripted input line "
            f"{line_number} field {key!r} must not be negative"
        )
    return result
