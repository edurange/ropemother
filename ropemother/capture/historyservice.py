#!/usr/bin/env python3
# ropemother/capture/historyservice.py

"""Request/reply adapters for message history queries."""

from typing import Any, Iterable

from ropemother.capture.history import (
    MessageHistory,
    MessageHistoryEntry,
    MessageHistoryPage,
    decode_history_payload,
)
from ropemother.capture.historyselection import (
    DEFAULT_HISTORY_MAX_COUNT,
    HistorySelection,
    history_selection_from_args,
)
from ropemother.client.asyncrequest import (
    AsyncRequestClient,
    AsyncRequestService,
    AsyncServiceRequest,
)
from ropemother.client.request import (
    RequestClient,
    RequestHandle,
    RequestService,
    ServiceRequest,
)
from ropemother.exceptions import MessageBusBaseException
from ropemother.format.defaults import default_portable_format_registry
from ropemother.format.formattable import PortableFormatTable
from ropemother.format.portableformat import (
    COMPOSITE_PORTABLE_FORMAT,
    JSON_PORTABLE_FORMAT,
    PortableFormat,
    PortableFormatKey,
)
from ropemother.message.messageidentity import CorrelationID, MessageID
from ropemother.message.records import BusOperation
from ropemother.util.compositeblobserializer import CompositeRecord

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-22T15:45:35+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


HISTORY_SELECTION_RECORD_TYPE = "HistorySelection"
HISTORY_PAGE_RECORD_TYPE = "MessageHistoryPage"
HISTORY_ENTRY_RECORD_TYPE = "MessageHistoryEntry"
DEFAULT_HISTORY_SELECTION_FORMAT = JSON_PORTABLE_FORMAT
DEFAULT_HISTORY_PAGE_FORMAT = COMPOSITE_PORTABLE_FORMAT


class MessageHistoryServiceError(MessageBusBaseException):
    """Base exception for message history service errors."""
    pass


class InvalidMessageHistoryRecordError(
    ValueError, MessageHistoryServiceError
):
    """Raised when a history service payload is not a valid record."""
    pass


class HistoryClient:
    """Request/reply client for querying captured message history."""
    _client: RequestClient
    _selection_format: PortableFormat[Any, Any]
    _page_format: PortableFormat[Any, Any]
    _format_registry: PortableFormatTable

    def __init__(
        self,
        client: RequestClient,
        *,
        extra_formats: Iterable[PortableFormat[Any, Any]] = (),
        selection_format: PortableFormat[Any, Any] | None = None,
        page_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        format_registry = default_portable_format_registry(
            extra_formats=extra_formats
        )
        self._initialize(
            client,
            format_registry=format_registry,
            selection_format=selection_format,
            page_format=page_format,
        )

    def send(
        self,
        *,
        msg_topic: str | None = None,
        msg_type: str | None = None,
        msg_producer: str | None = None,
        bus_operation: BusOperation | None = None,
        start_sequence: int | None = None,
        stop_sequence: int | None = None,
        max_count: int = DEFAULT_HISTORY_MAX_COUNT,
    ) -> RequestHandle:
        selection = history_selection_from_args(
            msg_topic=msg_topic,
            msg_type=msg_type,
            msg_producer=msg_producer,
            bus_operation=bus_operation,
            start_sequence=start_sequence,
            stop_sequence=stop_sequence,
            max_count=max_count,
        )
        request_payload = message_history_selection_record(selection)
        handle = self._client.send(
            request_payload, payload_format=self._selection_format
        )
        return handle

    def receive(self, handle: RequestHandle) -> MessageHistoryPage:
        reply = self._client.receive(handle)
        page = message_history_page_from_record(
            reply.payload, format_table=self._format_registry
        )
        return page

    def select(
        self,
        *,
        msg_topic: str | None = None,
        msg_type: str | None = None,
        msg_producer: str | None = None,
        bus_operation: BusOperation | None = None,
        start_sequence: int | None = None,
        stop_sequence: int | None = None,
        max_count: int = DEFAULT_HISTORY_MAX_COUNT,
    ) -> MessageHistoryPage:
        handle = self.send(
            msg_topic=msg_topic,
            msg_type=msg_type,
            msg_producer=msg_producer,
            bus_operation=bus_operation,
            start_sequence=start_sequence,
            stop_sequence=stop_sequence,
            max_count=max_count,
        )
        return self.receive(handle)

    @property
    def request_format(self) -> PortableFormat[Any, Any]:
        return self._selection_format

    @property
    def reply_format(self) -> PortableFormat[Any, Any]:
        return self._page_format

    def _initialize(
        self,
        client: RequestClient,
        *,
        format_registry: PortableFormatTable,
        selection_format: PortableFormat[Any, Any] | None,
        page_format: PortableFormat[Any, Any] | None,
    ) -> None:
        if selection_format is None:
            selection_format = DEFAULT_HISTORY_SELECTION_FORMAT
        if page_format is None:
            page_format = DEFAULT_HISTORY_PAGE_FORMAT
        self._client = client
        self._selection_format = selection_format
        self._page_format = page_format
        self._format_registry = format_registry

    @classmethod
    def _from_format_registry(
        cls,
        client: RequestClient,
        *,
        format_registry: PortableFormatTable,
        selection_format: PortableFormat[Any, Any] | None = None,
        page_format: PortableFormat[Any, Any] | None = None,
    ) -> "HistoryClient":
        history_client = cls.__new__(cls)
        history_client._initialize(
            client,
            format_registry=format_registry,
            selection_format=selection_format,
            page_format=page_format,
        )
        return history_client


class HistoryService:
    """Request/reply service that answers message history queries."""
    _history: MessageHistory
    _service: RequestService
    _page_format: PortableFormat[Any, Any]

    def __init__(
        self,
        history: MessageHistory,
        service: RequestService,
        *,
        page_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        if page_format is None:
            page_format = DEFAULT_HISTORY_PAGE_FORMAT
        self._history = history
        self._service = service
        self._page_format = page_format

    def handle(self) -> None:
        request = self._service.receive()
        self._handle_request(request)

    def handle_nowait(self) -> bool:
        request = self._service.receive_nowait()
        handled = False
        if request is not None:
            self._handle_request(request)
            handled = True
        return handled

    def handle_available(self) -> int:
        requests = self._service.receive_available()
        for request in requests:
            self._handle_request(request)
        return len(requests)

    def handle_many(self, max_count: int) -> int:
        requests = self._service.receive_many(max_count)
        for request in requests:
            self._handle_request(request)
        return len(requests)

    def _handle_request(self, request: ServiceRequest) -> None:
        selection = message_history_selection_from_record(request.payload)
        page = self._history.select(
            msg_topic=selection.msg_topic,
            msg_type=selection.msg_type,
            msg_producer=selection.msg_producer,
            bus_operation=selection.bus_operation,
            start_sequence=selection.start_sequence,
            stop_sequence=selection.stop_sequence,
            max_count=selection.max_count,
        )
        reply_payload = message_history_page_record(page)
        request.reply(reply_payload, payload_format=self._page_format)


class AsyncHistoryClient:
    """Async request/reply client for querying captured message history."""
    _client: AsyncRequestClient
    _selection_format: PortableFormat[Any, Any]
    _page_format: PortableFormat[Any, Any]
    _format_registry: PortableFormatTable

    def __init__(
        self,
        client: AsyncRequestClient,
        *,
        extra_formats: Iterable[PortableFormat[Any, Any]] = (),
        selection_format: PortableFormat[Any, Any] | None = None,
        page_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        format_registry = default_portable_format_registry(
            extra_formats=extra_formats
        )
        self._initialize(
            client,
            format_registry=format_registry,
            selection_format=selection_format,
            page_format=page_format,
        )

    async def send(
        self,
        *,
        msg_topic: str | None = None,
        msg_type: str | None = None,
        msg_producer: str | None = None,
        bus_operation: BusOperation | None = None,
        start_sequence: int | None = None,
        stop_sequence: int | None = None,
        max_count: int = DEFAULT_HISTORY_MAX_COUNT,
    ) -> RequestHandle:
        selection = history_selection_from_args(
            msg_topic=msg_topic,
            msg_type=msg_type,
            msg_producer=msg_producer,
            bus_operation=bus_operation,
            start_sequence=start_sequence,
            stop_sequence=stop_sequence,
            max_count=max_count,
        )
        request_payload = message_history_selection_record(selection)
        handle = await self._client.send(
            request_payload, payload_format=self._selection_format
        )
        return handle

    async def receive(self, handle: RequestHandle) -> MessageHistoryPage:
        reply = await self._client.receive(handle)
        page = message_history_page_from_record(
            reply.payload, format_table=self._format_registry
        )
        return page

    async def select(
        self,
        *,
        msg_topic: str | None = None,
        msg_type: str | None = None,
        msg_producer: str | None = None,
        bus_operation: BusOperation | None = None,
        start_sequence: int | None = None,
        stop_sequence: int | None = None,
        max_count: int = DEFAULT_HISTORY_MAX_COUNT,
    ) -> MessageHistoryPage:
        handle = await self.send(
            msg_topic=msg_topic,
            msg_type=msg_type,
            msg_producer=msg_producer,
            bus_operation=bus_operation,
            start_sequence=start_sequence,
            stop_sequence=stop_sequence,
            max_count=max_count,
        )
        return await self.receive(handle)

    @property
    def request_format(self) -> PortableFormat[Any, Any]:
        return self._selection_format

    @property
    def reply_format(self) -> PortableFormat[Any, Any]:
        return self._page_format

    def _initialize(
        self,
        client: AsyncRequestClient,
        *,
        format_registry: PortableFormatTable,
        selection_format: PortableFormat[Any, Any] | None,
        page_format: PortableFormat[Any, Any] | None,
    ) -> None:
        if selection_format is None:
            selection_format = DEFAULT_HISTORY_SELECTION_FORMAT
        if page_format is None:
            page_format = DEFAULT_HISTORY_PAGE_FORMAT
        self._client = client
        self._selection_format = selection_format
        self._page_format = page_format
        self._format_registry = format_registry

    @classmethod
    def _from_format_registry(
        cls,
        client: AsyncRequestClient,
        *,
        format_registry: PortableFormatTable,
        selection_format: PortableFormat[Any, Any] | None = None,
        page_format: PortableFormat[Any, Any] | None = None,
    ) -> "AsyncHistoryClient":
        history_client = cls.__new__(cls)
        history_client._initialize(
            client,
            format_registry=format_registry,
            selection_format=selection_format,
            page_format=page_format,
        )
        return history_client


class AsyncHistoryService:
    """Async request/reply service that answers message history queries."""
    _history: MessageHistory
    _service: AsyncRequestService
    _page_format: PortableFormat[Any, Any]

    def __init__(
        self,
        history: MessageHistory,
        service: AsyncRequestService,
        *,
        page_format: PortableFormat[Any, Any] | None = None,
    ) -> None:
        if page_format is None:
            page_format = DEFAULT_HISTORY_PAGE_FORMAT
        self._history = history
        self._service = service
        self._page_format = page_format

    async def handle(self) -> None:
        request = await self._service.receive()
        await self._handle_request(request)

    async def handle_nowait(self) -> bool:
        request = self._service.receive_nowait()
        handled = False
        if request is not None:
            await self._handle_request(request)
            handled = True
        return handled

    async def handle_available(self) -> int:
        requests = self._service.receive_available()
        for request in requests:
            await self._handle_request(request)
        return len(requests)

    async def handle_many(self, max_count: int) -> int:
        requests = await self._service.receive_many(max_count)
        for request in requests:
            await self._handle_request(request)
        return len(requests)

    async def _handle_request(self, request: AsyncServiceRequest) -> None:
        selection = message_history_selection_from_record(request.payload)
        page = self._history.select(
            msg_topic=selection.msg_topic,
            msg_type=selection.msg_type,
            msg_producer=selection.msg_producer,
            bus_operation=selection.bus_operation,
            start_sequence=selection.start_sequence,
            stop_sequence=selection.stop_sequence,
            max_count=selection.max_count,
        )
        reply_payload = message_history_page_record(page)
        await request.reply(reply_payload, payload_format=self._page_format)


def message_history_selection_record(
    selection: HistorySelection
) -> CompositeRecord:
    bus_operation = None
    if selection.bus_operation is not None:
        bus_operation = selection.bus_operation.value
    record: CompositeRecord = {
        "record_type": HISTORY_SELECTION_RECORD_TYPE,
        "msg_topic": selection.msg_topic,
        "msg_type": selection.msg_type,
        "msg_producer": selection.msg_producer,
        "bus_operation": bus_operation,
        "start_sequence": selection.start_sequence,
        "stop_sequence": selection.stop_sequence,
        "max_count": selection.max_count,
    }
    return record


def message_history_selection_from_record(value: Any) -> HistorySelection:
    record = _record_from(value, HISTORY_SELECTION_RECORD_TYPE)
    operation_value = _optional_str(record, "bus_operation")
    bus_operation = None
    if operation_value is not None:
        try:
            bus_operation = BusOperation(operation_value)
        except ValueError as e:
            raise InvalidMessageHistoryRecordError(
                f"unknown bus operation: {operation_value!r}"
            ) from e

    selection = history_selection_from_args(
        msg_topic=_optional_str(record, "msg_topic"),
        msg_type=_optional_str(record, "msg_type"),
        msg_producer=_optional_str(record, "msg_producer"),
        bus_operation=bus_operation,
        start_sequence=_optional_int(record, "start_sequence"),
        stop_sequence=_optional_int(record, "stop_sequence"),
        max_count=_required_int(record, "max_count"),
    )
    return selection


def message_history_page_record(page: MessageHistoryPage) -> CompositeRecord:
    entries = []
    for entry in page.entries:
        entries.append(message_history_entry_record(entry))
    record: CompositeRecord = {
        "record_type": HISTORY_PAGE_RECORD_TYPE,
        "entries": entries,
        "next_sequence": page.next_sequence,
    }
    return record


def message_history_page_from_record(
    value: Any, *, format_table: PortableFormatTable
) -> MessageHistoryPage:
    record = _record_from(value, HISTORY_PAGE_RECORD_TYPE)
    values = record.get("entries")
    if not isinstance(values, list):
        raise InvalidMessageHistoryRecordError(
            "history page entries must be a list"
        )

    entries = []
    for entry_value in values:
        entry = message_history_entry_from_record(
            entry_value, format_table=format_table
        )
        entries.append(entry)

    page = MessageHistoryPage(
        entries=tuple(entries),
        next_sequence=_optional_int(record, "next_sequence"),
    )
    return page


def message_history_entry_record(
    entry: MessageHistoryEntry
) -> CompositeRecord:
    record: CompositeRecord = {
        "record_type": HISTORY_ENTRY_RECORD_TYPE,
        "payload_format": entry.payload_format_key.registration_key,
        "payload_bytes": entry.payload_bytes,
        "msg_topic": entry.msg_topic,
        "msg_type": entry.msg_type,
        "msg_producer": entry.msg_producer,
        "msg_id": int(entry.msg_id),
        "bus_operation": entry.bus_operation.value,
        "bus_sequence": entry.bus_sequence,
        "topic_sequence": entry.topic_sequence,
        "bus_received_at": entry.bus_received_at,
        "correlation_id": _optional_typed_int(entry.correlation_id),
        "reply_to": _optional_typed_int(entry.reply_to),
    }
    return record


def message_history_entry_from_record(
    value: Any, *, format_table: PortableFormatTable
) -> MessageHistoryEntry:
    record = _record_from(value, HISTORY_ENTRY_RECORD_TYPE)
    payload_format_value = _required_str(record, "payload_format")
    try:
        payload_format_key = PortableFormatKey.from_registration_key(
            payload_format_value
        )
    except ValueError as e:
        raise InvalidMessageHistoryRecordError(
            "history payload format registration key is invalid: "
            f"{payload_format_value!r}"
        ) from e

    payload_bytes = _required_bytes(record, "payload_bytes")
    payload = decode_history_payload(
        format_table, payload_format_key, payload_bytes
    )

    operation_value = _required_str(record, "bus_operation")
    try:
        bus_operation = BusOperation(operation_value)
    except ValueError as e:
        raise InvalidMessageHistoryRecordError(
            f"unknown bus operation: {operation_value!r}"
        ) from e

    entry = MessageHistoryEntry(
        payload=payload,
        payload_format_key=payload_format_key,
        payload_bytes=payload_bytes,
        msg_topic=_required_str(record, "msg_topic"),
        msg_type=_required_str(record, "msg_type"),
        msg_producer=_required_str(record, "msg_producer"),
        msg_id=MessageID(_required_int(record, "msg_id")),
        bus_operation=bus_operation,
        bus_sequence=_required_int(record, "bus_sequence"),
        topic_sequence=_required_int(record, "topic_sequence"),
        bus_received_at=_required_int(record, "bus_received_at"),
        correlation_id=_optional_correlation_id(record, "correlation_id"),
        reply_to=_optional_message_id(record, "reply_to"),
    )
    return entry


def _record_from(value: Any, record_type: str) -> CompositeRecord:
    if not isinstance(value, dict):
        raise InvalidMessageHistoryRecordError(
            f"history payload must be a record: got {value!r}"
        )
    for key in value:
        if not isinstance(key, str):
            raise InvalidMessageHistoryRecordError(
                "history record keys must be strings"
            )
    record = value
    received_record_type = record.get("record_type")
    if received_record_type != record_type:
        raise InvalidMessageHistoryRecordError(
            "expected history record type "
            f"{record_type!r}, got {received_record_type!r}"
        )
    return record


def _required_str(record: CompositeRecord, key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str):
        raise InvalidMessageHistoryRecordError(
            f"history field {key!r} must be a string"
        )
    return value


def _required_bytes(record: CompositeRecord, key: str) -> bytes:
    value = record.get(key)
    if type(value) is not bytes:
        raise InvalidMessageHistoryRecordError(
            f"history field {key!r} must be bytes"
        )
    return value


def _optional_str(record: CompositeRecord, key: str) -> str | None:
    value = record.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidMessageHistoryRecordError(
            f"history field {key!r} must be a string or None"
        )
    return value


def _required_int(record: CompositeRecord, key: str) -> int:
    value = record.get(key)
    if type(value) is not int:
        raise InvalidMessageHistoryRecordError(
            f"history field {key!r} must be an integer"
        )
    return value


def _optional_int(record: CompositeRecord, key: str) -> int | None:
    value = record.get(key)
    if value is None:
        return None
    if type(value) is not int:
        raise InvalidMessageHistoryRecordError(
            f"history field {key!r} must be an integer or None"
        )
    return value


def _optional_message_id(
    record: CompositeRecord, key: str
) -> MessageID | None:
    value = _optional_int(record, key)
    result = None
    if value is not None:
        result = MessageID(value)
    return result


def _optional_correlation_id(
    record: CompositeRecord, key: str
) -> CorrelationID | None:
    value = _optional_int(record, key)
    result = None
    if value is not None:
        result = CorrelationID(value)
    return result


def _optional_typed_int(value: int | None) -> int | None:
    result = None
    if value is not None:
        result = int(value)
    return result
