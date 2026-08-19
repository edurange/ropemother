#!/usr/bin/env python3
# ropemother/capture/historyselection.py

"""Shared internal selection state for message history reads."""

import dataclasses
import typing

from ropemother.message.records import BusOperation
from ropemother.util.symbol import Symbol

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-08-13T20:25:47+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev7"
__status__ = "Development"


DEFAULT_HISTORY_MAX_COUNT = 100


class HistoryCursor(Symbol):
    """Opaque continuation token for a message history selection."""
    pass


class HistorySequenceOrder(Symbol):
    """Ordering direction for message history selections."""
    ASCENDING: typing.ClassVar[typing.Final["HistorySequenceOrder"]]
    DESCENDING: typing.ClassVar[typing.Final["HistorySequenceOrder"]]


HistorySequenceOrder.ASCENDING = HistorySequenceOrder("ascending")
HistorySequenceOrder.DESCENDING = HistorySequenceOrder("descending")


@dataclasses.dataclass(frozen=True, kw_only=True)
class HistorySelection:
    """Filter and pagination request for captured message history."""
    msg_topic: str | None = None
    msg_type: str | None = None
    msg_producer: str | None = None
    bus_operation: BusOperation | None = None
    sequence_order: HistorySequenceOrder = HistorySequenceOrder.ASCENDING
    cursor: HistoryCursor | None = None
    max_count: int = DEFAULT_HISTORY_MAX_COUNT


def history_selection_from_args(
    *,
    msg_topic: str | None = None,
    msg_type: str | None = None,
    msg_producer: str | None = None,
    bus_operation: BusOperation | None = None,
    sequence_order: HistorySequenceOrder = HistorySequenceOrder.ASCENDING,
    cursor: HistoryCursor | None = None,
    max_count: int = DEFAULT_HISTORY_MAX_COUNT,
) -> HistorySelection:
    selection = HistorySelection(
        msg_topic=msg_topic,
        msg_type=msg_type,
        msg_producer=msg_producer,
        bus_operation=bus_operation,
        sequence_order=sequence_order,
        cursor=cursor,
        max_count=max_count,
    )
    return selection
