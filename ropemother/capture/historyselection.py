#!/usr/bin/env python3
# ropemother/capture/historyselection.py

"""Shared internal selection state for message history reads."""

from dataclasses import dataclass

from ropemother.message.records import BusOperation

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-04T03:43:03+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


DEFAULT_HISTORY_MAX_COUNT = 100


@dataclass(frozen=True, kw_only=True)
class HistorySelection:
    """Filter and pagination request for captured message history."""
    msg_topic: str | None = None
    msg_type: str | None = None
    msg_producer: str | None = None
    bus_operation: BusOperation | None = None
    start_sequence: int | None = None
    stop_sequence: int | None = None
    max_count: int = DEFAULT_HISTORY_MAX_COUNT


def history_selection_from_args(
    *,
    msg_topic: str | None = None,
    msg_type: str | None = None,
    msg_producer: str | None = None,
    bus_operation: BusOperation | None = None,
    start_sequence: int | None = None,
    stop_sequence: int | None = None,
    max_count: int = DEFAULT_HISTORY_MAX_COUNT,
) -> HistorySelection:
    selection = HistorySelection(
        msg_topic=msg_topic,
        msg_type=msg_type,
        msg_producer=msg_producer,
        bus_operation=bus_operation,
        start_sequence=start_sequence,
        stop_sequence=stop_sequence,
        max_count=max_count,
    )
    return selection