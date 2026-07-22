#!/usr/bin/env python3
# ropemother/capture/__init__.py

"""Support for strict message capture."""

from importlib import import_module
from typing import Any

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-22T15:46:06+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


_EXPORTS = {
    "AsyncHistoryClient": "ropemother.capture.historyservice",
    "AsyncHistoryService": "ropemother.capture.historyservice",
    "HistoryClient": "ropemother.capture.historyservice",
    "HistoryService": "ropemother.capture.historyservice",
    "InMemoryCaptureHistory": "ropemother.capture.history",
    "InMemoryCaptureSink": "ropemother.capture.memorysink",
    "IncompleteMessageHistoryError": "ropemother.capture.history",
    "InvalidMessageHistoryRecordError": "ropemother.capture.historyservice",
    "InvalidHistorySelectionError": "ropemother.capture.history",
    "JSONLinesCaptureSink": "ropemother.capture.filesink",
    "JSONLinesCaptureHistory": "ropemother.capture.filehistory",
    "MessageHistory": "ropemother.capture.history",
    "MessageHistoryEntry": "ropemother.capture.history",
    "MessageHistoryError": "ropemother.capture.history",
    "MessageHistoryFormatError": "ropemother.capture.filehistory",
    "MessageHistoryPage": "ropemother.capture.history",
    "MessageHistoryPayloadDecodeError": "ropemother.capture.history",
    "MessageHistoryServiceError": "ropemother.capture.historyservice",
    "MessageHistorySourceError": "ropemother.capture.filehistory",
    "history_for": "ropemother.capture.runtime",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(name)

    module = import_module(_EXPORTS[name])
    value = getattr(module, name)
    globals()[name] = value
    return value
