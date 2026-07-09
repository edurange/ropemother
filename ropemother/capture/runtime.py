#!/usr/bin/env python3
# ropemother/capture/runtime.py

"""Capture/history construction helpers for configured bus runtimes."""

from ropemother.broker.asyncdirect import AsyncDirectMessageBus
from ropemother.broker.direct import DirectMessageBus
from ropemother.capture.history import InMemoryCaptureHistory
from ropemother.capture.memorysink import InMemoryCaptureSink

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T02:59:48+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


type CaptureHistoryBus = DirectMessageBus | AsyncDirectMessageBus


def history_for(bus: CaptureHistoryBus) -> InMemoryCaptureHistory:
    """Build an in-memory history view for a configured direct bus."""
    source = bus.capture_source()
    if source is None:
        sink = InMemoryCaptureSink()
        bus.set_capture_sink(sink)
        source = sink

    return InMemoryCaptureHistory(source, format_table=bus.format_table())
