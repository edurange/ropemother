#!/usr/bin/env python3
# ropemother/client/lifecycle.py

"""Client-facing lifecycle message helpers."""

from typing import Any

from ropemother.bootstrap.policy import LifecycleMessageType
from ropemother.broker.asyncendpoints import AsyncEmitter
from ropemother.broker.endpoints import Emitter

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T16:19:23+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class LifecyclePublisher:
    """Synchronous helper for publishing lifecycle messages."""
    _emitters: dict[LifecycleMessageType, Emitter]

    def __init__(
        self, emitters: dict[LifecycleMessageType, Emitter]
    ) -> None:
        self._emitters = emitters

    def started(self, payload: Any) -> None:
        self._publish(LifecycleMessageType.STARTED, payload)

    def ready(self, payload: Any) -> None:
        self._publish(LifecycleMessageType.READY, payload)

    def failed(self, payload: Any) -> None:
        self._publish(LifecycleMessageType.FAILED, payload)

    def stopping(self, payload: Any) -> None:
        self._publish(LifecycleMessageType.STOPPING, payload)

    def stopped(self, payload: Any) -> None:
        self._publish(LifecycleMessageType.STOPPED, payload)

    def _publish(self, msg_type: LifecycleMessageType, payload: Any) -> None:
        self._emitters[msg_type].emit(payload)


class AsyncLifecyclePublisher:
    """Async helper for publishing lifecycle messages."""
    _emitters: dict[LifecycleMessageType, AsyncEmitter]

    def __init__(
        self, emitters: dict[LifecycleMessageType, AsyncEmitter]
    ) -> None:
        self._emitters = emitters

    async def started(self, payload: Any) -> None:
        await self._publish(LifecycleMessageType.STARTED, payload)

    async def ready(self, payload: Any) -> None:
        await self._publish(LifecycleMessageType.READY, payload)

    async def failed(self, payload: Any) -> None:
        await self._publish(LifecycleMessageType.FAILED, payload)

    async def stopping(self, payload: Any) -> None:
        await self._publish(LifecycleMessageType.STOPPING, payload)

    async def stopped(self, payload: Any) -> None:
        await self._publish(LifecycleMessageType.STOPPED, payload)

    async def _publish(
        self, msg_type: LifecycleMessageType, payload: Any
    ) -> None:
        await self._emitters[msg_type].emit(payload)