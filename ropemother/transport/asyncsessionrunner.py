#!/usr/bin/env python3
# ropemother/transport/asyncsessionrunner.py

"""Async task runner for servicing one broker transport session."""

from collections.abc import Callable
from asyncio import CancelledError, Task, create_task

from ropemother.exceptions import MessageBusBaseException
from ropemother.transport.asyncsession import AsyncBrokerTransportSession

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-04T22:54:37+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


class AsyncTransportSessionRunnerError(MessageBusBaseException):
    """Base exception for async transport session runner errors."""
    pass


class AsyncTransportSessionAlreadyStartedError(
    RuntimeError, AsyncTransportSessionRunnerError
):
    """Raised when an async transport session runner starts twice."""
    pass


class AsyncTransportSessionFailedError(
    RuntimeError, AsyncTransportSessionRunnerError
):
    """Raised when an async transport session runner fails."""
    pass


class AsyncBrokerTransportSessionRunner:
    """Lifecycle runner for one async broker transport session."""
    _session: AsyncBrokerTransportSession
    _close_connection: Callable[[], None] | None
    _stop_requested: bool
    _task: Task[None] | None
    _error: Exception | None

    def __init__(
        self,
        *,
        session: AsyncBrokerTransportSession,
        close_connection: Callable[[], None] | None = None,
    ) -> None:
        self._session = session
        self._close_connection = close_connection
        self._stop_requested = False
        self._task = None
        self._error = None

    def start(self) -> None:
        if self._task is not None:
            raise AsyncTransportSessionAlreadyStartedError(
                "async broker transport session runner has already started"
            )

        self._task = create_task(self.run())

    async def run(self) -> None:
        try:
            while not self._stop_requested:
                try:
                    await self._session.handle_next_frame()
                except TimeoutError:
                    continue
        except EOFError:
            self._stop_requested = True
        except CancelledError:
            stop_was_requested = self._stop_requested
            self._stop_requested = True
            if not stop_was_requested:
                raise
        except OSError as error:
            stop_was_requested = self._stop_requested
            self._stop_requested = True
            if not stop_was_requested:
                self._error = error
        except Exception as error:
            self._error = error
            self._stop_requested = True

    def request_stop(self) -> None:
        self._stop_requested = True
        if self._close_connection is not None:
            self._close_connection()
        if self._task is not None:
            self._task.cancel()

    async def wait(self) -> None:
        task = self._task
        if task is None:
            return

        await task
        if self._error is not None:
            raise AsyncTransportSessionFailedError(
                "async broker transport session runner failed"
            ) from self._error
