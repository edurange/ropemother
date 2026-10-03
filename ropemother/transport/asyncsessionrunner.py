#!/usr/bin/env python3
# ropemother/transport/asyncsessionrunner.py

"""Async task runner for servicing one broker transport session."""

import collections
import asyncio

from ropemother.exceptions import MessageBusBaseException
from ropemother.transport.asyncsession import AsyncBrokerTransportSession

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-10-03T03:30:49+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev10"
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
    _close_connection: collections.abc.Callable[[], None] | None
    _stop_requested: bool
    _task: asyncio.Task[None] | None
    _error: Exception | None

    def __init__(
        self,
        *,
        session: AsyncBrokerTransportSession,
        close_connection: collections.abc.Callable[[], None] | None = None,
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

        self._task = asyncio.create_task(self.run())

    async def run(self) -> None:
        try:
            while not self._stop_requested:
                try:
                    await self._session.handle_next_frame()
                except TimeoutError:
                    continue
        except EOFError:
            pass
        except asyncio.CancelledError:
            if not self._stop_requested:
                raise
        except OSError as error:
            delivery_was_stopped = self._session._delivery_was_stopped()
            if not self._stop_requested and not delivery_was_stopped:
                self._record_error(error)
        except Exception as error:
            self._record_error(error)
        finally:
            delivery_error = self._session._delivery_failure()
            if delivery_error is not None:
                self._record_error(delivery_error)
            self._stop_transport()
            self._session.close()

    def request_stop(self) -> None:
        self._stop_transport()
        task = self._task
        if task is not None:
            task.cancel()

    async def wait(self) -> None:
        task = self._task
        if task is None:
            return

        await task
        if self._error is not None:
            raise AsyncTransportSessionFailedError(
                "async broker transport session runner failed"
            ) from self._error

    def _record_error(self, error: Exception) -> None:
        if self._error is None:
            self._error = error

    def _stop_transport(self) -> None:
        if self._stop_requested:
            return
        self._stop_requested = True
        close_connection = self._close_connection
        if close_connection is not None:
            close_connection()
