#!/usr/bin/env python3
# ropemother/transport/sessionrunner.py

"""Thread runner for servicing one broker transport session."""

from collections.abc import Callable
from threading import Event, Thread

from ropemother.exceptions import MessageBusBaseException
from ropemother.transport.session import BrokerTransportSession

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T18:47:54+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


class TransportSessionRunnerError(MessageBusBaseException):
    """Base exception for transport session runner errors."""
    pass


class TransportSessionAlreadyStartedError(
    RuntimeError, TransportSessionRunnerError
):
    """Raised when a transport session runner is started more than once."""
    pass


class TransportSessionFailedError(RuntimeError, TransportSessionRunnerError):
    """Raised when a transport session runner stops after a failure."""
    pass


class BrokerTransportSessionRunner:
    """Lifecycle runner for one broker transport session."""
    _session: BrokerTransportSession
    _close_connection: Callable[[], None] | None
    _daemon: bool
    _stop_requested: Event
    _thread: Thread | None
    _error: Exception | None

    def __init__(
        self,
        *,
        session: BrokerTransportSession,
        close_connection: Callable[[], None] | None = None,
        daemon: bool = True,
    ) -> None:
        self._session = session
        self._close_connection = close_connection
        self._daemon = daemon
        self._stop_requested = Event()
        self._thread = None
        self._error = None

    def start(self) -> None:
        if self._thread is not None:
            raise TransportSessionAlreadyStartedError(
                "broker transport session runner has already been started"
            )

        thread = Thread(target=self.run, daemon=self._daemon)
        self._thread = thread
        thread.start()

    def run(self) -> None:
        try:
            while not self._stop_requested.is_set():
                try:
                    self._session.handle_next_frame()
                except TimeoutError:
                    continue
        except EOFError:
            self._stop_requested.set()
        except OSError as error:
            stop_was_requested = self._stop_requested.is_set()
            self._stop_requested.set()
            if not stop_was_requested:
                self._error = error
        except Exception as error:
            self._error = error
            self._stop_requested.set()

    def request_stop(self) -> None:
        self._stop_requested.set()
        if self._close_connection is not None:
            self._close_connection()

    def join(self, timeout: float | None = None) -> None:
        thread = self._thread
        if thread is None:
            return

        thread.join(timeout)
        if thread.is_alive():
            return

        if self._error is not None:
            raise TransportSessionFailedError(
                "broker transport session runner failed"
            ) from self._error
