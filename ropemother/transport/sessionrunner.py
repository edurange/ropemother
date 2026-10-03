#!/usr/bin/env python3
# ropemother/transport/sessionrunner.py

"""Thread runner for servicing one broker transport session."""

import collections.abc
import threading
import time

from ropemother.exceptions import MessageBusBaseException
from ropemother.transport.session import BrokerTransportSession

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-10-02T02:55:13+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev11"
__status__ = "Development"


_DELIVERY_POLL_SECONDS = 0.05


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
    _close_connection: collections.abc.Callable[[], None] | None
    _daemon: bool
    _stop_requested: threading.Event
    _thread: threading.Thread | None
    _delivery_thread: threading.Thread | None
    _error: Exception | None
    _error_lock: threading.Lock
    _stop_lock: threading.Lock

    def __init__(
        self,
        *,
        session: BrokerTransportSession,
        close_connection: collections.abc.Callable[[], None] | None = None,
        daemon: bool = True,
    ) -> None:
        self._session = session
        self._close_connection = close_connection
        self._daemon = daemon
        self._stop_requested = threading.Event()
        self._thread = None
        self._delivery_thread = None
        self._error = None
        self._error_lock = threading.Lock()
        self._stop_lock = threading.Lock()

    def start(self) -> None:
        if self._thread is not None:
            raise TransportSessionAlreadyStartedError(
                "broker transport session runner has already been started"
            )

        self._session._enable_queued_delivery()
        delivery_thread = threading.Thread(
            target=self._run_deliveries, daemon=self._daemon
        )
        thread = threading.Thread(target=self.run, daemon=self._daemon)
        self._delivery_thread = delivery_thread
        self._thread = thread
        delivery_thread.start()
        thread.start()

    def run(self) -> None:
        try:
            while not self._stop_requested.is_set():
                try:
                    self._session.handle_next_frame()
                except TimeoutError:
                    continue
        except (EOFError, BrokenPipeError, ConnectionResetError):
            pass
        except OSError as error:
            if (
                not self._stop_requested.is_set()
                and not self._session._delivery_was_stopped()
            ):
                self._record_error(error)
        except Exception as error:
            self._record_error(error)
        finally:
            self._stop_transport()
            self._session.close()

    def join(self, timeout: float | None = None) -> None:
        thread = self._thread
        if thread is None:
            return

        deadline = None
        if timeout is not None:
            deadline = time.monotonic() + timeout

        thread.join(timeout)
        delivery_thread = self._delivery_thread
        if delivery_thread is not None:
            remaining = None
            if deadline is not None:
                remaining = max(0.0, deadline - time.monotonic())
            delivery_thread.join(remaining)

        if thread.is_alive():
            return
        if delivery_thread is not None and delivery_thread.is_alive():
            return

        if self._error is not None:
            raise TransportSessionFailedError(
                "broker transport session runner failed"
            ) from self._error

    def is_alive(self) -> bool:
        thread = self._thread
        delivery_thread = self._delivery_thread
        thread_alive = thread is not None and thread.is_alive()
        delivery_alive = (
            delivery_thread is not None and delivery_thread.is_alive()
        )
        return thread_alive or delivery_alive

    def request_stop(self) -> None:
        self._stop_transport()

    def _run_deliveries(self) -> None:
        try:
            while not self._stop_requested.is_set():
                self._session._handle_next_delivery(
                    _DELIVERY_POLL_SECONDS
                )
        except (BrokenPipeError, ConnectionResetError):
            pass
        except OSError as error:
            if (
                not self._stop_requested.is_set()
                and not self._session._delivery_was_stopped()
            ):
                self._record_error(error)
        except Exception as error:
            self._record_error(error)
        finally:
            self._stop_transport()

    def _record_error(self, error: Exception) -> None:
        with self._error_lock:
            if self._error is None:
                self._error = error

    def _stop_transport(self) -> None:
        with self._stop_lock:
            if self._stop_requested.is_set():
                return
            self._stop_requested.set()
            close_connection = self._close_connection

        if close_connection is not None:
            close_connection()
