#!/usr/bin/env python3
# ropemother/service/brokerhistory.py

"""Built-in broker history support for the freestanding service helper."""

from threading import Event, Thread
from time import sleep

from ropemother.capture.history import MessageHistory
from ropemother.capture.historyservice import (
    DEFAULT_HISTORY_PAGE_FORMAT,
    DEFAULT_HISTORY_SELECTION_FORMAT,
    HistoryClient,
    HistoryService,
)
from ropemother.client.endpointfactory import MessageEndpointFactory
from ropemother.exceptions import MessageBusBaseException
from ropemother.service.brokerextension import (
    BrokerExtension,
    BrokerExtensionRunner,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-06T04:27:23+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


BROKER_HISTORY_REQUEST_TOPIC = "ropemother.broker-history.requests"
BROKER_HISTORY_REPLY_TOPIC = "ropemother.broker-history.replies"
BROKER_HISTORY_REQUESTER_PRODUCER = "ropemother-broker-history-client"
BROKER_HISTORY_RESPONDER_PRODUCER = "ropemother-broker-history-service"
BROKER_HISTORY_REQUEST_TYPE = "ropemother-broker-history-request"
BROKER_HISTORY_REPLY_TYPE = "ropemother-broker-history-reply"

_BROKER_HISTORY_IDLE_SLEEP_SECONDS = 0.05


class BrokerHistoryRunnerError(MessageBusBaseException):
    """Base exception for broker history service runner errors."""
    pass


class BrokerHistoryAlreadyStartedError(RuntimeError, BrokerHistoryRunnerError):
    """Raised when a broker history service runner starts twice."""
    pass


class BrokerHistoryFailedError(RuntimeError, BrokerHistoryRunnerError):
    """Raised when a broker history service runner fails."""
    pass


class BrokerHistoryRunner(BrokerExtensionRunner):
    """Thread runner for the freestanding broker history extension."""
    _history_service: HistoryService
    _stop_requested: Event
    _thread: Thread | None
    _daemon: bool
    _error: Exception | None

    def __init__(
        self, history_service: HistoryService, *, daemon: bool = True
    ) -> None:
        self._history_service = history_service
        self._stop_requested = Event()
        self._thread = None
        self._daemon = daemon
        self._error = None

    def start(self) -> None:
        if self._thread is not None:
            raise BrokerHistoryAlreadyStartedError(
                "broker history runner has already been started"
            )

        thread = Thread(target=self.run, daemon=self._daemon)
        self._thread = thread
        thread.start()

    def run(self) -> None:
        try:
            while not self._stop_requested.is_set():
                handled = self._history_service.handle_available()
                if handled == 0:
                    sleep(_BROKER_HISTORY_IDLE_SLEEP_SECONDS)
        except Exception as error:
            self._error = error
            self._stop_requested.set()

    def request_stop(self) -> None:
        self._stop_requested.set()

    def join(self) -> None:
        thread = self._thread
        if thread is None:
            return

        thread.join()
        if self._error is not None:
            raise BrokerHistoryFailedError(
                "broker history runner failed"
            ) from self._error


class BrokerHistoryExtension(BrokerExtension):
    """Turnkey history extension for freestanding broker hosts."""
    _history: MessageHistory

    def __init__(self, history: MessageHistory) -> None:
        self._history = history

    def create_runner(
        self,
        bus: MessageEndpointFactory,
        *,
        daemon: bool,
    ) -> BrokerExtensionRunner:
        history_service = bus.create_history_service(
            history=self._history,
            request_topic=BROKER_HISTORY_REQUEST_TOPIC,
            reply_topic=BROKER_HISTORY_REPLY_TOPIC,
            requester_producer=BROKER_HISTORY_REQUESTER_PRODUCER,
            responder_producer=BROKER_HISTORY_RESPONDER_PRODUCER,
            request_msg_type=BROKER_HISTORY_REQUEST_TYPE,
            reply_msg_type=BROKER_HISTORY_REPLY_TYPE,
            reply_payload_format=DEFAULT_HISTORY_PAGE_FORMAT,
        )
        return BrokerHistoryRunner(history_service, daemon=daemon)


def preconfigured_history_client(bus: MessageEndpointFactory) -> HistoryClient:
    """Return a convenience client for the built-in broker history service."""
    history_client = bus.create_history_client(
        request_topic=BROKER_HISTORY_REQUEST_TOPIC,
        reply_topic=BROKER_HISTORY_REPLY_TOPIC,
        requester_producer=BROKER_HISTORY_REQUESTER_PRODUCER,
        responder_producer=BROKER_HISTORY_RESPONDER_PRODUCER,
        request_msg_type=BROKER_HISTORY_REQUEST_TYPE,
        reply_msg_type=BROKER_HISTORY_REPLY_TYPE,
        request_payload_format=DEFAULT_HISTORY_SELECTION_FORMAT,
        reply_payload_format=DEFAULT_HISTORY_PAGE_FORMAT,
    )
    return history_client
