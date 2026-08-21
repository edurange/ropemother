#!/usr/bin/env python3
# ropemother/service/brokerhistory.py

"""Built-in message history service profile."""

from ropemother.capture.history import MessageHistory
from ropemother.capture.historyservice import (
    DEFAULT_HISTORY_PAGE_FORMAT,
    DEFAULT_HISTORY_SELECTION_FORMAT,
    HistoryClient,
    HistoryService,
)
from ropemother.client.endpointfactory import MessageEndpointFactory
from ropemother.service.brokerextension import (
    BrokerExtension,
    BrokerExtensionHandler,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-08-21T00:08:14+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev7"
__status__ = "Development"


BROKER_HISTORY_REQUEST_TOPIC = "ropemother.broker-history.requests"
BROKER_HISTORY_REPLY_TOPIC = "ropemother.broker-history.replies"
BROKER_HISTORY_REQUESTER_PRODUCER = "ropemother-broker-history-client"
BROKER_HISTORY_RESPONDER_PRODUCER = "ropemother-broker-history-service"
BROKER_HISTORY_REQUEST_TYPE = "ropemother-broker-history-request"
BROKER_HISTORY_REPLY_TYPE = "ropemother-broker-history-reply"


class BrokerHistoryExtension(BrokerExtension):
    """Turnkey history extension for freestanding broker hosts."""
    _history: MessageHistory

    def __init__(self, history: MessageHistory) -> None:
        self._history = history

    def create_handler(
        self, bus: MessageEndpointFactory
    ) -> BrokerExtensionHandler:
        service = preconfigured_history_service(bus, self._history)
        return service.handle_available


def preconfigured_history_service(
    bus: MessageEndpointFactory, history: MessageHistory
) -> HistoryService:
    """Return a service for the built-in history profile."""
    service = bus.create_history_service(
        history=history,
        request_topic=BROKER_HISTORY_REQUEST_TOPIC,
        reply_topic=BROKER_HISTORY_REPLY_TOPIC,
        requester_producer=BROKER_HISTORY_REQUESTER_PRODUCER,
        responder_producer=BROKER_HISTORY_RESPONDER_PRODUCER,
        request_msg_type=BROKER_HISTORY_REQUEST_TYPE,
        reply_msg_type=BROKER_HISTORY_REPLY_TYPE,
        reply_payload_format=DEFAULT_HISTORY_PAGE_FORMAT,
    )
    return service


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
