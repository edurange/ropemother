#!/usr/bin/env python3
# ropemother/service/brokerextension.py

"""Extension points for capabilities attached to freestanding broker hosts."""

from abc import ABC, abstractmethod

from ropemother.client.endpointfactory import MessageEndpointFactory

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-05T01:38:06+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


class BrokerExtensionRunner(ABC):
    """Lifecycle runner for one broker extension."""

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def request_stop(self) -> None:
        pass

    @abstractmethod
    def join(self) -> None:
        pass


class BrokerExtension(ABC):
    """Capability that can be attached to a freestanding broker host."""

    @abstractmethod
    def create_runner(
        self, bus: MessageEndpointFactory, *, daemon: bool
    ) -> BrokerExtensionRunner:
        pass