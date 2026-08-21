#!/usr/bin/env python3
# ropemother/service/brokerextension.py

"""Extension points for capabilities attached to freestanding broker hosts."""

import abc
import collections.abc

from ropemother.client.endpointfactory import MessageEndpointFactory

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-08-21T00:02:33+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev7"
__status__ = "Development"


type BrokerExtensionHandler = collections.abc.Callable[[], int]
type BrokerExtensionHandlers = collections.abc.Iterable[BrokerExtensionHandler]


class BrokerExtension(abc.ABC):
    """Capability that can be attached to a freestanding broker host."""

    @abc.abstractmethod
    def create_handler(
        self, bus: MessageEndpointFactory
    ) -> BrokerExtensionHandler:
        """Return a nonblocking handler for available extension work."""
        pass
