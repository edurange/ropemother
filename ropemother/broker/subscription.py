#!/usr/bin/env python3
# ropemother/broker/subscription.py

"""Subscription matching helpers for broker implementations."""

from dataclasses import dataclass

from ropemother.message.records import BusMessage
from ropemother.message.selectors import (
    SymbolSelector,
    SubscriptionTopicFilter,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T16:53:44+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


@dataclass(frozen=True, kw_only=True)
class Subscription:
    """Normalized subscription filter used by broker routing."""
    msg_topic_filter: SubscriptionTopicFilter
    msg_producer_filter: SymbolSelector
    msg_type_filter: SymbolSelector

    def matches(self, message: BusMessage) -> bool:
        if not self.msg_producer_filter.matches(message.msg_producer):
            return False
        if not self.msg_type_filter.matches(message.msg_type):
            return False
        return self.msg_topic_filter.matches(message.msg_topic)
