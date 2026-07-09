#!/usr/bin/env python3
# ropemother/bootstrap/policy.py

"""Bootstrap traffic policy objects."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum

from ropemother.exceptions import MessageBusBaseException
from ropemother.message.records import BusMessage, BusOperation

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T07:03:33+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


DEFAULT_LIFECYCLE_TOPIC_ROOT = "lifecycle"
DEFAULT_LIFECYCLE_OPERATIONS = frozenset({BusOperation.PUBLISH})


class BootstrapPolicyError(MessageBusBaseException):
    """Base exception for bootstrap policy errors."""
    pass


class BootstrapMessageRejectedError(RuntimeError, BootstrapPolicyError):
    """Raised when bootstrap message traffic is rejected."""
    pass


class LifecycleMessageType(StrEnum):
    """Built-in lifecycle message types allowed during bootstrap."""
    STARTED = "started"
    READY = "ready"
    FAILED = "failed"
    STOPPING = "stopping"
    STOPPED = "stopped"


DEFAULT_LIFECYCLE_MESSAGE_TYPES = frozenset(
    message_type.value for message_type in LifecycleMessageType
)


@dataclass(frozen=True, kw_only=True)
class BootstrapDecision:
    """Decision result for bootstrap message traffic."""
    accepted: bool
    reason: str = ""


class BootstrapPolicy(ABC):
    """Policy interface for deciding bootstrap message traffic."""
    @abstractmethod
    def decide_message(self, message: BusMessage) -> BootstrapDecision:
        pass


class RejectBootstrapPolicy(BootstrapPolicy):
    """Bootstrap policy that rejects all message traffic."""
    def decide_message(self, message: BusMessage) -> BootstrapDecision:
        decision = BootstrapDecision(
            accepted=False,
            reason="bootstrap message traffic is not allowed",
        )
        return decision


@dataclass(frozen=True, kw_only=True)
class LifecycleBootstrapPolicy(BootstrapPolicy):
    """Bootstrap policy that permits selected lifecycle messages."""
    topic_root: str = DEFAULT_LIFECYCLE_TOPIC_ROOT
    msg_types: frozenset[str] = DEFAULT_LIFECYCLE_MESSAGE_TYPES
    operations: frozenset[BusOperation] = DEFAULT_LIFECYCLE_OPERATIONS

    def decide_message(self, message: BusMessage) -> BootstrapDecision:
        if message.bus_operation not in self.operations:
            decision = BootstrapDecision(
                accepted=False,
                reason="bootstrap message operation is not allowed",
            )
        elif not _topic_matches_root(message.msg_topic, self.topic_root):
            decision = BootstrapDecision(
                accepted=False,
                reason="bootstrap message topic is not allowed",
            )
        elif message.msg_type not in self.msg_types:
            decision = BootstrapDecision(
                accepted=False,
                reason="bootstrap message type is not allowed",
            )
        else:
            decision = BootstrapDecision(accepted=True)

        return decision


def _topic_matches_root(msg_topic: str, topic_root: str) -> bool:
    matches_root = msg_topic == topic_root
    matches_child = msg_topic.startswith(topic_root + ".")
    return matches_root or matches_child
