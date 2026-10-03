#!/usr/bin/env python3
# ropemother/transport/sessionstate.py

"""Shared transport session state."""

import collections.abc
import dataclasses

from ropemother.broker.directcore import EmitterBinding, SubscriptionBinding
from ropemother.capture.writer import RegistrationRecord
from ropemother.message.messageidentity import MessageID
from ropemother.message.records import BusMessage, BusOperation
from ropemother.message.symbols import MessageTypeID, ProducerID, TopicID
from ropemother.transport.endpointregistration import EndpointRegistrationView
from ropemother.transport.frames import EmitFrame, TransportSubscriptionID

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-10-02T21:21:13+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev10"
__status__ = "Development"


@dataclasses.dataclass(frozen=True, kw_only=True)
class EmitterBindingKey:
    """Session-local key for a registered transport emitter."""
    msg_topic_id: TopicID
    msg_producer_id: ProducerID
    msg_type_id: MessageTypeID


class TransportSessionState:
    """Mutable endpoint state for one transport session."""
    _emitter_bindings: dict[EmitterBindingKey, EmitterBinding]
    _registrations: EndpointRegistrationView
    _subscription_bindings: list[SubscriptionBinding]
    _request_reply_subscriptions: set[TransportSubscriptionID]
    _request_reply_owners: dict[MessageID, TransportSubscriptionID]
    _active_request_reply_subscription: TransportSubscriptionID | None
    _active_request_message_id: MessageID | None

    def __init__(self) -> None:
        self._emitter_bindings = {}
        self._registrations = EndpointRegistrationView()
        self._subscription_bindings = []
        self._request_reply_subscriptions = set()
        self._request_reply_owners = {}
        self._active_request_reply_subscription = None
        self._active_request_message_id = None

    def add_emitter_binding(self, binding: EmitterBinding) -> None:
        binding_keys = self._emitter_binding_keys(binding)
        for binding_key in binding_keys:
            self._emitter_bindings[binding_key] = binding

    def add_subscription_binding(
        self,
        binding: SubscriptionBinding,
        *,
        request_reply_subscription: bool = False,
    ) -> TransportSubscriptionID:
        subscription_id = TransportSubscriptionID(
            len(self._subscription_bindings)
        )
        self._subscription_bindings.append(binding)
        if request_reply_subscription:
            self._request_reply_subscriptions.add(subscription_id)
        return subscription_id

    def request_reply_subscription_is_valid(self, frame: EmitFrame) -> bool:
        subscription_id = frame.request_reply_subscription_id
        if subscription_id is None:
            return True
        if frame.bus_operation != BusOperation.REQUEST:
            return False
        return subscription_id in self._request_reply_subscriptions

    def begin_request_emit(
        self, frame: EmitFrame
    ) -> collections.abc.Callable[[MessageID], None] | None:
        subscription_id = frame.request_reply_subscription_id
        observer = None
        if subscription_id is not None:
            self._active_request_reply_subscription = subscription_id
            self._active_request_message_id = None
            observer = self._identify_active_request
        return observer

    def finish_request_emit(self, *, discard_owner: bool = False) -> None:
        message_id = self._active_request_message_id
        if discard_owner and message_id is not None:
            self._request_reply_owners.pop(message_id, None)
        self._active_request_reply_subscription = None
        self._active_request_message_id = None

    def accept_delivery(
        self, *, subscription_id: TransportSubscriptionID, message: BusMessage
    ) -> bool:
        if subscription_id not in self._request_reply_subscriptions:
            return True
        if message.bus_operation != BusOperation.REPLY:
            return True
        deliverable = self._accept_request_reply(
            subscription_id=subscription_id, message=message
        )
        return deliverable

    def _accept_request_reply(
        self, *, subscription_id: TransportSubscriptionID, message: BusMessage
    ) -> bool:
        reply_to = message.reply_to
        if reply_to is None:
            return False

        owner = self._request_reply_owners.get(reply_to)
        if owner != subscription_id:
            return False
        del self._request_reply_owners[reply_to]
        return True

    def _identify_active_request(self, message_id: MessageID) -> None:
        subscription_id = self._active_request_reply_subscription
        if subscription_id is None:
            return
        self._active_request_message_id = message_id
        self._request_reply_owners[message_id] = subscription_id

    def emitter_binding_for_frame(
        self, frame: EmitFrame
    ) -> EmitterBinding | None:
        binding_key = self._emitter_frame_key(frame)
        binding = self._emitter_bindings.get(binding_key)
        if binding is not None:
            return binding

        matching_binding = None
        for candidate in self._emitter_bindings.values():
            if not candidate.allow_unlisted_type_formats:
                continue
            if candidate.msg_topic_id != frame.msg_topic_id:
                continue
            if candidate.msg_producer_id != frame.msg_producer_id:
                continue
            matching_binding = candidate
            break

        return matching_binding

    def msg_type_for_frame(
        self, *, binding: EmitterBinding, frame: EmitFrame
    ) -> str:
        if frame.msg_type_id == binding.msg_type_id:
            return binding.msg_type

        for msg_type, msg_type_id in binding.additional_msg_type_ids.items():
            if frame.msg_type_id == msg_type_id:
                return msg_type

        return self._registrations.msg_type_for_id(frame.msg_type_id)

    def registrations_to_send(
        self, registrations: tuple[RegistrationRecord, ...]
    ) -> tuple[RegistrationRecord, ...]:
        self._registrations.apply_registrations(registrations)
        return self._registrations.take_unsent(registrations)

    def _emitter_binding_keys(
        self, binding: EmitterBinding
    ) -> tuple[EmitterBindingKey, ...]:
        msg_type_ids = [binding.msg_type_id]
        for msg_type_id in binding.additional_msg_type_ids.values():
            if msg_type_id not in msg_type_ids:
                msg_type_ids.append(msg_type_id)

        binding_keys = []
        for msg_type_id in msg_type_ids:
            binding_key = EmitterBindingKey(
                msg_topic_id=binding.msg_topic_id,
                msg_producer_id=binding.msg_producer_id,
                msg_type_id=msg_type_id,
            )
            binding_keys.append(binding_key)

        return tuple(binding_keys)

    def _emitter_frame_key(self, frame: EmitFrame) -> EmitterBindingKey:
        binding_key = EmitterBindingKey(
            msg_topic_id=frame.msg_topic_id,
            msg_producer_id=frame.msg_producer_id,
            msg_type_id=frame.msg_type_id,
        )
        return binding_key
