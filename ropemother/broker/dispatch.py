#!/usr/bin/env python3
# ropemother/broker/dispatch.py

"""Helpers for binding receivers to message handlers."""

from collections.abc import Awaitable, Callable

from ropemother.broker.asyncendpoints import AsyncReceiver
from ropemother.broker.endpoints import Receiver
from ropemother.message.records import ReceivedMessage

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-06-18T04:20:41+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


MessageHandler = Callable[[ReceivedMessage], None]
AsyncMessageHandler = Callable[[ReceivedMessage], Awaitable[None]]


# Should min_count and max_count have defaults?
def run_receiver_batch(
    receiver: Receiver,
    handler: MessageHandler,
    *,
    min_count: int,
    max_count: int | None,
) -> int:
    messages = receiver.receive_batch(min_count=min_count, max_count=max_count)
    return _handle_messages(messages, handler)


async def run_async_receiver_batch(
    receiver: AsyncReceiver,
    handler: AsyncMessageHandler,
    *,
    min_count: int,
    max_count: int | None,
) -> int:
    messages = await receiver.receive_batch(
        min_count=min_count, max_count=max_count
    )
    return await _async_handle_messages(messages, handler)


def _handle_messages(
    messages: list[ReceivedMessage], handler: MessageHandler
) -> int:
    handled = 0
    for message in messages:
        handler(message)
        handled += 1
    return handled


async def _async_handle_messages(
    messages: list[ReceivedMessage], handler: AsyncMessageHandler
) -> int:
    handled = 0
    for message in messages:
        await handler(message)
        handled += 1
    return handled
