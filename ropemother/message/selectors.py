#!/usr/bin/env python3
# ropemother/message/selectors.py

"""Selector helpers for message subscription setup."""

from dataclasses import dataclass

from ropemother.exceptions import MessageBusBaseException

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-07T01:06:14+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class SelectorError(MessageBusBaseException):
    """Base exception for invalid selector construction."""
    pass


class InvalidSelectorInputError(ValueError, SelectorError):
    """Raised when selector input cannot be normalized."""
    pass


@dataclass(frozen=True, kw_only=True)
class SymbolSelector:
    """Matcher for optional exact-symbol subscription filters."""
    symbols: tuple[str, ...] | None

    def matches(self, symbol: str) -> bool:
        if self.symbols is None:
            return True
        return symbol in self.symbols


@dataclass(frozen=True, kw_only=True)
class SubscriptionTopicSelector:
    """Topic selector describing exact or subtree matching."""
    topic: str
    include_subtopics: bool


@dataclass(frozen=True)
class SubscriptionTopicFilter:
    """Matcher for one or more topic selectors."""
    selectors: tuple[SubscriptionTopicSelector, ...]

    def matches(self, topic: str) -> bool:
        for selector in self.selectors:
            if topic == selector.topic:
                return True
            if selector.include_subtopics:
                if topic.startswith(f"{selector.topic}."):
                    return True
        return False


SymbolCollectionInput = tuple[str, ...] | list[str]
SymbolInput = str | SymbolCollectionInput
OptionalSymbolInput = SymbolInput | None
SubscriptionTopicLeaf = str | SubscriptionTopicSelector
SubscriptionTopicInput = (
    SubscriptionTopicLeaf
    | tuple[SubscriptionTopicLeaf, ...]
    | list[SubscriptionTopicLeaf]
)


def exact_topic(topic: str) -> SubscriptionTopicSelector:
    """Create a selector that matches only one topic."""
    return SubscriptionTopicSelector(topic=topic, include_subtopics=False)


def topic_tree(topic: str) -> SubscriptionTopicSelector:
    """Create a selector that matches a topic and its subtopics."""
    return SubscriptionTopicSelector(topic=topic, include_subtopics=True)


def resolve_topic_selector(
    msg_topic: SubscriptionTopicLeaf,
) -> tuple[str, bool]:
    """Return the topic string and subtree flag for a topic selector."""
    selector = _topic_selector_from_leaf(msg_topic)
    return (selector.topic, selector.include_subtopics)


def normalize_symbol_collection_input(
    value: SymbolCollectionInput, *, argument_name: str
) -> tuple[str, ...]:
    """Normalize a required symbol collection into a tuple."""
    if not isinstance(value, tuple | list):
        value_type = type(value).__name__
        raise InvalidSelectorInputError(
            f"{argument_name} must be a tuple or list, got {value_type}"
        )

    symbols = []
    for item in value:
        if not isinstance(item, str):
            item_type = type(item).__name__
            raise InvalidSelectorInputError(
                f"{argument_name} entries must be strings, got {item_type}"
            )
        symbols.append(item)

    return tuple(symbols)


def normalize_symbol_input(
    value: SymbolInput, argument_name: str
) -> tuple[str, ...]:
    """Normalize one symbol or a symbol collection into a tuple."""
    items = _input_items(value, argument_name)
    symbols = []

    for item in items:
        if not isinstance(item, str):
            item_type = type(item).__name__
            raise InvalidSelectorInputError(
                f"{argument_name} entries must be strings, got {item_type}"
            )
        symbols.append(item)

    return tuple(symbols)


def normalize_optional_symbol_input(
    value: OptionalSymbolInput, *, argument_name: str
) -> tuple[str, ...] | None:
    """Normalize an optional symbol filter into a tuple or wildcard."""
    if value is None:
        return None
    return normalize_symbol_input(value, argument_name=argument_name)


def symbol_selector_from_input(
    value: OptionalSymbolInput, *, argument_name: str
) -> SymbolSelector:
    """Build a symbol selector from subscription filter input."""
    symbols = normalize_optional_symbol_input(
        value, argument_name=argument_name
    )
    return SymbolSelector(symbols=symbols)


def topic_filter_from_input(
    msg_topic: SubscriptionTopicInput,
) -> SubscriptionTopicFilter:
    """Build a topic filter from subscription topic input."""
    items = _input_items(msg_topic, "msg_topic")
    selectors = []

    for item in items:
        selector = _topic_selector_from_leaf(item)
        selectors.append(selector)

    return SubscriptionTopicFilter(tuple(selectors))


def _input_items(value: object, argument_name: str) -> tuple[object, ...]:
    if isinstance(value, tuple | list):
        items = tuple(value)
    else:
        items = (value,)

    if not items:
        raise InvalidSelectorInputError(
            f"{argument_name} cannot be an empty collection"
        )

    return items


def _topic_selector_from_leaf(
    leaf: object,
) -> SubscriptionTopicSelector:
    if isinstance(leaf, str):
        selector = exact_topic(leaf)
    elif isinstance(leaf, SubscriptionTopicSelector):
        selector = leaf
    else:
        leaf_type = type(leaf).__name__
        raise InvalidSelectorInputError(
            "msg_topic entries must be strings or topic selectors, "
            f"got {leaf_type}"
        )

    return selector
