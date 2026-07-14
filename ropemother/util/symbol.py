#!/usr/bin/env python3
# ropemother/util/symbol.py

"""A shared utility class for deriving symbolic types."""

from dataclasses import dataclass

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T20:20:32+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


@dataclass(frozen=True, slots=True)
class Symbol:
    """String-backed symbolic value object."""
    value: str

    def __str__(self) -> str:
        return self.value


def is_ascii_alphanumeric(char: str) -> bool:
    is_digit = "0" <= char <= "9"
    is_upper = "A" <= char <= "Z"
    is_lower = "a" <= char <= "z"
    return is_digit or is_upper or is_lower


def is_simple_symbol_character(char: str) -> bool:
    is_symbol_punctuation = char == "-" or char == "_"
    return is_ascii_alphanumeric(char) or is_symbol_punctuation
