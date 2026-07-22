#!/usr/bin/env python3
# ropemother/util/lengthprefixed.py

"""Helpers for encoding length-prefixed byte arrays."""

from collections.abc import Sequence
import struct
from typing import Final

from ropemother.exceptions import MessageBusBaseException

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T16:43:03+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"

MARKER_START: Final[bytes] = b"["
MARKER_END: Final[bytes] = b"]"
NEWLINE: Final[bytes] = b"\n"
LENGTH_VALUE_FORMAT: Final[str] = ">I"
HEADER_SIZE: Final[int] = struct.calcsize(LENGTH_VALUE_FORMAT) + len(
    MARKER_START + MARKER_END + NEWLINE
)


# Need to unite exception hierarchy across module
class FramingError(MessageBusBaseException):
    """Base exception for length-prefixed framing errors."""
    pass


# Would this also be something like a ValueError? Should it inherit from
# FramingError? More cleanup needed
class IncompletePayloadError(EOFError, FramingError):
    """Raised when a length-prefixed payload is incomplete."""
    pass


def compute_prefixed_size(payload_len: int) -> int:
    return HEADER_SIZE + payload_len + 1


def encode_prefixed(payload: bytes) -> bytes:
    header = (
        MARKER_START
        + struct.pack(LENGTH_VALUE_FORMAT, len(payload))
        + MARKER_END
        + NEWLINE
    )
    return header + payload + NEWLINE


def decode_prefixed_length(header_bytes: bytes) -> int:
    # Maybe be more specific about this name
    if len(header_bytes) != HEADER_SIZE:
        raise FramingError(f"header must be exactly {HEADER_SIZE} in length")

    if not (
        header_bytes.startswith(MARKER_START)
        and header_bytes.endswith(MARKER_END + NEWLINE)
    ):
        raise FramingError(
            f'invalid record header framing: "{header_bytes!r}"'
        )

    return struct.unpack(LENGTH_VALUE_FORMAT, header_bytes[1:5])[0]


def decode_prefixed_binary(
    buffer: bytes, at_pos: int = 0
) -> tuple[bytes, int]:
    buffer_len = len(buffer)
    payload_start = at_pos + HEADER_SIZE
    if payload_start > buffer_len:
        raise FramingError("buffer ends before header is complete")

    header = buffer[at_pos : at_pos + HEADER_SIZE]
    payload_len = decode_prefixed_length(header)
    payload_end = payload_start + payload_len
    next_pos = payload_end + 1

    if next_pos > buffer_len:
        raise IncompletePayloadError(
            f"buffer ended at {buffer_len} while seeking {next_pos}"
        )

    if buffer[payload_end:next_pos] != NEWLINE:
        raise FramingError(
            f"missing trailing newline at positon {payload_end}"
        )

    return buffer[payload_start:payload_end], next_pos


def pack_prefixed_sequence(elements: Sequence[bytes]) -> bytes:
    inner_content = b"".join(encode_prefixed(e) for e in elements)
    return encode_prefixed(inner_content)


def unpack_prefixed_sequence(data: bytes) -> tuple[bytes, ...]:
    results = []
    cursor = 0
    inner_content, _ = decode_prefixed_binary(data, 0)
    data_len = len(inner_content)

    while cursor < data_len:
        payload_element, cursor = decode_prefixed_binary(inner_content, cursor)
        results.append(payload_element)

    return tuple(results)
