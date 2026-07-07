#!/usr/bin/env python3
# ropemother/transport/zeromq/address.py

"""ZMQ transport address value objects."""

from dataclasses import dataclass

from ropemother.util.symbol import Symbol

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T20:19:07+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


@dataclass(frozen=True, slots=True)
class ZMQAddress(Symbol):
    """ZeroMQ endpoint address used by frame connections."""
    @classmethod
    def inproc(cls, name: str) -> "ZMQAddress":
        address = cls(f"inproc://{name}")
        return address

    @classmethod
    def tcp(cls, host: str, port: int) -> "ZMQAddress":
        address = cls(f"tcp://{host}:{port}")
        return address
