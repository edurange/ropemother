#!/usr/bin/env python3
# ropemother/client/requestoptions.py

"""Request/reply option symbols."""

from typing import Final

from ropemother.util.symbol import Symbol

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T20:21:05+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


class RequestOption(Symbol):
    """Symbolic option for request/reply helper configuration."""
    pass


SAME_MSG_TYPE: Final = RequestOption("SAME_MSG_TYPE")
