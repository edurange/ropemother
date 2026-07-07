#!/usr/bin/env python3
# ropemother/service/defaults.py

"""Default service configuration helpers."""

from ropemother.client.procedure import PROCEDURE_INVOCATION_JSON_FORMAT
from ropemother.format.formattable import (
    PortableFormatTable,
    StaticPortableFormatTable,
)
from ropemother.format.portableformat import (
    COMPOSITE_PORTABLE_FORMAT,
    JSON_PORTABLE_FORMAT,
    RAW_BYTES_PORTABLE_FORMAT,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-06T07:41:53+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


def default_portable_format_table() -> PortableFormatTable:
    formats = (
        RAW_BYTES_PORTABLE_FORMAT,
        JSON_PORTABLE_FORMAT,
        COMPOSITE_PORTABLE_FORMAT,
        PROCEDURE_INVOCATION_JSON_FORMAT,
    )
    return StaticPortableFormatTable(*formats)
