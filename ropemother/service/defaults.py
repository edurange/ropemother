#!/usr/bin/env python3
# ropemother/service/defaults.py

"""Default service configuration helpers."""

from collections.abc import Iterable
from typing import Any

from ropemother.client.procedure import PROCEDURE_INVOCATION_JSON_FORMAT
from ropemother.format.formattable import LocalPortableFormatTable
from ropemother.format.portableformat import (
    COMPOSITE_PORTABLE_FORMAT,
    JSON_PORTABLE_FORMAT,
    RAW_BYTES_PORTABLE_FORMAT,
    PortableFormat,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T04:49:01+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev1"
__status__ = "Development"


def default_portable_formats() -> tuple[PortableFormat[Any, Any], ...]:
    """Return portable formats known to the base ropemother runtime."""
    formats = (
        RAW_BYTES_PORTABLE_FORMAT,
        JSON_PORTABLE_FORMAT,
        COMPOSITE_PORTABLE_FORMAT,
        PROCEDURE_INVOCATION_JSON_FORMAT,
    )
    return formats


def default_portable_format_table(
    extra_formats: Iterable[PortableFormat[Any, Any]] = (),
) -> LocalPortableFormatTable:
    """Build a mutable local table with default and application formats."""
    formats = default_portable_formats()
    table = LocalPortableFormatTable(formats)
    table.add_formats(extra_formats)
    return table
