#!/usr/bin/env python3
# ropemother/format/defaults.py

"""Default portable payload formats for local ropemother runtimes."""

from collections.abc import Iterable
from typing import Any

from ropemother.client.procedure import PROCEDURE_INVOCATION_JSON_FORMAT
from ropemother.format.portableformat import (
    COMPOSITE_PORTABLE_FORMAT,
    JSON_PORTABLE_FORMAT,
    RAW_BYTES_PORTABLE_FORMAT,
    PortableFormat,
)
from ropemother.format.registry import PortableFormatRegistry

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T15:55:31+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


def default_portable_formats() -> tuple[PortableFormat[Any, Any], ...]:
    """Return portable formats installed by the base ropemother runtime."""
    formats = (
        RAW_BYTES_PORTABLE_FORMAT,
        JSON_PORTABLE_FORMAT,
        COMPOSITE_PORTABLE_FORMAT,
        PROCEDURE_INVOCATION_JSON_FORMAT,
    )
    return formats


def default_portable_format_registry(
    extra_formats: Iterable[PortableFormat[Any, Any]] = (),
) -> PortableFormatRegistry:
    """Build a local format registry with default and application formats."""
    registry = PortableFormatRegistry(*default_portable_formats())
    registry.install_formats(extra_formats)
    return registry
