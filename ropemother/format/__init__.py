#!/usr/bin/env python3
# ropemother/format/__init__.py

"""Record format conversion utilities."""

from ropemother.format.defaults import (
    default_portable_format_registry,
    default_portable_formats,
)
from ropemother.format.formattable import (
    ConflictingPortableFormatError,
    PortableFormatTable,
    PortableFormatTableError,
    UnknownPortableFormatError,
)
from ropemother.format.portableformat import (
    COMPOSITE_PORTABLE_FORMAT,
    JSON_PORTABLE_FORMAT,
    RAW_BYTES_PORTABLE_FORMAT,
    PortableFormat,
    PortableFormatError,
    PortableFormatKey,
)
from ropemother.format.registry import (
    ConflictingPortableFormatRegistrationError,
    FormatRegistryError,
    PortableFormatID,
    PortableFormatRegistration,
    PortableFormatRegistry,
    UnknownPortableFormatIDError,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-09T16:24:55+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev2"
__status__ = "Development"


__all__ = [
    "COMPOSITE_PORTABLE_FORMAT",
    "ConflictingPortableFormatError",
    "ConflictingPortableFormatRegistrationError",
    "FormatRegistryError",
    "JSON_PORTABLE_FORMAT",
    "PortableFormat",
    "PortableFormatError",
    "PortableFormatID",
    "PortableFormatKey",
    "PortableFormatRegistration",
    "PortableFormatRegistry",
    "PortableFormatTable",
    "PortableFormatTableError",
    "RAW_BYTES_PORTABLE_FORMAT",
    "UnknownPortableFormatError",
    "UnknownPortableFormatIDError",
    "default_portable_format_registry",
    "default_portable_formats",
]