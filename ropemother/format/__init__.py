#!/usr/bin/env python3
# ropemother/format/__init__.py

"""Record format conversion utilities."""

from importlib import import_module
from typing import Any

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-09-26T23:28:09+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev10"
__status__ = "Development"


_EXPORTS = {
    "COMPOSITE_PORTABLE_FORMAT": "ropemother.format.portableformat",
    "ConflictingPortableFormatError": "ropemother.format.formattable",
    "ConflictingPortableFormatRegistrationError": "ropemother.format.registry",
    "FormatRegistryError": "ropemother.format.registry",
    "JSON_PORTABLE_FORMAT": "ropemother.format.portableformat",
    "PortableFormat": "ropemother.format.portableformat",
    "PortableFormatError": "ropemother.format.portableformat",
    "PortableFormatID": "ropemother.format.registry",
    "PortableFormatKey": "ropemother.format.portableformat",
    "PortableFormatRegistration": "ropemother.format.registry",
    "PortableFormatRegistry": "ropemother.format.registry",
    "PortableFormatTable": "ropemother.format.formattable",
    "PortableFormatTableError": "ropemother.format.formattable",
    "RAW_BYTES_PORTABLE_FORMAT": "ropemother.format.portableformat",
    "UnknownPortableFormatError": "ropemother.format.formattable",
    "UnknownPortableFormatIDError": "ropemother.format.registry",
    "default_portable_format_registry": "ropemother.format.defaults",
    "default_portable_formats": "ropemother.format.defaults",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(name)

    module = import_module(_EXPORTS[name])
    value = getattr(module, name)
    globals()[name] = value
    return value
