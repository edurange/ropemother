#!/usr/bin/env python3
# ropemother/service/descriptor.py

"""Connection descriptors for freestanding message bus services."""

from dataclasses import dataclass
from pathlib import Path
from typing import Self
from urllib.parse import quote, unquote, urlparse

from ropemother.exceptions import MessageBusBaseException

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-02T16:28:09+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


UNIX_SOCKET_DESCRIPTOR_SCHEME = "ropemother+unix"


class ConnectionDescriptorError(MessageBusBaseException):
    """Base exception for service connection descriptor errors."""
    pass


class InvalidConnectionDescriptorError(ValueError, ConnectionDescriptorError):
    """Raised when a service connection descriptor is malformed."""
    pass


class UnsupportedConnectionDescriptorError(
    ValueError, ConnectionDescriptorError
):
    """Raised when a service connection descriptor scheme is unsupported."""
    pass


@dataclass(frozen=True, kw_only=True)
class ConnectionDescriptor:
    """URI descriptor for connecting to a freestanding message bus service."""
    uri: str

    def __post_init__(self) -> None:
        self._validate()

    def __str__(self) -> str:
        return self.uri

    @classmethod
    def parse(cls, value: str) -> Self:
        return cls(uri=value)

    @classmethod
    def for_unix_socket(cls, socket_path: Path | str) -> Self:
        path = Path(socket_path)
        if not path.is_absolute():
            raise InvalidConnectionDescriptorError(
                "Unix socket connection descriptors require an absolute path"
            )

        quoted_path = quote(path.as_posix(), safe="/")
        uri = f"{UNIX_SOCKET_DESCRIPTOR_SCHEME}://{quoted_path}"
        return cls(uri=uri)

    def to_uri(self) -> str:
        return self.uri

    def unix_socket_path(self) -> Path:
        parsed = urlparse(self.uri)
        if parsed.scheme != UNIX_SOCKET_DESCRIPTOR_SCHEME:
            raise UnsupportedConnectionDescriptorError(
                f"unsupported connection descriptor scheme: {parsed.scheme!r}"
            )
        if parsed.netloc != "":
            raise InvalidConnectionDescriptorError(
                "Unix socket connection descriptors must not include a host"
            )
        if parsed.path == "":
            raise InvalidConnectionDescriptorError(
                "Unix socket connection descriptors require a socket path"
            )

        socket_path = Path(unquote(parsed.path))
        if not socket_path.is_absolute():
            raise InvalidConnectionDescriptorError(
                "Unix socket connection descriptors require an absolute path"
            )
        return socket_path

    def _validate(self) -> None:
        parsed = urlparse(self.uri)
        if parsed.scheme == "":
            raise InvalidConnectionDescriptorError(
                "connection descriptor requires a URI scheme"
            )
        if parsed.scheme != UNIX_SOCKET_DESCRIPTOR_SCHEME:
            raise UnsupportedConnectionDescriptorError(
                f"unsupported connection descriptor scheme: {parsed.scheme!r}"
            )

        self.unix_socket_path()