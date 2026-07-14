#!/usr/bin/env python3
# ropemother/util/serializer.py

"""An abstraction for mapping arbitrary types to serialization strategies."""

from abc import ABC, abstractmethod
from typing import Final, Type

from ropemother.exceptions import MessageBusBaseException

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-11T01:35:15+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


class SerializationError(MessageBusBaseException):  # Clean this up more later
    """Raised when serialization or adaptation fails."""
    pass


class TypeAdapter[DomainT, SerializableT](ABC):
    """Adapter between runtime values and serializable projections."""
    # Set these only in the class declaration, never change them at runtime
    domain_type: Type[DomainT]
    serial_type: Type[SerializableT]

    @abstractmethod
    def encode(self, value: DomainT) -> SerializableT:
        pass

    @abstractmethod
    def decode(self, data: SerializableT) -> DomainT:
        pass


class IdentityAdapter[T](TypeAdapter[T, T]):
    """Adapter that returns values unchanged in both directions."""
    def encode(self, value: T) -> T:
        return value

    def decode(self, value: T) -> T:
        return value


IDENTITY_BYTES_ADAPTER: Final[TypeAdapter[bytes, bytes]] = (
    IdentityAdapter[bytes]()
)


class Serializer[T](TypeAdapter[T, bytes]):
    """Adapter between serializable projections and bytes."""
    # domain_type is expected to be T in implementation
    serial_type = bytes

    @abstractmethod
    def encode(self, value: T) -> bytes:
        ...

    @abstractmethod
    def decode(self, data: bytes) -> T:
        ...


class IdentitySerializer(Serializer[bytes]):
    """Serializer that returns byte payloads unchanged."""
    domain_type = bytes
    # serial_type is bytes, inherited from Serializer

    def encode(self, value: bytes) -> bytes:
        return value

    def decode(self, data: bytes) -> bytes:
        return data


IDENTITY_SERIALIZER: Final[Serializer[bytes]] = IdentitySerializer()


class NoneSerializer(Serializer[None]):
    """Serializer that maps None to an empty byte payload."""
    domain_type = type(None)

    def encode(self, value: None) -> bytes:
        if value is not None:
            raise SerializationError(
                f"{type(self).__name__} can only encode None"
            )
        return b""

    def decode(self, buffer: bytes) -> None:
        if buffer:
            raise SerializationError(
                f"{type(self).__name__} expected an empty byte buffer"
            )
        return None


NONE_SERIALIZER: Final[Serializer[None]] = NoneSerializer()
