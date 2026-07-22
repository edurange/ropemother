#!/usr/bin/env python3
# ropemother/service/broker.py

"""Freestanding local message bus broker process."""

from argparse import ArgumentParser, Namespace
from collections.abc import Sequence
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from time import sleep

from ropemother.broker.directcore import CaptureMode
from ropemother.capture.filesink import JSONLinesCaptureSink
from ropemother.capture.filehistory import JSONLinesCaptureHistory
from ropemother.capture.history import MessageHistory
from ropemother.capture.sink import CaptureSink
from ropemother.service.brokerextension import BrokerExtension
from ropemother.service.brokerhistory import BrokerHistoryExtension
from ropemother.service.environment import BUS_CONTACT_URI_VARIABLE
from ropemother.service.host import (
    InvalidLocalMessageBusHostError,
    LocalMessageBusHost,
)

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-10T22:42:59+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev4"
__status__ = "Development"


BROKER_IDLE_SLEEP_SECONDS = 3600
DEFAULT_RUNTIME_DIRECTORY = ".ropemother"
DEFAULT_CAPTURE_FILENAME = "capture.jsonl"


def serve_local_message_bus(
    *,
    runtime_directory: Path | str | None = DEFAULT_RUNTIME_DIRECTORY,
    socket_path: Path | str | None = None,
    replace_existing_socket: bool = False,
    capture_mode: CaptureMode = CaptureMode.CAPTURE_ENABLED,
    capture_sink: CaptureSink | None = None,
    broker_extensions: list[BrokerExtension] | None = None,
) -> None:
    """Run a local message bus broker until interrupted."""
    with LocalMessageBusHost(
        runtime_directory=runtime_directory,
        socket_path=socket_path,
        replace_existing_socket=replace_existing_socket,
        daemon_service=False,
        capture_mode=capture_mode,
        capture_sink=capture_sink,
        broker_extensions=broker_extensions,
    ) as host:
        descriptor = host.connection_descriptor()
        broker_uri = descriptor.to_uri()
        environment = f"{BUS_CONTACT_URI_VARIABLE}={broker_uri}"
        print("Message bus broker is running", flush=True)
        print(f"broker URI: {broker_uri}", flush=True)
        print(f"environment: {environment}", flush=True)
        print("Press Ctrl-C to stop", flush=True)
        try:
            while True:
                sleep(BROKER_IDLE_SLEEP_SECONDS)
        except KeyboardInterrupt:
            print("Stopping message bus broker", flush=True)


def run_local_broker_command(argv: Sequence[str] | None = None) -> int:
    args = _parse_arguments(argv)

    temporary_runtime = None

    if args.temporary:
        temporary_runtime = TemporaryDirectory(prefix="ropemother-")
        runtime_directory = temporary_runtime.name
    else:
        runtime_directory = _runtime_directory_from_arguments(args)

    capture_path = _capture_path_from_arguments(args, runtime_directory)
    capture_mode = _capture_mode_from_arguments(args)
    capture_sink = _capture_sink_from_arguments(args, capture_path)
    broker_extensions = _broker_extensions_from_arguments(args, capture_path)

    try:
        serve_local_message_bus(
            runtime_directory=runtime_directory,
            socket_path=args.socket_path,
            replace_existing_socket=args.replace_existing_socket,
            capture_mode=capture_mode,
            capture_sink=capture_sink,
            broker_extensions=broker_extensions,
        )
    finally:
        if temporary_runtime is not None:
            temporary_runtime.cleanup()

    return 0


def _runtime_directory_from_arguments(args: Namespace) -> Path | str | None:
    runtime_directory = args.runtime_directory
    if runtime_directory is None and args.socket_path is None:
        runtime_directory = DEFAULT_RUNTIME_DIRECTORY

    return runtime_directory


def _parse_arguments(argv: Sequence[str] | None) -> Namespace:
    parser = ArgumentParser(
        description="Run a freestanding local message bus broker."
    )
    location_group = parser.add_mutually_exclusive_group()
    location_group.add_argument(
        "-r",
        "--runtime-directory",
        metavar="PATH",
        help="directory to create ropemother.sock; default: .ropemother",
    )
    location_group.add_argument(
        "-s",
        "--socket-path",
        metavar="PATH",
        help="exact Unix-domain socket path for the broker",
    )
    location_group.add_argument(
        "-t",
        "--temporary",
        action="store_true",
        help="use a temporary runtime directory",
    )
    parser.add_argument(
        "-x",
        "--replace-existing-socket",
        action="store_true",
        help="replace an existing socket at the selected path",
    )
    parser.add_argument(
        "-c",
        "--capture-path",
        metavar="PATH",
        help="write captured records to this JSON Lines file",
    )
    parser.add_argument(
        "-f",
        "--replace-capture",
        action="store_true",
        help="replace the capture file instead of appending",
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="serve the built-in broker history profile from the capture log",
    )
    parser.add_argument(
        "--transport-only",
        action="store_true",
        help= ("routing without capture; disables replay/history guarantees"),
    )
    args = parser.parse_args(argv)
    if args.history and args.transport_only:
        parser.error("--history cannot be used with --transport-only")
    return args


def _capture_sink_from_arguments(
    args: Namespace, capture_path: Path | None
) -> CaptureSink | None:
    if capture_path is None:
        return None

    capture_path.parent.mkdir(parents=True, exist_ok=True)
    return JSONLinesCaptureSink(capture_path, append=not args.replace_capture)


def _capture_mode_from_arguments(args: Namespace) -> CaptureMode:
    if args.transport_only:
        return CaptureMode.TRANSPORT_ONLY

    return CaptureMode.CAPTURE_ENABLED


def _capture_path_from_arguments(
    args: Namespace, runtime_directory: Path | str | None
) -> Path | None:
    if args.transport_only:
        return None

    if args.capture_path is not None:
        return Path(args.capture_path).expanduser()

    if runtime_directory is None:
        capture_path = Path(DEFAULT_CAPTURE_FILENAME)
    else:
        capture_path = Path(runtime_directory) / DEFAULT_CAPTURE_FILENAME

    return capture_path.expanduser()


def _broker_extensions_from_arguments(
    args: Namespace, capture_path: Path | None
) -> list[BrokerExtension]:
    broker_extensions = []
    history = _history_from_arguments(args, capture_path)
    if history is not None:
        broker_extensions.append(BrokerHistoryExtension(history))

    return broker_extensions


def _history_from_arguments(
    args: Namespace, capture_path: Path | None
) -> MessageHistory | None:
    if not args.history:
        return None

    if capture_path is None:
        raise InvalidLocalMessageBusHostError(
            "broker history requires capture to be enabled"
        )

    capture_path.parent.mkdir(parents=True, exist_ok=True)
    capture_path.touch(exist_ok=True)
    return JSONLinesCaptureHistory(capture_path)


if __name__ == "__main__":
    sys.exit(run_local_broker_command())
