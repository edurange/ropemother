#!/usr/bin/env python3
# ropemother/service/broker.py

"""Freestanding local message bus broker process."""

import argparse
import collections.abc
import pathlib
import sys
import tempfile
import time

from ropemother.broker.directcore import CaptureMode
from ropemother.capture.filesink import JSONLinesCaptureSink
from ropemother.capture.filehistory import JSONLinesCaptureHistory
from ropemother.capture.sink import CaptureSink
from ropemother.format.portableformat import PortableFormat
from ropemother.service.brokerextension import BrokerExtension
from ropemother.service.brokerhistory import BrokerHistoryExtension
from ropemother.service.environment import BUS_CONTACT_URI_VARIABLE
from ropemother.service.host import LocalMessageBusHost

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-08-25T21:02:29+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev8"
__status__ = "Development"


BROKER_IDLE_SLEEP_SECONDS = 3600
DEFAULT_RUNTIME_DIRECTORY = ".ropemother"
DEFAULT_CAPTURE_FILENAME = "capture.jsonl"


def serve_local_message_bus(
    *extensions: BrokerExtension,
    extra_formats: collections.abc.Iterable[PortableFormat] = (),
    runtime_directory: pathlib.Path | str | None = DEFAULT_RUNTIME_DIRECTORY,
    socket_path: pathlib.Path | str | None = None,
    replace_existing_socket: bool = False,
    capture_mode: CaptureMode = CaptureMode.CAPTURE_ENABLED,
    capture_sink: CaptureSink | None = None,
) -> None:
    """Run a local message bus broker until interrupted."""
    host = LocalMessageBusHost(
        *extensions,
        extra_formats=extra_formats,
        runtime_directory=runtime_directory,
        socket_path=socket_path,
        replace_existing_socket=replace_existing_socket,
        daemon_service=False,
        capture_mode=capture_mode,
        capture_sink=capture_sink,
    )
    _serve_local_message_bus_host(host)


def run_local_broker_command(
    argv: collections.abc.Sequence[str] | None = None,
    *,
    extensions: collections.abc.Iterable[BrokerExtension] = (),
    extra_formats: collections.abc.Iterable[PortableFormat] = (),
) -> int:
    args = _parse_arguments(argv)
    portable_formats = tuple(extra_formats)

    temporary_runtime = None

    if args.temporary:
        temporary_runtime = tempfile.TemporaryDirectory(prefix="ropemother-")
        runtime_directory = temporary_runtime.name
    else:
        runtime_directory = _runtime_directory_from_arguments(args)

    if args.history:
        host = preconfigured_history_host(
            *extensions,
            extra_formats=portable_formats,
            runtime_directory=runtime_directory,
            socket_path=args.socket_path,
            replace_existing_socket=args.replace_existing_socket,
            capture_path=args.capture_path,
            replace_capture=args.replace_capture,
            daemon_service=False,
        )
    else:
        capture_path = _capture_path_from_arguments(args, runtime_directory)
        capture_sink = _capture_sink_from_arguments(args, capture_path)
        host = LocalMessageBusHost(
            *extensions,
            extra_formats=portable_formats,
            runtime_directory=runtime_directory,
            socket_path=args.socket_path,
            replace_existing_socket=args.replace_existing_socket,
            daemon_service=False,
            capture_mode=_capture_mode_from_arguments(args),
            capture_sink=capture_sink,
        )

    try:
        _serve_local_message_bus_host(host)
    finally:
        if temporary_runtime is not None:
            temporary_runtime.cleanup()

    return 0


def preconfigured_history_host(
    *extensions: BrokerExtension,
    extra_formats: collections.abc.Iterable[PortableFormat] = (),
    runtime_directory: pathlib.Path | str | None = DEFAULT_RUNTIME_DIRECTORY,
    socket_path: pathlib.Path | str | None = None,
    replace_existing_socket: bool = False,
    capture_path: pathlib.Path | str | None = None,
    replace_capture: bool = False,
    daemon_service: bool = True,
) -> LocalMessageBusHost:
    """Return a local broker host with captured history enabled."""
    portable_formats = tuple(extra_formats)
    selected_capture_path = _capture_path(capture_path, runtime_directory)
    sink = _json_lines_capture_sink(
        selected_capture_path, replace_capture=replace_capture
    )
    history = JSONLinesCaptureHistory(
        selected_capture_path, extra_formats=portable_formats
    )
    host = LocalMessageBusHost(
        *extensions,
        BrokerHistoryExtension(history),
        extra_formats=portable_formats,
        runtime_directory=runtime_directory,
        socket_path=socket_path,
        replace_existing_socket=replace_existing_socket,
        capture_sink=sink,
        daemon_service=daemon_service,
    )
    return host


def _serve_local_message_bus_host(host: LocalMessageBusHost) -> None:
    with host:
        descriptor = host.connection_descriptor()
        broker_uri = descriptor.to_uri()
        environment = f"{BUS_CONTACT_URI_VARIABLE}={broker_uri}"
        print("Message bus broker is running", flush=True)
        print(f"broker URI: {broker_uri}", flush=True)
        print(f"environment: {environment}", flush=True)
        print("Press Ctrl-C to stop", flush=True)
        try:
            while True:
                time.sleep(BROKER_IDLE_SLEEP_SECONDS)
        except KeyboardInterrupt:
            print("Stopping message bus broker", flush=True)


def _runtime_directory_from_arguments(
    args: argparse.Namespace
) -> pathlib.Path | str | None:
    runtime_directory = args.runtime_directory
    if runtime_directory is None and args.socket_path is None:
        runtime_directory = DEFAULT_RUNTIME_DIRECTORY

    return runtime_directory


def _parse_arguments(
    argv: collections.abc.Sequence[str] | None
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
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


def _json_lines_capture_sink(
    capture_path: pathlib.Path, *, replace_capture: bool
) -> JSONLinesCaptureSink:
    capture_path.parent.mkdir(parents=True, exist_ok=True)
    capture_path.touch(exist_ok=True)
    return JSONLinesCaptureSink(capture_path, append=not replace_capture)


def _capture_sink_from_arguments(
    args: argparse.Namespace, capture_path: pathlib.Path | None
) -> CaptureSink | None:
    if capture_path is None:
        return None

    sink = _json_lines_capture_sink(
        capture_path, replace_capture=args.replace_capture
    )
    return sink


def _capture_mode_from_arguments(args: argparse.Namespace) -> CaptureMode:
    if args.transport_only:
        return CaptureMode.TRANSPORT_ONLY

    return CaptureMode.CAPTURE_ENABLED


def _capture_path_from_arguments(
    args: argparse.Namespace, runtime_directory: pathlib.Path | str | None
) -> pathlib.Path | None:
    if args.transport_only:
        return None

    return _capture_path(args.capture_path, runtime_directory)


def _capture_path(
    capture_path: pathlib.Path | str | None,
    runtime_directory: pathlib.Path | str | None,
) -> pathlib.Path:
    if capture_path is not None:
        path = pathlib.Path(capture_path)
    elif runtime_directory is None:
        path = pathlib.Path(DEFAULT_CAPTURE_FILENAME)
    else:
        path = pathlib.Path(runtime_directory) / DEFAULT_CAPTURE_FILENAME

    return path.expanduser()


if __name__ == "__main__":
    sys.exit(run_local_broker_command())
