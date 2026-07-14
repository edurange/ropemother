#!/usr/bin/env python3
# ropemother/service/__main__.py

"""Command-line entry point for the local message bus broker."""

import sys

from ropemother.service.broker import run_local_broker_command

__author__ = "Joe Granville"
__email__ = "874605+jwgranville@users.noreply.github.com"
__date__ = "2026-07-10T22:43:52+00:00"
__license__ = "MIT"
__version__ = "0.1.0.dev3"
__status__ = "Development"


if __name__ == "__main__":
    sys.exit(run_local_broker_command())
