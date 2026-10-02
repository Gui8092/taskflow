"""Entrada do módulo: ``python -m taskflow.cli ...``."""

from __future__ import annotations

import sys

from taskflow.cli.main import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))