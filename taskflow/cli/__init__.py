"""CLI do taskflow: ``python -m taskflow.cli <comando>``."""

from taskflow.cli.main import (
    EXIT_ERROR,
    EXIT_INTERRUPTED,
    EXIT_OK,
    EXIT_USAGE,
    build_parser,
    main,
)

__all__ = [
    "EXIT_ERROR",
    "EXIT_INTERRUPTED",
    "EXIT_OK",
    "EXIT_USAGE",
    "build_parser",
    "main",
]