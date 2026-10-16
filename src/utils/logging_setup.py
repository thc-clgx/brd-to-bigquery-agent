"""Central logging configuration for CLI, Airflow and test runs."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_CONFIGURED = False
_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-38s | %(message)s"


def setup_logging(level: str = "INFO", log_file: str | Path | None = None) -> None:
    """Configure root logging once per process."""
    global _CONFIGURED
    if _CONFIGURED:
        logging.getLogger().setLevel(level.upper())
        return

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file is not None:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(path, encoding="utf-8"))

    logging.basicConfig(level=level.upper(), format=_FORMAT, handlers=handlers, force=True)
    # These libraries are extremely chatty at INFO.
    for noisy in ("google.auth", "urllib3", "google.cloud", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
