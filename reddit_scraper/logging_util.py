"""Shared logging setup.

The pipeline prints progress via tqdm (see ``backfill.py`` / ``parallel.py``);
everything else — stage banners, per-subreddit results, warnings, errors — goes
through the standard ``logging`` module so verbosity is controllable with
``--verbose`` / ``--quiet`` and log lines interleave cleanly with tqdm bars.
"""
from __future__ import annotations

import logging
import sys

LOGGER_NAME = "reddit_scraper"


class _TqdmHandler(logging.Handler):
    """Emit log records through ``tqdm.write`` so they don't corrupt progress bars.

    Falls back to plain stderr if tqdm isn't importable for some reason.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            try:
                from tqdm import tqdm
                tqdm.write(msg, file=sys.stderr)
            except Exception:
                print(msg, file=sys.stderr)
        except Exception:
            self.handleError(record)


def setup_logging(verbosity: int = 0) -> logging.Logger:
    """Configure and return the package logger.

    ``verbosity``: 0 = INFO (default), >0 = DEBUG, <0 = WARNING (quiet).
    """
    if verbosity > 0:
        level = logging.DEBUG
    elif verbosity < 0:
        level = logging.WARNING
    else:
        level = logging.INFO

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.handlers.clear()
    handler = _TqdmHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)
