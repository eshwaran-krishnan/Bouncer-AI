"""
bouncer/utils/logger.py — Structured JSON logger.

Modal captures stdout/stderr from every container run. Logging to stdout
in JSON lines format makes every run searchable and filterable via:
    modal app logs bouncer-qc

Usage:
    from bouncer.utils.logger import get_logger
    log = get_logger(__name__)
    log.info("stage_start", stage="classify", n_files=4)
    log.info("api_call", model="claude-sonnet-4-6", input_tokens=312, output_tokens=88)
    log.error("extraction_failed", path="/data/counts.tsv", error=str(e))
"""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager
from typing import Any

# LogRecord built-in attributes that cannot be used as extra= keys.
# Python raises KeyError if extra= tries to overwrite any of these.
_RESERVED = frozenset({
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "taskName",
})


class _SafeLogger(logging.Logger):
    """Logger subclass that never raises on extra= key conflicts."""

    def makeRecord(self, name, level, fn, lno, msg, args, exc_info,
                   func=None, extra=None, sinfo=None):
        if extra:
            # Prefix any key that would overwrite a built-in field
            safe_extra = {
                (f"f_{k}" if k in _RESERVED else k): v
                for k, v in extra.items()
            }
        else:
            safe_extra = extra
        return super().makeRecord(
            name, level, fn, lno, msg, args, exc_info,
            func=func, extra=safe_extra, sinfo=sinfo,
        )


logging.setLoggerClass(_SafeLogger)


class _JSONFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts":     self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level":  record.levelname,
            "logger": record.name,
            "msg":    record.getMessage(),
        }
        # Merge extra fields added by the caller
        skip = {
            "name", "msg", "args", "levelname", "levelno", "pathname",
            "filename", "module", "exc_info", "exc_text", "stack_info",
            "lineno", "funcName", "created", "msecs", "relativeCreated",
            "thread", "threadName", "processName", "process", "message",
            "taskName",
        }
        for key, val in record.__dict__.items():
            if key not in skip:
                payload[key] = val

        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def _build_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JSONFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
    return logger


def get_logger(name: str) -> "_BoundLogger":
    return _BoundLogger(_build_logger(name))


class _BoundLogger:
    """Thin wrapper so we can call log.info("msg", key=val, ...) directly."""

    def __init__(self, logger: logging.Logger) -> None:
        self._l = logger

    def _emit(self, level: int, msg: str, **kwargs: Any) -> None:
        exc_info   = kwargs.pop("exc_info", False)
        stack_info = kwargs.pop("stack_info", False)
        self._l.log(level, msg, stacklevel=3, exc_info=exc_info,
                    stack_info=stack_info, extra=kwargs)

    def debug(self, msg: str, **kwargs: Any) -> None:
        self._emit(logging.DEBUG, msg, **kwargs)

    def info(self, msg: str, **kwargs: Any) -> None:
        self._emit(logging.INFO, msg, **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:
        self._emit(logging.WARNING, msg, **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:
        self._emit(logging.ERROR, msg, **kwargs)

    def exception(self, msg: str, **kwargs: Any) -> None:
        kwargs.setdefault("exc_info", True)
        self._emit(logging.ERROR, msg, **kwargs)


@contextmanager
def timer(log: _BoundLogger, stage: str, **ctx: Any):
    """Context manager that logs stage start, end, and elapsed_ms."""
    log.info("stage_start", stage=stage, **ctx)
    t0 = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = round((time.perf_counter() - t0) * 1000)
        log.info("stage_end", stage=stage, elapsed_ms=elapsed_ms, **ctx)
