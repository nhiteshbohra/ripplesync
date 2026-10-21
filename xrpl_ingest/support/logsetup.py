"""Logging: stderr (+ optional rotating LOG_FILE) in the parent; pool workers send their
records through a multiprocessing queue to the parent, so only one process writes the file.

Timestamps are UTC ISO 8601 with a Z suffix, e.g. 2026-10-18T09:15:02.123Z.
"""
from __future__ import annotations

import logging
import logging.handlers
import os
import sys
import time
from typing import Any, Callable, List, Optional

from ..errors import ConfigError

log = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
_OURS = "_xrpl_ingest_handler"  # marks handlers we installed, so setup can be repeated


class UtcFormatter(logging.Formatter):
    """asctime = 2026-10-18T09:15:02.123Z (UTC)."""
    converter = time.gmtime

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        t = time.strftime("%Y-%m-%dT%H:%M:%S", self.converter(record.created))
        return f"{t}.{int(record.msecs):03d}Z"


def _mark(h: logging.Handler) -> logging.Handler:
    setattr(h, _OURS, True)
    h.setFormatter(UtcFormatter(LOG_FORMAT))
    return h


def setup_logging(level: str, log_file: str = "", *, max_mb: int = 100, backups: int = 10,
                  stream: Any = None) -> List[logging.Handler]:
    """Root logging for the main process: stderr, plus a RotatingFileHandler if log_file.

    Replaces handlers installed by an earlier call (not foreign ones such as pytest's).
    An unwritable log file is a ConfigError."""
    root = logging.getLogger()
    for h in [h for h in root.handlers if getattr(h, _OURS, False)]:
        root.removeHandler(h)
        h.close()
    handlers: List[logging.Handler] = [_mark(logging.StreamHandler(stream or sys.stderr))]
    if log_file:
        try:
            parent = os.path.dirname(log_file)
            if parent:
                os.makedirs(parent, exist_ok=True)
            fh = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=max_mb * 1024 * 1024, backupCount=backups, encoding="utf-8")
        except OSError as e:
            raise ConfigError(f"cannot open LOG_FILE {log_file}: {e}") from None
        handlers.append(_mark(fh))
    for h in handlers:
        root.addHandler(h)
    root.setLevel(getattr(logging, level))
    return handlers


class _Dispatch:
    """QueueListener target: hand worker records to the parent's own loggers, so they go
    through the parent's handlers (stderr, LOG_FILE) and levels."""
    level = logging.NOTSET

    def handle(self, record: logging.LogRecord) -> None:
        logger = logging.getLogger(record.name)
        if logger.isEnabledFor(record.levelno):
            logger.handle(record)


def start_queue_listener(queue: Any) -> logging.handlers.QueueListener:
    listener = logging.handlers.QueueListener(queue, _Dispatch(), respect_handler_level=False)
    listener.start()
    return listener


def stop_queue_listener(listener: logging.handlers.QueueListener, timeout: float = 5.0) -> bool:
    """QueueListener.stop() that gives up after `timeout` seconds instead of waiting forever.

    On Linux a multiprocessing queue has a write lock shared by all processes; a worker that
    is killed while it holds it (OOM kill) leaves the stop sentinel undeliverable, and the
    plain stop() would hang. The listener thread is a daemon, so leaving it does not block exit.
    Returns True if the listener stopped in time."""
    thread = getattr(listener, "_thread", None)
    try:
        listener.enqueue_sentinel()
    except Exception as e:  # noqa: BLE001  best effort: a broken queue must not stop the caller
        log.warning("log queue: cannot send the stop marker: %s", e)
    stopped = True
    if thread is not None:
        thread.join(timeout)
        stopped = not thread.is_alive()
        if not stopped:
            log.warning("log queue listener did not stop within %.0fs; continuing without it", timeout)
    listener._thread = None  # type: ignore[attr-defined]  (what QueueListener.stop() does too)
    return stopped


def setup_worker_logging(queue: Any, level: str) -> None:
    """In a pool worker: every record goes to the parent through `queue`."""
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(logging.handlers.QueueHandler(queue))
    root.setLevel(getattr(logging, level))


def format_duration(seconds: float) -> str:
    """2d04h, 3h07m, 12m05s, 42s."""
    s = max(0, int(seconds))
    d, rem = divmod(s, 86400)
    h, rem = divmod(rem, 3600)
    m, sec = divmod(rem, 60)
    if d:
        return f"{d}d{h:02d}h"
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{sec:02d}s"
    return f"{sec}s"


class ChunkProgress:
    """`chunks 1234/98000 (1.3%), 512 ledgers/s, ETA 2d04h` for the backfill log.

    Counts include chunks done by earlier runs; the rate uses this run's ledgers and a
    monotonic clock (immune to wall-clock jumps)."""

    def __init__(self, already_done: int, chunks: List[Any],
                 clock: Callable[[], float] = time.monotonic):
        self.done = already_done
        self.total = already_done + len(chunks)
        self.ledgers_left = sum(b - a + 1 for a, b in chunks)
        self.ledgers_this_run = 0
        self._clock = clock
        self._t0 = clock()

    def update(self, chunk: Any) -> str:
        a, b = chunk
        self.done += 1
        self.ledgers_this_run += b - a + 1
        self.ledgers_left = max(0, self.ledgers_left - (b - a + 1))
        return self.line()

    def line(self) -> str:
        pct = 100.0 * self.done / self.total if self.total else 100.0
        elapsed = self._clock() - self._t0
        rate = self.ledgers_this_run / elapsed if elapsed > 0 else 0.0
        eta = format_duration(self.ledgers_left / rate) if rate > 0 else "?"
        if not self.ledgers_left:
            eta = "0s"
        return f"chunks {self.done}/{self.total} ({pct:.1f}%), {rate:.0f} ledgers/s, ETA {eta}"
