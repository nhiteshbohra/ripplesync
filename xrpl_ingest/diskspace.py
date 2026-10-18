"""MIN_FREE_GB guard: stop ingesting before MongoDB's disk fills up.

Free space is read from MongoDB itself (dbStats fsTotalSize - fsUsedSize = the filesystem
holding the database), so it also works when MongoDB runs on another host or in Docker.
"""
from __future__ import annotations

import logging
import shutil
import time
from typing import Any, Callable, Optional

from pymongo.errors import PyMongoError

from .errors import IngestError

log = logging.getLogger(__name__)

GB = 1024 ** 3
CHECK_INTERVAL = 60.0  # seconds between dbStats calls while running


class LowDiskSpace(IngestError):
    """MongoDB's filesystem has less than MIN_FREE_GB free (CLI exit code 1)."""


def mongo_free_bytes(store: Any) -> Optional[int]:
    """Free bytes on the filesystem of the database; None if MongoDB does not say.

    dbStats of a database that does not exist yet reports 0/0, so fall back to admin's."""
    for stats in (lambda: store.db_stats(), lambda: store.client.admin.command("dbStats")):
        s = stats()
        total, used = s.get("fsTotalSize"), s.get("fsUsedSize")
        if isinstance(total, (int, float)) and isinstance(used, (int, float)) and total > 0:
            return int(total - used)
    return None


def local_free_bytes(path: str) -> Optional[int]:
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return None


class DiskGuard:
    """Stop check: True once MongoDB's free space dropped below `min_free_gb` (checked at
    most every `interval` s). min_free_gb == 0 disables it. A failing check (Mongo hiccup,
    no fs fields) never stops ingestion by itself; the writes will report real errors."""

    def __init__(self, store: Any, min_free_gb: float, *, interval: float = CHECK_INTERVAL,
                 clock: Callable[[], float] = time.monotonic):
        self.store = store
        self.min_bytes = int(min_free_gb * GB)
        self.interval = interval
        self._clock = clock
        self._next: Optional[float] = None
        self.tripped = False
        self._warned_unknown = False
        self.last_free: Optional[int] = None

    @property
    def enabled(self) -> bool:
        return self.min_bytes > 0

    def check_now(self) -> None:
        """Raise LowDiskSpace if below the limit (used once at start)."""
        if self.enabled and self._measure() and self.tripped:
            raise LowDiskSpace(self.message())

    def message(self) -> str:
        free = (self.last_free or 0) / GB
        return (f"MongoDB disk has {free:.1f} GB free, below MIN_FREE_GB={self.min_bytes / GB:g}; "
                "free space or lower MIN_FREE_GB, then rerun")

    def _measure(self) -> bool:
        try:
            free = mongo_free_bytes(self.store)
        except PyMongoError as e:
            log.warning("disk space check failed: %s", e)
            return False
        if free is None:
            if not self._warned_unknown:
                self._warned_unknown = True
                log.warning("MongoDB reports no filesystem size; MIN_FREE_GB cannot be enforced")
            return False
        self.last_free = free
        if free < self.min_bytes and not self.tripped:
            self.tripped = True
            log.error("%s", self.message())
        return True

    def __call__(self) -> bool:
        if not self.enabled or self.tripped:
            return self.tripped
        now = self._clock()
        if self._next is None or now >= self._next:
            self._next = now + self.interval
            self._measure()
        return self.tripped
