"""Monitoring: MIN_FREE_GB disk guard and the `health` command.

diskspace.py (merged): MIN_FREE_GB guard: stop ingesting before MongoDB's disk fills up.

Free space is read from MongoDB itself (dbStats fsTotalSize - fsUsedSize = the filesystem
holding the database), so it also works when MongoDB runs on another host.

health.py (merged): `health`: is the live tail keeping up? For monitoring (cron, systemd timers, Task Scheduler).

Compares the tail checkpoint (index + updated_at) with the node's validated ledger.
Exit 0 healthy · 1 lagging, stale or no checkpoint · 2 node on the wrong network ·
4 node or MongoDB unreachable. Uses short (5 s) timeouts.
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Tuple

from pymongo.errors import PyMongoError

from ..config import Config
from ..errors import IngestError, NetworkMismatch
from ..source import WsSource
from ..store import LAST_LEDGER_ID, MongoStore
from .logsetup import format_duration

log = logging.getLogger(__name__)


# ======================================================================================
# diskspace: MIN_FREE_GB guard: stop ingesting before MongoDB's disk fills up.
# ======================================================================================


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


# ======================================================================================
# health: `health`: is the live tail keeping up? For monitoring (cron, systemd timers, Task Scheduler).
# ======================================================================================

FAST_TIMEOUT_S = 5
FAST_MONGO = {"serverSelectionTimeoutMS": FAST_TIMEOUT_S * 1000,
              "connectTimeoutMS": FAST_TIMEOUT_S * 1000,
              "socketTimeoutMS": FAST_TIMEOUT_S * 1000}
DEFAULT_MAX_LAG = 50
DEFAULT_MAX_AGE = 120


def fast_store(cfg: Config) -> MongoStore:
    return MongoStore.from_config(cfg, **FAST_MONGO)


def fast_source(cfg: Config, *, check_network: bool = True) -> WsSource:
    """One connect attempt per server, 5 s per request."""
    return WsSource(cfg.servers, request_timeout=FAST_TIMEOUT_S, connect_retries=1,
                    backoff_base=0, backoff_max=0,
                    expected_network_id=cfg.expected_network_id if check_network else None,
                    max_message_bytes=cfg.max_message_bytes)


def _utc(dt: Any) -> Optional[datetime]:
    if not isinstance(dt, datetime):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)  # pymongo: naive = UTC


def read_checkpoint(store: Any) -> Tuple[int, Optional[datetime]]:
    """(index, updated_at) of the tail checkpoint; (0, None) if there is none. A legacy
    string index is read as a number."""
    doc = store.meta.find_one({"_id": LAST_LEDGER_ID}) or {}
    try:
        index = int(doc.get("index") or 0)
    except (TypeError, ValueError):
        index = 0
    return index, _utc(doc.get("updated_at"))


def evaluate(checkpoint: int, updated_at: Optional[datetime], latest: int, *,
             max_lag: int, max_age: float, now: Optional[datetime] = None) -> Tuple[int, str]:
    """(exit code, one-line verdict)."""
    now = now or datetime.now(timezone.utc)
    if not checkpoint:
        return 1, f"unhealthy: no tail checkpoint yet (node validated {latest})"
    lag = latest - checkpoint
    age = (now - updated_at).total_seconds() if updated_at else None
    age_text = f"updated {format_duration(age)} ago" if age is not None else "never updated"
    detail = (f"checkpoint {checkpoint}, node validated {latest} (lag {lag}, max {max_lag}), "
              f"{age_text} (max {format_duration(max_age)})")
    if lag > max_lag:
        return 1, f"lagging: {detail}"
    if age is None or age > max_age:
        return 1, f"stale: {detail}"
    return 0, f"healthy: {detail}"


async def run_health(cfg: Config, *, max_lag: int = DEFAULT_MAX_LAG,
                     max_age: float = DEFAULT_MAX_AGE, out: Callable[[str], None] = print,
                     store_factory: Callable[[Config], Any] = fast_store,
                     source_factory: Callable[[Config], Any] = fast_source) -> int:
    store = store_factory(cfg)
    try:
        checkpoint, updated_at = await asyncio.to_thread(read_checkpoint, store)
    except PyMongoError as e:
        out(f"unhealthy: MongoDB unreachable: {type(e).__name__}: {e}")
        return 4
    finally:
        store.close()
    source = source_factory(cfg)
    try:
        latest = await source.latest_validated()
    except NetworkMismatch as e:
        out(f"unhealthy: node on the wrong network: {e}")
        return 2
    except IngestError as e:
        out(f"unhealthy: node unreachable: {type(e).__name__}: {e}")
        return 4
    finally:
        await source.close()
    code, line = evaluate(checkpoint, updated_at, latest, max_lag=max_lag, max_age=max_age)
    out(line)
    return code
