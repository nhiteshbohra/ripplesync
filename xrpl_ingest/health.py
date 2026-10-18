"""`health`: is the live tail keeping up? For monitoring (cron, systemd timers, Docker).

Compares the tail checkpoint (index + updated_at) with the node's validated ledger.
Exit 0 healthy · 1 lagging, stale or no checkpoint · 2 node on the wrong network ·
4 node or MongoDB unreachable. Uses short (5 s) timeouts.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Tuple

from pymongo.errors import PyMongoError

from .config import Config
from .errors import IngestError, NetworkMismatch
from .logsetup import format_duration
from .source import WsSource
from .store import LAST_LEDGER_ID, MongoStore

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
