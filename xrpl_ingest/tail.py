"""Live tail: follow validated ledgers as they close, resuming from a stored checkpoint."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

from pymongo.errors import PyMongoError

from .config import redact_url
from .errors import RpcError, TransientError
from .fetcher import RetryPolicy, Sleep, ingest_ledger

log = logging.getLogger(__name__)

BIG_GAP = 1000  # ledgers behind before we suggest a backfill


def start_after(highest_stored: int, latest: int) -> int:
    """Ledger to continue after when there is no checkpoint: the highest stored one if it is
    at most BIG_GAP behind, else just before the node's current ledger (go live now)."""
    if highest_stored and latest - highest_stored <= BIG_GAP:
        return highest_stored
    return latest - 1


async def tail(source: Any, store: Any, retry: RetryPolicy, *, poll_interval: float,
               stop_check: Callable[[], bool], sleep: Sleep = asyncio.sleep,
               max_iterations: Optional[int] = None) -> int:
    """Run until stop_check() is true (or max_iterations, for tests). Returns last checkpoint.

    A ledger that fails all retries is recorded in the failed collection and the checkpoint
    still advances, so one bad ledger cannot stall the tail; `retry-failed` picks it up later.
    If the failure cannot be recorded either (Mongo down), the checkpoint stays put and the
    same ledger is retried after a backoff, so nothing is skipped silently.

    Without a checkpoint, tail continues after the highest stored ledger (e.g. where a
    backfill ended) if that is at most BIG_GAP behind; further behind, or on an empty
    database, it starts at the node's current ledger and logs the backfill for the hole.
    """
    last = await asyncio.to_thread(store.get_checkpoint)
    highest = await asyncio.to_thread(store.highest_ledger) if last == 0 else 0
    iterations = 0
    node_failures = 0
    stalls = 0

    while not stop_check():
        if max_iterations is not None and iterations >= max_iterations:
            break
        iterations += 1

        try:
            latest = await source.latest_validated()
            node_failures = 0
        except (TransientError, RpcError) as e:
            if stop_check():  # e.g. StopRequested while reconnecting: exit, no backoff first
                break
            log.warning("cannot read latest validated ledger: %s", redact_url(str(e)))
            await sleep(retry.delay(node_failures))
            node_failures += 1
            continue

        if last == 0:
            last = start_after(highest, latest)
            if last == highest:
                log.info("no checkpoint; resuming after highest stored ledger %d", highest)
            elif highest:
                log.warning("no checkpoint; highest stored ledger %d is %d ledgers behind, so "
                            "starting at the current ledger %d. Fill the hole with: "
                            "python -m xrpl_ingest backfill --start %d --end %d",
                            highest, latest - highest, latest, highest + 1, latest - 1)
            else:
                log.info("no checkpoint and no stored ledgers; starting at ledger %d", latest)

        if latest <= last:
            await sleep(poll_interval)
            continue

        if latest - last > BIG_GAP:
            log.warning("%d ledgers behind; consider `backfill` for the catch-up range", latest - last)

        for idx in range(last + 1, latest + 1):
            if stop_check():
                return last
            res = await ingest_ledger(source, store, idx, retry)
            if not res.ok and not res.recorded:
                if stop_check():  # stopped mid-ledger: checkpoint stays, no backoff first
                    return last
                log.error("ledger %d failed and could not be recorded; holding checkpoint at %d",
                          idx, last)
                await sleep(retry.delay(stalls))
                stalls += 1
                break  # next while-iteration retries from last + 1
            stalls = 0
            if not res.ok:
                log.error("ledger %d recorded as failed; continuing", idx)
            last = idx
            try:
                await asyncio.to_thread(store.set_checkpoint, idx)
            except PyMongoError as e:
                log.warning("checkpoint write failed at %d (%s); will catch up", idx, redact_url(str(e)))
    return last
