"""Fetch one ledger, convert, store — with retry/backoff and failure recording."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from .constants import MAX_BACKOFF_EXPONENT
from .errors import is_retryable
from .transform import parse_ledger_result

log = logging.getLogger(__name__)

Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 5
    base: float = 1.0
    max_delay: float = 30.0

    def delay(self, attempt: int) -> float:
        return min(self.max_delay, self.base * (2 ** min(attempt, MAX_BACKOFF_EXPONENT)))


@dataclass
class IngestResult:
    block_number: int
    ok: bool
    num_transactions: int = 0
    error: Optional[str] = None
    recorded: bool = True  # False: failed AND missing from the failed collection


async def ingest_ledger(source: Any, store: Any, index: int, retry: RetryPolicy, *,
                        clear_failure: bool = False, sleep: Sleep = asyncio.sleep) -> IngestResult:
    """Never raises (except cancellation). Permanent failure => recorded in the failed collection."""
    last_error = "unknown"
    for attempt in range(retry.attempts):
        try:
            result = await source.get_ledger(index)
            ledger_doc, tx_docs, meta_docs = parse_ledger_result(index, result)
            await asyncio.to_thread(store.save_ledger_bundle, ledger_doc, tx_docs, meta_docs)
            if clear_failure:
                await asyncio.to_thread(store.clear_failure, index)
            return IngestResult(index, True, len(tx_docs))
        except Exception as e:  # noqa: BLE001 - classified below
            last_error = f"{type(e).__name__}: {e}"
            if not is_retryable(e):
                log.error("ledger %d: permanent failure: %s", index, last_error)
                break
            log.warning("ledger %d: attempt %d/%d failed: %s", index, attempt + 1, retry.attempts, last_error)
            if attempt < retry.attempts - 1:
                await sleep(retry.delay(attempt))

    try:
        await asyncio.to_thread(store.record_failure, index, last_error)
    except Exception as e:  # noqa: BLE001 - store is down; ledger stays unrecorded
        log.error("ledger %d: could not record failure (%s); rerun backfill to cover it", index, e)
        return IngestResult(index, False, error=last_error, recorded=False)
    return IngestResult(index, False, error=last_error)
