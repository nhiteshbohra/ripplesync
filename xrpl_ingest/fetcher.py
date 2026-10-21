"""Fetch one ledger, convert, store — with retry/backoff and failure recording."""
from __future__ import annotations

import asyncio
import logging
import random
import time
from concurrent.futures import Executor
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from .config import MAX_BACKOFF_EXPONENT, redact_url
from .errors import StopRequested, is_retryable
from .transform import parse_ledger_result

log = logging.getLogger(__name__)

Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 5
    base: float = 1.0
    max_delay: float = 30.0
    jitter: bool = True  # tests that assert exact delays turn it off
    rng: Callable[[], float] = field(default=random.random, repr=False, compare=False)

    def delay(self, attempt: int) -> float:
        """Exponential backoff capped at max_delay, with "equal jitter" (d/2 + random d/2)
        so that many ledgers failing together do not all retry at the same instant."""
        d = min(self.max_delay, self.base * (2 ** min(attempt, MAX_BACKOFF_EXPONENT)))
        return d / 2 + self.rng() * d / 2 if self.jitter else d


@dataclass
class IngestResult:
    block_number: int
    ok: bool
    num_transactions: int = 0
    error: Optional[str] = None
    recorded: bool = True  # False: failed AND missing from the failed collection


async def prepare_ledger(source: Any, index: int, retry: RetryPolicy, *,
                         sleep: Sleep = asyncio.sleep, timings: Optional[dict] = None,
                         executor: Optional[Executor] = None) -> tuple:
    """Fetch and validate/transform, retrying node failures; no database writes yet."""
    for attempt in range(retry.attempts):
        try:
            started = time.perf_counter()
            try:
                result = await source.get_ledger(index)
            finally:
                if timings is not None:
                    timings["fetch_seconds"] = timings.get("fetch_seconds", 0) + time.perf_counter() - started
            started = time.perf_counter()
            try:
                ledger = result.get("ledger") if isinstance(result, dict) else None
                txs = ledger.get("transactions") if isinstance(ledger, dict) else None
                # ponytail: transaction-count cutoff avoids thread overhead on small ledgers;
                # unusually large per-tx metadata needs a byte/CPU-based cutoff instead.
                if isinstance(txs, list) and len(txs) < 64:
                    return parse_ledger_result(index, result)
                # Keep large transformations off the WebSocket reader's event loop.
                return await asyncio.get_running_loop().run_in_executor(executor, parse_ledger_result, index, result)
            finally:
                if timings is not None:
                    timings["transform_seconds"] = timings.get("transform_seconds", 0) + time.perf_counter() - started
        except Exception as e:
            if not is_retryable(e) or attempt == retry.attempts - 1:
                raise
            log.warning("ledger %d: fetch/transform attempt %d/%d failed: %s", index, attempt + 1, retry.attempts, redact_url(str(e)))
            await sleep(retry.delay(attempt))
    raise ValueError("retry.attempts must be positive")


async def persist_bundles(store: Any, bundles: list, retry: RetryPolicy, *,
                          clear_failure: bool = False, sleep: Sleep = asyncio.sleep,
                          timings: Optional[dict] = None) -> None:
    """Retry writes using the same prepared documents, without fetching them again."""
    for attempt in range(retry.attempts):
        started = time.perf_counter()
        try:
            if len(bundles) == 1:
                await asyncio.to_thread(store.save_ledger_bundle, *bundles[0])
            else:
                await asyncio.to_thread(store.save_ledger_bundles, bundles)
            if clear_failure:
                for ledger, _, _ in bundles:
                    await asyncio.to_thread(store.clear_failure, ledger["block_number"])
            return
        except Exception as e:
            if not is_retryable(e) or attempt == retry.attempts - 1:
                raise
            log.warning("saving %d ledger(s): attempt %d/%d failed: %s", len(bundles), attempt + 1, retry.attempts, redact_url(str(e)))
        finally:
            if timings is not None:
                timings["write_seconds"] = timings.get("write_seconds", 0) + time.perf_counter() - started
        await sleep(retry.delay(attempt))
    raise ValueError("retry.attempts must be positive")


async def failed_ledger(store: Any, index: int, error: Exception) -> IngestResult:
    """Preserve failure recording and stop semantics for both ingestion paths."""
    last_error = redact_url(f"{type(error).__name__}: {error}")
    if isinstance(error, StopRequested):
        log.info("ledger %d: not fetched, stop requested", index)
        return IngestResult(index, False, error=last_error, recorded=False)
    log.error("ledger %d: failed: %s", index, last_error)
    try:
        await asyncio.to_thread(store.record_failure, index, last_error)
    except Exception as e:  # noqa: BLE001 - store is down; ledger stays unrecorded
        log.error("ledger %d: could not record failure (%s); rerun backfill to cover it", index, redact_url(str(e)))
        return IngestResult(index, False, error=last_error, recorded=False)
    return IngestResult(index, False, error=last_error)


async def ingest_ledger(source: Any, store: Any, index: int, retry: RetryPolicy, *,
                        clear_failure: bool = False, sleep: Sleep = asyncio.sleep) -> IngestResult:
    """Never raises (except cancellation). Reuses prepared data on database retries."""
    try:
        bundle = await prepare_ledger(source, index, retry, sleep=sleep)
        await persist_bundles(store, [bundle], retry, clear_failure=clear_failure, sleep=sleep)
        return IngestResult(index, True, len(bundle[1]))
    except Exception as e:
        return await failed_ledger(store, index, e)
