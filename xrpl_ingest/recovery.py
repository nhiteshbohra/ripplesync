"""Lease-based worker for durable historical range recovery jobs."""
from __future__ import annotations

import asyncio
import logging
import os
import uuid
from typing import Any, Callable, Optional

from .backfill import ChunkResult, discover_node, process_chunk
from .errors import StopRequested
from .fetcher import RetryPolicy, Sleep
from .ledger_ranges import intersect, subtract
from .repair import repair_range

log = logging.getLogger(__name__)
LEASE_SECONDS = 300


class _FencedStore:
    """Reject recovery-worker writes after their database lease has been superseded."""

    _WRITE_METHODS = {
        "clear_failure", "mark_ledgers_verified", "record_failure",
        "record_ledger_attempt", "save_ledger_bundle", "save_ledger_bundles",
        "schedule_recovery_range",
    }

    def __init__(self, store: Any, job_id: str, token: int,
                 lease_lost: Callable[[], None]):
        self._store = store
        self._job_id = job_id
        self._token = token
        self._lease_lost = lease_lost

    def __getattr__(self, name: str) -> Any:
        method = getattr(self._store, name)
        if name not in self._WRITE_METHODS:
            return method

        def fenced_write(*args: Any, **kwargs: Any) -> Any:
            if not self._store.recovery_lease_valid(self._job_id, self._token):
                self._lease_lost()
                raise StopRequested("recovery lease was lost before a database write")
            return method(*args, **kwargs)

        return fenced_write


async def recover_jobs(source: Any, store: Any, retry: RetryPolicy, *, chunk_size: int,
                       stop_check: Callable[[], bool], poll_interval: float = 30,
                       once: bool = False, sleep: Sleep = asyncio.sleep) -> int:
    """Claim, ingest, and deeply verify due range jobs until stopped or `once` is set."""
    worker_id = f"{os.getpid()}-{uuid.uuid4().hex}"
    processed = 0
    while not stop_check():
        job = await asyncio.to_thread(
            store.claim_recovery_job, worker_id, lease_seconds=LEASE_SECONDS)
        if job is None:
            if once:
                return processed
            await sleep(poll_interval)
            continue
        processed += 1
        job_id = job["_id"]
        token = job["fencing_token"]
        start, end = job["start"], job["end"]
        retry_delay = min(3600, retry.delay(min(job.get("attempts", 1), 12)))
        status = "pending"
        reason: Optional[str] = None
        verification: Optional[dict] = None
        lease_lost = False
        heartbeat_done = asyncio.Event()

        def should_stop() -> bool:
            return stop_check() or lease_lost

        def note_lease_lost() -> None:
            nonlocal lease_lost
            lease_lost = True

        fenced_store = _FencedStore(store, job_id, token, note_lease_lost)

        async def heartbeat() -> None:
            nonlocal lease_lost
            while not heartbeat_done.is_set():
                try:
                    await asyncio.wait_for(
                        heartbeat_done.wait(), timeout=LEASE_SECONDS / 3)
                except asyncio.TimeoutError:
                    try:
                        renewed = await asyncio.to_thread(
                            store.renew_recovery_lease, job_id, token,
                            lease_seconds=LEASE_SECONDS)
                    except Exception:
                        log.exception("could not renew recovery job %s lease", job_id)
                        lease_lost = True
                        return
                    if not renewed:
                        lease_lost = True
                        return

        heartbeat_task = asyncio.create_task(heartbeat())
        try:
            node_ranges, _ = await discover_node(source)
            available = intersect(node_ranges, start, end)
            missing = subtract(start, end, node_ranges)
            if not available:
                status = "blocked"
                reason = "source complete_ledgers does not include this range"
            else:
                failed = False
                stopped = False
                for a, b in available:
                    for lo in range(a, b + 1, chunk_size):
                        if should_stop():
                            stopped = True
                            break
                        hi = min(b, lo + chunk_size - 1)
                        result: ChunkResult = await process_chunk(
                            source, fenced_store, (lo, hi), concurrency=1, retry=retry,
                            stop_check=should_stop, sleep=sleep)
                        failed = failed or result.failed > 0 or result.unrecorded > 0
                        stopped = stopped or not result.completed
                    if stopped:
                        break
                if stopped:
                    reason = "recovery stopped before the range completed"
                elif failed:
                    reason = "one or more ledger ingestions failed"
                else:
                    verified = []
                    for a, b in available:
                        verified.append(await repair_range(
                            source, fenced_store, a, b, node_ranges, retry=retry, concurrency=1,
                            stop_check=should_stop, repair=False, deep=True, sleep=sleep))
                    if missing:
                        status = "blocked"
                        reason = "source complete_ledgers does not include the full range"
                    elif any(not result.ok or not result.node_checked for result in verified):
                        remaining = sum(result.remaining for result in verified)
                        reason = f"deep verification found {remaining} unresolved issue(s)"
                    else:
                        status = "verified"
                        verification = {
                            "start": start, "end": end, "checked_ranges": available,
                            "node_checked": True, "remaining": 0, "stopped": False,
                        }
        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
            log.exception("recovery job %s failed", job_id)

        heartbeat_done.set()
        await heartbeat_task
        if lease_lost:
            log.warning("recovery job %s lease was lost; leaving it for the next worker",
                        job_id)
            if once:
                return processed
            continue
        was_stopped = should_stop()
        if was_stopped:
            status = "pending"
            reason = reason or "stop requested"
        if status != "verified":
            delay = 300 if status == "blocked" else retry_delay
        else:
            delay = 0
        updated = await asyncio.to_thread(
            store.finish_recovery_job, job_id, token, status, reason=reason,
            retry_after_seconds=delay, verification=verification)
        if not updated:
            log.warning("recovery job %s lease was superseded before completion", job_id)
        else:
            log.info("recovery job %s (%d-%d): %s%s", job_id, start, end, status,
                     f" ({reason})" if reason else "")
        if once:
            return processed
    return processed
