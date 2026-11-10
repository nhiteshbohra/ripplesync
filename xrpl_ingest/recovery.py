"""Lease-based worker for durable historical range recovery jobs."""
from __future__ import annotations

import asyncio
import logging
import os
import uuid
from typing import Any, Callable, Optional

from .backfill import ChunkResult, discover_node, process_chunk
from .fetcher import RetryPolicy, Sleep
from .ledger_ranges import intersect, subtract
from .repair import repair_range

log = logging.getLogger(__name__)
LEASE_SECONDS = 300


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
        lease_lost = False
        heartbeat_done = asyncio.Event()

        def should_stop() -> bool:
            return stop_check() or lease_lost

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
                            source, store, (lo, hi), concurrency=1, retry=retry,
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
                            source, store, a, b, node_ranges, retry=retry, concurrency=1,
                            stop_check=should_stop, repair=False, deep=True, sleep=sleep))
                    if missing:
                        status = "blocked"
                        reason = "source complete_ledgers does not include the full range"
                    elif any(not result.ok for result in verified):
                        remaining = sum(result.remaining for result in verified)
                        reason = f"deep verification found {remaining} unresolved issue(s)"
                    else:
                        status = "verified"
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
            retry_after_seconds=delay)
        if not updated:
            log.warning("recovery job %s lease was superseded before completion", job_id)
        else:
            log.info("recovery job %s (%d-%d): %s%s", job_id, start, end, status,
                     f" ({reason})" if reason else "")
        if once:
            return processed
    return processed
