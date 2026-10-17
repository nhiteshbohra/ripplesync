"""Historical backfill: chunked, resumable, multi-process; plus failed-ledger retry."""
from __future__ import annotations

import asyncio
import logging
import multiprocessing
import re
import time
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from itertools import islice
from typing import Any, Callable, List, Optional, Set, Tuple

from pymongo.errors import PyMongoError

from .config import Config, setup_logging
from .constants import FIRST_LEDGER
from .errors import IngestError, TransientError
from .fetcher import RetryPolicy, Sleep, ingest_ledger
from .ledger_ranges import Range, parse_complete_ledgers, plan_chunks
from .source import WsSource
from .stopflag import install_stop_signal, stop_requested, stop_signalled
from .store import MongoStore

log = logging.getLogger(__name__)


@dataclass
class ChunkResult:
    chunk: Range
    ok: int = 0
    failed: int = 0
    unrecorded: int = 0  # failures not in the failed collection; chunk is then not marked done
    completed: bool = True  # False when stopped early; such chunks are not marked done


def retry_policy(cfg: Config) -> RetryPolicy:
    return RetryPolicy(cfg.max_retries, cfg.reconnect_delay_base, cfg.backoff_max)


async def process_chunk(source: Any, store: Any, chunk: Range, *, concurrency: int,
                        retry: RetryPolicy, stop_check: Callable[[], bool],
                        sleep: Sleep = asyncio.sleep) -> ChunkResult:
    res = ChunkResult(chunk)
    indices = iter(range(chunk[0], chunk[1] + 1))  # shared; next() has no await so it is race-free

    async def worker() -> None:
        for idx in indices:
            if stop_check():
                res.completed = False
                return
            r = await ingest_ledger(source, store, idx, retry, sleep=sleep)
            if r.ok:
                res.ok += 1
            else:
                res.failed += 1
                if not r.recorded:
                    res.unrecorded += 1

    await asyncio.gather(*(worker() for _ in range(max(1, concurrency))))
    if res.unrecorded:
        log.error("chunk %d-%d: %d failed ledgers could not be recorded; chunk left unmarked so "
                  "the next backfill redoes it", chunk[0], chunk[1], res.unrecorded)
    elif res.completed:
        try:
            await asyncio.to_thread(store.mark_chunk_done, chunk[0], chunk[1], res.ok, res.failed)
        except PyMongoError as e:
            log.error("chunk %d-%d: could not mark done (%s); the next backfill redoes it",
                      chunk[0], chunk[1], e)
    return res


# -- process-pool entry (module-level so it pickles under spawn) ------------------

_store_cache: Optional[MongoStore] = None


def _run_chunk_in_process(args: Tuple[Config, Range]) -> ChunkResult:
    global _store_cache
    cfg, chunk = args
    if _store_cache is None:  # first chunk in this process
        setup_logging(cfg.log_level)  # spawn children inherit no logging config
        install_stop_signal()  # systemd may SIGTERM the whole group: stop the chunk cleanly
        _store_cache = MongoStore.from_config(cfg)

    def stop() -> bool:
        return stop_requested(cfg.process_status_file)

    async def go() -> ChunkResult:
        source = WsSource.from_config(cfg, stop_check=stop)
        try:
            return await process_chunk(
                source, _store_cache, chunk, concurrency=cfg.concurrency, retry=retry_policy(cfg),
                stop_check=stop)
        finally:
            await source.close()

    return asyncio.run(go())


async def discover_node(source: Any) -> Tuple[List[Range], int]:
    info = await source.server_info()
    try:
        ranges = parse_complete_ledgers(info.get("complete_ledgers", ""))
    except ValueError as e:  # a node problem, not a bad argument
        raise IngestError(f"node returned malformed complete_ledgers: {e}") from None
    latest = info.get("validated_ledger", {}).get("seq") or 0
    return ranges, int(latest)


def run_backfill(cfg: Config, start: int, end: Optional[int] = None, *,
                 source: Any = None, store: Any = None, inline: bool = False) -> dict:
    """Backfill [start, end]. `inline=True` runs chunks in this process using the given
    source/store (tests only; an injected source must not be bound to one event loop);
    otherwise a spawn-based process pool is used."""
    if start < FIRST_LEDGER:
        raise ValueError(f"start must be >= {FIRST_LEDGER} (first XRPL ledger)")
    own_store = store is None
    store = store or MongoStore.from_config(cfg)
    own_source = source is None
    source = source or WsSource.from_config(
        cfg, stop_check=lambda: stop_requested(cfg.process_status_file))
    t0 = time.time()
    try:
        store.ensure_indexes()

        async def discover():
            try:
                return await discover_node(source)
            finally:
                if own_source:
                    await source.close()
        node_ranges, latest = asyncio.run(discover())

        if end is None and latest <= 0:
            raise TransientError("node has no validated ledger yet (still syncing?); "
                                 "wait, or pass --end")
        end = latest if end is None else end
        if end < start:
            raise ValueError(f"end ({end}) < start ({start}); node latest validated is {latest}")
        done = store.done_chunks(start, end)
        chunks, unavailable = plan_chunks(start, end, node_ranges, cfg.chunk_size, done)
        for a, b in unavailable:
            log.warning("node does not have ledgers %d-%d (skipped; check complete_ledgers)", a, b)
        log.info("backfill %d-%d: %d chunks to run (%d already done)", start, end, len(chunks), len(done))

        results: List[ChunkResult] = []
        lost = 0  # chunks whose worker process died; never ran to completion
        if inline:
            async def run_all():
                for c in chunks:
                    r = await process_chunk(source, store, c, concurrency=cfg.concurrency,
                                            retry=retry_policy(cfg),
                                            stop_check=lambda: stop_requested(cfg.process_status_file))
                    results.append(r)
                    _log_chunk(r, t0)
                    if not r.completed:
                        break
            asyncio.run(run_all())
        else:
            lost = _run_pool(cfg, chunks, results, t0)

        summary = {
            "chunks_run": len(results),
            "chunks_incomplete": sum(1 for r in results if not r.completed) + lost,
            "chunks_unmarked": sum(1 for r in results if r.unrecorded),
            "ledgers_ok": sum(r.ok for r in results),
            "ledgers_failed": sum(r.failed for r in results),
            "unavailable_ranges": unavailable,
            "seconds": round(time.time() - t0, 1),
        }
        log.info("backfill summary: %s", summary)
        return summary
    finally:
        if own_store:
            store.close()


def _run_pool(cfg: Config, chunks: List[Range], results: List[ChunkResult], t0: float) -> int:
    """Run chunks in a spawn process pool, appending to `results`. Returns chunks lost.

    At most one chunk per process is in flight, so after a stopped chunk (stop flag) no
    new chunk is started. A worker that dies (OOM kill, os._exit) breaks the pool: we
    stop, and every chunk without a result is lost; none of those were marked done, so
    a rerun redoes them.
    """
    todo = iter(chunks)
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=cfg.num_processes, mp_context=ctx) as pool:
        def submit(n: int) -> Set[Future]:
            return {pool.submit(_run_chunk_in_process, (cfg, c)) for c in islice(todo, n)}

        pending = submit(cfg.num_processes)
        stopping = False
        try:
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                broken: Optional[BrokenProcessPool] = None
                for fut in done:  # collect every finished chunk before reacting to a dead worker
                    try:
                        r = fut.result()
                    except BrokenProcessPool as e:
                        broken = e
                        continue
                    results.append(r)
                    _log_chunk(r, t0)
                    stopping = stopping or not r.completed
                if broken is not None:
                    raise broken
                stopping = stopping or stop_signalled()  # SIGTERM to this (parent) process
                if not stopping:
                    pending |= submit(len(done))
        except BrokenProcessPool as e:
            lost = len(chunks) - len(results)
            log.error("a backfill worker process died (%s); stopping with %d chunks unfinished "
                      "(not marked done; rerun backfill to redo them)", e, lost)
            return lost
    # after SIGTERM the in-flight chunks may all have completed; the never-started ones
    # still count as unfinished, so the exit code says "stopped early" like the stop file
    return len(chunks) - len(results) if stop_signalled() else 0


def _log_chunk(r: ChunkResult, t0: float) -> None:
    log.info("chunk %d-%d: ok=%d failed=%d unrecorded=%d completed=%s (elapsed %.0fs)",
             r.chunk[0], r.chunk[1], r.ok, r.failed, r.unrecorded, r.completed, time.time() - t0)


_CLOSE_1009 = re.compile(r"\b1009 \(message too big\)")  # websockets close-code text


def may_have_killed_neighbours(reason: str) -> bool:
    """A lost socket (e.g. websockets 1009 message too big) fails every request on it.
    Matches the close text exactly, not any '1009' (ledger numbers contain it too)."""
    return reason.startswith("ConnectionLost") or bool(_CLOSE_1009.search(reason))


async def _reingest(source: Any, store: Any, indices: List[int], retry: RetryPolicy,
                    concurrency: int, stop_check: Callable[[], bool], sleep: Sleep) -> None:
    it = iter(indices)  # shared; next() has no await so it is race-free

    async def worker() -> None:
        for idx in it:
            if stop_check():
                return
            await ingest_ledger(source, store, idx, retry, clear_failure=True, sleep=sleep)

    await asyncio.gather(*(worker() for _ in range(max(1, concurrency))))


async def retry_failed(source: Any, store: Any, retry: RetryPolicy, *, max_rounds: int = 5,
                       concurrency: int = 4, stop_check: Callable[[], bool] = lambda: False,
                       sleep: Sleep = asyncio.sleep) -> int:
    """Re-ingest ledgers in the failed collection. Returns how many remain.

    Ledgers whose failure was a lost connection go last, one at a time: an oversized
    ledger kills the shared socket, so retrying it next to its neighbours would make them
    fail again every round. Alone, the neighbours recover and only the poison one remains.
    """
    for rnd in range(1, max_rounds + 1):
        failed = await asyncio.to_thread(store.list_failed_with_reasons)
        if not failed:
            return 0
        alone = [n for n, reason in failed if may_have_killed_neighbours(reason)]
        together = [n for n, reason in failed if not may_have_killed_neighbours(reason)]
        log.info("retry round %d/%d: %d failed ledgers (%d one at a time after the others)",
                 rnd, max_rounds, len(failed), len(alone))
        for indices, workers in ((together, concurrency), (alone, 1)):
            await _reingest(source, store, indices, retry, workers, stop_check, sleep)
        if stop_check():
            break
        if await asyncio.to_thread(store.count_failed) and rnd < max_rounds:
            await sleep(retry.delay(rnd - 1))
    return await asyncio.to_thread(store.count_failed)
