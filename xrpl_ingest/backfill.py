"""Historical backfill: chunked, resumable, multi-process; plus failed-ledger retry."""
from __future__ import annotations

import asyncio
import logging
import multiprocessing
import re
import time
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, ThreadPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from itertools import islice
from typing import Any, Callable, List, Optional, Set, Tuple

from pymongo.errors import PyMongoError

from .config import Config, redact_url
from .config import FIRST_LEDGER
from .support.monitor import DiskGuard
from .errors import IngestError, TransientError
from .fetcher import RetryPolicy, Sleep, failed_ledger, ingest_ledger, persist_bundles, prepare_ledger
from .ledger_ranges import Range, parse_complete_ledgers, plan_chunks
from .support.logsetup import (ChunkProgress, setup_logging, setup_worker_logging,
                       start_queue_listener, stop_queue_listener)
from .source import WsSource
from .support.runcontrol import StopFlag, any_of, ignore_sigint, install_stop_signal, stop_signalled
from .store import MongoStore

log = logging.getLogger(__name__)

COMMAND = "backfill"  # stop-file key and lock name
POLL_SECONDS = 1.0  # the parent re-checks stop file / signals / disk this often
WRITE_BATCH_LEDGERS = 16
WRITE_BATCH_DOCUMENTS = 2000
WRITE_WORKERS = 16  # capped by CONCURRENCY; preserve the existing default write parallelism


@dataclass
class ChunkResult:
    chunk: Range
    ok: int = 0
    failed: int = 0
    unrecorded: int = 0  # failures not in the failed collection; chunk is then not marked done
    completed: bool = True  # False when stopped early; such chunks are not marked done
    timings: dict = field(default_factory=dict)  # cumulative stage wall time, not additive elapsed time
    marker_failed: bool = False  # data saved, but the chunk completion write failed


def retry_policy(cfg: Config) -> RetryPolicy:
    return RetryPolicy(cfg.max_retries, cfg.reconnect_delay_base, cfg.backoff_max)


async def process_chunk(source: Any, store: Any, chunk: Range, *, concurrency: int,
                        retry: RetryPolicy, stop_check: Callable[[], bool],
                        sleep: Sleep = asyncio.sleep) -> ChunkResult:
    res = ChunkResult(chunk)
    indices = iter(range(chunk[0], chunk[1] + 1))  # shared; next() has no await so it is race-free
    # ponytail: bounds queued ledger count, not bytes; add a byte budget if large responses
    # dominate RAM. Each producer holds at most one additional ledger; no full-chunk buffer.
    ready: asyncio.Queue = asyncio.Queue(maxsize=max(1, min(concurrency, 8)))
    writers = max(1, min(concurrency, WRITE_WORKERS))
    # CPU parallelism comes from the existing process pool. One transform thread per
    # process keeps the socket reader responsive without competing with MongoDB threads.
    transforms = ThreadPoolExecutor(max_workers=1, thread_name_prefix="xrpl-transform")

    def count(r) -> None:
        if r.ok:
            res.ok += 1
        else:
            res.failed += 1
            if not r.recorded:
                res.unrecorded += 1

    async def producer() -> None:
        for idx in indices:
            if stop_check():
                res.completed = False
                return
            try:
                bundle = await prepare_ledger(source, idx, retry, sleep=sleep, timings=res.timings,
                                              executor=transforms)
            except Exception as e:
                count(await failed_ledger(store, idx, e))
                continue
            await ready.put(bundle)

    async def write(batch: list) -> None:
        if len(batch) > 1:
            try:
                await persist_bundles(store, batch, retry, sleep=sleep, timings=res.timings)
                res.ok += len(batch)
                return
            except Exception as e:
                log.warning("batch save failed (%s); isolating %d ledger(s)", redact_url(str(e)), len(batch))
        for bundle in batch:
            idx = bundle[0]["block_number"]
            try:
                await persist_bundles(store, [bundle], retry, sleep=sleep, timings=res.timings)
                res.ok += 1
            except Exception as e:
                count(await failed_ledger(store, idx, e))

    async def writer() -> None:
        finished = False
        while not finished:
            item = await ready.get()
            if item is None:
                return
            batch = [item]
            documents = 1 + len(item[1]) + len(item[2])
            # Drain available work without waiting to fill a batch (also flushes the tail).
            # A single large ledger can exceed the soft document cap; PyMongo splits commands.
            while len(batch) < WRITE_BATCH_LEDGERS and documents < WRITE_BATCH_DOCUMENTS:
                try:
                    item = ready.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if item is None:
                    finished = True
                    break
                batch.append(item)
                documents += 1 + len(item[1]) + len(item[2])
            await write(batch)

    producers = [asyncio.create_task(producer()) for _ in range(max(1, concurrency))]

    async def finish_producers() -> None:
        await asyncio.gather(*producers)
        for _ in range(writers):
            await ready.put(None)

    consumers = [asyncio.create_task(writer()) for _ in range(writers)]
    tasks = producers + [asyncio.create_task(finish_producers())] + consumers
    try:
        await asyncio.gather(*tasks[len(producers):])
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.to_thread(transforms.shutdown, wait=True, cancel_futures=True)
    if res.unrecorded:
        log.error("chunk %d-%d: %d failed ledgers could not be recorded; chunk left unmarked so "
                  "the next backfill redoes it", chunk[0], chunk[1], res.unrecorded)
    elif res.completed:
        try:
            await asyncio.to_thread(store.mark_chunk_done, chunk[0], chunk[1], res.ok, res.failed)
        except PyMongoError as e:
            res.marker_failed = True
            log.error("chunk %d-%d: could not mark done (%s); the next backfill redoes it",
                      chunk[0], chunk[1], redact_url(str(e)))
    return res


# -- process-pool entry (module-level so it pickles under spawn) ------------------

_store_cache: Optional[MongoStore] = None
_worker_stop_event: Any = None  # multiprocessing.Event shared with the parent (initializer)
_worker_ready = False  # _init_worker ran in this process


def _init_worker(stop_event: Any, log_queue: Any, log_level: str) -> None:
    """ProcessPoolExecutor initializer (once per worker process)."""
    global _worker_stop_event, _worker_ready
    ignore_sigint()  # Ctrl+C hits the whole console group; the parent decides and sets the event
    install_stop_signal()  # SIGTERM (systemd stops the whole cgroup) / SIGBREAK: stop the chunk
    if log_queue is not None:
        setup_worker_logging(log_queue, log_level)  # records go to the parent's handlers
    else:
        setup_logging(log_level)
    _worker_stop_event = stop_event
    _worker_ready = True


def _run_chunk_in_process(args: Tuple[Config, Range]) -> ChunkResult:
    global _store_cache
    cfg, chunk = args
    if _store_cache is None:  # first chunk in this process
        if not _worker_ready:  # called without the pool initializer (tests)
            setup_logging(cfg.log_level)  # spawn children inherit no logging config
            install_stop_signal()  # systemd may SIGTERM the whole group: stop the chunk cleanly
        _store_cache = MongoStore.from_config(cfg)

    stop = StopFlag(cfg.process_status_file, COMMAND, event=_worker_stop_event)

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
                 source: Any = None, store: Any = None, inline: bool = False,
                 stop_check: Optional[Callable[[], bool]] = None) -> dict:
    """Backfill [start, end]. `inline=True` runs chunks in this process using the given
    source/store (tests only; an injected source must not be bound to one event loop);
    otherwise a spawn-based process pool is used. `stop_check` defaults to the stop file
    (`backfill` / `current_process` keys) and signals; the MIN_FREE_GB guard is added."""
    if start < FIRST_LEDGER:
        raise ValueError(f"start must be >= {FIRST_LEDGER} (first XRPL ledger)")
    own_store = store is None
    store = store or MongoStore.from_config(cfg)
    file_stop = stop_check or StopFlag(cfg.process_status_file, COMMAND)
    disk = DiskGuard(store, cfg.min_free_gb)
    stop = any_of(file_stop, disk)
    own_source = source is None
    source = source or WsSource.from_config(cfg, stop_check=file_stop)
    t0 = time.time()
    try:
        disk.check_now()  # LowDiskSpace (exit 1) before anything starts
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
        progress = ChunkProgress(len(done), chunks)

        results: List[ChunkResult] = []
        lost = 0  # chunks whose worker process died; never ran to completion
        if inline:
            async def run_all():
                for c in chunks:
                    r = await process_chunk(source, store, c, concurrency=cfg.concurrency,
                                            retry=retry_policy(cfg), stop_check=stop)
                    results.append(r)
                    _log_chunk(r, t0, progress)
                    if not r.completed:
                        break
            asyncio.run(run_all())
        else:
            lost = _run_pool(cfg, chunks, results, t0, stop_check=stop, progress=progress)

        summary = {
            "chunks_run": len(results),
            "chunks_incomplete": sum(1 for r in results if not r.completed) + lost,
            "chunks_unmarked": sum(bool(r.unrecorded or r.marker_failed) for r in results),
            "ledgers_ok": sum(r.ok for r in results),
            "ledgers_failed": sum(r.failed for r in results),
            "unavailable_ranges": unavailable,
            "seconds": round(time.time() - t0, 1),
            "low_disk": disk.tripped,
        }
        log.info("backfill summary: %s", summary)
        return summary
    finally:
        if own_store:
            store.close()


def _run_pool(cfg: Config, chunks: List[Range], results: List[ChunkResult], t0: float, *,
              stop_check: Callable[[], bool] = stop_signalled,
              progress: Optional[ChunkProgress] = None) -> int:
    """Run chunks in a spawn process pool, appending to `results`. Returns chunks lost.

    At most one chunk per process is in flight. The parent checks `stop_check` (stop file,
    signals, disk guard) every POLL_SECONDS; on stop it sets the shared event, so workers
    finish the ledgers in flight and return their chunk unmarked, and starts nothing new.
    Every chunk without a completed result then counts as unfinished (exit code 1).
    A worker that dies (OOM kill, os._exit) breaks the pool: we stop, and every chunk
    without a result is lost; none of those were marked done, so a rerun redoes them.
    A KeyboardInterrupt (second Ctrl+C) terminates the workers and is re-raised.
    """
    todo = iter(chunks)
    ctx = multiprocessing.get_context("spawn")
    stop_event = ctx.Event()
    log_queue = ctx.Queue()
    listener = start_queue_listener(log_queue)
    stopping = False
    pool = ProcessPoolExecutor(max_workers=cfg.num_processes, mp_context=ctx,
                               initializer=_init_worker,
                               initargs=(stop_event, log_queue, cfg.log_level))
    clean = False  # True: every worker is healthy, so a normal (waiting) shutdown is safe
    try:
        def submit(n: int) -> Set[Future]:
            return {pool.submit(_run_chunk_in_process, (cfg, c)) for c in islice(todo, n)}

        def check_stop() -> bool:
            nonlocal stopping
            if not stopping and (stop_signalled() or stop_check()):
                stopping = True
                stop_event.set()
                log.warning("backfill stopping: in-flight chunks finish their current "
                            "ledgers, no new chunk starts")
            return stopping

        pending = set() if check_stop() else submit(cfg.num_processes)
        try:
            while pending:
                done, pending = wait(pending, timeout=POLL_SECONDS,
                                     return_when=FIRST_COMPLETED)
                broken: Optional[BrokenProcessPool] = None
                for fut in done:  # collect every finished chunk before reacting to a dead worker
                    try:
                        r = fut.result()
                    except BrokenProcessPool as e:
                        broken = e
                        continue
                    results.append(r)
                    _log_chunk(r, t0, progress)
                    if not r.completed and not stopping:
                        stopping = True
                        stop_event.set()
                if broken is not None:
                    raise broken
                if pending and not done:
                    # Our own check: on Linux a killed worker has been seen to leave its
                    # future pending forever instead of breaking the pool.
                    dead = _dead_worker(pool)
                    if dead is not None:
                        raise BrokenProcessPool(f"worker process {dead[0]} exited with code {dead[1]}")
                if not check_stop() and done:
                    pending |= submit(len(done))
            clean = True
        except BrokenProcessPool as e:
            lost = len(chunks) - len(results)
            log.error("a backfill worker process died (%s); stopping with %d chunks unfinished "
                      "(not marked done; rerun backfill to redo them)", e, lost)
            return lost
        except KeyboardInterrupt:
            log.warning("aborting backfill now: terminating worker processes (unfinished "
                        "chunks are not marked done; rerun backfill to redo them)")
            raise
    finally:
        if clean:
            pool.shutdown(wait=True)
        else:  # a dead or interrupted worker: never wait for a pool that may not finish
            _terminate(pool)
        stop_queue_listener(listener)
        log_queue.close()
        log_queue.cancel_join_thread()  # exit even if a dead worker left the queue locked
    # stopped: in-flight chunks may all have completed, but the never-started ones still
    # count as unfinished, so the exit code says "stopped early"
    return len(chunks) - len(results) if stopping else 0


def _dead_worker(pool: ProcessPoolExecutor) -> Optional[Tuple[int, int]]:
    """(pid, exit code) of a pool worker that has exited, else None (private API, best effort).

    Pool workers only exit at shutdown, so any exit code while chunks are pending means the
    worker died (OOM kill, os._exit, crash)."""
    for proc in list((getattr(pool, "_processes", None) or {}).values()):
        try:
            code = proc.exitcode
        except (OSError, AttributeError, ValueError):
            continue
        if code is not None:
            return proc.pid, code
    return None


def _terminate(pool: ProcessPoolExecutor) -> None:
    """Hard stop: cancel queued chunks and kill the workers (private API, best effort)."""
    procs = list((getattr(pool, "_processes", None) or {}).values())
    pool.shutdown(wait=False, cancel_futures=True)
    for proc in procs:
        try:
            proc.terminate()
        except (OSError, AttributeError, ValueError):
            pass


def _log_chunk(r: ChunkResult, t0: float, progress: Optional[ChunkProgress] = None) -> None:
    tail = ""
    if progress is not None and r.completed and not r.unrecorded and not r.marker_failed:
        tail = "; " + progress.update(r.chunk)
    log.info("chunk %d-%d: ok=%d failed=%d unrecorded=%d completed=%s marker_failed=%s (elapsed %.0fs)%s",
             r.chunk[0], r.chunk[1], r.ok, r.failed, r.unrecorded, r.completed, r.marker_failed,
             time.time() - t0, tail)
    log.info("chunk %d-%d stage totals (overlapping seconds): fetch=%.3f transform=%.3f write=%.3f",
             *r.chunk, r.timings.get("fetch_seconds", 0), r.timings.get("transform_seconds", 0),
             r.timings.get("write_seconds", 0))


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
        total = await asyncio.to_thread(store.count_failed)
        if not total:
            return 0
        log.info("retry round %d/%d: %d failed ledgers", rnd, max_rounds, total)
        page_size = max(1000, concurrency * 4)
        for suspects, workers in ((False, concurrency), (True, 1)):
            after = -1
            while not stop_check():
                page = await asyncio.to_thread(store.failed_page, after, page_size)
                if not page:
                    break
                after = page[-1][0]
                indices = [n for n, reason in page
                           if may_have_killed_neighbours(reason) is suspects]
                await _reingest(source, store, indices, retry, workers, stop_check, sleep)
        if stop_check():
            break
        if await asyncio.to_thread(store.count_failed) and rnd < max_rounds:
            await sleep(retry.delay(rnd - 1))
    return await asyncio.to_thread(store.count_failed)
