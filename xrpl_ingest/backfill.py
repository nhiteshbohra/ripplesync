"""Historical backfill: chunked, resumable, multi-process; plus failed-ledger retry."""
from __future__ import annotations

import asyncio
import logging
import multiprocessing
import time
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple

from .config import Config
from .constants import FIRST_LEDGER
from .fetcher import RetryPolicy, Sleep, ingest_ledger
from .ledger_ranges import Range, parse_complete_ledgers, plan_chunks
from .source import WsSource
from .stopflag import stop_requested
from .store import MongoStore

log = logging.getLogger(__name__)


@dataclass
class ChunkResult:
    chunk: Range
    ok: int = 0
    failed: int = 0
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

    await asyncio.gather(*(worker() for _ in range(max(1, concurrency))))
    if res.completed:
        await asyncio.to_thread(store.mark_chunk_done, chunk[0], chunk[1], res.ok, res.failed)
    return res


# -- process-pool entry (module-level so it pickles under spawn) ------------------

_store_cache: Optional[MongoStore] = None


def _run_chunk_in_process(args: Tuple[Config, Range]) -> ChunkResult:
    global _store_cache
    cfg, chunk = args
    if _store_cache is None:
        _store_cache = MongoStore.from_config(cfg)

    async def go() -> ChunkResult:
        source = WsSource.from_config(cfg)
        try:
            return await process_chunk(
                source, _store_cache, chunk, concurrency=cfg.concurrency, retry=retry_policy(cfg),
                stop_check=lambda: stop_requested(cfg.process_status_file))
        finally:
            await source.close()

    return asyncio.run(go())


async def discover_node(source: Any) -> Tuple[List[Range], int]:
    info = await source.server_info()
    ranges = parse_complete_ledgers(info.get("complete_ledgers", ""))
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
    source = source or WsSource.from_config(cfg)
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

        end = latest if end is None else end
        if end < start:
            raise ValueError(f"end ({end}) < start ({start}); node latest validated is {latest}")
        done = store.done_chunks(start, end)
        chunks, unavailable = plan_chunks(start, end, node_ranges, cfg.chunk_size, done)
        for a, b in unavailable:
            log.warning("node does not have ledgers %d-%d (skipped; check complete_ledgers)", a, b)
        log.info("backfill %d-%d: %d chunks to run (%d already done)", start, end, len(chunks), len(done))

        results: List[ChunkResult] = []
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
            ctx = multiprocessing.get_context("spawn")
            with ctx.Pool(cfg.num_processes) as pool:
                for r in pool.imap_unordered(_run_chunk_in_process, [(cfg, c) for c in chunks]):
                    results.append(r)
                    _log_chunk(r, t0)

        summary = {
            "chunks_run": len(results),
            "chunks_incomplete": sum(1 for r in results if not r.completed),
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


def _log_chunk(r: ChunkResult, t0: float) -> None:
    log.info("chunk %d-%d: ok=%d failed=%d completed=%s (elapsed %.0fs)",
             r.chunk[0], r.chunk[1], r.ok, r.failed, r.completed, time.time() - t0)


async def retry_failed(source: Any, store: Any, retry: RetryPolicy, *, max_rounds: int = 5,
                       concurrency: int = 4, stop_check: Callable[[], bool] = lambda: False,
                       sleep: Sleep = asyncio.sleep) -> int:
    """Re-ingest ledgers in the failed collection. Returns how many remain."""
    for rnd in range(1, max_rounds + 1):
        failed = await asyncio.to_thread(store.list_failed)
        if not failed:
            return 0
        log.info("retry round %d/%d: %d failed ledgers", rnd, max_rounds, len(failed))
        it = iter(failed)

        async def worker() -> None:
            for idx in it:
                if stop_check():
                    return
                await ingest_ledger(source, store, idx, retry, clear_failure=True, sleep=sleep)

        await asyncio.gather(*(worker() for _ in range(max(1, concurrency))))
        if stop_check():
            break
        if await asyncio.to_thread(store.count_failed) and rnd < max_rounds:
            await sleep(retry.delay(rnd - 1))
    return await asyncio.to_thread(store.count_failed)
