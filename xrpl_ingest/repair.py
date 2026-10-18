"""verify --repair: find bad ledgers in a stored range and re-ingest exactly those.

Bad = missing ledger document (gap) or stored tx count != num_transactions. Work goes window
by window (VERIFY_WINDOW ledgers) so memory stays bounded for any range. Repaired ledgers
go through ingest_ledger(clear_failure=True) like retry-failed; no chunk is ever marked.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, List, Sequence, Tuple

from .fetcher import IngestResult, RetryPolicy, Sleep, ingest_ledger
from .ledger_ranges import Range

log = logging.getLogger(__name__)

VERIFY_WINDOW = 10_000
Emit = Callable[[str], None]
Mismatch = Tuple[int, int, int]  # (block_number, expected, stored)


@dataclass
class RepairSummary:
    found: int = 0  # bad ledgers found before repairing
    attempted: int = 0
    repaired: int = 0
    failed: int = 0
    node_lacks: int = 0  # bad, but outside the node's complete_ledgers: not attempted
    remaining: int = 0  # bad ledgers found again after repairing
    stopped: bool = False

    @property
    def ok(self) -> bool:
        return not self.remaining and not self.stopped


def windows(start: int, end: int, size: int = VERIFY_WINDOW) -> Iterator[Range]:
    if size < 1:
        raise ValueError("window size must be >= 1")
    for a in range(start, end + 1, size):
        yield a, min(end, a + size - 1)


def ledgers_to_repair(gaps: Iterable[Range], mismatches: Iterable[Mismatch],
                      extra: Iterable[int] = ()) -> List[int]:
    """Sorted unique ledger numbers: every ledger of every gap, every mismatch, `extra`."""
    todo = {n for a, b in gaps for n in range(a, b + 1)}
    todo.update(m[0] for m in mismatches)
    todo.update(extra)
    return sorted(todo)


def on_node(index: int, node_ranges: Sequence[Range]) -> bool:
    return any(a <= index <= b for a, b in node_ranges)


def describe_problems(gaps: Iterable[Range], mismatches: Iterable[Mismatch]) -> List[str]:
    """Same lines as plain `verify`."""
    lines = [f"missing ledgers {a}-{b}" for a, b in gaps]
    lines += [f"ledger {n}: expected {exp} transactions, stored {got}"
              for n, exp, got in mismatches]
    return lines


async def find_problems(store: Any, a: int, b: int) -> Tuple[List[Range], List[Mismatch]]:
    gaps = await asyncio.to_thread(store.find_gaps, a, b)
    mismatches = await asyncio.to_thread(store.tx_count_mismatches, a, b)
    return gaps, mismatches


async def ingest_many(source: Any, store: Any, indices: List[int], *, retry: RetryPolicy,
                      concurrency: int, stop_check: Callable[[], bool],
                      sleep: Sleep = asyncio.sleep) -> Tuple[List[IngestResult], bool]:
    """Re-ingest `indices` (clear_failure=True). Returns (results, stopped)."""
    results: List[IngestResult] = []
    stopped = False
    it = iter(indices)  # shared; next() has no await so it is race-free

    async def worker() -> None:
        nonlocal stopped
        for idx in it:
            if stop_check():
                stopped = True
                return
            results.append(await ingest_ledger(source, store, idx, retry, clear_failure=True,
                                               sleep=sleep))

    await asyncio.gather(*(worker() for _ in range(max(1, concurrency))))
    return results, stopped or stop_check()


async def repair_range(source: Any, store: Any, start: int, end: int,
                       node_ranges: Sequence[Range], *, retry: RetryPolicy, concurrency: int,
                       stop_check: Callable[[], bool], emit: Emit = lambda _line: None,
                       window: int = VERIFY_WINDOW,
                       sleep: Sleep = asyncio.sleep) -> RepairSummary:
    """Verify [start, end] window by window, re-ingest the bad ledgers the node has, then
    verify each window again. `emit` receives the report lines."""
    s = RepairSummary()
    for a, b in windows(start, end, window):
        gaps, mismatches = await find_problems(store, a, b)
        todo = ledgers_to_repair(gaps, mismatches)
        if not todo:
            continue
        for line in describe_problems(gaps, mismatches):
            emit(line)
        s.found += len(todo)
        available = [n for n in todo if on_node(n, node_ranges)]
        lacking = len(todo) - len(available)
        if lacking:
            s.node_lacks += lacking
            emit(f"node lacks {lacking} of them in {a}-{b} (see its complete_ledgers); "
                 "not attempted")
        emit(f"repairing {len(available)} ledger(s) in {a}-{b}")
        results, stopped = await ingest_many(source, store, available, retry=retry,
                                             concurrency=concurrency, stop_check=stop_check,
                                             sleep=sleep)
        s.attempted += len(results)
        s.repaired += sum(1 for r in results if r.ok)
        for r in results:
            if not r.ok:
                s.failed += 1
                emit(f"ledger {r.block_number}: repair failed: {r.error}")
        gaps, mismatches = await find_problems(store, a, b)
        s.remaining += len(ledgers_to_repair(gaps, mismatches))
        if stopped:
            s.stopped = True
            emit("stop requested; repair ended early")
            break
    log.info("repair %d-%d: %s", start, end, s)
    return s
