"""verify --repair / --deep: find bad ledgers in a stored range and (optionally) re-ingest them.
Also the pure comparison behind `tx --hash` (stored tx vs node, at the end).

Checks, window by window (VERIFY_WINDOW ledgers, so memory stays bounded for any range):
- always: missing ledger documents (gaps) and stored tx count != num_transactions;
- deep, offline: ledger N's parent_hash == stored block_hash of N-1 (when N-1 is stored);
- deep, with node: stored tx hashes of each ledger == the node's `ledger` hash list
  (transactions, expand=false), one ledger at a time.
Repair re-ingests exactly the missing, count-mismatch and (deep) hash-mismatch ledgers the node
has, through ingest_ledger(clear_failure=True) like retry-failed, then checks the window again.
No chunk is ever marked. Extra stored tx docs are reported, never deleted.
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import (Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set,
                    Tuple)

from .errors import MalformedLedger, NotValidated, StopRequested, is_retryable
from .fetcher import IngestResult, RetryPolicy, Sleep, ingest_ledger
from .ledger_ranges import Range
from .transform import to_int

log = logging.getLogger(__name__)

VERIFY_WINDOW = 10_000
Emit = Callable[[str], None]
Mismatch = Tuple[int, int, int]  # (block_number, expected, stored)
Link = Tuple[int, str, str]  # (block_number, block_hash, parent_hash)


@dataclass(frozen=True)
class ChainBreak:
    block_number: int
    parent_hash: str  # stored in ledger N
    previous_hash: str  # stored block_hash of ledger N-1


@dataclass(frozen=True)
class HashDiff:
    block_number: int
    missing: Tuple[str, ...]  # on the node, not stored
    extra: Tuple[str, ...]  # stored, not on the node


@dataclass
class RepairSummary:
    found: int = 0  # bad ledgers (gap, count or hash mismatch) found before repairing
    attempted: int = 0
    repaired: int = 0
    failed: int = 0
    node_lacks: int = 0  # bad, but outside the node's complete_ledgers: not attempted
    chain_breaks: int = 0
    hash_mismatches: int = 0
    unchecked: int = 0  # deep: the node could not give the hash list (after retries)
    remaining: int = 0  # problems found again after repairing (or found, without repair)
    stopped: bool = False
    node_checked: bool = True  # False: deep run without a reachable node (offline part only)

    @property
    def ok(self) -> bool:
        return not self.remaining and not self.stopped


# -- pure helpers ---------------------------------------------------------------------------

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


def find_chain_breaks(links: Iterable[Link], start: int) -> List[ChainBreak]:
    """Ledgers N >= start whose parent_hash differs from the stored block_hash of N-1.
    `links` sorted by block_number; a ledger whose N-1 is not stored is not judged."""
    breaks: List[ChainBreak] = []
    prev: Optional[Link] = None
    for link in links:
        n, _, parent = link
        if n >= start and prev is not None and prev[0] == n - 1 and prev[1] != parent:
            breaks.append(ChainBreak(n, parent, prev[1]))
        prev = link
    return breaks


def diff_hashes(index: int, stored: Iterable[str], node: Iterable[str]) -> Optional[HashDiff]:
    stored_set, node_set = set(stored), set(node)
    if stored_set == node_set:
        return None
    return HashDiff(index, tuple(sorted(node_set - stored_set)),
                    tuple(sorted(stored_set - node_set)))


def node_tx_hashes(index: int, result: Dict[str, Any]) -> List[str]:
    """Hash list from a `ledger` (transactions, expand=false) result. Raises NotValidated
    (retryable) or MalformedLedger."""
    if result.get("validated") is not True:
        raise NotValidated(f"ledger {index} is not validated")
    ledger = result.get("ledger")
    if not isinstance(ledger, dict):
        raise MalformedLedger(f"ledger {index}: empty or missing 'ledger'")
    got = to_int(ledger.get("ledger_index", result.get("ledger_index")))
    if got != index:
        raise MalformedLedger(f"asked for ledger {index}, node returned {got!r}")
    txs = ledger.get("transactions")
    if not isinstance(txs, list) or not all(isinstance(h, str) and h for h in txs):
        raise MalformedLedger(f"ledger {index}: 'transactions' is not a list of hashes")
    if len(txs) != len(set(txs)):
        raise MalformedLedger(f"ledger {index}: duplicate transaction hash")
    return txs


def describe_breaks(breaks: Iterable[ChainBreak]) -> List[str]:
    return [f"ledger {b.block_number}: parent_hash {b.parent_hash} != block_hash "
            f"{b.previous_hash} of ledger {b.block_number - 1}" for b in breaks]


def describe_diff(d: HashDiff, limit: int = 5) -> str:
    def few(hashes: Tuple[str, ...]) -> str:
        more = f" (+{len(hashes) - limit} more)" if len(hashes) > limit else ""
        return ", ".join(hashes[:limit]) + more
    parts = []
    if d.missing:
        parts.append(f"{len(d.missing)} missing: {few(d.missing)}")
    if d.extra:
        parts.append(f"{len(d.extra)} extra: {few(d.extra)}")
    return f"ledger {d.block_number}: tx hashes differ from node; " + "; ".join(parts)


# -- store / node access --------------------------------------------------------------------

async def find_problems(store: Any, a: int, b: int) -> Tuple[List[Range], List[Mismatch]]:
    gaps = await asyncio.to_thread(store.find_gaps, a, b)
    mismatches = await asyncio.to_thread(store.tx_count_mismatches, a, b)
    return gaps, mismatches


async def chain_breaks(store: Any, a: int, b: int) -> List[ChainBreak]:
    return find_chain_breaks(await asyncio.to_thread(store.ledger_links, a - 1, b), a)


async def fetch_hashes(source: Any, index: int, retry: RetryPolicy,
                       sleep: Sleep = asyncio.sleep) -> List[str]:
    """Node hash list for one ledger, retried like ingest_ledger. Raises the last error."""
    for attempt in range(retry.attempts):
        try:
            return node_tx_hashes(index, await source.get_ledger_hashes(index))
        except Exception as e:  # noqa: BLE001 - classified below
            if isinstance(e, StopRequested) or not is_retryable(e) \
                    or attempt == retry.attempts - 1:
                raise
            await sleep(retry.delay(attempt))
    raise AssertionError("unreachable")  # retry.attempts >= 1


async def compare_hashes(source: Any, store: Any, indices: List[int], *, retry: RetryPolicy,
                         concurrency: int, stop_check: Callable[[], bool],
                         sleep: Sleep = asyncio.sleep
                         ) -> Tuple[List[HashDiff], List[Tuple[int, str]], bool]:
    """Compare stored vs node tx hashes per ledger (one ledger's hashes in memory per worker).
    Returns (diffs, unchecked [(ledger, error)], stopped)."""
    diffs: List[HashDiff] = []
    unchecked: List[Tuple[int, str]] = []
    stopped = False
    it = iter(indices)

    async def worker() -> None:
        nonlocal stopped
        for idx in it:
            if stop_check():
                stopped = True
                return
            try:
                node = await fetch_hashes(source, idx, retry, sleep)
            except StopRequested:
                stopped = True
                return
            except Exception as e:  # noqa: BLE001 - reported per ledger
                unchecked.append((idx, f"{type(e).__name__}: {e}"))
                continue
            stored: Set[str] = await asyncio.to_thread(store.tx_hashes, idx)
            d = diff_hashes(idx, stored, node)
            if d is not None:
                diffs.append(d)

    await asyncio.gather(*(worker() for _ in range(max(1, concurrency))))
    return (sorted(diffs, key=lambda d: d.block_number), sorted(unchecked),
            stopped or stop_check())


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


# -- driver ---------------------------------------------------------------------------------

@dataclass
class _Window:
    gaps: List[Range]
    mismatches: List[Mismatch]
    breaks: List[ChainBreak]
    diffs: List[HashDiff]
    unchecked: List[Tuple[int, str]]
    stopped: bool = False

    def bad_ledgers(self) -> List[int]:
        return ledgers_to_repair(self.gaps, self.mismatches,
                                 [d.block_number for d in self.diffs])

    def count(self) -> int:
        return len(self.bad_ledgers()) + len(self.breaks) + len(self.unchecked)


async def _check_window(source: Any, store: Any, a: int, b: int, *, deep: bool,
                        node_ranges: Optional[Sequence[Range]], only: Optional[List[int]],
                        retry: RetryPolicy, concurrency: int, stop_check: Callable[[], bool],
                        sleep: Sleep) -> _Window:
    """`only`: limit the node hash comparison to these ledgers (re-check after a repair)."""
    gaps, mismatches = await find_problems(store, a, b)
    w = _Window(gaps, mismatches, [], [], [])
    if not deep:
        return w
    w.breaks = await chain_breaks(store, a, b)
    if source is None or node_ranges is None:
        return w
    stored = await asyncio.to_thread(store.ledger_numbers, a, b)
    wanted = set(only) if only is not None else None
    targets = [n for n in stored if on_node(n, node_ranges) and (wanted is None or n in wanted)]
    w.diffs, w.unchecked, w.stopped = await compare_hashes(
        source, store, targets, retry=retry, concurrency=concurrency, stop_check=stop_check,
        sleep=sleep)
    return w


async def repair_range(source: Any, store: Any, start: int, end: int,
                       node_ranges: Optional[Sequence[Range]], *, retry: RetryPolicy,
                       concurrency: int, stop_check: Callable[[], bool],
                       emit: Emit = lambda _line: None, window: int = VERIFY_WINDOW,
                       repair: bool = True, deep: bool = False,
                       sleep: Sleep = asyncio.sleep) -> RepairSummary:
    """Check [start, end] window by window; with `repair`, re-ingest the bad ledgers the node
    has and check the window again. `source`/`node_ranges` None: offline checks only (deep
    hash comparison skipped; repair impossible). `emit` receives the report lines."""
    s = RepairSummary(node_checked=source is not None and node_ranges is not None)
    kw = dict(retry=retry, concurrency=concurrency, stop_check=stop_check, sleep=sleep)
    for a, b in windows(start, end, window):
        if stop_check():
            s.stopped = True
            break
        w = await _check_window(source, store, a, b, deep=deep, node_ranges=node_ranges,
                                only=None, **kw)
        lines = describe_problems(w.gaps, w.mismatches) + describe_breaks(w.breaks)
        lines += [describe_diff(d) for d in w.diffs]
        lines += [f"ledger {n}: could not compare with node: {err}" for n, err in w.unchecked]
        for line in lines:
            emit(line)
        todo = w.bad_ledgers()
        s.found += len(todo)
        s.chain_breaks += len(w.breaks)
        s.hash_mismatches += len(w.diffs)
        s.unchecked += len(w.unchecked)
        stopped = w.stopped
        if repair and todo and not stopped and source is not None and node_ranges is not None:
            stopped = await _repair_window(source, store, a, b, w, todo, node_ranges, s, emit,
                                           deep=deep, **kw)
        else:
            s.remaining += w.count()
        if stopped:
            s.stopped = True
            break
    if s.stopped:
        emit("stop requested; ended early")
    log.info("verify %d-%d (repair=%s deep=%s): %s", start, end, repair, deep, s)
    return s


async def _repair_window(source: Any, store: Any, a: int, b: int, w: _Window, todo: List[int],
                         node_ranges: Sequence[Range], s: RepairSummary, emit: Emit, *,
                         deep: bool, retry: RetryPolicy, concurrency: int,
                         stop_check: Callable[[], bool], sleep: Sleep) -> bool:
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
    # Recheck every repair target, including gaps/count mismatches that had no hash diff.
    again = await _check_window(source, store, a, b, deep=deep, node_ranges=node_ranges,
                                only=todo, retry=retry,
                                concurrency=concurrency, stop_check=stop_check, sleep=sleep)
    s.hash_mismatches += len({d.block_number for d in again.diffs} -
                             {d.block_number for d in w.diffs})
    s.unchecked += len({n for n, _ in again.unchecked} - {n for n, _ in w.unchecked})
    again.unchecked += [u for u in w.unchecked if u[0] not in {x[0] for x in again.unchecked}]
    lines = describe_problems(again.gaps, again.mismatches) + describe_breaks(again.breaks)
    lines += [describe_diff(d) for d in again.diffs]
    lines += [f"ledger {n}: could not compare with node: {err}" for n, err in again.unchecked]
    for line in lines:
        emit(line + " (after repair)")
    s.remaining += again.count()
    return stopped or again.stopped or stop_check()


# -- tx spot check (`tx --hash H`): stored tx doc vs the node's `tx` result -------------------

# Amount = Amount, else DeliverMax (API v2 Payments have only DeliverMax), on both sides
TX_KEY_FIELDS = ("TransactionType", "Account", "Destination", "Amount", "Fee", "ledger_index",
                 "TransactionResult")
_TX_HASH_RE = re.compile(r"^[0-9A-Fa-f]{64}$")


def normalize_tx_hash(text: str) -> str:
    """64 hex digits, returned upper-case (as xrpld writes hashes). Else ValueError."""
    t = text.strip()
    if not _TX_HASH_RE.match(t):
        raise ValueError(f"expected a 64-hex-digit transaction hash, got {text!r}")
    return t.upper()


def _norm(value: Any) -> Any:
    return to_int(value) if isinstance(value, str) else value


def stored_tx_fields(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Key fields of a stored tx doc, under the node's field names."""
    return {k: _norm(v) for k, v in {
        "TransactionType": doc.get("TransactionType"), "Account": doc.get("from_address"),
        "Destination": doc.get("to_address"),
        "Amount": doc.get("Amount", doc.get("DeliverMax")), "Fee": doc.get("Fee"),
        "ledger_index": doc.get("block_number"), "TransactionResult": doc.get("result"),
    }.items()}


def node_tx_fields(result: Dict[str, Any]) -> Dict[str, Any]:
    """Key fields of a `tx` result: API v2 (tx_json + meta) or v1 (flat + meta)."""
    body = result.get("tx_json") if isinstance(result.get("tx_json"), dict) else result
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    return {k: _norm(v) for k, v in {
        "TransactionType": body.get("TransactionType"), "Account": body.get("Account"),
        "Destination": body.get("Destination"),
        "Amount": body.get("Amount", body.get("DeliverMax")), "Fee": body.get("Fee"),
        "ledger_index": result.get("ledger_index", body.get("ledger_index")),
        "TransactionResult": meta.get("TransactionResult"),
    }.items()}


def diff_tx_fields(stored: Dict[str, Any], node: Dict[str, Any]) -> List[Tuple[str, Any, Any]]:
    return [(k, stored.get(k), node.get(k)) for k in TX_KEY_FIELDS
            if stored.get(k) != node.get(k)]


def describe_tx(f: Dict[str, Any]) -> str:
    return (f"ledger {f['ledger_index']} {f['TransactionType']} {f['Account']} -> "
            f"{f['Destination']} amount {f['Amount']!r} fee {f['Fee']} "
            f"result {f['TransactionResult']}")
