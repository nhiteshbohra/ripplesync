import asyncio

import pytest

from conftest import FAST_RETRY, NOW, FakeSource, make_result, no_sleep
from xrpl_ingest.errors import RpcError
from xrpl_ingest.repair import (RepairSummary, describe_problems, ledgers_to_repair, on_node,
                                repair_range, windows)
from xrpl_ingest.transform import parse_ledger_result


def save(store, *indices, n_tx=2):
    for i in indices:
        store.save_ledger_bundle(*parse_ledger_result(i, make_result(i, n_tx), NOW))


def repair(source, store, start, end, node_ranges=((1, 100),), stop=lambda: False, window=10_000,
           concurrency=3):
    lines = []
    s = asyncio.run(repair_range(source, store, start, end, list(node_ranges), retry=FAST_RETRY,
                                 concurrency=concurrency, stop_check=stop, emit=lines.append,
                                 window=window, sleep=no_sleep))
    return s, lines


# -- pure planning ---------------------------------------------------------------------------

def test_windows():
    assert list(windows(1, 10, 4)) == [(1, 4), (5, 8), (9, 10)]
    assert list(windows(5, 5, 4)) == [(5, 5)]
    assert list(windows(6, 5, 4)) == []
    with pytest.raises(ValueError):
        list(windows(1, 2, 0))


def test_ledgers_to_repair_sorted_unique():
    assert ledgers_to_repair([(3, 5), (9, 9)], [(7, 2, 1), (4, 1, 0)], extra=[9, 2]) == \
        [2, 3, 4, 5, 7, 9]
    assert ledgers_to_repair([], []) == []


def test_on_node():
    assert on_node(5, [(1, 3), (5, 9)]) and not on_node(4, [(1, 3), (5, 9)])
    assert not on_node(1, [])


def test_describe_problems_matches_plain_verify():
    assert describe_problems([(3, 4)], [(7, 2, 1)]) == [
        "missing ledgers 3-4", "ledger 7: expected 2 transactions, stored 1"]


def test_summary_ok():
    assert RepairSummary().ok
    assert not RepairSummary(remaining=1).ok and not RepairSummary(stopped=True).ok


# -- repair_range ------------------------------------------------------------------------------

def test_repairs_exactly_the_missing_and_mismatched_ledgers(store):
    save(store, 1, 2, 5, 6, 7, 8, 9, 10)  # 3, 4 missing
    store.txs.delete_one({"block_number": 7})  # 7: tx count mismatch
    store.record_failure(3, "earlier failure")
    src = FakeSource()
    s, lines = repair(src, store, 1, 10)
    assert sorted(src.calls) == [3, 4, 7]
    assert (s.found, s.attempted, s.repaired, s.failed, s.remaining) == (3, 3, 3, 0, 0) and s.ok
    assert "missing ledgers 3-4" in lines and "ledger 7: expected 2 transactions, stored 1" in lines
    assert store.find_gaps(1, 10) == [] and store.tx_count_mismatches(1, 10) == []
    assert store.list_failed() == []  # clear_failure=True
    assert store.meta.count_documents({"kind": "backfill_chunk"}) == 0  # never marks chunks


def test_nothing_to_repair_makes_no_requests(store):
    save(store, *range(1, 6))
    src = FakeSource()
    s, lines = repair(src, store, 1, 5)
    assert src.calls == [] and lines == [] and s.ok and s.found == 0


def test_failed_repair_is_reported_and_recorded(store):
    save(store, 1, 2, 4)
    src = FakeSource({3: [RpcError("lgrNotFound")]})
    s, lines = repair(src, store, 1, 4)
    assert (s.found, s.repaired, s.failed, s.remaining) == (1, 0, 1, 1) and not s.ok
    assert any("ledger 3: repair failed: RpcError" in x for x in lines)
    assert store.list_failed() == [3]


def test_ledgers_the_node_lacks_are_not_attempted(store):
    save(store, 1, 2)
    src = FakeSource()
    s, lines = repair(src, store, 1, 4, node_ranges=[(1, 3)])
    assert src.calls == [3]
    assert (s.found, s.repaired, s.node_lacks, s.remaining) == (2, 1, 1, 1) and not s.ok
    assert any("node lacks 1" in x for x in lines)
    assert store.list_failed() == []  # not polluted with ledgers the node cannot serve


def test_stop_requested_ends_repair_early(store):
    src = FakeSource()
    s, lines = repair(src, store, 1, 30, stop=lambda: True, window=10)
    assert src.calls == [] and s.stopped and not s.ok and s.attempted == 0
    assert "stop requested; repair ended early" in lines
    assert s.found == 10  # only the first window was looked at


def test_stop_midway_keeps_what_was_done(store):
    calls = {"n": 0}

    def stop():
        calls["n"] += 1
        return calls["n"] > 3

    src = FakeSource()
    s, _ = repair(src, store, 1, 10, stop=stop, concurrency=1)
    assert s.stopped and s.repaired == 3 and s.remaining == 7
    assert store.meta.count_documents({"kind": "backfill_chunk"}) == 0


def test_works_window_by_window(store, monkeypatch):
    save(store, 1, 2, 3, 5, 6, 7, 8, 9)  # 4 and 10 missing
    asked = []
    real = store.find_gaps
    monkeypatch.setattr(store, "find_gaps", lambda a, b: asked.append((a, b)) or real(a, b))
    src = FakeSource()
    s, _ = repair(src, store, 1, 10, window=4)
    assert sorted(src.calls) == [4, 10] and s.ok
    # each window verified, and re-verified after a repair (window 5-8 had nothing to repair)
    assert asked == [(1, 4), (1, 4), (5, 8), (9, 10), (9, 10)]
