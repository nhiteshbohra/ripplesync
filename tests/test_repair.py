import asyncio

import pytest

from conftest import FAST_RETRY, NOW, FakeSource, make_result, no_sleep
from xrpl_ingest.errors import MalformedLedger, NotValidated, RequestTimeout, RpcError
from xrpl_ingest.repair import (ChainBreak, HashDiff, RepairSummary, describe_diff,
                                describe_problems, describe_tx, diff_hashes, diff_tx_fields,
                                find_chain_breaks, ledgers_to_repair, node_tx_fields,
                                node_tx_hashes, normalize_tx_hash, on_node, repair_range,
                                stored_tx_fields, windows)
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
    assert "stop requested; ended early" in lines
    assert s.found == 0  # stopped before the first window


def test_stop_midway_keeps_what_was_done(store):
    calls = {"n": 0}

    def stop():
        calls["n"] += 1
        return calls["n"] > 4  # 1 check before the window, then one per ledger

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


# -- verify --deep ---------------------------------------------------------------------------

def deep(source, store, start, end, repair=False, node_ranges=((1, 100),), stop=lambda: False,
         window=10_000, concurrency=3):
    lines = []
    ranges = None if node_ranges is None else list(node_ranges)
    s = asyncio.run(repair_range(source, store, start, end, ranges, retry=FAST_RETRY,
                                 concurrency=concurrency, stop_check=stop, emit=lines.append,
                                 window=window, repair=repair, deep=True, sleep=no_sleep))
    return s, lines


def test_find_chain_breaks():
    links = [(1, "A", "Z"), (2, "B", "A"), (3, "C", "X"), (5, "E", "D"), (6, "F", "E")]
    assert find_chain_breaks(links, 1) == [ChainBreak(3, "X", "B")]  # 1: no N-1; 5: 4 absent
    assert find_chain_breaks(links, 4) == []  # only N >= start is judged
    assert find_chain_breaks([], 1) == []


def test_diff_hashes():
    assert diff_hashes(1, ["a", "b"], ["b", "a"]) is None
    assert diff_hashes(1, ["a", "x", "y"], ["a", "c", "b"]) == HashDiff(1, ("b", "c"), ("x", "y"))


def test_node_tx_hashes_validation():
    good = {"ledger": {"ledger_index": 5, "transactions": ["h1", "h2"]}, "validated": True}
    assert node_tx_hashes(5, good) == ["h1", "h2"]
    assert node_tx_hashes(5, {"ledger": {"ledger_index": "5", "transactions": []},
                              "validated": True}) == []
    with pytest.raises(NotValidated):
        node_tx_hashes(5, {**good, "validated": False})
    with pytest.raises(MalformedLedger, match="asked for ledger 6"):
        node_tx_hashes(6, good)
    with pytest.raises(MalformedLedger, match="list of hashes"):
        node_tx_hashes(5, {"ledger": {"ledger_index": 5, "transactions": [{"hash": "h"}]},
                           "validated": True})
    with pytest.raises(MalformedLedger):
        node_tx_hashes(5, {"validated": True})


def test_describe_diff_truncates_long_lists():
    d = HashDiff(9, tuple(f"m{i}" for i in range(7)), ("x",))
    text = describe_diff(d, limit=2)
    assert text == "ledger 9: tx hashes differ from node; 7 missing: m0, m1 (+5 more); 1 extra: x"


def test_deep_clean_range_is_ok(store):
    save(store, *range(1, 6))
    s, lines = deep(FakeSource(), store, 1, 5)
    assert s.ok and lines == [] and s.node_checked


def test_deep_finds_chain_break_offline_without_node(store):
    save(store, *range(1, 6))
    store.ledgers.update_one({"block_number": 3}, {"$set": {"parent_hash": "WRONG"}})
    s, lines = deep(None, store, 1, 5, node_ranges=None)
    assert s.chain_breaks == 1 and s.remaining == 1 and not s.ok and not s.node_checked
    assert lines == ["ledger 3: parent_hash WRONG != block_hash LH2 of ledger 2"]


def test_deep_chain_check_crosses_window_boundaries(store):
    save(store, *range(1, 7))
    store.ledgers.update_one({"block_number": 3}, {"$set": {"parent_hash": "WRONG"}})
    s, lines = deep(None, store, 1, 6, node_ranges=None, window=2)  # windows 1-2, 3-4, 5-6
    assert s.chain_breaks == 1 and len(lines) == 1


def test_deep_finds_tx_hash_mismatch(store):
    save(store, *range(1, 4))
    store.txs.update_one({"hash": "H2_0"}, {"$set": {"hash": "BOGUS"}})  # count still fine
    s, lines = deep(FakeSource(), store, 1, 3)
    assert (s.hash_mismatches, s.found, s.remaining) == (1, 1, 1) and not s.ok
    assert lines == ["ledger 2: tx hashes differ from node; 1 missing: H2_0; 1 extra: BOGUS"]


def test_deep_repair_reingests_hash_mismatch_ledger(store):
    save(store, *range(1, 4))
    store.txs.delete_one({"hash": "H2_1"})
    store.ledgers.update_one({"block_number": 2}, {"$set": {"num_transactions": 1}})  # hide it
    src = FakeSource()
    s, lines = deep(src, store, 1, 3, repair=True)
    assert src.calls == [2] and s.repaired == 1 and s.remaining == 0 and s.ok
    assert store.tx_hashes(2) == {"H2_0", "H2_1"}


def test_plain_repair_does_not_see_hash_mismatches(store):
    save(store, *range(1, 4))
    store.txs.delete_one({"hash": "H2_1"})
    store.ledgers.update_one({"block_number": 2}, {"$set": {"num_transactions": 1}})
    src = FakeSource()
    s, _ = repair(src, store, 1, 3)
    assert src.calls == [] and s.ok  # only --deep compares hashes


def test_deep_repair_never_deletes_extra_tx_docs(store):
    save(store, *range(1, 4))
    store.txs.update_one({"hash": "H2_0"}, {"$set": {"hash": "BOGUS"}})
    s, lines = deep(FakeSource(), store, 1, 3, repair=True)
    assert s.repaired == 1 and s.remaining == 1 and not s.ok  # the extra one is only reported
    assert "BOGUS" in store.tx_hashes(2)
    assert lines[-1].endswith("1 extra: BOGUS (after repair)")


def test_deep_node_error_counts_as_not_compared(store):
    save(store, *range(1, 4))
    src = FakeSource()
    src.hash_script = {2: [RpcError("lgrNotFound")]}
    s, lines = deep(src, store, 1, 3)
    assert s.unchecked == 1 and s.remaining == 1 and not s.ok
    assert "ledger 2: could not compare with node: RpcError: lgrNotFound" in lines


def test_deep_transient_node_error_is_retried(store):
    save(store, *range(1, 4))
    src = FakeSource()
    src.hash_script = {2: [RequestTimeout("slow"), ["H2_0", "H2_1"]]}
    s, _ = deep(src, store, 1, 3)
    assert s.ok and s.unchecked == 0


def test_deep_skips_comparison_for_ledgers_the_node_lacks(store):
    save(store, *range(1, 4))
    src = FakeSource()
    src.hash_script = {3: [RpcError("lgrNotFound")]}  # would fail if asked
    s, _ = deep(src, store, 1, 3, node_ranges=[(1, 2)])
    assert s.ok


def test_deep_stop_during_comparison(store):
    save(store, *range(1, 11))
    calls = {"n": 0}

    def stop():
        calls["n"] += 1
        return calls["n"] > 3

    s, lines = deep(FakeSource(), store, 1, 10, stop=stop, concurrency=1)
    assert s.stopped and not s.ok and "stop requested; ended early" in lines


# -- tx spot check ---------------------------------------------------------------------------

HX = "ab" * 32


def test_normalize_tx_hash():
    assert normalize_tx_hash(f" {HX} ") == HX.upper()
    for bad in ["", "AB" * 31, "AB" * 33, "G" * 64, "0x" + "A" * 62]:
        with pytest.raises(ValueError, match="64-hex-digit"):
            normalize_tx_hash(bad)


V2_NODE_TX = {"tx_json": {"Account": "rA", "Destination": "rB", "DeliverMax": "5", "Fee": "12",
                          "TransactionType": "Payment"},
              "meta": {"TransactionResult": "tesSUCCESS"}, "hash": HX.upper(),
              "ledger_index": 9, "validated": True}


def test_node_tx_fields_v2_and_v1():
    want = {"TransactionType": "Payment", "Account": "rA", "Destination": "rB", "Amount": 5,
            "Fee": 12, "ledger_index": 9, "TransactionResult": "tesSUCCESS"}
    assert node_tx_fields(V2_NODE_TX) == want
    v1 = {"Account": "rA", "Destination": "rB", "Amount": "5", "Fee": "12",
          "TransactionType": "Payment", "ledger_index": 9,
          "meta": {"TransactionResult": "tesSUCCESS"}}
    assert node_tx_fields(v1) == want


def test_stored_tx_fields_and_diff():
    doc = {"TransactionType": "Payment", "from_address": "rA", "to_address": "rB",
           "Amount": 5, "DeliverMax": 5, "Fee": 12, "block_number": 9, "result": "tesSUCCESS"}
    assert diff_tx_fields(stored_tx_fields(doc), node_tx_fields(V2_NODE_TX)) == []
    doc.update(Fee=10, result="tecPATH_DRY")
    assert diff_tx_fields(stored_tx_fields(doc), node_tx_fields(V2_NODE_TX)) == [
        ("Fee", 10, 12), ("TransactionResult", "tecPATH_DRY", "tesSUCCESS")]


def test_stored_v1_amount_matches_node_v2_deliver_max():
    doc = {"TransactionType": "Payment", "from_address": "rA", "to_address": "rB",
           "Amount": 5, "Fee": 12, "block_number": 9, "result": "tesSUCCESS"}  # no DeliverMax
    assert diff_tx_fields(stored_tx_fields(doc), node_tx_fields(V2_NODE_TX)) == []
    assert "Payment rA -> rB amount 5 fee 12" in describe_tx(stored_tx_fields(doc))
