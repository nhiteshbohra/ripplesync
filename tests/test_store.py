from conftest import NOW, make_result
from xrpl_ingest.store import LAST_LEDGER_ID
from xrpl_ingest.transform import parse_ledger_result


def bundle(idx, n=2):
    return parse_ledger_result(idx, make_result(idx, n), NOW)


def test_indexes_created(store):
    # unique indexes are what make every upsert idempotent; a plain index would allow duplicates
    for coll, name in [(store.txs, "hash_1"), (store.ledgers, "block_number_1"),
                       (store.metas, "transaction_hash_1"), (store.failed, "block_number_1")]:
        assert coll.index_information()[name].get("unique") is True, (coll.name, name)


def test_save_is_idempotent_and_replaces(store):
    store.save_ledger_bundle(*bundle(1))
    store.save_ledger_bundle(*bundle(1))
    assert store.txs.count_documents({}) == 2
    assert store.metas.count_documents({}) == 2
    assert store.ledgers.count_documents({}) == 1


def test_save_empty_ledger(store):
    store.save_ledger_bundle(*bundle(1, 0))
    assert store.ledgers.count_documents({}) == 1 and store.txs.count_documents({}) == 0


def test_checkpoint_roundtrip(store):
    assert store.get_checkpoint() == 0
    store.set_checkpoint(50)
    store.set_checkpoint(51)
    assert store.get_checkpoint() == 51


def test_legacy_string_checkpoint_is_converted_once(store, caplog):
    """A legacy doc with index "100" (a string) sorts above every number in BSON, so $max
    alone would never move it again: tail would restart from 100 forever."""
    store.meta.insert_one({"_id": LAST_LEDGER_ID, "index": "100"})
    assert store.get_checkpoint() == 100
    with caplog.at_level("WARNING", logger="xrpl_ingest.store"):
        store.set_checkpoint(150)
        store.set_checkpoint(160)
    assert store.get_checkpoint() == 160
    assert store.meta.find_one({"_id": LAST_LEDGER_ID})["index"] == 160  # stored as a number
    assert len([m for m in caplog.messages if "not a number" in m]) == 1


def test_legacy_string_checkpoint_ahead_is_kept(store):
    store.meta.insert_one({"_id": LAST_LEDGER_ID, "index": "500"})
    store.set_checkpoint(150)
    assert store.meta.find_one({"_id": LAST_LEDGER_ID})["index"] == 500  # never moves back


def test_highest_ledger(store):
    assert store.highest_ledger() == 0
    for idx in (7, 30, 12):
        store.save_ledger_bundle(*bundle(idx))
    assert store.highest_ledger() == 30


def test_checkpoint_never_moves_back(store):
    store.set_checkpoint(100)
    store.set_checkpoint(50)  # e.g. a second, older tail process
    assert store.get_checkpoint() == 100
    assert store.meta.find_one({"_id": LAST_LEDGER_ID})["updated_at"] is not None


def test_chunk_progress(store):
    store.mark_chunk_done(10, 19, ok=10, failed=0)
    store.mark_chunk_done(20, 29, ok=9, failed=1)
    store.mark_chunk_done(10, 19, ok=10, failed=0)  # re-mark: no duplicate
    assert store.done_chunks(0, 100) == {(10, 19), (20, 29)}
    assert store.done_chunks(15, 100) == {(20, 29)}
    assert store.done_chunks(0, 25) == {(10, 19)}


def test_chunk_docs_do_not_disturb_checkpoint(store):
    store.mark_chunk_done(1, 5, 5, 0)
    assert store.get_checkpoint() == 0


def test_failure_counts_and_clear(store):
    store.record_failure(5, "a")
    store.record_failure(5, "b")
    store.record_failure(3, "c")
    assert store.list_failed() == [3, 5]
    assert store.failed.find_one({"block_number": 5})["retry_count"] == 2
    store.clear_failure(5)
    store.clear_failure(99)  # unknown: no error
    assert store.list_failed() == [3]


def test_find_gaps(store):
    for i in (1, 2, 5, 6, 9):
        store.save_ledger_bundle(*bundle(i, 0))
    assert store.find_gaps(1, 10) == [(3, 4), (7, 8), (10, 10)]
    assert store.find_gaps(1, 2) == []
    assert store.find_gaps(20, 25) == [(20, 25)]


def test_tx_count_mismatch_detected(store):
    store.save_ledger_bundle(*bundle(1, 3))
    store.save_ledger_bundle(*bundle(2, 2))
    store.txs.delete_one({"block_number": 2})  # simulate lost tx
    assert store.tx_count_mismatches(1, 2) == [(2, 2, 1)]
    store.txs.delete_many({"block_number": 1})
    assert store.tx_count_mismatches(1, 2) == [(1, 3, 0), (2, 2, 1)]


def test_stats(store):
    store.save_ledger_bundle(*bundle(1))
    s = store.stats()
    assert s["failed"] == 0 and s["checkpoint"] == 0
