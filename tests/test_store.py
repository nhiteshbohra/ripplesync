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


def test_removed_indexes_stay_removed_after_restart(store, caplog):
    from xrpl_ingest.remove_indexes import REMOVED_INDEXES, remove_indexes
    from xrpl_ingest.store import MongoStore

    db = store.ledgers.database
    store.save_ledger_bundle(*bundle(1))
    # Include an unexpected collection and protect a similar, unrequested name.
    db.extra.insert_one({"block_number": 1})
    for name in REMOVED_INDEXES:
        db.extra.create_index("retired_field", name=name)
    store.ledgers.create_index("close_time")
    store.txs.create_index("from_address")
    db.extra.create_index("keep_me")
    documents = {n: list(db[n].find()) for n in db.list_collection_names()}
    indexes = {n: db[n].index_information() for n in db.list_collection_names()}
    with caplog.at_level("INFO", logger="xrpl_ingest.remove_indexes"):
        removed = remove_indexes(db)
    assert len(removed) == 8
    for collection, name in removed:
        assert f"Removed index {db.name}.{collection}.{name}" in caplog.text
    assert remove_indexes(db) == []  # safe to run again
    restarted = MongoStore(store.client, store.cfg)
    restarted.ensure_indexes()
    for n, before in indexes.items():
        expected = {k: v for k, v in before.items() if k not in REMOVED_INDEXES}
        assert db[n].index_information() == expected
        assert list(db[n].find()) == documents[n]
    restarted.save_ledger_bundle(*bundle(1))
    assert store.txs.count_documents({}) == 2


def test_new_tx_fields_roundtrip_and_query(store):
    from bson.decimal128 import Decimal128
    a, b = "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh", "rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe"
    result = make_result(1, 1)
    tx = result["ledger"]["transactions"][0]
    tx["tx_json"].update(Account=a, Destination=b)
    tx["meta"]["delivered_amount"] = "1000000"
    store.save_ledger_bundle(*parse_ledger_result(1, result, NOW))
    doc = store.txs.find_one({"affected_accounts": b})
    assert doc["affected_accounts"] == sorted([a, b])
    assert doc["delivered_xrp"] == Decimal128("1.000000") and doc["delivered_amount"] == 1000000


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


def test_deep_verify_queries(store):
    for i in (3, 1, 2, 5):
        store.save_ledger_bundle(*bundle(i))
    assert store.ledger_numbers(1, 4) == [1, 2, 3] and store.ledger_numbers(6, 9) == []
    assert store.ledger_links(2, 5) == [(2, "LH2", "LH1"), (3, "LH3", "LH2"), (5, "LH5", "LH4")]
    assert store.tx_hashes(2) == {"H2_0", "H2_1"} and store.tx_hashes(4) == set()


def test_find_tx(store):
    store.save_ledger_bundle(*bundle(4))
    doc = store.find_tx("H4_1")
    assert doc["block_number"] == 4 and "_id" not in doc
    assert store.find_tx("nope") is None


def test_stats(store):
    store.save_ledger_bundle(*bundle(1))
    s = store.stats()
    assert s["failed"] == 0 and s["checkpoint"] == 0
