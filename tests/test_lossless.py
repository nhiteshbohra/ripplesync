"""Every value the node sends must be recoverable from what we store.

Mapping (documented in transform.py): a ledger's header fields and result-level fields go
to the ledger doc; each transaction's outer fields and tx_json fields go to its tx doc; its
meta is stored whole in the meta doc. Mapped fields keep their names (FIELD_RENAMES /
LEDGER_RENAMES) and int coercion; everything else is stored as received under its own name,
or under raw_<key> when that name already holds a different value. Keys are escaped
('.' -> '．', NUL -> '␀', leading '$' -> '＄').
"""
import copy

import mongomock
import pytest

from conftest import NOW
from xrpl_ingest.transform import (FIELD_RENAMES, LEDGER_RENAMES, escape_key, escape_keys,
                                   merge_field, parse_ledger_result, same_value, to_int,
                                   unescape_key)

INDEX = 95_000_000
ALICE = "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"
BOB = "rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe"
ISSUER = "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"


def modified_root(account, balance, previous):
    return {"ModifiedNode": {"LedgerEntryType": "AccountRoot", "LedgerIndex": "A" * 64,
                             "FinalFields": {"Account": account, "Balance": balance,
                                             "Flags": 0, "OwnerCount": 1, "Sequence": 7},
                             "PreviousFields": {"Balance": previous},
                             "PreviousTxnID": "B" * 64, "PreviousTxnLgrSeq": INDEX - 3}}


def v2_ledger_result():
    """A realistic API v2 `ledger` (transactions, expand) result, plus fields we do not map."""
    payment = {
        "hash": "1" * 64,
        "tx_json": {"Account": ALICE, "Destination": BOB, "DeliverMax": "2500000",
                    "DestinationTag": 42, "Fee": "12", "Flags": 0, "Sequence": 10,
                    "LastLedgerSequence": INDEX + 4, "SigningPubKey": "ED" + "0" * 64,
                    "TransactionType": "Payment", "TxnSignature": "F" * 128},
        "meta": {"AffectedNodes": [modified_root(ALICE, "97499988", "100000000"),
                                   modified_root(BOB, "2500000", "0")],
                 "TransactionIndex": 0, "TransactionResult": "tesSUCCESS",
                 "delivered_amount": "2500000"},
    }
    offer = {
        "hash": "2" * 64,
        "tx_json": {"Account": BOB, "Fee": "15", "Flags": 524288, "Sequence": 3,
                    "OfferSequence": 2, "TransactionType": "OfferCreate",
                    "TakerGets": {"currency": "USD", "issuer": ISSUER, "value": "1.5"},
                    "TakerPays": "3000000",
                    "Expiration": 800000100},
        "meta": {"AffectedNodes": [{"CreatedNode": {
                     "LedgerEntryType": "Offer", "LedgerIndex": "C" * 64,
                     "NewFields": {"Account": BOB, "BookDirectory": "D" * 64,
                                   "TakerGets": {"currency": "USD", "issuer": ISSUER,
                                                 "value": "1.5"},
                                   "TakerPays": "3000000", "Sequence": 3}}}],
                 "TransactionIndex": 1, "TransactionResult": "tesSUCCESS"},
    }
    memo = {
        "hash": "3" * 64,
        "tx_json": {"Account": ALICE, "Fee": "10", "Sequence": 11,
                    "TransactionType": "AccountSet",
                    "Memos": [{"Memo": {"MemoData": "48656C6C6F", "MemoType": "746578742F"}},
                              {"Memo": {"MemoFormat": "6A736F6E"}}]},
        "meta": {"AffectedNodes": [modified_root(ALICE, "97499978", "97499988")],
                 "TransactionIndex": 2, "TransactionResult": "tesSUCCESS"},
    }
    future = {  # an amendment we have never heard of, with awkward key names
        "hash": "4" * 64,
        "ledger_index": INDEX,              # same as block_number: nothing new
        "ledger_hash": "E" * 64,            # differs from our block_hash -> raw_ledger_hash
        "close_time_iso": "2025-05-09T12:00:00Z",  # differs from ours -> raw_close_time_iso
        "validated": True,
        "_id": "not-a-mongo-id",
        "tx_json": {"Account": ALICE, "Fee": "99999999999999999999",  # > int64: kept as str
                    "Sequence": 12, "TransactionType": "FutureThing",
                    "NewField.With.Dots": {"$inner": 1, "a.b": [1, {"$x": None}]},
                    "$dollar": "top", "Flags": 1, "status": "theirs",
                    "BigCount": "1" * 30, "Ratio": 0.5, "Enabled": False},
        "meta": {"AffectedNodes": [], "TransactionIndex": 3, "TransactionResult": "tecNO_DST",
                 "FutureMeta": {"k.e.y": "v"}},
    }
    return {
        "ledger": {
            "account_hash": "AH" * 32, "close_flags": 0, "close_time": 800000000,
            "close_time_human": "2025-May-09 12:00:00.000000000 UTC",
            "close_time_iso": "2025-05-09T12:00:00Z", "close_time_resolution": 10,
            "closed": True, "ledger_hash": "LH" * 32, "ledger_index": INDEX,
            "parent_close_time": 799999990, "parent_hash": "PH" * 32,
            "total_coins": "99986000000000000", "transaction_hash": "TH" * 32,
            "future_header": {"nested": [1, {"x.y": "dotted"}], "$k": [], "empty": {}},
            "transactions": [payment, offer, memo, future],
        },
        "ledger_hash": "LH" * 32, "ledger_index": INDEX, "validated": True,
        "result_level_future": {"a": [True, 0, None]},
    }


def leaves(obj, path=()):
    """(path, value) for every leaf; empty dicts/lists are leaves themselves."""
    if isinstance(obj, dict) and obj:
        for k, v in obj.items():
            yield from leaves(v, path + (k,))
    elif isinstance(obj, list) and obj:
        for i, v in enumerate(obj):
            yield from leaves(v, path + (i,))
    else:
        yield path, obj


def walk(value, path):
    for p in path:
        if isinstance(p, int):
            if not isinstance(value, list) or p >= len(value):
                return _MISSING
            value = value[p]
        else:
            if not isinstance(value, dict) or escape_key(p) not in value:
                return _MISSING
            value = value[escape_key(p)]
    return value


_MISSING = object()


def found_in(doc, path, value, renames):
    """True when `value` (at `path` of the raw input) is in `doc` per the documented mapping."""
    key, rest = path[0], path[1:]
    candidates = [escape_key(renames.get(key, key))]
    candidates += [r * n + escape_key(key) for r in ("raw_",) for n in range(1, 4)]
    for name in candidates:
        if name in doc:
            got = walk(doc[name], rest)
            if got is not _MISSING and same_value(got, escape_keys(value)):
                return True
    return False


def recoverable(path, value, ledger_doc, tx_docs, meta_docs):
    if path[:2] == ("ledger", "transactions"):
        i, rest = path[2], path[3:]
        if rest[0] in ("meta", "metaData"):
            got = walk(meta_docs[i]["meta"], rest[1:])
            return got is not _MISSING and got == escape_keys(value)
        if rest[0] == "tx_json":
            rest = rest[1:]
        return found_in(tx_docs[i], rest, value, FIELD_RENAMES)
    if path[0] == "ledger":
        return found_in(ledger_doc, path[1:], value, LEDGER_RENAMES)
    return found_in(ledger_doc, path, value, LEDGER_RENAMES)


def lost_leaves(raw):
    ledger_doc, tx_docs, meta_docs = parse_ledger_result(INDEX, raw, NOW)
    return [(p, v) for p, v in leaves(raw)
            if not recoverable(p, v, ledger_doc, tx_docs, meta_docs)]


def test_every_leaf_of_a_v2_ledger_result_is_stored():
    raw = v2_ledger_result()
    assert len(list(leaves(raw))) > 100  # the fixture really is rich
    assert lost_leaves(raw) == []


def test_lossless_check_detects_a_dropped_field():
    """The checker itself works: a value that is nowhere in the docs is reported."""
    raw = v2_ledger_result()
    ledger_doc, tx_docs, meta_docs = parse_ledger_result(INDEX, raw, NOW)
    del ledger_doc["close_flags"]
    assert not recoverable(("ledger", "close_flags"), 0, ledger_doc, tx_docs, meta_docs)


def test_unmapped_header_and_result_fields_stored_as_received():
    ledger_doc, _, _ = parse_ledger_result(INDEX, v2_ledger_result(), NOW)
    assert ledger_doc["close_flags"] == 0 and ledger_doc["closed"] is True
    assert ledger_doc["close_time_resolution"] == 10
    assert ledger_doc["parent_close_time"] == 799999990
    assert ledger_doc["validated"] is True
    assert ledger_doc["result_level_future"] == {"a": [True, 0, None]}
    assert ledger_doc["future_header"] == {"nested": [1, {"x．y": "dotted"}], "＄k": [],
                                           "empty": {}}
    assert "transactions" not in ledger_doc


def test_existing_ledger_fields_unchanged_and_collision_goes_to_raw():
    ledger_doc, _, _ = parse_ledger_result(INDEX, v2_ledger_result(), NOW)
    assert ledger_doc["block_number"] == INDEX and ledger_doc["block_hash"] == "LH" * 32
    assert ledger_doc["total_coins"] == 99986000000000000
    assert ledger_doc["close_time_iso"] == "2025-05-08T06:13:20+00:00"  # ours, from close_time
    assert ledger_doc["raw_close_time_iso"] == "2025-05-09T12:00:00Z"  # the node's
    # identical values are not duplicated
    assert not {"ledger_index", "ledger_hash", "raw_ledger_index", "raw_ledger_hash",
                "raw_parent_hash", "raw_total_coins"} & set(ledger_doc)


def test_tx_collisions_go_to_raw_and_mapped_fields_win():
    _, txs, _ = parse_ledger_result(INDEX, v2_ledger_result(), NOW)
    doc = txs[3]
    assert doc["block_hash"] == "LH" * 32 and doc["raw_ledger_hash"] == "E" * 64
    assert doc["block_number"] == INDEX and "raw_ledger_index" not in doc
    assert doc["raw_close_time_iso"] == "2025-05-09T12:00:00Z"
    assert doc["status"] == "failed" and doc["raw_status"] == "theirs"
    assert doc["validated"] is True
    assert "_id" not in doc and doc["raw__id"] == "not-a-mongo-id"


def test_values_kept_lossless_no_float_created():
    _, txs, _ = parse_ledger_result(INDEX, v2_ledger_result(), NOW)
    doc = txs[3]
    assert doc["Fee"] == "99999999999999999999"  # > int64: original string
    assert doc["BigCount"] == "1" * 30  # unmapped: as received, no coercion
    assert doc["Ratio"] == 0.5  # a float only when the node sent one
    assert doc["Enabled"] is False


def test_keys_escaped_reversibly():
    _, txs, metas = parse_ledger_result(INDEX, v2_ledger_result(), NOW)
    doc = txs[3]
    assert doc["NewField．With．Dots"] == {"＄inner": 1, "a．b": [1, {"＄x": None}]}
    assert doc["＄dollar"] == "top"
    assert metas[3]["meta"]["FutureMeta"] == {"k．e．y": "v"}
    for key in ("a.b", "$x", "$a.$b", "x\x00y", "plain", "mid$dle", ""):
        assert unescape_key(escape_key(key)) == key


def test_meta_stored_whole_in_meta_doc():
    raw = v2_ledger_result()
    _, txs, metas = parse_ledger_result(INDEX, raw, NOW)
    for tx, meta_doc in zip(raw["ledger"]["transactions"], metas, strict=True):
        assert meta_doc["meta"] == escape_keys(tx["meta"])
        assert meta_doc["transaction_hash"] == tx["hash"]
    assert metas[2]["memos"] == raw["ledger"]["transactions"][2]["tx_json"]["Memos"]


def test_input_not_mutated():
    raw = v2_ledger_result()
    before = copy.deepcopy(raw)
    parse_ledger_result(INDEX, raw, NOW)
    assert raw == before


def test_docs_with_awkward_keys_can_be_written(store):
    """'$'-prefixed top-level keys made pymongo reject the whole ledger."""
    store.save_ledger_bundle(*parse_ledger_result(INDEX, v2_ledger_result(), NOW))
    assert store.txs.find_one({"hash": "4" * 64})["＄dollar"] == "top"
    assert store.ledgers.find_one({"block_number": INDEX})["validated"] is True
    assert store.metas.count_documents({}) == 4


def test_second_meta_and_non_object_tx_json_are_kept():
    tx = {"hash": "H", "meta": {"TransactionResult": "tesSUCCESS"},
          "metaData": {"TransactionResult": "tecX"}, "tx_json": "BINARY"}
    raw = {"ledger": {"ledger_index": 5, "ledger_hash": "L", "parent_hash": "P",
                      "account_hash": "A", "transaction_hash": "T", "transactions": [tx]},
           "validated": True}
    _, txs, metas = parse_ledger_result(5, raw, NOW)
    assert metas[0]["meta"] == {"TransactionResult": "tesSUCCESS"}
    assert txs[0]["metaData"] == {"TransactionResult": "tecX"} and txs[0]["tx_json"] == "BINARY"


def test_empty_meta_falls_back_to_metadata():
    tx = {"hash": "H", "meta": {}, "metaData": {"TransactionResult": "tesSUCCESS"}}
    raw = {"ledger": {"ledger_index": 5, "ledger_hash": "L", "parent_hash": "P",
                      "account_hash": "A", "transaction_hash": "T", "transactions": [tx]},
           "validated": True}
    _, txs, metas = parse_ledger_result(5, raw, NOW)
    assert metas[0]["meta"] == {"TransactionResult": "tesSUCCESS"}
    assert txs[0]["status"] == "success" and txs[0]["meta"] == {}


@pytest.mark.parametrize("stored,received,same", [
    (12, "12", True), ("a", "a", True), (12, "13", False), ("12", 12, False),
    (True, 1, False), (1, True, False), (1, 1.0, False), (0.5, 0.5, True), (None, None, True),
])
def test_same_value(stored, received, same):
    assert same_value(stored, received) is same


def test_merge_field_rules():
    doc = {"block_number": 5, "raw_x": "taken"}
    merge_field(doc, "ledger_index", "5", LEDGER_RENAMES)  # same as mapped: dropped as dup
    merge_field(doc, "ledger_index", 6, LEDGER_RENAMES)
    merge_field(doc, "ledger_index", 7, LEDGER_RENAMES)
    merge_field(doc, "x", "new")  # raw_x is taken by something else
    merge_field(doc, "x", "newer")
    merge_field(doc, "y.z", 1)
    assert doc == {"block_number": 5, "raw_ledger_index": 6, "raw_raw_ledger_index": 7,
                   "raw_x": "taken", "x": "new", "raw_raw_x": "newer", "y．z": 1}


def test_mongomock_accepts_escaped_docs():
    coll = mongomock.MongoClient().db.c
    coll.replace_one({"k": 1}, {"k": 1, **escape_keys({"$a": {"b.c": 1}})}, upsert=True)
    assert coll.find_one({"k": 1})["＄a"] == {"b．c": 1}


def test_to_int_never_makes_floats():
    for v in ("1.5", "1e3", "12", "-0", 10**30):
        assert not isinstance(to_int(v), float)
