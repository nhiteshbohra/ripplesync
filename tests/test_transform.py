from decimal import Decimal

import pytest
from bson.decimal128 import Decimal128

from conftest import NOW, make_result, make_tx
from xrpl_ingest.errors import MalformedLedger, NotValidated
from xrpl_ingest.transform import (close_time_iso, extract_affected_accounts,
                                   flatten_transactions, parse_ledger_result, readable_amount,
                                   to_int)


def test_v2_transaction_flattened_and_renamed():
    ledger_doc, txs, metas = parse_ledger_result(100, make_result(100, 1), NOW)
    tx = txs[0]
    assert tx["from_address"] == "rA0" and tx["to_address"] == "rB0"
    assert "Account" not in tx and "Destination" not in tx
    assert tx["Amount"] == 1000000 and tx["Fee"] == 12 and tx["Sequence"] == 0
    assert tx["status"] == "success" and tx["result"] == "tesSUCCESS"
    assert tx["block_number"] == 100 and tx["block_hash"] == "LH100"
    assert tx["transaction_index"] == 0
    assert metas[0]["transaction_hash"] == tx["hash"]
    assert ledger_doc["num_transactions"] == 1 and ledger_doc["total_coins"] == 99985687626634189


def test_v1_shape_supported():
    v1 = {"hash": "H1", "Account": "rA", "Destination": "rB", "TransactionType": "Payment",
          "Amount": "5", "Fee": "10", "Memos": [{"Memo": {"MemoData": "AB"}}],
          "metaData": {"TransactionResult": "tesSUCCESS", "TransactionIndex": 3}}
    txs, metas = flatten_transactions(1, "LH", 0, [v1], NOW)
    assert txs[0]["from_address"] == "rA" and txs[0]["Amount"] == 5
    assert metas[0]["memos"] == [{"Memo": {"MemoData": "AB"}}]
    assert metas[0]["meta"]["TransactionIndex"] == 3


def test_failed_status_and_unknown_status():
    txs, _ = flatten_transactions(1, "LH", 0, [make_tx(0, result="tecUNFUNDED_PAYMENT")], NOW)
    assert txs[0]["status"] == "failed" and txs[0]["result"] == "tecUNFUNDED_PAYMENT"
    no_meta = {"hash": "H", "tx_json": {"Account": "rA"}}
    txs, metas = flatten_transactions(1, "LH", 0, [no_meta], NOW)
    assert txs[0]["status"] == "unknown" and metas[0]["meta"] == {}


def test_issued_currency_amount_kept_as_dict():
    tx = make_tx(0)
    tx["tx_json"]["Amount"] = {"currency": "USD", "issuer": "rI", "value": "1.5"}
    txs, _ = flatten_transactions(1, "LH", 0, [tx], NOW)
    assert txs[0]["Amount"] == {"currency": "USD", "issuer": "rI", "value": "1.5"}


def test_non_numeric_and_oversized_values_are_not_corrupted():
    tx = make_tx(0)
    tx["tx_json"]["Amount"] = "not-a-number"
    tx["tx_json"]["Fee"] = str(2**70)
    txs, _ = flatten_transactions(1, "LH", 0, [tx], NOW)
    assert txs[0]["Amount"] == "not-a-number"
    assert txs[0]["Fee"] == str(2**70)


def test_top_level_ledger_fields_win_over_tx_fields():
    tx = make_tx(0)
    tx["ledger_index"] = 999
    txs, _ = flatten_transactions(5, "LH", 0, [tx], NOW)
    assert txs[0]["block_number"] == 5


def test_transaction_without_hash_is_malformed():
    with pytest.raises(MalformedLedger, match="without hash"):
        flatten_transactions(1, "LH", 0, [{"tx_json": {"Account": "rA"}}], NOW)


def test_bare_hash_transactions_rejected():
    with pytest.raises(MalformedLedger):
        flatten_transactions(1, "LH", 0, ["ABCDEF"], NOW)


def test_empty_ledger_ok():
    ledger_doc, txs, metas = parse_ledger_result(7, make_result(7, 0), NOW)
    assert txs == [] and metas == [] and ledger_doc["num_transactions"] == 0


def test_not_validated_is_retryable_type():
    with pytest.raises(NotValidated):
        parse_ledger_result(100, make_result(100, validated=False), NOW)


def test_missing_validated_flag_treated_as_not_validated():
    r = make_result(100)
    del r["validated"]
    with pytest.raises(NotValidated):
        parse_ledger_result(100, r, NOW)


def test_index_mismatch_rejected():
    with pytest.raises(MalformedLedger, match="asked for ledger 100"):
        parse_ledger_result(100, make_result(101), NOW)


@pytest.mark.parametrize("field", ["ledger_hash", "parent_hash", "account_hash", "transaction_hash"])
def test_missing_required_ledger_field(field):
    r = make_result(100)
    r["ledger"].pop(field, None)
    r.pop("ledger_hash", None)
    with pytest.raises(MalformedLedger, match=field):
        parse_ledger_result(100, r, NOW)


def test_empty_or_missing_ledger_body():
    with pytest.raises(MalformedLedger):
        parse_ledger_result(1, {"validated": True, "ledger": {}}, NOW)
    with pytest.raises(MalformedLedger):
        parse_ledger_result(1, {"validated": True}, NOW)


def test_transactions_not_a_list():
    with pytest.raises(MalformedLedger):
        parse_ledger_result(100, make_result(100, transactions="oops"), NOW)


def test_input_not_mutated():
    r = make_result(100, 1)
    before = repr(r)
    parse_ledger_result(100, r, NOW)
    assert repr(r) == before


def test_ledger_hash_fallback_to_result_level_v2():
    r = make_result(100)
    del r["ledger"]["ledger_hash"]
    r["ledger_hash"] = "TOPHASH"
    doc, _, _ = parse_ledger_result(100, r, NOW)
    assert doc["block_hash"] == "TOPHASH"


def test_close_time_iso():
    assert close_time_iso(0) == ""
    assert close_time_iso(1).startswith("2000-01-01T00:00:01")


@pytest.mark.parametrize("v,exp", [("12", 12), (12, 12), ("-3", -3), ("1.5", "1.5"), (True, True),
                                   (None, None), ({"a": 1}, {"a": 1}), ("", "")])
def test_to_int(v, exp):
    assert to_int(v) == exp


# -- API v2 Payment: DeliverMax replaces Amount (xrpld DeliverMax.cpp) ----------------------

def v2_payment(deliver_max, **extra):
    tx = make_tx(0)
    del tx["tx_json"]["Amount"]
    tx["tx_json"]["DeliverMax"] = deliver_max
    tx["tx_json"].update(extra)
    return tx


def test_v2_deliver_max_xrp_copied_into_amount():
    txs, _ = flatten_transactions(1, "LH", 0, [v2_payment("2500000")], NOW)
    assert txs[0]["Amount"] == 2500000 and txs[0]["DeliverMax"] == 2500000


def test_v2_deliver_max_token_copied_into_amount():
    token = {"currency": "USD", "issuer": "rI", "value": "1.5"}
    txs, _ = flatten_transactions(1, "LH", 0, [v2_payment(token)], NOW)
    assert txs[0]["Amount"] == token and txs[0]["DeliverMax"] == token
    txs[0]["Amount"]["value"] = "changed"  # independent copies, not one shared dict
    assert txs[0]["DeliverMax"]["value"] == "1.5"


def test_existing_amount_not_overwritten_by_deliver_max():
    tx = make_tx(0)  # Amount "1000000"
    tx["tx_json"]["DeliverMax"] = "7"
    txs, _ = flatten_transactions(1, "LH", 0, [tx], NOW)
    assert txs[0]["Amount"] == 1000000 and txs[0]["DeliverMax"] == 7


def test_no_deliver_max_no_amount_added():
    tx = make_tx(0)
    tx["tx_json"]["TransactionType"] = "AccountSet"
    del tx["tx_json"]["Amount"]
    txs, _ = flatten_transactions(1, "LH", 0, [tx], NOW)
    assert "Amount" not in txs[0] and "DeliverMax" not in txs[0]


def test_v2_deliver_max_non_numeric_kept_as_is():
    txs, _ = flatten_transactions(1, "LH", 0, [v2_payment("bad")], NOW)
    assert txs[0]["Amount"] == "bad" and txs[0]["DeliverMax"] == "bad"


# -- delivered_amount, affected_accounts, readable amounts ----------------------------------

ALICE = "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"
BOB = "rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe"
CAROL = "rf1BiGeXwwQoi8Z2ueFYTEXSwuJYfV2Jpn"
DAVE = "rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q"
ISSUER = "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
GW_HIGH = "rDsbeomae4FXwgQTJp9Rs64Qg9vDiTCdBv"
GW_LOW = "rLNaPoKeeBjZe2qs6x52yVPZpZ8td4dc6w"


def payment(amount="1000000", meta=None, **tx_json):
    body = {"Account": ALICE, "Destination": BOB, "TransactionType": "Payment",
            "Amount": amount, "Fee": "12", "Sequence": 1, **tx_json}
    if amount is None:
        del body["Amount"]
    return {"hash": "HP", "tx_json": body,
            "meta": {"TransactionResult": "tesSUCCESS", "TransactionIndex": 0,
                     **(meta or {})}}


def one(tx, **kw):
    txs, _ = flatten_transactions(1, "LH", 0, [tx], NOW, **kw)
    return txs[0]


AFFECTED_META = {"AffectedNodes": [
    {"CreatedNode": {"LedgerEntryType": "AccountRoot", "NewFields": {"Account": CAROL}}},
    {"ModifiedNode": {"LedgerEntryType": "AccountRoot",
                      "FinalFields": {"Account": ALICE, "Balance": "99"},
                      "PreviousFields": {"Balance": "100"}}},
    {"DeletedNode": {"LedgerEntryType": "Offer",
                     "FinalFields": {"Account": ALICE, "Owner": DAVE, "Destination": BOB},
                     "PreviousFields": {"Owner": DAVE}}},
    {"ModifiedNode": {"LedgerEntryType": "RippleState",
                      "FinalFields": {"HighLimit": {"currency": "USD", "issuer": GW_HIGH,
                                                    "value": "0"},
                                      "LowLimit": {"currency": "USD", "issuer": GW_LOW,
                                                   "value": "10"}}}},
    {"CreatedNode": {"LedgerEntryType": "MPTokenIssuance", "NewFields": {"Issuer": ISSUER}}},
]}


def test_affected_accounts_from_every_node_kind_and_field():
    got = extract_affected_accounts(AFFECTED_META)
    assert got == sorted({CAROL, ALICE, DAVE, BOB, GW_HIGH, GW_LOW, ISSUER})


def test_affected_accounts_include_tx_account_and_destination_sorted_unique():
    other = "rGWrZyQqhTp9Xu7G5Pkayo7bXjH4k4QYpf"
    got = extract_affected_accounts({"AffectedNodes": []}, other, ALICE, ALICE)
    assert got == sorted({other, ALICE})


@pytest.mark.parametrize("value", [
    "rA0",                                    # too short
    "r" + "x" * 40,                           # too long
    "X7AcgcsBL6XDcUb289X4mJ8djcdyKaB5hJDWMArnXr61cqZ",  # X-address, not classic
    "B5F762798A53D543A014CAF8B297CFF8F2F937E8",          # hex, does not start with 'r'
    12345, None, {"issuer": ALICE}, ["r" * 30],
])
def test_affected_accounts_ignore_values_that_are_not_classic_addresses(value):
    meta = {"AffectedNodes": [{"ModifiedNode": {"FinalFields": {"Account": value}}}]}
    assert extract_affected_accounts(meta, value) == []


@pytest.mark.parametrize("meta", [
    {}, {"AffectedNodes": "oops"}, {"AffectedNodes": [None, "x", {"ModifiedNode": None},
                                                      {"ModifiedNode": {"FinalFields": "x"}},
                                                      {"ModifiedNode": {"FinalFields":
                                                                        {"HighLimit": "x"}}}]},
])
def test_affected_accounts_tolerate_malformed_meta(meta):
    assert extract_affected_accounts(meta, ALICE) == [ALICE]


def test_affected_accounts_on_tx_doc():
    doc = one(payment(meta=AFFECTED_META))
    assert doc["affected_accounts"] == sorted({CAROL, ALICE, DAVE, BOB, GW_HIGH, GW_LOW, ISSUER})


def test_affected_accounts_without_meta_are_tx_parties():
    doc = one({"hash": "H", "tx_json": {"Account": ALICE, "Destination": BOB}})
    assert doc["affected_accounts"] == sorted([ALICE, BOB])


def test_delivered_amount_xrp_converted_to_int_drops():
    doc = one(payment(meta={"delivered_amount": "2500000"}))
    assert doc["delivered_amount"] == 2500000
    assert doc["delivered_xrp"] == Decimal128("2.500000") and doc["delivered_currency"] == "XRP"


def test_delivered_amount_token_kept_as_object():
    token = {"currency": "EUR", "issuer": ISSUER, "value": "0.25"}
    doc = one(payment(meta={"delivered_amount": token}))
    assert doc["delivered_amount"] == token
    assert doc["delivered_currency"] == "EUR" and doc["delivered_issuer"] == ISSUER
    assert doc["delivered_value"] == Decimal128("0.25") and "delivered_xrp" not in doc


def test_delivered_amount_unavailable_kept_without_readable_fields():
    doc = one(payment(meta={"delivered_amount": "unavailable"}))
    assert doc["delivered_amount"] == "unavailable"
    assert not [k for k in doc if k.startswith("delivered_") and k != "delivered_amount"]


def test_delivered_amount_falls_back_to_binary_field_name():
    doc = one(payment(meta={"DeliveredAmount": "7"}))
    assert doc["delivered_amount"] == 7 and doc["delivered_xrp"] == Decimal128("0.000007")


def test_delivered_amount_prefers_api_field_over_binary_field():
    doc = one(payment(meta={"delivered_amount": "5", "DeliveredAmount": "7"}))
    assert doc["delivered_amount"] == 5


def test_no_delivered_amount_no_fields():
    doc = one(payment())
    assert not [k for k in doc if k.startswith("delivered")]


@pytest.mark.parametrize("drops,xrp", [
    ("1", "0.000001"), ("1000000", "1.000000"), ("2500001", "2.500001"),
    ("100000000000000000", "100000000000.000000"),  # all XRP ever: still exact
])
def test_amount_xrp_is_exact_decimal128(drops, xrp):
    doc = one(payment(drops))
    assert isinstance(doc["amount_xrp"], Decimal128)
    assert doc["amount_xrp"].to_decimal() == Decimal(xrp)
    assert str(doc["amount_xrp"]) == xrp
    assert doc["amount_currency"] == "XRP"
    assert "amount_issuer" not in doc and "amount_value" not in doc


@pytest.mark.parametrize("value,expected", [
    ("1.5", "1.5"), ("-0.000001", "-0.000001"), ("1000", "1000"),
    ("9999999999999999e80", "9.999999999999999E+95"), ("1234567890123456e-96", None),
])
def test_amount_token_value_is_decimal128(value, expected):
    doc = one(payment({"currency": "USD", "issuer": ISSUER, "value": value}))
    assert doc["amount_currency"] == "USD" and doc["amount_issuer"] == ISSUER
    assert isinstance(doc["amount_value"], Decimal128)
    assert doc["amount_value"].to_decimal() == Decimal(value)
    if expected:
        assert str(doc["amount_value"]) == expected
    assert "amount_xrp" not in doc


def test_amount_readable_from_v2_deliver_max():
    doc = one(payment(None, DeliverMax="3000000"))
    assert doc["amount_xrp"] == Decimal128("3.000000") and doc["amount_currency"] == "XRP"


def test_hex_currency_code_kept_as_is():
    code = "0158415500000000C1F76FF6ECB0BAC600000000"
    doc = one(payment({"currency": code, "issuer": ISSUER, "value": "1"}))
    assert doc["amount_currency"] == code


@pytest.mark.parametrize("value", ["abc", "NaN", "Infinity", "1_000", "", "1e99999", None, 5,
                                   "1" * 40])
def test_unparseable_token_value_omits_value_and_logs_debug(caplog, value):
    with caplog.at_level("DEBUG", logger="xrpl_ingest.transform"):
        doc = one(payment({"currency": "USD", "issuer": ISSUER, "value": value}))
    assert "amount_value" not in doc
    assert doc["amount_currency"] == "USD" and doc["amount_issuer"] == ISSUER
    assert doc["Amount"]["value"] == value  # the raw amount is never touched
    assert any(r.levelname == "DEBUG" and "amount_value" in r.getMessage() for r in caplog.records)


def test_non_numeric_xrp_amount_has_no_readable_fields():
    doc = one(payment("not-a-number"))
    assert not [k for k in doc if k.startswith("amount_")]


def test_readable_fields_never_float():
    doc = one(payment("123", meta={"delivered_amount": {"currency": "USD", "issuer": ISSUER,
                                                        "value": "0.1"}}))
    assert not [k for k, v in doc.items() if isinstance(v, float)]


def test_node_fields_with_derived_names_are_kept_under_raw():
    tx = payment("10", meta={"delivered_amount": "10"}, affected_accounts="theirs",
                 amount_xrp="theirs-too", _inserted_at="theirs-three")
    doc = one(tx)
    assert doc["_inserted_at"] == NOW and doc["raw__inserted_at"] == "theirs-three"
    assert doc["affected_accounts"] == sorted([ALICE, BOB])
    assert doc["raw_affected_accounts"] == "theirs"
    assert doc["amount_xrp"] == Decimal128("0.000010") and doc["raw_amount_xrp"] == "theirs-too"


def test_readable_fields_pure_helper():
    assert readable_amount("amount", 10) == {"amount_xrp": Decimal128("0.000010"),
                                             "amount_currency": "XRP"}
    assert readable_amount("delivered", "unavailable") == {}
    assert readable_amount("delivered", None) == {}
    assert readable_amount("amount", True) == {}  # bool is not drops
    mpt = readable_amount("amount", {"mpt_issuance_id": "00ABC", "value": "100"})
    assert mpt == {"amount_value": Decimal128("100")}  # no currency/issuer to report
