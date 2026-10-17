import pytest

from conftest import NOW, make_result, make_tx
from xrpl_ingest.errors import MalformedLedger, NotValidated
from xrpl_ingest.transform import close_time_iso, flatten_transactions, parse_ledger_result, to_int


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
