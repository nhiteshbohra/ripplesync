"""Pure conversion of a raw xrpld `ledger` response into Mongo documents.

Handles both API v2 (tx_json + meta) and v1 (flat tx + metaData) shapes.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .constants import INT64_MAX, RIPPLE_EPOCH_OFFSET
from .errors import MalformedLedger, NotValidated

FIELD_RENAMES = {
    "ledger_index": "block_number",
    "ledger_hash": "block_hash",
    "Account": "from_address",
    "Destination": "to_address",
}

INT_FIELDS = (
    "Fee", "Flags", "DestinationTag", "SourceTag", "Sequence",
    "LastLedgerSequence", "OfferSequence", "PreviousTxnLgrSeq",
)
AMOUNT_FIELDS = ("Amount", "TakerGets", "TakerPays")  # str drops (XRP) or dict (issued)

_INT_RE = re.compile(r"^-?\d+$")


def to_int(value: Any) -> Any:
    """Coerce numeric strings to int. Anything else (or > int64) is returned unchanged."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value if abs(value) <= INT64_MAX else str(value)
    if isinstance(value, str) and _INT_RE.match(value):
        n = int(value)
        return n if abs(n) <= INT64_MAX else value
    return value


def close_time_iso(close_time: int) -> str:
    if not close_time:
        return ""
    return datetime.fromtimestamp(close_time + RIPPLE_EPOCH_OFFSET, timezone.utc).isoformat()


def _require(ledger: Dict[str, Any], key: str, index: int) -> Any:
    value = ledger.get(key)
    if value in (None, ""):
        raise MalformedLedger(f"ledger {index}: missing '{key}'")
    return value


def build_ledger_doc(index: int, ledger: Dict[str, Any], num_transactions: int,
                     now: datetime) -> Dict[str, Any]:
    block_hash = _require(ledger, "ledger_hash", index)
    return {
        "block_number": index,
        "block_hash": str(block_hash),
        "close_time": to_int(ledger.get("close_time", 0)) or 0,
        "close_time_human": ledger.get("close_time_human", ""),
        "close_time_iso": close_time_iso(to_int(ledger.get("close_time", 0)) or 0),
        "total_coins": to_int(ledger.get("total_coins")),
        "parent_hash": str(_require(ledger, "parent_hash", index)),
        "account_hash": str(_require(ledger, "account_hash", index)),
        "transaction_hash": str(_require(ledger, "transaction_hash", index)),
        "num_transactions": num_transactions,
        "_inserted_at": now,
    }


def flatten_transactions(block_number: int, block_hash: str, close_time: int,
                         transactions: List[Any], now: datetime
                         ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return (tx_docs, meta_memo_docs)."""
    iso = close_time_iso(close_time)
    tx_docs: List[Dict[str, Any]] = []
    meta_docs: List[Dict[str, Any]] = []

    for tx in transactions:
        if not isinstance(tx, dict):
            # expand=False would give bare hashes; we always request expand=True.
            raise MalformedLedger(f"ledger {block_number}: transaction is not an object: {tx!r}")

        meta = tx.get("meta") or tx.get("metaData") or {}
        tx_json = tx.get("tx_json") if isinstance(tx.get("tx_json"), dict) else None

        doc: Dict[str, Any] = {
            "block_number": block_number,
            "block_hash": block_hash,
            "close_time": close_time,
            "close_time_iso": iso,
        }
        result = meta.get("TransactionResult")
        doc["status"] = "unknown" if result is None else ("success" if result == "tesSUCCESS" else "failed")
        if result is not None:
            doc["result"] = result
        if "TransactionIndex" in meta:
            doc["transaction_index"] = to_int(meta["TransactionIndex"])

        for k, v in tx.items():
            if k in ("meta", "metaData", "tx_json"):
                continue
            doc.setdefault(FIELD_RENAMES.get(k, k), v)
        if tx_json:
            for k, v in tx_json.items():
                doc.setdefault(FIELD_RENAMES.get(k, k), v)

        tx_hash = doc.get("hash")
        if not tx_hash:
            raise MalformedLedger(f"ledger {block_number}: transaction without hash")

        for field in INT_FIELDS:
            if doc.get(field) is not None:
                doc[field] = to_int(doc[field])
        for field in AMOUNT_FIELDS:
            if isinstance(doc.get(field), str):
                doc[field] = to_int(doc[field])

        doc["_inserted_at"] = now
        tx_docs.append(doc)

        body = tx_json if tx_json is not None else tx
        memos = body.get("Memos", [])
        meta_docs.append({
            "block_number": block_number,
            "block_hash": block_hash,
            "transaction_hash": tx_hash,
            "memos": memos if isinstance(memos, list) else [],
            "meta": meta,
            "close_time_iso": iso,
            "_inserted_at": now,
        })

    return tx_docs, meta_docs


def parse_ledger_result(index: int, result: Dict[str, Any], now: Optional[datetime] = None
                        ) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Validate an xrpld `ledger` result for `index` and convert it.

    Raises NotValidated (retryable) or MalformedLedger (permanent).
    """
    now = now or datetime.now(timezone.utc)
    if result.get("validated") is not True:
        raise NotValidated(f"ledger {index} is not validated")
    ledger = result.get("ledger")
    if not isinstance(ledger, dict) or not ledger:
        raise MalformedLedger(f"ledger {index}: empty or missing 'ledger'")

    ledger = dict(ledger)  # don't mutate caller's data
    ledger.setdefault("ledger_index", result.get("ledger_index"))
    ledger.setdefault("ledger_hash", result.get("ledger_hash"))
    got = to_int(ledger.get("ledger_index"))
    if got != index:
        raise MalformedLedger(f"asked for ledger {index}, node returned {ledger.get('ledger_index')!r}")

    txs = ledger.get("transactions", [])
    if not isinstance(txs, list):
        raise MalformedLedger(f"ledger {index}: 'transactions' is not a list")

    ledger_doc = build_ledger_doc(index, ledger, len(txs), now)
    tx_docs, meta_docs = flatten_transactions(
        index, ledger_doc["block_hash"], ledger_doc["close_time"], txs, now)
    return ledger_doc, tx_docs, meta_docs
