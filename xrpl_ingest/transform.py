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
# str drops (XRP) or dict (issued). DeliverMax: API v2 Payment field that replaces Amount.
AMOUNT_FIELDS = ("Amount", "DeliverMax", "TakerGets", "TakerPays")

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


def copy_deliver_max(doc: Dict[str, Any]) -> None:
    """API v2 sends a Payment's Amount as DeliverMax only (xrpld DeliverMax.cpp). Keep both:
    `Amount` (legacy schema, existing queries) gets a copy when it is absent."""
    if "Amount" not in doc and "DeliverMax" in doc:
        dm = doc["DeliverMax"]
        doc["Amount"] = dict(dm) if isinstance(dm, dict) else dm


def _require(ledger: Dict[str, Any], key: str, index: int) -> Any:
    value = ledger.get(key)
    if value in (None, ""):
        raise MalformedLedger(f"ledger {index}: missing '{key}'")
    return value


# -- lossless storage -----------------------------------------------------------------------
# Every field the node sends is stored. Mapped fields keep their names/conversions; every
# other field is stored as received under its own name. When that name is already taken in
# the document by a different value, the received value goes to raw_<key> (raw_raw_<key>, ...
# if that is taken too). An identical value is not stored twice. Keys Mongo cannot hold are
# escaped (see escape_key).

RAW_PREFIX = "raw_"
RESERVED_KEYS = frozenset({"_id"})  # a node field may never become the Mongo _id
LEDGER_RENAMES = {"ledger_index": "block_number", "ledger_hash": "block_hash"}
_KEY_ESCAPES = (("\x00", "␀"), (".", "．"))  # NUL -> '␀', '.' -> fullwidth '．'
_DOLLAR, _DOLLAR_ESCAPED = "$", "＄"  # leading '$' -> fullwidth '＄'


def escape_key(key: Any) -> str:
    """Mongo field names must not contain '.' or NUL nor start with '$': such a key would make
    the whole write fail. Replaced by look-alike characters (reversed by unescape_key)."""
    key = key if isinstance(key, str) else str(key)
    for raw, escaped in _KEY_ESCAPES:
        key = key.replace(raw, escaped)
    return _DOLLAR_ESCAPED + key[1:] if key.startswith(_DOLLAR) else key


def unescape_key(key: str) -> str:
    if key.startswith(_DOLLAR_ESCAPED):
        key = _DOLLAR + key[1:]
    for raw, escaped in _KEY_ESCAPES:
        key = key.replace(escaped, raw)
    return key


def escape_keys(value: Any) -> Any:
    """Deep copy of dicts/lists with every key escaped; other values unchanged."""
    if isinstance(value, dict):
        return {escape_key(k): escape_keys(v) for k, v in value.items()}
    if isinstance(value, list):
        return [escape_keys(v) for v in value]
    return value


def same_value(stored: Any, received: Any) -> bool:
    """True when `received` adds nothing to `stored` (equal, or equal after our int coercion).
    bool/float compare type-strictly: True == 1 == 1.0 in Python, not in the data."""
    if isinstance(stored, (bool, float)) or isinstance(received, (bool, float)):
        return type(stored) is type(received) and stored == received
    return stored == received or stored == to_int(received)


def merge_field(doc: Dict[str, Any], key: Any, value: Any,
                renames: Dict[str, str] = FIELD_RENAMES) -> None:
    """Store a field received from the node in `doc` without losing it (rule above)."""
    value = escape_keys(value)
    name = escape_key(renames.get(key, key))
    if name not in doc and name not in RESERVED_KEYS:
        doc[name] = value
        return
    if name in doc and same_value(doc[name], value):
        return
    raw = escape_key(key)
    while True:
        raw = RAW_PREFIX + raw
        if raw not in doc:
            doc[raw] = value
            return
        if same_value(doc[raw], value):
            return


def build_ledger_doc(index: int, ledger: Dict[str, Any], num_transactions: int,
                     now: datetime) -> Dict[str, Any]:
    """Mapped header fields, then every other field of `ledger` except `transactions`
    (stored per transaction), losslessly."""
    block_hash = _require(ledger, "ledger_hash", index)
    doc = {
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
    for k, v in ledger.items():
        if k != "transactions":
            merge_field(doc, k, v, LEDGER_RENAMES)
    return doc


def _meta_key(tx: Dict[str, Any]) -> Optional[str]:
    """'meta' (v2) or 'metaData' (v1): the first non-empty object, else the first object."""
    keys = [k for k in ("meta", "metaData") if isinstance(tx.get(k), dict)]
    return next((k for k in keys if tx[k]), keys[0] if keys else None)


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

        meta_key = _meta_key(tx)
        meta = tx[meta_key] if meta_key else {}
        tx_json = tx.get("tx_json") if isinstance(tx.get("tx_json"), dict) else None
        # stored elsewhere: meta in the meta collection, tx_json merged below. Anything else
        # (a second meta, a non-object tx_json) is merged as a field so nothing is dropped.
        nested = {meta_key, "tx_json" if tx_json is not None else None}

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
            if k not in nested:
                merge_field(doc, k, v)
        if tx_json:
            for k, v in tx_json.items():
                merge_field(doc, k, v)

        tx_hash = doc.get("hash")
        if not tx_hash:
            raise MalformedLedger(f"ledger {block_number}: transaction without hash")

        copy_deliver_max(doc)
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
            "memos": escape_keys(memos) if isinstance(memos, list) else [],
            "meta": escape_keys(meta),  # stored whole
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
    for k, v in result.items():  # result level: validated, ledger_hash, ledger_index, ...
        if k != "ledger":
            merge_field(ledger_doc, k, v, LEDGER_RENAMES)
    tx_docs, meta_docs = flatten_transactions(
        index, ledger_doc["block_hash"], ledger_doc["close_time"], txs, now)
    return ledger_doc, tx_docs, meta_docs
