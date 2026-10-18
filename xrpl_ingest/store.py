"""MongoDB persistence. Synchronous pymongo; callers use asyncio.to_thread."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Tuple

from pymongo import ASCENDING, DESCENDING, MongoClient, ReplaceOne

from .config import Config

log = logging.getLogger(__name__)

LAST_LEDGER_ID = "last_processed_ledger"
CHUNK_KIND = "backfill_chunk"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class MongoStore:
    def __init__(self, client: Any, cfg: Config):
        self.client = client
        self.cfg = cfg
        db = client[cfg.db_name]
        self.ledgers = db[cfg.ledger_collection]
        self.txs = db[cfg.tx_collection]
        self.metas = db[cfg.meta_memos_collection]
        self.failed = db[cfg.failed_collection]
        self.meta = db[cfg.metadata_collection]

    @classmethod
    def from_config(cls, cfg: Config) -> "MongoStore":
        return cls(MongoClient(cfg.mongo_uri), cfg)

    def close(self) -> None:
        self.client.close()

    def ensure_indexes(self) -> None:
        self.ledgers.create_index([("block_number", ASCENDING)], unique=True)
        self.ledgers.create_index([("close_time", ASCENDING)])
        self.txs.create_index([("hash", ASCENDING)], unique=True, sparse=True)
        for f in ("block_number", "from_address", "TransactionType", "close_time"):
            self.txs.create_index([(f, ASCENDING)])
        self.txs.create_index([("to_address", ASCENDING)], sparse=True)
        self.txs.create_index([("affected_accounts", ASCENDING)])  # multikey (array field)
        self.metas.create_index([("transaction_hash", ASCENDING)], unique=True, sparse=True)
        self.metas.create_index([("block_number", ASCENDING)])
        self.failed.create_index([("block_number", ASCENDING)], unique=True)
        self.meta.create_index([("kind", ASCENDING), ("start", ASCENDING)], sparse=True)

    # -- ledger data -------------------------------------------------------------

    def save_ledger_bundle(self, ledger_doc: Dict[str, Any], tx_docs: List[Dict[str, Any]],
                           meta_docs: List[Dict[str, Any]]) -> None:
        """Idempotent upsert. The ledger document is written LAST: its presence means the
        ledger's transactions are complete, so a crash mid-way is repaired by re-running."""
        if tx_docs:
            self.txs.bulk_write(
                [ReplaceOne({"hash": d["hash"]}, d, upsert=True) for d in tx_docs], ordered=False)
        if meta_docs:
            self.metas.bulk_write(
                [ReplaceOne({"transaction_hash": d["transaction_hash"]}, d, upsert=True)
                 for d in meta_docs], ordered=False)
        self.ledgers.replace_one({"block_number": ledger_doc["block_number"]}, ledger_doc, upsert=True)

    def highest_ledger(self) -> int:
        """Highest stored block_number (0 if none); uses the unique block_number index."""
        doc = self.ledgers.find_one({}, {"block_number": 1}, sort=[("block_number", DESCENDING)])
        return int(doc["block_number"]) if doc else 0

    # -- live-tail checkpoint ----------------------------------------------------

    def get_checkpoint(self) -> int:
        doc = self.meta.find_one({"_id": LAST_LEDGER_ID})
        return int(doc.get("index", 0)) if doc else 0

    def set_checkpoint(self, index: int) -> None:
        """Never moves back ($max), e.g. if an older tail process is still running."""
        doc = self.meta.find_one({"_id": LAST_LEDGER_ID}, {"index": 1})
        old = doc.get("index") if doc else None
        if old is not None and (isinstance(old, bool) or not isinstance(old, (int, float))):
            # legacy doc storing a string: in BSON order it sorts above every number, so
            # $max would never move it again. Rewrite it as a number once.
            try:
                legacy = int(old)
            except (TypeError, ValueError):
                legacy = 0
            log.warning("checkpoint index %r is not a number (legacy document); rewriting as %d",
                        old, max(index, legacy))
            self.meta.update_one({"_id": LAST_LEDGER_ID},
                                 {"$set": {"index": max(index, legacy), "updated_at": _now()}})
            return
        self.meta.update_one({"_id": LAST_LEDGER_ID},
                             {"$max": {"index": index}, "$set": {"updated_at": _now()}},
                             upsert=True)

    # -- backfill chunk progress -------------------------------------------------

    @staticmethod
    def _chunk_id(start: int, end: int) -> str:
        return f"{CHUNK_KIND}:{start}-{end}"

    def mark_chunk_done(self, start: int, end: int, ok: int, failed: int) -> None:
        self.meta.update_one(
            {"_id": self._chunk_id(start, end)},
            {"$set": {"kind": CHUNK_KIND, "start": start, "end": end, "ok": ok,
                      "failed": failed, "completed_at": _now()}},
            upsert=True)

    def done_chunks(self, start: int, end: int) -> Set[Tuple[int, int]]:
        cur = self.meta.find({"kind": CHUNK_KIND, "start": {"$gte": start}, "end": {"$lte": end}},
                             {"start": 1, "end": 1})
        return {(d["start"], d["end"]) for d in cur}

    # -- failed ledgers ----------------------------------------------------------

    def record_failure(self, block_number: int, reason: str) -> None:
        self.failed.update_one(
            {"block_number": block_number},
            {"$set": {"block_number": block_number, "reason": reason, "last_failed_at": _now()},
             "$inc": {"retry_count": 1}},
            upsert=True)

    def clear_failure(self, block_number: int) -> None:
        self.failed.delete_one({"block_number": block_number})

    def list_failed(self) -> List[int]:
        return sorted(d["block_number"] for d in self.failed.find({}, {"block_number": 1}))

    def list_failed_with_reasons(self) -> List[Tuple[int, str]]:
        docs = self.failed.find({}, {"block_number": 1, "reason": 1})
        return sorted((d["block_number"], str(d.get("reason", ""))) for d in docs)

    def count_failed(self) -> int:
        return self.failed.count_documents({})

    # -- verification ------------------------------------------------------------

    def find_gaps(self, start: int, end: int) -> List[Tuple[int, int]]:
        """Ranges inside [start, end] with no ledger document."""
        gaps: List[Tuple[int, int]] = []
        cursor = start
        cur = self.ledgers.find({"block_number": {"$gte": start, "$lte": end}},
                                {"block_number": 1, "_id": 0}).sort("block_number", ASCENDING)
        for d in cur:
            n = d["block_number"]
            if n > cursor:
                gaps.append((cursor, n - 1))
            cursor = n + 1
        if cursor <= end:
            gaps.append((cursor, end))
        return gaps

    def tx_count_mismatches(self, start: int, end: int) -> List[Tuple[int, int, int]]:
        """(block_number, expected, actual) where stored tx count != ledger.num_transactions."""
        rng = {"block_number": {"$gte": start, "$lte": end}}
        actual = {d["_id"]: d["n"] for d in self.txs.aggregate(
            [{"$match": rng}, {"$group": {"_id": "$block_number", "n": {"$sum": 1}}}])}
        out = []
        for d in self.ledgers.find(rng, {"block_number": 1, "num_transactions": 1, "_id": 0}):
            expected = d.get("num_transactions", 0)
            got = actual.get(d["block_number"], 0)
            if expected != got:
                out.append((d["block_number"], expected, got))
        return sorted(out)

    def stats(self) -> Dict[str, Any]:
        return {
            "ledgers": self.ledgers.estimated_document_count(),
            "transactions": self.txs.estimated_document_count(),
            "failed": self.count_failed(),
            "checkpoint": self.get_checkpoint(),
            "chunks_done": self.meta.count_documents({"kind": CHUNK_KIND}),
        }
