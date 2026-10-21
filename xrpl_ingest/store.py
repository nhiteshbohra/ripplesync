"""MongoDB persistence. Synchronous pymongo; callers use asyncio.to_thread."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from pymongo import ASCENDING, DESCENDING, MongoClient, ReplaceOne

from .config import Config
from .errors import ConfigError

log = logging.getLogger(__name__)

LAST_LEDGER_ID = "last_processed_ledger"
CHUNK_KIND = "backfill_chunk"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def checkpoint_index(doc: Optional[Dict[str, Any]]) -> int:
    """Accept legacy integer strings; never guess progress from damaged metadata."""
    if not doc:
        return 0
    value = doc.get("index")
    try:
        index = int(value)
        if isinstance(value, bool) or index < 0 or (isinstance(value, float) and value != index):
            raise ValueError
        return index
    except (TypeError, ValueError, OverflowError):
        raise ConfigError(
            f"Invalid checkpoint {LAST_LEDGER_ID}.index in metadata collection; "
            "restore a verified non-negative ledger index from backup or repair it manually "
            "after checking stored ledgers and failures. No progress was reset.") from None


class MongoStore:
    def __init__(self, client: Any, cfg: Config):
        self.client = client
        self.cfg = cfg
        db = client[cfg.db_name]
        if not db.write_concern.acknowledged:
            raise ConfigError("MongoDB writes must be acknowledged; remove w=0 from MONGO_URI")
        self.ledgers = db[cfg.ledger_collection]
        self.txs = db[cfg.tx_collection]
        self.metas = db[cfg.meta_memos_collection]
        self.failed = db[cfg.failed_collection]
        self.meta = db[cfg.metadata_collection]

    @classmethod
    def from_config(cls, cfg: Config, **client_options: Any) -> "MongoStore":
        """Client with the configured timeouts and an appname (see config.mongo_client_kwargs);
        `client_options` (e.g. doctor's short timeouts) win."""
        from .config import mongo_client_kwargs
        client = MongoClient(cfg.mongo_uri, **mongo_client_kwargs(cfg, **client_options))
        try:
            return cls(client, cfg)
        except Exception:
            client.close()
            raise

    def close(self) -> None:
        self.client.close()

    def ensure_indexes(self) -> None:
        self.ledgers.create_index([("block_number", ASCENDING)], unique=True)
        self.txs.create_index([("hash", ASCENDING)], unique=True, sparse=True)
        self.txs.create_index([("block_number", ASCENDING)])
        self.metas.create_index([("transaction_hash", ASCENDING)], unique=True, sparse=True)
        self.metas.create_index([("block_number", ASCENDING)])
        self.failed.create_index([("block_number", ASCENDING)], unique=True)
        self.meta.create_index([("kind", ASCENDING), ("start", ASCENDING)], sparse=True)

    # -- ledger data -------------------------------------------------------------

    def save_ledger_bundle(self, ledger_doc: Dict[str, Any], tx_docs: List[Dict[str, Any]],
                           meta_docs: List[Dict[str, Any]]) -> None:
        """Idempotent upsert; ledger completion is written after transactions and metadata."""
        self.save_ledger_bundles([(ledger_doc, tx_docs, meta_docs)])

    def save_ledger_bundles(self, bundles: list) -> None:
        """Batch across ledgers, preserving the same completion ordering as a single save.

        Partial bulk writes are safe to replay: every document is upserted by its unique
        key. No ledger in this batch is marked complete until ALL transaction and metadata
        writes succeed. The caller bounds batch size and isolates errors per ledger.
        """
        tx_ops = [ReplaceOne({"hash": d["hash"]}, d, upsert=True)
                  for _, txs, _ in bundles for d in txs]
        if tx_ops:
            self.txs.bulk_write(tx_ops, ordered=False)
        meta_ops = [ReplaceOne({"transaction_hash": d["transaction_hash"]}, d, upsert=True)
                    for _, _, metas in bundles for d in metas]
        if meta_ops:
            self.metas.bulk_write(meta_ops, ordered=False)
        if len(bundles) == 1:
            d = bundles[0][0]
            self.ledgers.replace_one({"block_number": d["block_number"]}, d, upsert=True)
        elif bundles:
            self.ledgers.bulk_write(
                [ReplaceOne({"block_number": d["block_number"]}, d, upsert=True)
                 for d, _, _ in bundles], ordered=False)

    def highest_ledger(self) -> int:
        """Highest stored block_number (0 if none); uses the unique block_number index."""
        doc = self.ledgers.find_one({}, {"block_number": 1}, sort=[("block_number", DESCENDING)])
        return int(doc["block_number"]) if doc else 0

    # -- live-tail checkpoint ----------------------------------------------------

    def get_checkpoint(self) -> int:
        doc = self.meta.find_one({"_id": LAST_LEDGER_ID})
        return checkpoint_index(doc)

    def set_checkpoint(self, index: int) -> None:
        """Never moves back ($max), e.g. if an older tail process is still running."""
        doc = self.meta.find_one({"_id": LAST_LEDGER_ID}, {"index": 1})
        old = doc.get("index") if doc else None
        legacy = checkpoint_index(doc)
        if old is not None and (isinstance(old, bool) or not isinstance(old, (int, float))):
            # legacy doc storing a string: in BSON order it sorts above every number, so
            # $max would never move it again. Rewrite it as a number once.
            log.warning("checkpoint index %r is not a number (legacy document); rewriting as %d",
                        old, max(index, legacy))
            converted = self.meta.update_one(
                {"_id": LAST_LEDGER_ID, "index": {"$eq": old}},
                {"$set": {"index": max(index, legacy), "updated_at": _now()}})
            if converted.matched_count:
                return
            # Another writer converted it first. Apply the ordinary numeric $max below
            # instead of overwriting its newer checkpoint with our stale read.
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

    def ledger_numbers(self, start: int, end: int) -> List[int]:
        """Stored block_numbers in [start, end], ascending (keep the range window-sized)."""
        cur = self.ledgers.find({"block_number": {"$gte": start, "$lte": end}},
                                {"block_number": 1, "_id": 0}).sort("block_number", ASCENDING)
        return [d["block_number"] for d in cur]

    def ledger_links(self, start: int, end: int) -> List[Tuple[int, str, str]]:
        """(block_number, block_hash, parent_hash) of stored ledgers in [start, end], ascending."""
        cur = self.ledgers.find({"block_number": {"$gte": start, "$lte": end}},
                                {"block_number": 1, "block_hash": 1, "parent_hash": 1, "_id": 0}
                                ).sort("block_number", ASCENDING)
        return [(d["block_number"], d.get("block_hash"), d.get("parent_hash")) for d in cur]

    def tx_hashes(self, block_number: int) -> Set[str]:
        """Hashes of the stored transactions of one ledger."""
        return {d["hash"] for d in self.txs.find({"block_number": block_number},
                                                 {"hash": 1, "_id": 0}) if "hash" in d}

    def find_tx(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        return self.txs.find_one({"hash": tx_hash}, {"_id": 0})

    def stats(self) -> Dict[str, Any]:
        return {
            "ledgers": self.ledgers.estimated_document_count(),
            "transactions": self.txs.estimated_document_count(),
            "failed": self.count_failed(),
            "checkpoint": self.get_checkpoint(),
            "chunks_done": self.meta.count_documents({"kind": CHUNK_KIND}),
        }

    # -- runtime helpers ---------------------------------------------------------

    def db_stats(self) -> Dict[str, Any]:
        """dbStats of the configured database (fsTotalSize / fsUsedSize: disk guard)."""
        return self.ledgers.database.command("dbStats")
