"""MongoDB persistence. Synchronous pymongo; callers use asyncio.to_thread."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from pymongo import (ASCENDING, DESCENDING, MongoClient, ReturnDocument, ReplaceOne,
                     UpdateOne)
from pymongo.errors import DuplicateKeyError

from .config import Config
from .errors import ConfigError
from .ledger_ranges import subtract

log = logging.getLogger(__name__)

LAST_LEDGER_ID = "last_processed_ledger"
CHUNK_KIND = "backfill_chunk"
RECOVERY_KIND = "recovery_range"
LEDGER_PROGRESS_KIND = "ledger_progress"
TAIL_OWNER_ID = "live_tail_owner"
TAIL_LEASE_SECONDS = 300


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
        for ledger_doc, tx_docs, meta_docs in bundles:
            self._validate_bundle(ledger_doc, tx_docs, meta_docs)

        tx_ops = [ReplaceOne({"hash": d["hash"], "block_number": d["block_number"]}, d,
                             upsert=True)
                  for _, txs, _ in bundles for d in txs]
        if tx_ops:
            self.txs.bulk_write(tx_ops, ordered=False)
        meta_ops = [ReplaceOne({"transaction_hash": d["transaction_hash"],
                                "block_number": d["block_number"]}, d, upsert=True)
                    for _, _, metas in bundles for d in metas]
        if meta_ops:
            self.metas.bulk_write(meta_ops, ordered=False)
        for ledger_doc, tx_docs, meta_docs in bundles:
            self._verify_bundle_readback(ledger_doc, tx_docs, meta_docs)
        if len(bundles) == 1:
            d = bundles[0][0]
            self.ledgers.replace_one({"block_number": d["block_number"]}, d, upsert=True)
        elif bundles:
            self.ledgers.bulk_write(
                [ReplaceOne({"block_number": d["block_number"]}, d, upsert=True)
                 for d, _, _ in bundles], ordered=False)
        if bundles:
            self.failed.delete_many({"block_number": {"$in": [d["block_number"] for d, _, _ in bundles]}})
            now = _now()
            self.meta.bulk_write([
                UpdateOne(
                    {"_id": self._ledger_progress_id(d["block_number"])},
                    {"$set": {"kind": LEDGER_PROGRESS_KIND, "ledger": d["block_number"],
                              "status": "ingested", "verified": False,
                              "updated_at": now}},
                    upsert=True,
                )
                for d, _, _ in bundles
            ], ordered=False)

    @staticmethod
    def _validate_bundle(ledger: Dict[str, Any], txs: List[Dict[str, Any]],
                         metas: List[Dict[str, Any]]) -> None:
        index = ledger.get("block_number")
        expected = ledger.get("num_transactions")
        if type(index) is not int or index < 0:
            raise ValueError("ledger bundle has an invalid block_number")
        if type(expected) is not int or expected != len(txs) or expected != len(metas):
            raise ValueError(f"ledger {index}: expected {expected!r} transactions, "
                             f"got {len(txs)} transaction and {len(metas)} metadata documents")
        tx_hashes = [doc.get("hash") for doc in txs]
        meta_hashes = [doc.get("transaction_hash") for doc in metas]
        if (any(not isinstance(value, str) or not value for value in tx_hashes)
                or len(set(tx_hashes)) != expected):
            raise ValueError(f"ledger {index}: transaction hashes are missing or duplicated")
        if (any(not isinstance(value, str) or not value for value in meta_hashes)
                or set(tx_hashes) != set(meta_hashes)):
            raise ValueError(f"ledger {index}: transaction and metadata hashes differ")
        if any(doc.get("block_number") != index for doc in txs + metas):
            raise ValueError(f"ledger {index}: child document belongs to another ledger")

    def _verify_bundle_readback(self, ledger: Dict[str, Any], txs: List[Dict[str, Any]],
                                metas: List[Dict[str, Any]]) -> None:
        index = ledger["block_number"]
        expected = {doc["hash"] for doc in txs}
        tx_rows = list(self.txs.find({"block_number": index}, {"hash": 1, "_id": 0}))
        meta_rows = list(self.metas.find(
            {"block_number": index}, {"transaction_hash": 1, "_id": 0}))
        actual = {doc["hash"] for doc in tx_rows if isinstance(doc.get("hash"), str)}
        actual_meta = {doc["transaction_hash"] for doc in meta_rows
                       if isinstance(doc.get("transaction_hash"), str)}
        if (len(tx_rows) != len(txs) or actual != expected
                or len(meta_rows) != len(metas) or actual_meta != expected):
            raise RuntimeError(
                f"ledger {index}: MongoDB read-back incomplete "
                f"(transactions {len(actual)}/{len(expected)}, "
                f"metadata {len(actual_meta)}/{len(expected)})")

    def highest_ledger(self) -> int:
        """Highest stored block_number (0 if none); uses the unique block_number index."""
        doc = self.ledgers.find_one({}, {"block_number": 1}, sort=[("block_number", DESCENDING)])
        if not doc:
            return 0
        value = doc.get("block_number")
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ConfigError("Invalid block_number in ledger collection; repair or remove the "
                              "damaged document after checking it against the node")
        return value

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
        if failed:
            raise ValueError("cannot mark a chunk done while it contains failed ledgers")
        self.meta.update_one(
            {"_id": self._chunk_id(start, end)},
            {"$set": {"kind": CHUNK_KIND, "start": start, "end": end, "ok": ok,
                      "failed": failed, "completed_at": _now()}},
            upsert=True)

    def done_chunks(self, start: int, end: int) -> Set[Tuple[int, int]]:
        cur = self.meta.find({"kind": CHUNK_KIND, "start": {"$gte": start}, "end": {"$lte": end}},
                             {"start": 1, "end": 1})
        return {(d["start"], d["end"]) for d in cur}

    # -- durable range recovery / ledger progress --------------------------------

    @staticmethod
    def recovery_job_id(start: int, end: int) -> str:
        return f"{RECOVERY_KIND}:{start}-{end}"

    @staticmethod
    def _ledger_progress_id(index: int) -> str:
        return f"{LEDGER_PROGRESS_KIND}:{index}"

    def schedule_recovery_range(self, start: int, end: int, reason: str, *,
                                blocked: bool = False,
                                retry_after_seconds: float = 0) -> str:
        if start < 0 or end < start:
            raise ValueError("recovery range must be a non-negative inclusive interval")
        job_id = self.recovery_job_id(start, end)
        now = _now()
        self.meta.update_one(
            {"_id": job_id},
            {"$setOnInsert": {
                "_id": job_id, "kind": RECOVERY_KIND, "start": start, "end": end,
                "status": "blocked" if blocked else "pending", "attempts": 0,
                "created_at": now,
            }, "$set": {
                "reason": reason, "updated_at": now,
                "next_attempt_at": now + timedelta(seconds=max(0, retry_after_seconds)),
            }},
            upsert=True,
        )
        return job_id

    def recovery_jobs(self, *, status: Optional[str] = None) -> List[Dict[str, Any]]:
        query = {"kind": RECOVERY_KIND}
        if status is not None:
            query["status"] = status
        return list(self.meta.find(query, {"_id": 0}).sort(
            [("start", ASCENDING), ("end", ASCENDING)]))

    def claim_recovery_job(self, worker_id: str, *, now: Optional[datetime] = None,
                           lease_seconds: int = 300) -> Optional[Dict[str, Any]]:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be >= 1")
        now = now or _now()
        return self.meta.find_one_and_update(
            {"kind": RECOVERY_KIND, "status": {"$in": ["pending", "blocked", "running"]},
             "next_attempt_at": {"$lte": now},
             "$or": [{"status": {"$ne": "running"}}, {"lease_until": {"$lte": now}}]},
            {"$set": {"status": "running", "worker_id": worker_id,
                      "lease_until": now + timedelta(seconds=lease_seconds),
                      "updated_at": now},
             "$inc": {"attempts": 1, "fencing_token": 1}},
            sort=[("start", ASCENDING), ("end", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

    def renew_recovery_lease(self, job_id: str, fencing_token: int, *,
                             now: Optional[datetime] = None,
                             lease_seconds: int = 300) -> bool:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be >= 1")
        now = now or _now()
        result = self.meta.update_one(
            {"_id": job_id, "kind": RECOVERY_KIND, "status": "running",
             "fencing_token": fencing_token},
            {"$set": {"lease_until": now + timedelta(seconds=lease_seconds),
                      "updated_at": now}},
        )
        return result.matched_count == 1

    def finish_recovery_job(self, job_id: str, fencing_token: int, status: str, *,
                            reason: Optional[str] = None,
                            retry_after_seconds: float = 0,
                            verification: Optional[Dict[str, Any]] = None) -> bool:
        if status not in {"pending", "blocked", "ingested", "verified"}:
            raise ValueError(f"invalid recovery status: {status}")
        if status == "verified":
            job = self.meta.find_one(
                {"_id": job_id, "kind": RECOVERY_KIND, "status": "running",
                 "fencing_token": fencing_token},
                {"start": 1, "end": 1},
            )
            if not job:
                return False
            if not verification:
                raise ValueError("verified recovery requires verification evidence")
            start, end = job["start"], job["end"]
            checked = verification.get("checked_ranges", [])
            try:
                uncovered = subtract(start, end, checked)
            except (TypeError, ValueError):
                uncovered = [(start, end)]
            if (verification.get("start") != start or verification.get("end") != end
                    or verification.get("node_checked") is not True
                    or verification.get("remaining") != 0
                    or verification.get("stopped") is not False or uncovered):
                raise ValueError(
                    "verified recovery requires deep node checks covering the complete job range")
        now = _now()
        update = {"$set": {"status": status, "updated_at": now,
                           "next_attempt_at": now + timedelta(
                               seconds=max(0, retry_after_seconds))},
                  "$unset": {"worker_id": "", "lease_until": ""}}
        if reason is not None:
            update["$set"]["reason"] = reason
        else:
            update["$unset"]["reason"] = ""
        result = self.meta.update_one(
            {"_id": job_id, "kind": RECOVERY_KIND, "status": "running",
             "fencing_token": fencing_token},
            update,
        )
        return result.modified_count == 1

    def recovery_lease_valid(self, job_id: str, fencing_token: int, *,
                             now: Optional[datetime] = None) -> bool:
        now = now or _now()
        return self.meta.find_one(
            {"_id": job_id, "kind": RECOVERY_KIND, "status": "running",
             "fencing_token": fencing_token, "lease_until": {"$gt": now}},
            {"_id": 1},
        ) is not None

    def claim_tail_lease(self, worker_id: str, *, now: Optional[datetime] = None,
                         lease_seconds: int = 300) -> Optional[int]:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be >= 1")
        now = now or _now()
        try:
            self.meta.update_one(
                {"_id": TAIL_OWNER_ID},
                {"$setOnInsert": {"kind": "tail_owner", "lease_until": now}},
                upsert=True,
            )
        except DuplicateKeyError:
            pass
        job = self.meta.find_one_and_update(
            {"_id": TAIL_OWNER_ID, "kind": "tail_owner",
             "lease_until": {"$lte": now}},
            {"$set": {"worker_id": worker_id,
                      "lease_until": now + timedelta(seconds=lease_seconds),
                      "updated_at": now},
             "$inc": {"fencing_token": 1}},
            return_document=ReturnDocument.AFTER,
        )
        return job.get("fencing_token") if job else None

    def renew_tail_lease(self, worker_id: str, fencing_token: int, *,
                         now: Optional[datetime] = None,
                         lease_seconds: int = 300) -> bool:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be >= 1")
        now = now or _now()
        result = self.meta.update_one(
            {"_id": TAIL_OWNER_ID, "kind": "tail_owner", "worker_id": worker_id,
             "fencing_token": fencing_token, "lease_until": {"$gt": now}},
            {"$set": {"lease_until": now + timedelta(seconds=lease_seconds),
                      "updated_at": now}},
        )
        return result.matched_count == 1

    def tail_lease_valid(self, worker_id: str, fencing_token: int, *,
                         now: Optional[datetime] = None) -> bool:
        now = now or _now()
        return self.meta.find_one(
            {"_id": TAIL_OWNER_ID, "kind": "tail_owner", "worker_id": worker_id,
             "fencing_token": fencing_token, "lease_until": {"$gt": now}},
            {"_id": 1},
        ) is not None

    def release_tail_lease(self, worker_id: str, fencing_token: int, *,
                           now: Optional[datetime] = None) -> bool:
        now = now or _now()
        result = self.meta.update_one(
            {"_id": TAIL_OWNER_ID, "kind": "tail_owner", "worker_id": worker_id,
             "fencing_token": fencing_token},
            {"$set": {"lease_until": now, "updated_at": now},
             "$unset": {"worker_id": ""}},
        )
        return result.modified_count == 1

    def record_ledger_attempt(self, index: int) -> None:
        progress_id = self._ledger_progress_id(index)
        now = _now()
        self.meta.update_one(
            {"_id": progress_id},
            {"$setOnInsert": {"_id": progress_id, "kind": LEDGER_PROGRESS_KIND,
                              "ledger": index, "status": "attempted", "verified": False},
             "$set": {"last_attempted_at": now, "updated_at": now},
             "$inc": {"attempts": 1}},
            upsert=True,
        )

    def mark_ledgers_verified(self, start: int, end: int) -> None:
        if start > end:
            return
        self.meta.update_many(
            {"kind": LEDGER_PROGRESS_KIND, "ledger": {"$gte": start, "$lte": end},
             "status": "ingested"},
            {"$set": {"verified": True, "verified_at": _now(), "updated_at": _now()}},
        )

    def record_reconciliation_pass(self, start: int, end: int, *,
                                   source_ranges: List[Tuple[int, int]],
                                   checked_ranges: List[Tuple[int, int]],
                                   remaining: int, node_checked: bool,
                                   stopped: bool = False,
                                   reason: Optional[str] = None) -> Dict[str, Any]:
        if start < 0 or end < start:
            raise ValueError("reconciliation range must be a non-negative inclusive interval")
        if remaining < 0:
            raise ValueError("remaining must be non-negative")
        uncovered = subtract(start, end, checked_ranges)
        coverage_complete = not uncovered
        if coverage_complete and node_checked and not remaining and not stopped:
            status = "verified"
        elif not node_checked or uncovered or stopped:
            status = "incomplete"
        else:
            status = "issues"
        now = _now()
        doc = {
            "kind": "reconciliation_range", "start": start, "end": end,
            "status": status, "source_ranges": [list(r) for r in source_ranges],
            "checked_ranges": [list(r) for r in checked_ranges],
            "uncovered_ranges": [list(r) for r in uncovered],
            "coverage_complete": coverage_complete, "node_checked": node_checked,
            "remaining": remaining, "stopped": stopped, "last_checked_at": now,
        }
        if reason is not None:
            doc["reason"] = reason
        self.meta.update_one(
            {"_id": f"reconciliation_range:{start}-{end}"},
            {"$set": doc, "$setOnInsert": {"_id": f"reconciliation_range:{start}-{end}",
                                           "created_at": now}},
            upsert=True,
        )
        return doc

    def _bundle_complete_numbers(self, start: int, end: int) -> Set[int]:
        bad = {item[0] for item in self.bundle_mismatches(start, end)}
        return {index for index in self.ledger_numbers(start, end) if index not in bad}

    def ledger_progress(self, start: int, end: int) -> Dict[str, int]:
        if start > end:
            raise ValueError("start must be <= end")
        complete = self._bundle_complete_numbers(start, end)
        attempts = self.meta.find(
            {"kind": LEDGER_PROGRESS_KIND, "ledger": {"$gte": start, "$lte": end},
             "last_attempted_at": {"$exists": True}},
            {"ledger": 1},
        )
        attempted_numbers = {doc["ledger"] for doc in attempts}
        verified_numbers = {
            doc["ledger"] for doc in self.meta.find(
                {"kind": LEDGER_PROGRESS_KIND, "ledger": {"$gte": start, "$lte": end},
                 "verified": True}, {"ledger": 1})
        }

        def contiguous(numbers: Set[int]) -> int:
            index = start
            while index <= end and index in numbers:
                index += 1
            return index - start

        return {
            "attempted": len(attempted_numbers),
            "ingested": len(complete),
            "verified": len(complete & verified_numbers),
            "attempted_contiguous": contiguous(attempted_numbers),
            "ingested_contiguous": contiguous(complete),
            "verified_contiguous": contiguous(complete & verified_numbers),
        }

    def bundle_mismatches(self, start: int, end: int) -> List[Tuple[int, int, int, int]]:
        """(ledger, expected txs, actual txs, actual metadata) count/hash integrity errors."""
        ledgers = list(self.ledgers.find(
            {"block_number": {"$gte": start, "$lte": end}},
            {"block_number": 1, "num_transactions": 1, "_id": 0}))
        if not ledgers:
            return []
        tx_by_ledger: Dict[int, Set[str]] = {}
        meta_by_ledger: Dict[int, Set[str]] = {}
        tx_count_by_ledger: Dict[int, int] = {}
        meta_count_by_ledger: Dict[int, int] = {}
        for doc in self.txs.find({"block_number": {"$gte": start, "$lte": end}},
                                 {"block_number": 1, "hash": 1, "_id": 0}):
            tx_by_ledger.setdefault(doc["block_number"], set()).add(doc.get("hash"))
            tx_count_by_ledger[doc["block_number"]] = (
                tx_count_by_ledger.get(doc["block_number"], 0) + 1)
        for doc in self.metas.find({"block_number": {"$gte": start, "$lte": end}},
                                   {"block_number": 1, "transaction_hash": 1, "_id": 0}):
            meta_by_ledger.setdefault(doc["block_number"], set()).add(
                doc.get("transaction_hash"))
            meta_count_by_ledger[doc["block_number"]] = (
                meta_count_by_ledger.get(doc["block_number"], 0) + 1)
        out = []
        for ledger in ledgers:
            index = ledger["block_number"]
            expected = ledger.get("num_transactions", 0)
            tx_hashes = tx_by_ledger.get(index, set())
            meta_hashes = meta_by_ledger.get(index, set())
            if (tx_count_by_ledger.get(index, 0) != expected
                    or meta_count_by_ledger.get(index, 0) != expected
                    or tx_hashes != meta_hashes):
                out.append((index, expected, tx_count_by_ledger.get(index, 0),
                            meta_count_by_ledger.get(index, 0)))
        return sorted(out)

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

    def failed_page(self, after: int, limit: int) -> List[Tuple[int, str]]:
        docs = self.failed.find({"block_number": {"$gt": after}},
                                {"block_number": 1, "reason": 1}).sort(
                                    "block_number", ASCENDING).limit(limit)
        return [(d["block_number"], str(d.get("reason", ""))) for d in docs]

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

    def metadata_docs(self, block_number: int) -> List[Dict[str, Any]]:
        return list(self.metas.find({"block_number": block_number}, {"_id": 0}))

    def ledger_doc(self, block_number: int) -> Optional[Dict[str, Any]]:
        return self.ledgers.find_one({"block_number": block_number}, {"_id": 0})

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
