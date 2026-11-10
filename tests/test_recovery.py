import asyncio

from conftest import FAST_RETRY, FakeSource, no_sleep
from xrpl_ingest.errors import ConnectFailed
from pymongo.errors import AutoReconnect
import pytest

from xrpl_ingest.recovery import recover_jobs


def run(coro):
    return asyncio.run(coro)


def test_recovery_job_is_ingested_and_verified(store):
    job_id = store.schedule_recovery_range(1, 2, "backfill gap")
    source = FakeSource(ranges="1-2")

    processed = run(recover_jobs(
        source, store, FAST_RETRY, chunk_size=2, stop_check=lambda: False,
        once=True, sleep=no_sleep))

    job = store.meta.find_one({"_id": job_id})
    assert processed == 1
    assert job["status"] == "verified"
    assert store.ledger_progress(1, 2) == {
        "attempted": 2, "ingested": 2, "verified": 2,
        "attempted_contiguous": 2, "ingested_contiguous": 2,
        "verified_contiguous": 2,
    }


def test_deep_verification_failure_is_retained_as_retryable(store):
    job_id = store.schedule_recovery_range(1, 1, "verification retry")
    source = FakeSource(ranges="1-1")
    source.hash_script = {1: [["unexpected-hash"]]}

    run(recover_jobs(
        source, store, FAST_RETRY, chunk_size=1, stop_check=lambda: False,
        once=True, sleep=no_sleep))

    job = store.meta.find_one({"_id": job_id})
    assert job["status"] == "pending"
    assert "deep verification found 1 unresolved issue(s)" == job["reason"]
    assert store.ledger_progress(1, 1)["verified"] == 0


def test_unavailable_recovery_range_stays_blocked_and_retryable(store):
    job_id = store.schedule_recovery_range(1, 3, "missing history")
    source = FakeSource(ranges="10-20")

    run(recover_jobs(source, store, FAST_RETRY, chunk_size=2,
                     stop_check=lambda: False, once=True, sleep=no_sleep))

    job = store.meta.find_one({"_id": job_id})
    assert job["status"] == "blocked"
    assert job["reason"] == "source complete_ledgers does not include this range"
    assert job["next_attempt_at"] > job["updated_at"]
    assert store.ledger_numbers(1, 3) == []


def test_interrupted_recovery_releases_job_for_resume(store):
    stop = {"requested": False}

    class StopAfterFirstLedger(FakeSource):
        async def get_ledger(self, index):
            result = await super().get_ledger(index)
            stop["requested"] = True
            return result

    job_id = store.schedule_recovery_range(1, 3, "interrupted range")
    run(recover_jobs(
        StopAfterFirstLedger(ranges="1-3"), store, FAST_RETRY, chunk_size=3,
        stop_check=lambda: stop["requested"], once=True, sleep=no_sleep))

    job = store.meta.find_one({"_id": job_id})
    assert job["status"] == "pending"
    assert job["reason"] == "recovery stopped before the range completed"
    assert store.ledger_numbers(1, 3) == [1]
    assert store.ledger_progress(1, 3)["verified_contiguous"] == 0


def test_recovery_worker_surfaces_mongodb_unavailability():
    class UnavailableStore:
        def claim_recovery_job(self, *args, **kwargs):
            raise AutoReconnect("MongoDB unavailable")

    with pytest.raises(AutoReconnect, match="MongoDB unavailable"):
        run(recover_jobs(
            FakeSource(), UnavailableStore(), FAST_RETRY, chunk_size=2,
            stop_check=lambda: False, once=True, sleep=no_sleep))


def test_transient_source_errors_remain_pending_after_many_attempts(store):
    class DisconnectedSource(FakeSource):
        async def server_info(self):
            raise ConnectFailed("source disconnected")

    job_id = store.schedule_recovery_range(1, 2, "retry after disconnect")
    store.meta.update_one({"_id": job_id}, {"$set": {"attempts": 5}})
    run(recover_jobs(
        DisconnectedSource(), store, FAST_RETRY, chunk_size=2,
        stop_check=lambda: False, once=True, sleep=no_sleep))

    job = store.meta.find_one({"_id": job_id})
    assert job["status"] == "pending" and job["attempts"] == 6
    assert "ConnectFailed" in job["reason"]
