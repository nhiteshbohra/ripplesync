import asyncio

from pymongo.errors import AutoReconnect

from conftest import FAST_RETRY, FakeSource, make_result, no_sleep
from xrpl_ingest.errors import ConnectionLost, RequestTimeout, RpcError, StopRequested
from xrpl_ingest.fetcher import RetryPolicy, ingest_ledger


def run(coro):
    return asyncio.run(coro)


def ingest(source, store, idx=100, **kw):
    return run(ingest_ledger(source, store, idx, FAST_RETRY, sleep=no_sleep, **kw))


def test_success_stores_ledger_and_transactions(store):
    r = ingest(FakeSource(), store)
    assert r.ok and r.num_transactions == 2
    assert store.ledgers.count_documents({"block_number": 100}) == 1
    assert store.txs.count_documents({}) == 2 and store.metas.count_documents({}) == 2
    assert store.count_failed() == 0


def test_transient_errors_then_success(store):
    src = FakeSource({100: [RequestTimeout("t"), ConnectionLost("c")]})
    r = ingest(src, store)
    assert r.ok and src.calls == [100, 100, 100]
    assert store.count_failed() == 0


def test_retries_exhausted_records_failure(store):
    src = FakeSource({100: [RequestTimeout("t")] * 5})
    r = ingest(src, store)
    assert not r.ok and len(src.calls) == 3
    assert store.list_failed() == [100]
    assert "RequestTimeout" in store.failed.find_one({"block_number": 100})["reason"]
    assert store.ledgers.count_documents({}) == 0


def test_not_validated_retried(store):
    src = FakeSource({100: [make_result(100, validated=False)]})
    assert ingest(src, store).ok and len(src.calls) == 2


def test_permanent_rpc_error_not_retried(store):
    src = FakeSource({100: [RpcError("lgrNotFound", "ledgerNotFound")]})
    r = ingest(src, store)
    assert not r.ok and len(src.calls) == 1
    assert store.list_failed() == [100]


def test_retryable_rpc_error_retried(store):
    src = FakeSource({100: [RpcError("slowDown")]})
    assert ingest(src, store).ok and len(src.calls) == 2


def test_malformed_ledger_not_retried(store):
    src = FakeSource({100: [make_result(100, ledger_hash=None)]})
    src.script[100][0]["ledger"].pop("ledger_hash")
    r = ingest(src, store)
    assert not r.ok and len(src.calls) == 1 and store.list_failed() == [100]


def test_wrong_ledger_returned_not_stored(store):
    src = FakeSource({100: [make_result(101)]})
    assert not ingest(src, store).ok
    assert store.ledgers.count_documents({}) == 0 and store.txs.count_documents({}) == 0


def test_mongo_error_retried(store, monkeypatch):
    real = store.save_ledger_bundle
    state = {"n": 0}

    def flaky(*a):
        state["n"] += 1
        if state["n"] == 1:
            raise AutoReconnect("primary stepped down")
        return real(*a)

    monkeypatch.setattr(store, "save_ledger_bundle", flaky)
    src = FakeSource()
    assert ingest(src, store).ok and state["n"] == 2
    assert src.calls == [100]  # database retry reuses the already fetched and transformed data


def test_ledger_doc_written_last_so_partial_save_is_not_marked_complete(store, monkeypatch):
    def boom(*a, **k):
        raise AutoReconnect("down")
    monkeypatch.setattr(store.metas, "bulk_write", boom)
    r = ingest(FakeSource(), store)
    assert not r.ok
    assert store.ledgers.count_documents({}) == 0  # no commit marker => backfill redoes it


def test_record_failure_error_is_swallowed(store, monkeypatch):
    def boom(*a):
        raise AutoReconnect("down")
    monkeypatch.setattr(store, "record_failure", boom)
    r = ingest(FakeSource({100: [RpcError("lgrNotFound")]}), store)
    assert not r.ok  # no exception escaped
    assert not r.recorded


def test_stop_while_connecting_is_neither_retried_nor_recorded(store):
    src = FakeSource({100: [StopRequested("stopped while connecting")] * 5})
    r = ingest(src, store)
    assert not r.ok and not r.recorded and r.error.startswith("StopRequested")
    assert src.calls == [100]  # no retries
    assert store.list_failed() == []  # a stop is not a ledger failure


def test_recorded_failure_is_flagged_recorded(store):
    r = ingest(FakeSource({100: [RpcError("lgrNotFound")]}), store)
    assert not r.ok and r.recorded


def test_clear_failure_on_retry_success(store):
    store.record_failure(100, "old")
    assert ingest(FakeSource(), store, clear_failure=True).ok
    assert store.count_failed() == 0


def test_failure_kept_when_retry_fails_again(store):
    store.record_failure(100, "old")
    ingest(FakeSource({100: [RpcError("lgrNotFound")]}), store, clear_failure=True)
    assert store.failed.find_one({"block_number": 100})["retry_count"] == 2


def test_rerun_is_idempotent(store):
    ingest(FakeSource(), store)
    ingest(FakeSource(), store)
    assert store.txs.count_documents({}) == 2 and store.ledgers.count_documents({}) == 1


def test_backoff_delays_grow_and_cap():
    p = RetryPolicy(attempts=5, base=1, max_delay=5, jitter=False)
    assert [p.delay(i) for i in range(5)] == [1, 2, 4, 5, 5]


def test_backoff_delay_for_huge_attempt_is_capped_not_overflow():
    p = RetryPolicy(attempts=5, base=1.0, max_delay=30.0, jitter=False)
    assert p.delay(5000) == 30.0
    assert RetryPolicy(base=1.0, max_delay=float("inf")).delay(5000) < float("inf")


def test_jitter_is_equal_jitter_bounded_by_max_delay():
    low = RetryPolicy(base=1.0, max_delay=8.0, rng=lambda: 0.0)
    high = RetryPolicy(base=1.0, max_delay=8.0, rng=lambda: 1.0)
    assert [low.delay(i) for i in range(5)] == [0.5, 1.0, 2.0, 4.0, 4.0]
    assert [high.delay(i) for i in range(5)] == [1.0, 2.0, 4.0, 8.0, 8.0]
    assert high.delay(5000) == 8.0


def test_jitter_on_by_default_spreads_retries():
    p = RetryPolicy(base=1.0, max_delay=30.0)
    samples = [p.delay(3) for _ in range(200)]
    assert all(4.0 <= d <= 8.0 for d in samples) and len(set(samples)) > 1


def test_sleep_called_between_attempts_only(store):
    delays = []

    async def rec(d):
        delays.append(d)

    src = FakeSource({100: [RequestTimeout("t")] * 5})
    run(ingest_ledger(src, store, 100, RetryPolicy(3, 1, 30, jitter=False), sleep=rec))
    assert delays == [1, 2]
