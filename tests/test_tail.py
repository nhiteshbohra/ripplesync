import asyncio

from pymongo.errors import AutoReconnect

from conftest import FAST_RETRY, FakeSource, no_sleep
from xrpl_ingest.errors import ConnectFailed, RpcError
from xrpl_ingest.fetcher import RetryPolicy
from xrpl_ingest.tail import tail


def run_tail(src, store, iterations=3, stop=lambda: False, sleeps=None):
    async def sleep(d):
        if sleeps is not None:
            sleeps.append(d)

    return asyncio.run(tail(src, store, FAST_RETRY, poll_interval=2.0, stop_check=stop,
                            sleep=sleep, max_iterations=iterations))


def test_first_run_starts_at_current_ledger(store):
    src = FakeSource(latest=500)
    assert run_tail(src, store, iterations=1) == 500
    assert src.calls == [500]
    assert store.get_checkpoint() == 500


def test_resumes_from_checkpoint_and_catches_up(store):
    store.set_checkpoint(97)
    src = FakeSource(latest=100)
    assert run_tail(src, store, iterations=1) == 100
    assert src.calls == [98, 99, 100]


def test_no_new_ledger_sleeps_poll_interval(store):
    store.set_checkpoint(100)
    sleeps = []
    src = FakeSource(latest=100)
    run_tail(src, store, iterations=2, sleeps=sleeps)
    assert src.calls == [] and sleeps == [2.0, 2.0]


def test_failed_ledger_recorded_and_tail_moves_on(store):
    store.set_checkpoint(98)
    src = FakeSource({99: [RpcError("lgrNotFound")]}, latest=100)
    assert run_tail(src, store, iterations=1) == 100
    assert store.list_failed() == [99]
    assert store.ledgers.count_documents({"block_number": 100}) == 1
    assert store.get_checkpoint() == 100


def test_node_unreachable_backs_off_then_recovers(store):
    store.set_checkpoint(99)
    src = FakeSource(latest=100)
    src.latest_errors = [ConnectFailed("down"), RpcError("noNetwork")]
    sleeps = []
    assert run_tail(src, store, iterations=3, sleeps=sleeps) == 100
    assert len(sleeps) == 2


def test_stop_flag_halts_immediately(store):
    store.set_checkpoint(10)
    src = FakeSource(latest=100)
    assert run_tail(src, store, stop=lambda: True) == 10
    assert src.calls == []


def test_stop_mid_catchup_keeps_progress(store):
    store.set_checkpoint(10)
    src = FakeSource(latest=100)
    n = {"c": 0}

    def stop():
        n["c"] += 1
        return n["c"] > 4  # loop check + 3 inner checks pass

    last = run_tail(src, store, stop=stop)
    assert last == 13 and store.get_checkpoint() == 13


def test_checkpoint_write_failure_does_not_crash(store, monkeypatch):
    store.set_checkpoint(99)

    def boom(_):
        raise AutoReconnect("down")

    monkeypatch.setattr(store, "set_checkpoint", boom)
    src = FakeSource(latest=100)
    assert run_tail(src, store, iterations=1) == 100
    assert store.ledgers.count_documents({"block_number": 100}) == 1


def test_reprocessing_after_lost_checkpoint_is_idempotent(store):
    src = FakeSource(latest=100)
    store.set_checkpoint(98)
    run_tail(src, store, iterations=1)
    store.set_checkpoint(98)  # simulate lost checkpoint write
    run_tail(FakeSource(latest=100), store, iterations=1)
    assert store.ledgers.count_documents({}) == 2
    assert store.txs.count_documents({}) == 4


def mongo_outage(store, monkeypatch):
    """Ledger, failure and checkpoint writes raise AutoReconnect while state["down"] is true."""
    state = {"down": True}

    def guard(method):
        def wrapper(*args, **kwargs):
            if state["down"]:
                raise AutoReconnect("mongo down")
            return method(*args, **kwargs)
        return wrapper

    for name in ("save_ledger_bundle", "record_failure", "set_checkpoint"):
        monkeypatch.setattr(store, name, guard(getattr(store, name)))
    return state


def test_unrecorded_failure_holds_checkpoint_and_retries(store, monkeypatch):
    store.set_checkpoint(98)
    state = mongo_outage(store, monkeypatch)
    seen = []

    async def sleep(_):
        seen.append(store.get_checkpoint())
        state["down"] = False  # Mongo comes back during the backoff

    src = FakeSource(latest=100)
    last = asyncio.run(tail(src, store, FAST_RETRY, poll_interval=2.0, stop_check=lambda: False,
                            sleep=sleep, max_iterations=2))
    assert seen == [98]  # held at 98 while 99 could be neither stored nor recorded
    assert src.calls == [99] * FAST_RETRY.attempts + [99, 100]  # stalled on 99, then retried it
    assert last == 100 and store.get_checkpoint() == 100
    assert store.ledgers.count_documents({"block_number": 99}) == 1
    assert store.list_failed() == []


def test_unrecorded_failure_backoff_grows_and_resets(store, monkeypatch):
    store.set_checkpoint(98)
    state = mongo_outage(store, monkeypatch)
    sleeps = []

    async def sleep(d):
        sleeps.append(d)
        if len(sleeps) == 3:
            state["down"] = False

    retry = RetryPolicy(attempts=1, base=1.0, max_delay=100.0)
    asyncio.run(tail(FakeSource(latest=100), store, retry, poll_interval=0.5,
                     stop_check=lambda: False, sleep=sleep, max_iterations=5))
    assert sleeps == [1.0, 2.0, 4.0, 0.5]  # three stalls, recovery, then one idle poll
    assert store.get_checkpoint() == 100


def test_unrecorded_failure_backoff_is_bounded(store, monkeypatch):
    store.set_checkpoint(98)
    mongo_outage(store, monkeypatch)  # stays down
    sleeps = []

    async def sleep(d):
        sleeps.append(d)

    retry = RetryPolicy(attempts=1, base=1.0, max_delay=float("inf"))
    asyncio.run(tail(FakeSource(latest=100), store, retry, poll_interval=0.5,
                     stop_check=lambda: False, sleep=sleep, max_iterations=40))
    assert len(sleeps) == 40 and sleeps[-1] == sleeps[-2] < float("inf")
    assert store.get_checkpoint() == 98


def test_stop_flag_during_unrecorded_failure_backoff(store, monkeypatch):
    store.set_checkpoint(98)
    mongo_outage(store, monkeypatch)  # stays down
    stopped = {"v": False}

    async def sleep(_):
        stopped["v"] = True  # stop file flipped while tail is backing off

    src = FakeSource(latest=100)
    last = asyncio.run(tail(src, store, FAST_RETRY, poll_interval=2.0,
                            stop_check=lambda: stopped["v"], sleep=sleep, max_iterations=10))
    assert last == 98 and store.get_checkpoint() == 98
    assert src.calls == [99] * FAST_RETRY.attempts
