import asyncio

from pymongo.errors import AutoReconnect

from conftest import FAST_RETRY, FakeSource, no_sleep
from xrpl_ingest.errors import ConnectFailed, RpcError
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
