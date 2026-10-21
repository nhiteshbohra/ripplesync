import asyncio

import pytest
from pymongo.errors import AutoReconnect

from conftest import FAST_RETRY, NOW, FakeSource, make_result, no_sleep
from xrpl_ingest.support import runcontrol as stopflag
from xrpl_ingest.backfill import run_backfill
from xrpl_ingest.errors import ConnectFailed, RpcError, StopRequested
from xrpl_ingest.fetcher import RetryPolicy
from xrpl_ingest.store import LAST_LEDGER_ID
from xrpl_ingest.tail import BIG_GAP, tail
from xrpl_ingest.transform import parse_ledger_result


def run_tail(src, store, iterations=3, stop=lambda: False, sleeps=None):
    async def sleep(d):
        if sleeps is not None:
            sleeps.append(d)

    return asyncio.run(tail(src, store, FAST_RETRY, poll_interval=2.0, stop_check=stop,
                            sleep=sleep, max_iterations=iterations))


def test_first_run_on_empty_db_starts_at_current_ledger(store):
    src = FakeSource(latest=500)
    assert run_tail(src, store, iterations=1) == 500
    assert src.calls == [500]
    assert store.get_checkpoint() == 500


def store_ledgers(store, indices):
    for idx in indices:
        store.save_ledger_bundle(*parse_ledger_result(idx, make_result(idx), NOW))


def test_no_checkpoint_resumes_after_highest_stored_ledger(store, caplog):
    store_ledgers(store, [90, 100])  # e.g. left by a backfill
    src = FakeSource(latest=103)
    with caplog.at_level("INFO", logger="xrpl_ingest.tail"):
        assert run_tail(src, store, iterations=1) == 103
    assert src.calls == [101, 102, 103]
    assert any("highest stored ledger 100" in m for m in caplog.messages)


def test_checkpoint_wins_over_highest_stored_ledger(store):
    store_ledgers(store, [100])
    store.set_checkpoint(98)
    src = FakeSource(latest=100)
    run_tail(src, store, iterations=1)
    assert src.calls == [99, 100]


def test_backfill_then_tail_handoff_leaves_no_gap(cfg, store):
    lo, hi = 32570, 32599
    run_backfill(cfg, lo, hi, source=FakeSource(latest=hi, ranges=f"{lo}-{hi}"), store=store,
                 inline=True)
    src = FakeSource(latest=hi + 5)
    assert run_tail(src, store, iterations=1) == hi + 5
    assert src.calls == list(range(hi + 1, hi + 6))
    assert store.find_gaps(lo, hi + 5) == []


def stop_after_fetches(src, n):
    return lambda: len(src.calls) >= n


def test_no_checkpoint_far_behind_starts_at_latest_and_prints_backfill_command(store, caplog):
    """Auditor r14: after a smoke-test backfill ending at 33570, a first tail must not crawl
    ~100M ledgers one by one; it goes live and says how to fill the hole."""
    store_ledgers(store, [33570])
    src = FakeSource(latest=101_000_000)
    with caplog.at_level("INFO", logger="xrpl_ingest.tail"):
        run_tail(src, store, stop=stop_after_fetches(src, 1), iterations=None)
    assert src.calls == [101_000_000]
    cmd = "python -m xrpl_ingest backfill --start 33571 --end 100999999"
    assert [r for r in caplog.records if r.levelname == "WARNING" and cmd in r.getMessage()]


@pytest.mark.parametrize("gap,first_fetch", [(BIG_GAP, 101), (BIG_GAP + 1, 100 + BIG_GAP + 1)])
def test_no_checkpoint_gap_boundary(store, gap, first_fetch):
    store_ledgers(store, [100])
    src = FakeSource(latest=100 + gap)
    run_tail(src, store, stop=stop_after_fetches(src, 1), iterations=None)
    assert src.calls == [first_fetch]  # gap == BIG_GAP still resumes; one more goes live


def test_checkpoint_far_behind_still_catches_up_and_suggests_backfill(store, caplog):
    store.set_checkpoint(100)
    src = FakeSource(latest=100 + BIG_GAP + 5)
    with caplog.at_level("WARNING", logger="xrpl_ingest.tail"):
        run_tail(src, store, stop=stop_after_fetches(src, 2), iterations=None)
    assert src.calls == [101, 102]  # the checkpoint wins: no skipping
    assert any("consider `backfill`" in m for m in caplog.messages)


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


def test_tail_survives_more_than_1024_node_failures(store):
    store.set_checkpoint(99)
    src = FakeSource(latest=100)
    src.latest_errors = [ConnectFailed("down")] * 1100
    sleeps = []
    retry = RetryPolicy(attempts=5, base=1.0, max_delay=30.0, jitter=False)
    last = asyncio.run(tail(src, store, retry, poll_interval=2.0, stop_check=lambda: False,
                            sleep=lambda d: no_sleep(sleeps.append(d)), max_iterations=1101))
    assert last == 100 and len(sleeps) == 1100 and sleeps[-1] == 30.0


def test_sigterm_stops_tail_like_the_stop_file(store, tmp_path):
    store.set_checkpoint(10)

    class SignalAfterTwo(FakeSource):
        async def get_ledger(self, index):
            if len(self.calls) == 2:
                stopflag.request_stop(15, None)
            return await super().get_ledger(index)

    src = SignalAfterTwo(latest=100)
    path = str(tmp_path / "missing.txt")
    last = run_tail(src, store, stop=lambda: stopflag.stop_requested(path), iterations=None)
    assert last == 13 and store.get_checkpoint() == 13  # in-flight ledger finished, then stop


def test_stop_while_node_unreachable_exits_without_backoff(store):
    store.set_checkpoint(99)
    stopped = {"v": False}
    src = FakeSource(latest=100)

    class StopWhileConnecting(list):
        def __bool__(self):
            return True

        def pop(self, i):
            stopped["v"] = True
            return StopRequested("stopped while connecting")

    src.latest_errors = StopWhileConnecting()
    sleeps = []
    assert run_tail(src, store, iterations=10, stop=lambda: stopped["v"], sleeps=sleeps) == 99
    assert sleeps == []  # no backoff sleep between the stop and the exit


def test_stop_during_unrecorded_ledger_exits_without_backoff(store):
    store.set_checkpoint(99)
    stopped = {"v": False}

    class StopOnLedger(FakeSource):
        async def get_ledger(self, index):
            stopped["v"] = True
            raise StopRequested("stopped while connecting")

    sleeps = []
    assert run_tail(StopOnLedger(latest=100), store, iterations=10, stop=lambda: stopped["v"],
                    sleeps=sleeps) == 99
    assert store.get_checkpoint() == 99 and sleeps == [] and store.list_failed() == []


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
    # simulate a lost checkpoint write (set_checkpoint itself never moves back)
    store.meta.update_one({"_id": LAST_LEDGER_ID}, {"$set": {"index": 98}})
    src2 = FakeSource(latest=100)
    run_tail(src2, store, iterations=1)
    assert src2.calls == [99, 100]
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
    assert src.calls == [99, 99, 100]  # Mongo retries reuse the prepared ledger; tail retries after recovery
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

    retry = RetryPolicy(attempts=1, base=1.0, max_delay=100.0, jitter=False)
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

    retry = RetryPolicy(attempts=1, base=1.0, max_delay=float("inf"), jitter=False)
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
    assert src.calls == [99]  # Mongo retries do not download the ledger again
