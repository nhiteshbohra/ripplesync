import asyncio
import dataclasses
import os
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool

import pytest
from pymongo.errors import AutoReconnect
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from conftest import FAST_RETRY, FakeSource, FakeWs, make_result, no_sleep
from xrpl_ingest import backfill, stopflag
from xrpl_ingest.backfill import (ChunkResult, discover_node, process_chunk, retry_failed,
                                  run_backfill)
from xrpl_ingest.errors import IngestError, RequestTimeout, RpcError
from xrpl_ingest.source import WsSource


def run(coro):
    return asyncio.run(coro)


def chunk_run(source, store, chunk, stop=lambda: False, concurrency=3):
    return run(process_chunk(source, store, chunk, concurrency=concurrency, retry=FAST_RETRY,
                             stop_check=stop, sleep=no_sleep))


def test_chunk_fetches_every_ledger_once_and_marks_done(store):
    src = FakeSource()
    r = chunk_run(src, store, (10, 19))
    assert r.ok == 10 and r.failed == 0 and r.completed
    assert sorted(src.calls) == list(range(10, 20))
    assert store.done_chunks(0, 100) == {(10, 19)}
    assert store.ledgers.count_documents({}) == 10


def test_chunk_with_failures_still_marked_done_and_failures_recorded(store):
    src = FakeSource({13: [RpcError("lgrNotFound")], 15: [RequestTimeout("t")] * 9})
    r = chunk_run(src, store, (10, 19))
    assert (r.ok, r.failed) == (8, 2)
    assert store.list_failed() == [13, 15]
    assert store.done_chunks(0, 100) == {(10, 19)}


def test_stop_flag_halts_chunk_and_leaves_it_unmarked(store):
    src = FakeSource()
    seen = {"n": 0}

    def stop():
        seen["n"] += 1
        return seen["n"] > 4

    r = chunk_run(src, store, (10, 19), stop=stop, concurrency=1)
    assert not r.completed and r.ok == 4
    assert store.done_chunks(0, 100) == set()


def test_single_ledger_chunk_and_high_concurrency(store):
    r = chunk_run(FakeSource(), store, (5, 5), concurrency=50)
    assert r.ok == 1


def test_run_backfill_inline_end_to_end_and_resume(cfg, store):
    lo, hi = 32570, 32570 + 24
    src = FakeSource(latest=hi, ranges=f"{lo}-{hi}")
    summary = run_backfill(cfg, lo, hi, source=src, store=store, inline=True)
    assert summary["ledgers_ok"] == 25 and summary["ledgers_failed"] == 0
    assert store.ledgers.count_documents({}) == 25
    assert store.find_gaps(lo, hi) == []
    assert store.tx_count_mismatches(lo, hi) == []

    src2 = FakeSource(latest=hi, ranges=f"{lo}-{hi}")
    summary2 = run_backfill(cfg, lo, hi, source=src2, store=store, inline=True)
    assert summary2["chunks_run"] == 0 and src2.calls == []  # fully resumed


def test_run_backfill_partial_resume_only_redoes_missing_chunks(cfg, store):
    lo, hi = 32570, 32570 + 29
    src = FakeSource(latest=hi, ranges=f"{lo}-{hi}")
    run_backfill(cfg, lo, hi, source=src, store=store, inline=True)
    store.meta.delete_one({"_id": "backfill_chunk:32580-32589"})
    src2 = FakeSource(latest=hi, ranges=f"{lo}-{hi}")
    summary = run_backfill(cfg, lo, hi, source=src2, store=store, inline=True)
    assert summary["chunks_run"] == 1 and sorted(src2.calls) == list(range(32580, 32590))


def test_run_backfill_skips_ledgers_node_does_not_have(cfg, store):
    lo, hi = 32570, 32570 + 19
    src = FakeSource(latest=hi, ranges=f"{lo}-{lo+4},{lo+10}-{hi}")
    summary = run_backfill(cfg, lo, hi, source=src, store=store, inline=True)
    assert summary["unavailable_ranges"] == [(lo + 5, lo + 9)]
    assert summary["ledgers_ok"] == 15
    assert store.find_gaps(lo, hi) == [(lo + 5, lo + 9)]


def test_run_backfill_default_end_is_latest_validated(cfg, store):
    lo = 32570
    src = FakeSource(latest=lo + 4, ranges=f"{lo}-{lo+100}")
    summary = run_backfill(cfg, lo, None, source=src, store=store, inline=True)
    assert summary["ledgers_ok"] == 5


class NoValidatedNode(FakeSource):
    """Node still syncing: server_info has no validated_ledger at all."""

    async def server_info(self):
        return {"complete_ledgers": "empty"}


def test_discover_node_without_validated_ledger_or_ranges():
    assert run(discover_node(NoValidatedNode())) == ([], 0)


def test_run_backfill_default_end_on_node_without_validated_ledger(cfg, store):
    with pytest.raises(IngestError, match="no validated ledger"):
        run_backfill(cfg, 32570, None, source=NoValidatedNode(), store=store, inline=True)


def test_run_backfill_node_with_empty_complete_ledgers(cfg, store):
    src = FakeSource(latest=40000, ranges="empty")
    summary = run_backfill(cfg, 32570, 32579, source=src, store=store, inline=True)
    assert summary["chunks_run"] == 0 and summary["unavailable_ranges"] == [(32570, 32579)]
    assert src.calls == []


def test_malformed_complete_ledgers_is_a_clear_node_error(cfg, store):
    src = FakeSource(latest=40000, ranges="32570-abc")
    with pytest.raises(IngestError, match="malformed complete_ledgers.*32570-abc"):
        run_backfill(cfg, 32570, 32579, source=src, store=store, inline=True)


def test_run_backfill_rejects_start_below_first_ledger(cfg, store):
    with pytest.raises(ValueError, match="32570"):
        run_backfill(cfg, 1, 10, source=FakeSource(), store=store, inline=True)


def test_run_backfill_rejects_end_before_start(cfg, store):
    with pytest.raises(ValueError, match="end"):
        run_backfill(cfg, 40000, 39000, source=FakeSource(latest=50000, ranges="32570-50000"),
                     store=store, inline=True)


def test_run_backfill_stops_on_status_file(cfg, store, tmp_path):
    flag = tmp_path / "s.txt"
    flag.write_text("current_process=0")
    cfg = dataclasses.replace(cfg, process_status_file=str(flag))
    lo, hi = 32570, 32570 + 29
    src = FakeSource(latest=hi, ranges=f"{lo}-{hi}")
    summary = run_backfill(cfg, lo, hi, source=src, store=store, inline=True)
    assert summary["chunks_incomplete"] == 1 and src.calls == []
    assert store.done_chunks(lo, hi) == set()


def test_retry_failed_clears_recovered_and_reports_remaining(store):
    store.record_failure(5, "x")
    store.record_failure(6, "x")
    src = FakeSource({6: [RpcError("lgrNotFound")] * 20})
    remaining = run(retry_failed(src, store, FAST_RETRY, max_rounds=3, sleep=no_sleep))
    assert remaining == 1 and store.list_failed() == [6]
    assert store.ledgers.count_documents({"block_number": 5}) == 1


def test_retry_failed_nothing_to_do(store):
    src = FakeSource()
    assert run(retry_failed(src, store, FAST_RETRY, sleep=no_sleep)) == 0
    assert src.calls == []


def test_retry_failed_recovers_after_transient_rounds(store):
    store.record_failure(9, "x")
    src = FakeSource({9: [RpcError("lgrNotFound"), RpcError("lgrNotFound"), make_result(9)]})
    assert run(retry_failed(src, store, FAST_RETRY, max_rounds=5, sleep=no_sleep)) == 0


def test_retry_failed_honours_stop(store):
    store.record_failure(9, "x")
    src = FakeSource()
    remaining = run(retry_failed(src, store, FAST_RETRY, stop_check=lambda: True, sleep=no_sleep))
    assert remaining == 1 and src.calls == []


POISON = 33905
SHARED_SOCKET_DEATH = ("ConnectionLost: connection to ws://n closed: ConnectionClosedError: "
                       "sent 1009 (message too big) frame exceeds limit of 1000000 bytes")


def poison_node():
    """Real WsSource over a fake socket: requesting POISON closes the socket with 1009 (like
    an oversized ledger), failing every other request in flight on it. Others answer after
    20 ms, so they are in flight whenever POISON is fetched concurrently."""
    async def connect(url, **_):
        def handler(req):
            if req["command"] == "server_info":
                return {"id": req["id"], "status": "success", "result": {"info": {}}}
            idx = req["ledger_index"]
            if idx == POISON:
                ws.fail(ConnectionClosedError(None, Close(1009, "frame exceeds limit")))
                return None
            reply = {"id": req["id"], "status": "success", "result": make_result(idx)}
            asyncio.get_running_loop().call_later(0.02, ws.push, reply)
            return None
        ws = FakeWs(handler)
        return ws
    return WsSource(["ws://n"], connect_fn=connect, sleep=no_sleep, backoff_base=0,
                    request_timeout=5)


def test_list_failed_with_reasons(store):
    store.record_failure(7, "RpcError: lgrNotFound")
    store.record_failure(3, "ConnectionLost: x")
    assert store.list_failed_with_reasons() == [(3, "ConnectionLost: x"), (7, "RpcError: lgrNotFound")]
    assert store.list_failed() == [3, 7]  # unchanged for existing callers


@pytest.mark.parametrize("reason,suspect", [
    (SHARED_SOCKET_DEATH, True), ("ConnectionLost: connection dropped", True),
    ("ConnectionClosedError: received 1009 (message too big)", True),
    ("RpcError: lgrNotFound", False), ("RequestTimeout: ledger timed out after 30s", False),
    ("ConnectionClosedError: sent 1009 (message too big) frame exceeds limit", True),
    # false positives of the old substring match: ledger numbers containing 1009
    ("MalformedLedger: ledger 31009 has no ledger_hash", False),
    ("RpcError: lgrNotFound: ledger 1009 not found", False),
    ("NotValidated: ledger 10090 is not validated", False),
])
def test_shared_socket_suspects(reason, suspect):
    assert backfill.may_have_killed_neighbours(reason) is suspect


def test_retry_failed_isolates_poison_ledger_so_neighbours_recover(store):
    """Auditor C stage 18: one ledger stays oversized; the neighbours that died with it on
    the shared socket must recover instead of dying with it again every round."""
    for idx in range(POISON, POISON + 4):
        store.record_failure(idx, SHARED_SOCKET_DEATH)
    src = poison_node()
    remaining = run(retry_failed(src, store, FAST_RETRY, max_rounds=3, concurrency=4,
                                 sleep=no_sleep))
    run(src.close())
    assert store.list_failed() == [POISON] and remaining == 1
    assert store.ledgers.count_documents({"block_number": {"$gt": POISON}}) == 3


def test_retry_failed_runs_ordinary_failures_concurrently_first(store, monkeypatch):
    store.record_failure(5, "ConnectionLost: connection dropped")
    for idx in (6, 7):
        store.record_failure(idx, "RpcError: lgrNotFound")
    src = FakeSource()
    assert run(retry_failed(src, store, FAST_RETRY, max_rounds=1, sleep=no_sleep)) == 0
    assert src.calls[-1] == 5 and sorted(src.calls[:2]) == [6, 7]


def test_chunk_with_unrecorded_failure_is_not_marked_done(store, monkeypatch):
    def boom(*a):
        raise AutoReconnect("down")
    monkeypatch.setattr(store, "record_failure", boom)
    r = chunk_run(FakeSource({13: [RpcError("lgrNotFound")]}), store, (10, 19))
    assert (r.ok, r.failed, r.unrecorded) == (9, 1, 1)
    assert store.done_chunks(0, 100) == set()  # next backfill redoes the chunk


def test_summary_and_chunk_log_count_unmarked_chunks(cfg, store, monkeypatch, caplog):
    def boom(*a):
        raise AutoReconnect("down")
    monkeypatch.setattr(store, "record_failure", boom)
    lo, hi = 32570, 32570 + 19
    src = FakeSource({lo + 3: [RpcError("lgrNotFound")]}, latest=hi, ranges=f"{lo}-{hi}")
    with caplog.at_level("INFO", logger="xrpl_ingest.backfill"):
        summary = run_backfill(cfg, lo, hi, source=src, store=store, inline=True)
    assert summary["chunks_unmarked"] == 1 and summary["chunks_run"] == 2
    assert any("chunk 32570-32579" in m and "unrecorded=1" in m for m in caplog.messages)


# -- spawn process-pool path. Worker functions must be module-level so they pickle. --------

POOL_LO, POOL_HI = 32570, 32570 + 79  # 8 chunks of 10


def _ok_chunk(args):
    _cfg, chunk = args
    return ChunkResult(chunk, ok=chunk[1] - chunk[0] + 1)


def _die_on_third_chunk(args):
    if args[1][0] == POOL_LO + 20:
        os._exit(137)  # like an OOM kill: no exception, the process just vanishes
    return _ok_chunk(args)


def pool_run(cfg, store, **cfg_changes):
    cfg = dataclasses.replace(cfg, num_processes=2, **cfg_changes)
    src = FakeSource(latest=POOL_HI, ranges=f"{POOL_LO}-{POOL_HI}")
    return run_backfill(cfg, POOL_LO, POOL_HI, source=src, store=store)


def test_pool_runs_every_chunk(cfg, store, monkeypatch):
    monkeypatch.setattr(backfill, "_run_chunk_in_process", _ok_chunk)
    summary = pool_run(cfg, store)
    assert summary["chunks_run"] == 8 and summary["chunks_incomplete"] == 0
    assert summary["ledgers_ok"] == 80


def test_pool_worker_death_stops_backfill_and_counts_lost_chunks(cfg, store, monkeypatch, caplog):
    monkeypatch.setattr(backfill, "_run_chunk_in_process", _die_on_third_chunk)
    with caplog.at_level("ERROR", logger="xrpl_ingest.backfill"):
        summary = pool_run(cfg, store)  # old imap_unordered code hung here forever
    assert summary["chunks_run"] < 8
    assert summary["chunks_incomplete"] == 8 - summary["chunks_run"]
    assert any("worker process died" in m for m in caplog.messages)


class InstantPool:
    """Stands in for ProcessPoolExecutor: futures complete at submit; chunk BAD's worker
    'dies' (BrokenProcessPool), like the OOM case, in the same batch as finished chunks."""
    BAD = POOL_LO + 10

    def __init__(self, max_workers, mp_context, **kw):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def submit(self, fn, args):
        fut = Future()
        _cfg, chunk = args
        if chunk[0] == self.BAD:
            fut.set_exception(BrokenProcessPool("A process in the process pool was terminated"))
        else:
            fut.set_result(_ok_chunk(args))
        return fut


def broken_first(fs, return_when, timeout=None):
    """wait() stand-in returning the whole batch, the broken future first."""
    return sorted(fs, key=lambda f: f.exception() is None), set()


def test_pool_broken_batch_keeps_results_of_chunks_that_finished(cfg, store, monkeypatch):
    monkeypatch.setattr(backfill, "ProcessPoolExecutor", InstantPool)
    monkeypatch.setattr(backfill, "wait", broken_first)
    cfg = dataclasses.replace(cfg, num_processes=4)
    src = FakeSource(latest=POOL_HI, ranges=f"{POOL_LO}-{POOL_HI}")
    summary = run_backfill(cfg, POOL_LO, POOL_HI, source=src, store=store)
    # first window = 4 chunks: 3 finished (and marked done in their workers), 1 broke
    assert summary["chunks_run"] == 3 and summary["ledgers_ok"] == 30
    assert summary["chunks_incomplete"] == 8 - 3


@pytest.mark.parametrize("content", ["current_process=0", "backfill=0", "BACKFILL = 0  # pause"])
def test_pool_stop_flag_cancels_remaining_chunks(cfg, store, tmp_path, content):
    flag = tmp_path / "s.txt"
    flag.write_text(content)
    summary = pool_run(cfg, store, process_status_file=str(flag))
    # the parent sees the stop before handing out any chunk; old code ran all 8
    assert summary["chunks_run"] == 0
    assert summary["chunks_incomplete"] == 8  # never started: counted, so the exit code is 1
    assert store.done_chunks(POOL_LO, POOL_HI) == set()


def test_pool_other_commands_stop_key_does_not_stop_backfill(cfg, store, tmp_path, monkeypatch):
    monkeypatch.setattr(backfill, "_run_chunk_in_process", _ok_chunk)
    flag = tmp_path / "s.txt"
    flag.write_text("tail=0\nretry-failed=0\n")
    summary = pool_run(cfg, store, process_status_file=str(flag))
    assert summary["chunks_run"] == 8 and summary["chunks_incomplete"] == 0


def _signal_after_first_check():
    calls = []

    def stop_signalled():
        calls.append(1)
        return len(calls) > 1  # SIGTERM / Ctrl+C reached the parent after the first window
    return stop_signalled


def test_pool_parent_stops_submitting_after_sigterm(cfg, store, monkeypatch):
    monkeypatch.setattr(backfill, "_run_chunk_in_process", _ok_chunk)
    monkeypatch.setattr(backfill, "stop_signalled", _signal_after_first_check())
    summary = pool_run(cfg, store)
    assert summary["chunks_run"] == 2  # the in-flight ones finish, nothing new starts
    assert summary["chunks_incomplete"] == 6  # never started: counted, so the exit code is 1


def _slow_chunk_until_stopped(args):
    """Pool worker: runs until the parent sets the shared stop event (or 30 s)."""
    import time
    cfg, chunk = args
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if backfill._worker_stop_event is not None and backfill._worker_stop_event.is_set():
            return ChunkResult(chunk, completed=False)
        time.sleep(0.02)
    return ChunkResult(chunk, ok=chunk[1] - chunk[0] + 1)


def test_pool_parent_stop_reaches_workers_through_shared_event(cfg, store, monkeypatch, tmp_path):
    """Workers ignore SIGINT and never read a stop the parent got by signal; the parent
    tells them through the initializer's Event. Old code: workers ran their chunk to the end."""
    import time
    monkeypatch.setattr(backfill, "_run_chunk_in_process", _slow_chunk_until_stopped)
    monkeypatch.setattr(backfill, "stop_signalled", _signal_after_first_check())
    t0 = time.monotonic()
    summary = pool_run(cfg, store)
    assert time.monotonic() - t0 < 25
    assert summary["chunks_run"] == 2 and summary["chunks_incomplete"] == 8


def test_pool_second_ctrl_c_terminates_workers_and_raises(cfg, store, monkeypatch):
    import time
    monkeypatch.setattr(backfill, "_run_chunk_in_process", _slow_chunk_until_stopped)
    real_wait = backfill.wait
    calls = []

    def wait_then_interrupt(fs, timeout=None, return_when=None):
        calls.append(1)
        if len(calls) == 2:
            raise KeyboardInterrupt  # what the second Ctrl+C does in the parent
        return real_wait(fs, timeout=timeout, return_when=return_when)
    monkeypatch.setattr(backfill, "wait", wait_then_interrupt)
    t0 = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        pool_run(cfg, store)
    assert time.monotonic() - t0 < 20  # did not wait for the 30 s chunks


def test_pool_logs_progress_line_with_eta(cfg, store, monkeypatch, caplog):
    monkeypatch.setattr(backfill, "_run_chunk_in_process", _ok_chunk)
    with caplog.at_level("INFO", logger="xrpl_ingest.backfill"):
        pool_run(cfg, store)
    lines = [m for m in caplog.messages if "chunks " in m and "ledgers/s" in m]
    assert len(lines) == 8 and "chunks 8/8 (100.0%)" in lines[-1] and "ETA 0s" in lines[-1]


def test_pool_low_disk_stops_like_the_stop_file(cfg, store, monkeypatch):
    calls = []

    def db_stats():
        calls.append(1)
        free = 50 if len(calls) == 1 else 1  # start check ok, then the disk fills up
        return {"fsTotalSize": 100 * 1024 ** 3, "fsUsedSize": (100 - free) * 1024 ** 3}
    monkeypatch.setattr(store, "db_stats", db_stats, raising=False)
    monkeypatch.setattr(backfill.DiskGuard, "__init__", _guard_no_interval)
    monkeypatch.setattr(backfill, "_run_chunk_in_process", _ok_chunk)
    summary = pool_run(cfg, store, min_free_gb=10)
    assert summary["low_disk"] is True and summary["chunks_incomplete"] > 0


def test_backfill_refuses_to_start_on_low_disk(cfg, store, monkeypatch):
    from xrpl_ingest.diskspace import LowDiskSpace
    monkeypatch.setattr(store, "db_stats", lambda: {"fsTotalSize": 10 * 1024 ** 3,
                                                    "fsUsedSize": 9.5 * 1024 ** 3}, raising=False)
    src = FakeSource(latest=POOL_HI, ranges=f"{POOL_LO}-{POOL_HI}")
    with pytest.raises(LowDiskSpace, match="MIN_FREE_GB=1"):
        run_backfill(dataclasses.replace(cfg, min_free_gb=1), POOL_LO, POOL_HI, source=src,
                     store=store, inline=True)
    assert src.calls == []


_real_guard_init = backfill.DiskGuard.__init__


def _guard_no_interval(self, store, min_free_gb, **kw):
    _real_guard_init(self, store, min_free_gb, interval=0)


def test_sigterm_mid_chunk_stops_inline_backfill_unmarked(cfg, store):
    lo, hi = 32570, 32570 + 19

    class SignalAfterThree(FakeSource):
        async def get_ledger(self, index):
            if len(self.calls) == 3:
                stopflag.request_stop(15, None)
            return await super().get_ledger(index)

    src = SignalAfterThree(latest=hi, ranges=f"{lo}-{hi}")
    summary = run_backfill(cfg, lo, hi, source=src, store=store, inline=True)
    assert summary["chunks_incomplete"] == 1 and summary["chunks_run"] == 1
    assert store.done_chunks(lo, hi) == set() and len(src.calls) < 10


def test_backfill_sources_get_the_stop_check(cfg, store, tmp_path, monkeypatch):
    monkeypatch.setattr(stopflag, "CHECK_INTERVAL", 0)  # re-read the file on every check
    flag = tmp_path / "s.txt"
    cfg = dataclasses.replace(cfg, process_status_file=str(flag))
    seen = []

    def from_config(_cfg, **kw):
        seen.append(kw.get("stop_check"))
        return FakeSource(latest=32579, ranges="32570-32579")

    monkeypatch.setattr(backfill.WsSource, "from_config", from_config)
    monkeypatch.setattr(backfill, "_store_cache", store)
    monkeypatch.setattr(backfill, "setup_logging", lambda level: None)
    monkeypatch.setattr(backfill, "install_stop_signal", lambda: True)
    run_backfill(cfg, 32570, 32579, store=store, inline=True)  # discovery source
    backfill._run_chunk_in_process((cfg, (32570, 32579)))  # pool worker source
    assert len(seen) == 2 and all(s is not None and s() is False for s in seen)
    flag.write_text("current_process=0")
    assert all(s() is True for s in seen)


def test_pool_worker_installs_sigterm_handler_once(cfg, store, tmp_path, monkeypatch):
    flag = tmp_path / "s.txt"
    flag.write_text("current_process=0")
    cfg = dataclasses.replace(cfg, process_status_file=str(flag))
    calls = []
    monkeypatch.setattr(backfill, "install_stop_signal", lambda: calls.append(1) or True)
    monkeypatch.setattr(backfill, "setup_logging", lambda level: None)
    monkeypatch.setattr(backfill, "_store_cache", None)
    monkeypatch.setattr(backfill.MongoStore, "from_config", lambda _cfg: store)
    for chunk in [(10, 19), (20, 29)]:
        backfill._run_chunk_in_process((cfg, chunk))
    assert calls == [1]


def test_pool_worker_configures_logging_once_per_process(cfg, store, tmp_path, monkeypatch):
    flag = tmp_path / "s.txt"
    flag.write_text("current_process=0")  # chunks stop at once; no node needed
    cfg = dataclasses.replace(cfg, process_status_file=str(flag), log_level="DEBUG")
    calls = []
    monkeypatch.setattr(backfill, "setup_logging", calls.append)
    monkeypatch.setattr(backfill, "_store_cache", None)
    monkeypatch.setattr(backfill.MongoStore, "from_config", lambda _cfg: store)
    for chunk in [(10, 19), (20, 29)]:
        assert backfill._run_chunk_in_process((cfg, chunk)).completed is False
    assert calls == ["DEBUG"]  # spawn children start with no logging config at all


def test_mark_chunk_done_error_does_not_raise_and_leaves_chunk_unmarked(store, monkeypatch):
    def boom(*a):
        raise AutoReconnect("down")
    monkeypatch.setattr(store, "mark_chunk_done", boom)
    r = chunk_run(FakeSource(), store, (10, 19))
    assert r.ok == 10 and r.failed == 0
    assert store.done_chunks(0, 100) == set()
