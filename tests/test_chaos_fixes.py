"""Regression checks for the ingestion chaos-review findings."""
import asyncio
import io
import logging
import logging.handlers
import queue
import sys

import pytest
from pymongo import MongoClient
from pymongo.errors import AutoReconnect

from conftest import FAST_RETRY, FakeSource, FakeWs, make_result
from xrpl_ingest import backfill
from xrpl_ingest.cli import backfill_exit_code
from xrpl_ingest.config import Config
from xrpl_ingest.errors import ConfigError, MalformedLedger
from xrpl_ingest.fetcher import ingest_ledger
from xrpl_ingest.ledger_ranges import plan_chunks
from xrpl_ingest.repair import node_tx_hashes
from xrpl_ingest.source import WsSource
from xrpl_ingest.store import LAST_LEDGER_ID, MongoStore
from xrpl_ingest.support import logsetup
from xrpl_ingest.support.files import format_range_progress, range_progress
from xrpl_ingest.transform import parse_ledger_result


@pytest.mark.parametrize("parse", [parse_ledger_result, node_tx_hashes])
def test_both_ledger_parsers_require_requested_transactions(parse):
    result = make_result(32570, 0)
    assert parse(32570, result) is not None  # explicit empty list is valid
    del result["ledger"]["transactions"]
    with pytest.raises(MalformedLedger, match="transactions"):
        parse(32570, result)


def test_incomplete_payload_is_recorded_as_failed_without_saving(store):
    payload = make_result(32570)
    del payload["ledger"]["transactions"]
    result = asyncio.run(ingest_ledger(
        FakeSource({32570: [payload]}), store, 32570, FAST_RETRY))
    assert not result.ok and result.recorded
    assert store.list_failed() == [32570]
    assert store.ledgers.count_documents({}) == 0
    assert store.txs.count_documents({}) == store.metas.count_documents({}) == 0


def test_marker_failure_reaches_summary_exit_and_chunk_log(cfg, store, monkeypatch, caplog):
    def down(*args):
        raise AutoReconnect("marker unavailable")

    monkeypatch.setattr(store, "mark_chunk_done", down)
    source = FakeSource(latest=32579, ranges="32570-32579")
    with caplog.at_level(logging.INFO, logger="xrpl_ingest.backfill"):
        summary = backfill.run_backfill(
            cfg, 32570, 32579, source=source, store=store, inline=True,
            stop_check=lambda: False)
    assert summary["ledgers_ok"] == 10 and summary["ledgers_failed"] == 0
    assert store.ledgers.count_documents({}) == 10
    assert not store.done_chunks(32570, 32579)
    assert summary["chunks_unmarked"] == 1
    assert backfill_exit_code(summary) == 1
    assert any("marker_failed=True" in msg for msg in caplog.messages)
    assert not any("chunks 1/1 (100.0%)" in msg for msg in caplog.messages)


@pytest.mark.parametrize("unavailable", [[], [(32580, 32589)]])
def test_unmarked_chunk_exit_takes_priority_over_unavailable(unavailable):
    assert backfill_exit_code({"ledgers_failed": 0, "chunks_incomplete": 0,
                               "chunks_unmarked": 1, "unavailable_ranges": unavailable}) == 1


@pytest.mark.parametrize("marks,start,end,size,done,chunks", [
    ([(32570, 32579), (32570, 32574), (32575, 32579)], 32570, 32579, 5, 10, 2),
    ([(32570, 32579)], 32570, 32579, 5, 10, 2),  # retained old grid only
    ([(32570, 32574), (32575, 32579)], 32570, 32579, 10, 10, 1),
    ([(32570, 32572), (32574, 32579)], 32570, 32579, 5, 9, 1),  # real hole
    ([(32570, 32579)], 32573, 32577, 5, 5, 2),  # clipped at both ends
    ([(32570, 32571), (32573, 32574)], 32570, 32579, 5, 4, 0),
    ([(32570, 32579)], 32580, 32579, 5, 0, 0),  # empty requested range
])
def test_progress_counts_unique_coverage_and_full_current_chunks(
        store, marks, start, end, size, done, chunks):
    for a, b in marks:
        store.mark_chunk_done(a, b, b - a + 1, 0)
    progress = range_progress(store, start, end, size)
    assert progress["ledgers_done"] == done
    assert progress["chunks_done"] == chunks
    assert done <= progress["ledgers_total"] and chunks <= progress["chunks_total"]
    assert "150.0%" not in format_range_progress(progress)


def test_overlapping_progress_does_not_report_replayed_work_as_rate(store):
    store.mark_chunk_done(32570, 32579, 10, 0)
    store.mark_chunk_done(32570, 32574, 5, 0)
    progress = range_progress(store, 32570, 32589, 5)
    assert progress["rate"] is None and progress["eta_seconds"] is None


@pytest.mark.parametrize("queued", [False, True])
def test_shared_logs_escape_control_characters_and_preserve_tracebacks(
        restore_logging, tmp_path, queued):
    stream = io.StringIO()
    target = tmp_path / "ingest.log"
    handlers = logsetup.setup_logging("INFO", str(target), stream=stream)
    payload = "bad\r\nFORGED\x1b[2J\x85\u2028\u2029\x00"
    try:
        raise ValueError(payload)
    except ValueError:
        record = logging.LogRecord("xrpl_ingest.source", logging.ERROR, "", 0,
                                   "node: %s", (payload,), sys.exc_info())
    # Formatting must leave the original record usable by foreign handlers.
    foreign = logging.Formatter("%(message)s")
    if queued:
        q = queue.Queue()
        listener = logsetup.start_queue_listener(q)
        try:
            logging.handlers.QueueHandler(q).handle(record)
        finally:
            listener.stop()
    else:
        logging.getLogger(record.name).handle(record)
    for handler in handlers:
        handler.flush()
    output = stream.getvalue()
    assert output == target.read_text(encoding="utf-8")
    assert r"bad\r\nFORGED\x1b[2J\x85\u2028\u2029\x00" in output
    assert "Traceback (most recent call last)" in output and "ValueError:" in output
    assert "\nFORGED" not in output and "\r" not in output and "\x1b" not in output
    assert payload in foreign.format(record)  # no sanitizing/mutation of shared input


def test_cancelled_network_handshake_closes_unpublished_connection():
    async def run():
        requested = asyncio.Event()

        def response(request):
            requested.set()
            return None  # keep the initial network-id check pending

        ws = FakeWs(response)

        async def connect(*args, **kwargs):
            return ws

        source = WsSource(("ws://test",), connect_fn=connect)
        task = asyncio.create_task(source.latest_validated())
        try:
            await asyncio.wait_for(requested.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await source.close()
            assert ws.closed
            await asyncio.sleep(0)
            assert asyncio.all_tasks() == {asyncio.current_task()}
        finally:
            await ws.close()  # clean up even when checking the old broken implementation
            await source.close()

    asyncio.run(run())


@pytest.mark.parametrize("competing_index", [120, 200, 500])
def test_legacy_checkpoint_conversion_cannot_overwrite_competing_writer(
        store, monkeypatch, competing_index):
    store.meta.insert_one({"_id": LAST_LEDGER_ID, "index": "100"})
    update = store.meta.update_one
    first = True

    def race(*args, **kwargs):
        nonlocal first
        if first:
            first = False
            update({"_id": LAST_LEDGER_ID}, {"$set": {"index": competing_index}})
        return update(*args, **kwargs)

    monkeypatch.setattr(store.meta, "update_one", race)
    store.set_checkpoint(150)
    assert store.get_checkpoint() == max(150, competing_index)


def test_store_refuses_unacknowledged_writes_before_any_network_operation(cfg):
    with MongoClient("mongodb://unused", connect=False, w=0) as client:
        with pytest.raises(ConfigError, match="acknowledged"):
            MongoStore(client, cfg)


@pytest.mark.parametrize("concern", [1, "majority"])
def test_store_preserves_acknowledged_write_concern(cfg, concern):
    with MongoClient("mongodb://unused", connect=False, w=concern) as client:
        store = MongoStore(client, cfg)
        assert store.ledgers.write_concern.document == {"w": concern}


@pytest.mark.parametrize("name", ["NUM_PROCESSES", "CONCURRENCY", "CHUNK_SIZE"])
def test_huge_numeric_config_is_a_config_error(name):
    with pytest.raises(ConfigError, match=name):
        Config.from_env({"MONGO_URI": "mongodb://unused", name: "1" + "0" * 400})


def test_doctor_keeps_structured_config_error_for_unacknowledged_writes(cfg):
    from xrpl_ingest.support.doctor import Report, check_mongo

    def invalid_store(config):
        raise ConfigError("MongoDB writes must be acknowledged")

    report = Report()
    check_mongo(report, cfg, store_factory=invalid_store)
    assert report.exit_code() == 2
    assert any("acknowledged" in check.detail for check in report.checks)


def test_rejected_store_closes_owned_client(cfg, monkeypatch):
    from xrpl_ingest import store as store_module

    client = MongoClient("mongodb://unused", connect=False, w=0)
    closed = []
    close = client.close
    monkeypatch.setattr(client, "close", lambda: (closed.append(True), close()))
    monkeypatch.setattr(store_module, "MongoClient", lambda *args, **kwargs: client)
    with pytest.raises(ConfigError, match="acknowledged"):
        MongoStore.from_config(cfg)
    assert closed == [True]


def test_duplicate_transaction_identity_cannot_be_marked_complete(store):
    payload = make_result(32570, 2)
    txs = payload["ledger"]["transactions"]
    txs[1]["hash"] = txs[0]["hash"]
    result = asyncio.run(ingest_ledger(
        FakeSource({32570: [payload]}), store, 32570, FAST_RETRY))
    assert not result.ok and result.recorded
    assert store.ledgers.count_documents({}) == 0
    assert store.txs.count_documents({}) == store.metas.count_documents({}) == 0


@pytest.mark.parametrize("value", [42, True, ["hash"], {"$ne": None}])
def test_expanded_transactions_require_string_identity(value):
    payload = make_result(32570, 1)
    payload["ledger"]["transactions"][0]["hash"] = value
    with pytest.raises(MalformedLedger, match="hash"):
        parse_ledger_result(32570, payload)


@pytest.mark.parametrize("hashes", [["A", "A"], [""], ["A", ""]])
def test_deep_hash_lists_reject_duplicate_or_empty_identity(hashes):
    with pytest.raises(MalformedLedger, match="hash"):
        node_tx_hashes(32570, {"validated": True,
                              "ledger": {"ledger_index": 32570, "transactions": hashes}})


@pytest.mark.parametrize("ranges,expected,unavailable", [
    ([(10, 29), (10, 29)], [(10, 19), (20, 29)], []),
    ([(10, 24), (20, 29)], [(10, 19), (20, 29)], []),
    ([(10, 14), (15, 19), (22, 25)], [(10, 19), (22, 25)], [(20, 21), (26, 29)]),
])
def test_chunk_planning_uses_unique_available_coverage(ranges, expected, unavailable):
    chunks, missing = plan_chunks(10, 29, ranges, 10, set())
    assert chunks == expected and missing == unavailable
    visited = [n for a, b in chunks for n in range(a, b + 1)]
    assert len(visited) == len(set(visited))


@pytest.mark.parametrize("field,keys", [
    ("meta", ("a.b", "a\uff0eb")),
    ("meta", ("$x", "\uff04x")),
    ("tx_json", ("a\x00b", "a\u2400b")),
])
def test_colliding_escaped_metadata_keys_are_rejected_before_storage(store, field, keys):
    payload = make_result(32570, 1)
    payload["ledger"]["transactions"][0][field]["Extra"] = dict(zip(keys, (1, 2)))
    result = asyncio.run(ingest_ledger(
        FakeSource({32570: [payload]}), store, 32570, FAST_RETRY))
    assert not result.ok and result.recorded
    assert store.ledgers.count_documents({}) == 0
    assert store.txs.count_documents({}) == store.metas.count_documents({}) == 0
