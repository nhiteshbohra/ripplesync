"""New audit regressions: fixture responses, isolated in-memory databases only."""
import asyncio
import copy

import pytest

from conftest import FAST_RETRY, NOW, FakeSource, FakeWs, make_result, no_sleep
from xrpl_ingest.backfill import discover_node
from xrpl_ingest.errors import IngestError, TransientError
from xrpl_ingest.repair import repair_range
from xrpl_ingest.source import WsSource
from xrpl_ingest.transform import merge_field, parse_ledger_result


@pytest.mark.parametrize("initial", ["gap", "count"])
def test_deep_repair_checks_hashes_of_newly_repaired_ledgers(store, initial):
    if initial == "count":
        store.save_ledger_bundle(*parse_ledger_result(10, make_result(10, n_tx=1), NOW))
        store.ledgers.update_one({"block_number": 10}, {"$set": {"num_transactions": 2}})
    bad = make_result(10)
    bad["ledger"]["transactions"][1]["hash"] = "UNEXPECTED"
    source = FakeSource({10: [bad]})
    # Initially the sole stored hash in the count case matches the node's initial list;
    # the node's list after the repair has the full two transactions.
    if initial == "count":
        source.hash_script = {10: [["H10_0"], ["H10_0", "H10_1"]]}
    lines = []
    summary = asyncio.run(repair_range(
        source, store, 10, 10, [(10, 10)], retry=FAST_RETRY, concurrency=1,
        stop_check=lambda: False, emit=lines.append, deep=True, sleep=no_sleep))
    assert not summary.ok and summary.remaining > 0
    assert summary.hash_mismatches == 1
    assert any("after repair" in line and "UNEXPECTED" in line for line in lines)
    assert store.txs.find_one({"hash": "UNEXPECTED"})  # report extra data; never delete it


def test_deep_repair_honors_stop_before_post_repair_node_requests(store):
    store.save_ledger_bundle(*parse_ledger_result(10, make_result(10), NOW))
    store.txs.update_one({"hash": "H10_0"}, {"$set": {"hash": "WRONG"}})
    stopped = False
    hash_calls = []

    class Source(FakeSource):
        async def get_ledger(self, index):
            nonlocal stopped
            result = await super().get_ledger(index)
            stopped = True
            return result

        async def get_ledger_hashes(self, index):
            hash_calls.append(index)
            return await super().get_ledger_hashes(index)

    summary = asyncio.run(repair_range(
        Source(), store, 10, 10, [(10, 10)], retry=FAST_RETRY, concurrency=1,
        stop_check=lambda: stopped, deep=True, sleep=no_sleep))
    assert summary.stopped and not summary.ok
    assert hash_calls == [10]  # only the initial check; the stop prevents a second request


@pytest.mark.parametrize("value", [[], {}, ["1-100"], 123, True])
def test_malformed_complete_ledgers_is_a_deliberate_node_error(value):
    with pytest.raises(IngestError, match="malformed complete_ledgers"):
        asyncio.run(discover_node(FakeSource(ranges=value)))


@pytest.mark.parametrize("network_id", [False, True, 0.0, [], {}, "0"])
def test_invalid_network_id_types_do_not_pass_network_guard(network_id):
    async def run():
        ws = FakeWs(lambda req: {"id": req["id"], "result": {
            "info": {"network_id": network_id, "validated_ledger": {"seq": 10}}}})

        async def connect(*args, **kwargs):
            return ws

        source = WsSource(["ws://fixture"], connect_fn=connect, connect_retries=1,
                          expected_network_id=0)
        try:
            with pytest.raises(TransientError):
                await source.latest_validated()
        finally:
            await source.close()
    asyncio.run(run())


@pytest.mark.parametrize("stored,received", [
    ({"value": True}, {"value": 1}), ({"value": 1}, {"value": 1.0}),
    ([{"value": False}], [{"value": 0}]), ([1], [1.0]),
    ({"a": [1, {"b": True}]}, {"a": [1, {"b": 1}]}),
])
def test_nested_field_collisions_preserve_both_typed_values(stored, received):
    original = copy.deepcopy(stored)
    doc = {"future": stored}
    merge_field(doc, "future", received, {})
    assert doc["future"] == original
    assert "raw_future" in doc and doc["raw_future"] == received


def test_nested_numeric_strings_are_not_deduplicated_as_coerced_numbers():
    doc = {"future": {"value": 12}}
    merge_field(doc, "future", {"value": "12"}, {})
    assert doc == {"future": {"value": 12}, "raw_future": {"value": "12"}}


def test_typed_nested_collisions_survive_ingestion_storage(store):
    raw = make_result(10)
    tx = raw["ledger"]["transactions"][0]
    tx["FutureField"] = {"items": [True, 1]}
    tx["tx_json"]["FutureField"] = {"items": [1, 1.0]}
    store.save_ledger_bundle(*parse_ledger_result(10, raw, NOW))
    saved = store.find_tx("H10_0")
    assert type(saved["FutureField"]["items"][0]) is bool
    assert type(saved["raw_FutureField"]["items"][0]) is int
    assert type(saved["raw_FutureField"]["items"][1]) is float


def test_deep_repair_cli_does_not_report_ok_for_unverified_new_hashes(cfg, store, monkeypatch, capsys):
    from xrpl_ingest import cli
    raw = make_result(10)
    raw["ledger"]["transactions"][0]["hash"] = "UNEXPECTED"
    source = FakeSource({10: [raw]})
    monkeypatch.setattr(cli.WsSource, "from_config", lambda *a, **kw: source)
    result = asyncio.run(cli._verify_with_node(cfg, store, 10, 10, lambda: False,
                                              repair=True, deep=True))
    assert result == 1
    output = capsys.readouterr().out
    assert "UNEXPECTED" in output and "after repair" in output
    assert "OK" not in output.splitlines()


def test_deep_repair_postcheck_node_outage_is_counted(store):
    class Source(FakeSource):
        async def get_ledger_hashes(self, index):
            from xrpl_ingest.errors import RpcError
            raise RpcError("lgrNotFound")

    summary = asyncio.run(repair_range(
        Source(), store, 10, 10, [(10, 10)], retry=FAST_RETRY, concurrency=1,
        stop_check=lambda: False, deep=True, sleep=no_sleep))
    assert not summary.ok and summary.unchecked == 1 and summary.remaining == 1
    assert store.ledgers.count_documents({}) == 1


@pytest.mark.parametrize("failure", ["send", "reader", "rpc"])
def test_source_failures_redact_credentials_in_logs_and_saved_reasons(store, caplog, failure):
    from xrpl_ingest.errors import ConnectionLost
    from xrpl_ingest.fetcher import RetryPolicy, ingest_ledger

    async def run():
        uri = "ws://operator:fixture-secret@fixture"

        def reply(req):
            if failure == "send":
                raise OSError("send failed on " + uri)
            if failure == "reader":
                ws.fail(ConnectionLost("transport failed on " + uri))
                return None
            return {"id": req["id"], "status": "error", "error": "lgrNotFound",
                    "error_message": "request failed on " + uri}

        ws = FakeWs(reply)

        async def connect(*args, **kwargs):
            return ws

        source = WsSource([uri], connect_fn=connect, expected_network_id=None)
        try:
            result = await ingest_ledger(source, store, 10, RetryPolicy(attempts=1))
            assert not result.ok and result.recorded
            reason = store.failed.find_one({"block_number": 10})["reason"]
            assert "fixture-secret" not in reason
            assert "ws://fixture" in reason
        finally:
            await source.close()
    asyncio.run(run())
    assert all("fixture-secret" not in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize("failure", ["write", "failure-record", "chunk-marker"])
def test_pipeline_database_errors_redact_url_credentials(store, caplog, monkeypatch, failure):
    from pymongo.errors import AutoReconnect
    from xrpl_ingest import backfill
    from xrpl_ingest.errors import RpcError
    from xrpl_ingest.fetcher import RetryPolicy

    def unavailable(*args, **kwargs):
        raise AutoReconnect("operation failed at mongodb://operator:fixture-secret@fixture")

    source = FakeSource()
    if failure == "write":
        monkeypatch.setattr(store, "save_ledger_bundles", unavailable)
    elif failure == "failure-record":
        source.script = {10: [RpcError("lgrNotFound", "ws://operator:fixture-secret@fixture")]}
        monkeypatch.setattr(store, "record_failure", unavailable)
    else:
        monkeypatch.setattr(store, "mark_chunk_done", unavailable)
    monkeypatch.setattr(backfill, "WRITE_WORKERS", 1)
    result = asyncio.run(backfill.process_chunk(
        source, store, (10, 12), concurrency=3, retry=RetryPolicy(attempts=2, base=0),
        stop_check=lambda: False, sleep=no_sleep))
    assert result.failed or result.marker_failed
    assert caplog.records
    assert all("fixture-secret" not in record.getMessage() for record in caplog.records)
    assert all("fixture-secret" not in d["reason"] for d in store.failed.find({}))
