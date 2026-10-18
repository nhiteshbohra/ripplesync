"""End-to-end: real spawn process pool + real websockets + real MongoDB.

Uses a throwaway database `xrpl_ingest_test_<uuid>` that is dropped afterwards; no other
database is touched and `.env` is never loaded (the Config is built here). Skipped when
MongoDB does not answer a ping within 1s, or when XRPL_INGEST_NO_INTEGRATION=1.
Run only these: `python -m pytest -m integration`.
"""
import dataclasses
import os
import re
import uuid

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from fake_xrpld import FakeXrpld, tx_count
from xrpl_ingest.backfill import run_backfill
from xrpl_ingest.cli import _verify, backfill_exit_code
from xrpl_ingest.config import Config
from xrpl_ingest.logsetup import setup_logging
from xrpl_ingest.store import MongoStore

pytestmark = pytest.mark.integration

MONGO = "mongodb://127.0.0.1:27017"
DB_PREFIX = "xrpl_ingest_test_"
LO = 32570


@pytest.fixture(scope="module", autouse=True)
def mongo_available():
    if os.environ.get("XRPL_INGEST_NO_INTEGRATION") == "1":
        pytest.skip("XRPL_INGEST_NO_INTEGRATION=1")
    client = MongoClient(MONGO, serverSelectionTimeoutMS=1000)
    try:
        client.admin.command("ping")
    except PyMongoError as e:
        pytest.skip(f"no MongoDB at {MONGO}: {type(e).__name__}")
    finally:
        client.close()


@pytest.fixture
def node():
    with FakeXrpld(first=LO, latest=LO + 29) as n:
        yield n


@pytest.fixture
def cfg(node, tmp_path):
    db_name = f"{DB_PREFIX}{uuid.uuid4().hex}"
    yield Config(mongo_uri=f"{MONGO}/?serverSelectionTimeoutMS=5000", db_name=db_name,
                 servers=(node.url,), chunk_size=10, num_processes=2, concurrency=4,
                 max_retries=3, request_timeout=5, reconnect_delay_base=0.01, backoff_max=0.05,
                 process_status_file=str(tmp_path / "process_status.txt"))
    assert db_name.startswith(DB_PREFIX)  # never drop anything else
    client = MongoClient(MONGO, serverSelectionTimeoutMS=5000)
    try:
        client.drop_database(db_name)
    finally:
        client.close()


def test_backfill_spawn_pool_then_rerun_and_verify(cfg, capfd, restore_logging, tmp_path):
    hi = LO + 29
    log_file = tmp_path / "logs" / "ingest.log"
    setup_logging("INFO", str(log_file))  # what the CLI does in the parent
    first = run_backfill(cfg, LO, hi)
    assert (first["chunks_run"], first["ledgers_ok"], first["ledgers_failed"]) == (3, 30, 0)
    assert backfill_exit_code(first) == 0
    # worker records travel through the queue to the parent's handlers: stderr and LOG_FILE,
    # UTC timestamps with Z
    err = capfd.readouterr().err
    assert re.search(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z INFO xrpl_ingest.source: "
                     r"connected to ws://127\.0\.0\.1:", err), err[-2000:]
    text = log_file.read_text(encoding="utf-8")
    assert "xrpl_ingest.source: connected to ws://127.0.0.1:" in text
    assert "chunks 3/3 (100.0%)" in text

    again = run_backfill(cfg, LO, hi)
    assert again["chunks_run"] == 0 and backfill_exit_code(again) == 0  # resumed: nothing to do

    store = MongoStore.from_config(cfg)
    try:
        capfd.readouterr()
        assert _verify(store, LO, hi) == 0 and "OK" in capfd.readouterr().out
        assert store.txs.count_documents({}) == sum(tx_count(i) for i in range(LO, hi + 1))
        assert store.done_chunks(LO, hi) == {(LO, LO + 9), (LO + 10, LO + 19), (LO + 20, hi)}
    finally:
        store.close()


def test_oversized_ledger_failure_records_websocket_1009_reason(cfg, node):
    big = LO + 25
    node.big[big] = 3000  # ~2 MB message, over the 1 MB limit below
    cfg = dataclasses.replace(cfg, max_message_bytes=1024 * 1024, concurrency=1, max_retries=2)
    summary = run_backfill(cfg, LO + 20, LO + 29)
    assert summary["ledgers_failed"] == 1 and summary["ledgers_ok"] == 9
    store = MongoStore.from_config(cfg)
    try:
        assert store.list_failed() == [big]
        reason = store.failed.find_one({"block_number": big})["reason"]
        assert "1009" in reason and "message too big" in reason, reason
        assert store.done_chunks(LO, LO + 29) == {(LO + 20, LO + 29)}  # recorded => marked
    finally:
        store.close()


def test_lossless_bundle_roundtrips_through_real_mongodb(cfg):
    """Escaped awkward keys ('.', leading '$') and unmapped fields survive a real write."""
    from conftest import NOW
    from test_lossless import INDEX, leaves, recoverable, v2_ledger_result
    from xrpl_ingest.transform import parse_ledger_result

    raw = v2_ledger_result()
    store = MongoStore.from_config(cfg)
    try:
        store.ensure_indexes()
        store.save_ledger_bundle(*parse_ledger_result(INDEX, raw, NOW))
        ledger_doc = store.ledgers.find_one({"block_number": INDEX}, {"_id": 0})
        hashes = [t["hash"] for t in raw["ledger"]["transactions"]]
        txs = [store.txs.find_one({"hash": h}, {"_id": 0}) for h in hashes]
        metas = [store.metas.find_one({"transaction_hash": h}, {"_id": 0}) for h in hashes]
        lost = [(p, v) for p, v in leaves(raw) if not recoverable(p, v, ledger_doc, txs, metas)]
        assert lost == []
    finally:
        store.close()
