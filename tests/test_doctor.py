"""doctor / config / health. MongoDB is mongomock (plus a fake admin command), the node a
fake; one integration test runs doctor against the local MongoDB (throwaway DB)."""
import asyncio
import dataclasses
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

from xrpl_ingest.support import doctor
from xrpl_ingest.support import monitor as health
from xrpl_ingest.config import Config
from xrpl_ingest.support.doctor import Report, check_chunk_docs, check_indexes, run_doctor, satisfies
from xrpl_ingest.errors import ConnectFailed
from xrpl_ingest.support.runcontrol import InstanceLock, lock_path
from xrpl_ingest.support.files import EnvFile
from xrpl_ingest.store import LAST_LEDGER_ID, MongoStore

GB = 1024 ** 3
INFO = {"server_state": "full", "network_id": 0, "complete_ledgers": "32570-100000000",
        "validated_ledger": {"seq": 100000000}, "peers": 21, "build_version": "2.4.0"}


class FakeAdmin:
    def __init__(self, fail=None, users=(), free_gb=500):
        self.fail, self.users, self.free_gb = fail, list(users), free_gb

    def command(self, name, *a, **k):
        if self.fail:
            raise self.fail
        if name == "ping":
            return {"ok": 1}
        if name == "connectionStatus":
            return {"authInfo": {"authenticatedUsers": self.users}}
        if name == "dbStats":
            return {"fsTotalSize": 1000 * GB, "fsUsedSize": (1000 - self.free_gb) * GB}
        raise AssertionError(name)


class FakeClient:
    def __init__(self, version="8.0.4", **admin):
        self.mock = mongomock.MongoClient()
        self.admin = FakeAdmin(**admin)
        self.version = version

    def __getitem__(self, name):
        return self.mock[name]

    def server_info(self):
        return {"version": self.version}

    def close(self):
        pass


def make_store(cfg, **kw):
    s = MongoStore(FakeClient(**kw), cfg)
    s.db_stats = lambda: s.client.admin.command("dbStats")
    return s


class FakeNode:
    def __init__(self, info=None, error=None):
        self.info, self.error = info if info is not None else dict(INFO), error

    async def server_info(self):
        if self.error:
            raise self.error
        return self.info

    async def latest_validated(self):
        if self.error:
            raise self.error
        return self.info["validated_ledger"]["seq"]

    async def close(self):
        pass


@pytest.fixture
def dcfg(tmp_path):
    return Config(mongo_uri="mongodb://u:hunter2@h:27017", base_dir=str(tmp_path), chunk_size=10,
                  process_status_file=str(tmp_path / "process_status.txt"),
                  backfill_range_file=str(tmp_path / "backfill_range.txt"))


def run_dr(cfg, *, store=None, fake_node=None, env=None, json_out=False, versions=None, **kw):
    lines = []
    store = store or make_store(cfg)
    code = run_doctor(cfg, env or EnvFile(looked_for="x/.env"), {}, cfg.base_dir if cfg else ".",
                      out=lines.append, as_json=json_out,
                      store_factory=lambda c: store,
                      source_factory=lambda c, **k: fake_node or FakeNode(),
                      version_of=versions or (lambda n: {"pymongo": "4.10.1", "python-dotenv": "1.2.2",
                                                         "websockets": "15.0"}[n]),
                      **kw)
    return code, "\n".join(lines)


def test_all_ok_exit_0_and_shows_node_summary(dcfg):
    code, out = run_dr(dcfg)
    assert code == 0, out
    for text in ("node state", "full", "network_id 0", "complete_ledgers 32570-100000000",
                 "seq 100000000", "node peers", "2.4.0", "MongoDB 8.0.4", "500.0 GB free",
                 "result: 0 failure(s), 0 warning(s) -> exit 0"):
        assert text in out, text
    assert "hunter2" not in out


def test_old_dependency_is_a_warning_exit_1(dcfg):
    code, out = run_dr(dcfg, versions=lambda n: {"pymongo": "4.8.0"}.get(n, "15.0" if n == "websockets" else "1.2.2"))
    assert code == 1 and "4.8.0 installed; requires >=4.9,<4.11" in out


def test_missing_dependency_fails_exit_2(dcfg):
    def versions(n):
        if n == "websockets":
            raise doctor.metadata.PackageNotFoundError(n)
        return {"pymongo": "4.10.1", "python-dotenv": "1.2.2"}[n]
    code, out = run_dr(dcfg, versions=versions)
    assert code == 2 and "websockets" in out and "not installed" in out


@pytest.mark.parametrize("version,spec,ok", [("4.10.1", ">=4.9,<4.11", True), ("4.11", ">=4.9,<4.11", False),
                                             ("4.9", ">=4.9,<4.11", True), ("1.2.1", ">=1.2.2,<2", False),
                                             ("17.1rc1", ">=13,<18", True), ("18.0", ">=13,<18", False)])
def test_satisfies(version, spec, ok):
    assert satisfies(version, spec) is ok


def test_requirements_constant_matches_requirements_txt():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "requirements.txt"), encoding="utf-8") as f:
        names = {line.split(">")[0].split("<")[0].split("=")[0].strip().lower()
                 for line in f if line.strip() and not line.startswith("#")}
    assert {n for n, _ in doctor.REQUIREMENTS} <= names


def test_mongo_unreachable_exit_4(dcfg):
    store = make_store(dcfg, fail=ServerSelectionTimeoutError("h:27017 timed out"))
    code, out = run_dr(dcfg, store=store)
    assert code == 4 and "unreachable" in out and "hunter2" not in out


def test_mongo_bad_credentials_exit_2(dcfg):
    store = make_store(dcfg, fail=OperationFailure("Authentication failed.", 18))
    code, out = run_dr(dcfg, store=store)
    assert code == 2 and "credentials" in out


def test_node_unreachable_exit_4_and_config_beats_it(dcfg):
    code, _ = run_dr(dcfg, fake_node=FakeNode(error=ConnectFailed("all servers failed")))
    assert code == 4
    code, out = run_dr(dcfg, fake_node=FakeNode({**INFO, "network_id": 1}))
    assert code == 2 and "network_id 1 != EXPECTED_NETWORK_ID 0" in out


def test_node_flags_select_checks(dcfg):
    _, out = run_dr(dcfg, mongo=True, node=False)
    assert "MongoDB" in out and "node state" not in out
    _, out = run_dr(dcfg, mongo=False, node=True)
    assert "node state" in out and "MongoDB 8" not in out


def test_unsynced_node_and_mongo_9_warn(dcfg):
    code, out = run_dr(dcfg, store=make_store(dcfg, version="9.0.0"),
                       fake_node=FakeNode({**INFO, "server_state": "syncing", "peers": 0}))
    assert code == 1
    assert "syncing (not synced yet)" in out and "supported are 7.0 and 8.0" in out


def test_low_mongo_disk_with_min_free_gb_warns(dcfg):
    cfg = dataclasses.replace(dcfg, min_free_gb=100)
    code, out = run_dr(cfg, store=make_store(cfg, free_gb=50))
    assert code == 1 and "below MIN_FREE_GB=100" in out


def test_config_error_still_reports_exit_2(tmp_path):
    lines = []
    code = run_doctor(None, EnvFile(), {"MONGO_URI": "mongodb://a:b@h"}, str(tmp_path),
                      config_error=ValueError("CHUNK_SIZE=0 must be >= 1"), out=lines.append,
                      version_of=lambda n: "99")
    out = "\n".join(lines)
    assert code == 2 and "CHUNK_SIZE=0" in out and "mongodb://h" in out and ":b@" not in out


def test_json_output(dcfg):
    code, out = run_dr(dcfg, json_out=True)
    data = json.loads(out)
    assert data["exit_code"] == code == 0 and data["version"]
    assert {"name", "status", "detail", "code"} <= set(data["checks"][0])
    assert any(r["name"] == "MONGO_URI" and "hunter2" not in r["value"] for r in data["config"])


def test_control_characters_in_paths_warn(dcfg):
    cfg = dataclasses.replace(dcfg, process_status_file=dcfg.process_status_file + "\r")
    code, out = run_dr(cfg)
    assert code == 1 and "control characters" in out


@pytest.mark.skipif(os.name != "posix", reason="POSIX file modes")
def test_env_permissions_posix(dcfg, tmp_path):
    env = tmp_path / ".env"
    env.write_text("X=1\n")
    env.chmod(0o644)
    code, out = run_dr(dcfg, env=EnvFile(path=str(env)))
    assert code == 1 and "chmod 600" in out
    env.chmod(0o600)
    code, _ = run_dr(dcfg, env=EnvFile(path=str(env)))
    assert code == 0


@pytest.mark.skipif(os.name == "posix", reason="Windows message")
def test_env_permissions_not_checked_on_windows(dcfg, tmp_path):
    env = tmp_path / ".env"
    env.write_text("X=1\n")
    _, out = run_dr(dcfg, env=EnvFile(path=str(env)))
    assert "not checked on Windows" in out


def test_locks_report_last_holder_without_claiming_current_ownership(dcfg, tmp_path):
    with InstanceLock(lock_path(str(tmp_path), "tail"), "tail"):
        InstanceLock(lock_path(str(tmp_path), "backfill"), "backfill").acquire().release()
        code, out = run_dr(dcfg)
    assert code == 0
    assert out.count("last recorded holder: pid") == 2
    assert "current ownership unknown (read-only inspection)" in out


def test_windows_process_cap_shown(dcfg, monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(doctor, "ram_bytes", lambda: (None, None))
    r = Report()
    doctor.check_host(r, dcfg)
    assert any("Windows limit 61" in c.detail for c in r.checks)


def test_ram_probe_on_this_platform():
    total, avail = doctor.ram_bytes()
    if sys.platform in ("win32", "darwin") or sys.platform.startswith("linux"):
        assert total and total > 64 * 1024 * 1024
    if avail is not None:
        assert 0 < avail <= total


# -- legacy DB checks ----------------------------------------------------------------------

def _db(cfg):
    return mongomock.MongoClient()[cfg.db_name]


def test_indexes_clean_db_ok(dcfg):
    db = _db(dcfg)
    MongoStore(db.client, dcfg).ensure_indexes()
    r = Report()
    check_indexes(r, db, dcfg)
    assert [c.status for c in r.checks] == ["ok"]


def test_index_with_other_options_conflicts(dcfg):
    db = _db(dcfg)
    db[dcfg.tx_collection].create_index("hash")  # legacy: hash_1, not unique
    r = Report()
    check_indexes(r, db, dcfg)
    bad = [c for c in r.checks if c.status == "warn"]
    assert len(bad) == 1 and "transactions.hash_1" in bad[0].name and "unique=False" in bad[0].detail


def test_index_same_key_other_name_conflicts(dcfg):
    db = _db(dcfg)
    db[dcfg.ledger_collection].create_index("block_number", unique=True, name="legacy_bn")
    r = Report()
    check_indexes(r, db, dcfg)
    assert any("'legacy_bn'" in c.detail and c.status == "warn" for c in r.checks)


def test_duplicate_hashes_block_unique_index(dcfg):
    db = _db(dcfg)
    db[dcfg.tx_collection].insert_many([{"hash": "A"}, {"hash": "A"}, {"hash": "B"}, {"x": 1}, {"x": 2}])
    r = Report()
    check_indexes(r, db, dcfg)
    dup = [c for c in r.checks if c.name.startswith("duplicates")]
    assert len(dup) == 1 and "'A'" in dup[0].detail and dup[0].status == "warn"


def test_wanted_indexes_follow_ensure_indexes(dcfg):
    wanted = doctor.wanted_indexes(dcfg)
    assert (dcfg.ledger_collection, [("block_number", 1)], {"unique": True}) in wanted
    assert any(c == dcfg.tx_collection and k == [("hash", 1)] for c, k, _ in wanted)


def test_chunk_docs_size_mismatch(store):
    store.mark_chunk_done(100, 109, 10, 0)  # cfg chunk_size 10: full
    store.mark_chunk_done(32570, 32579, 10, 0)  # full
    store.mark_chunk_done(32580, 32585, 6, 0)  # partial (range edge)
    r = Report()
    check_chunk_docs(r, store.meta, 10)
    assert r.checks[0].status == "ok" and "2 full, 1 partial" in r.checks[0].detail
    store.mark_chunk_done(1000, 1999, 1000, 0)  # from CHUNK_SIZE=1000
    store.mark_chunk_done(2000, 2999, 1000, 0)
    r = Report()
    check_chunk_docs(r, store.meta, 10)
    assert r.checks[0].status == "warn" and "2 chunk docs" in r.checks[0].detail
    assert "1000 ledgers x2" in r.checks[0].detail


# -- config command ---------------------------------------------------------------------------

def test_config_rows_sources(dcfg):
    env = EnvFile(path="/x/.env", from_file=frozenset({"DB_NAME"}))
    rows = {n: (v, s) for n, v, s in doctor.config_rows(dcfg, env, {"DB_NAME": "d", "CHUNK_SIZE": "10"}, "/x")}
    assert rows["DB_NAME"][1] == ".env" and rows["CHUNK_SIZE"] == ("10", "env")
    assert rows["LOG_LEVEL"] == ("INFO", "default") and rows["MONGO_URI"][0] == "mongodb://h:27017"
    assert rows["ENV_FILE"][0] == "/x/.env"


# -- health -----------------------------------------------------------------------------------

NOW = datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("cp,age,latest,code,word", [
    (1000, 5, 1010, 0, "healthy"), (1000, 5, 1051, 1, "lagging"), (1000, 500, 1001, 1, "stale"),
    (0, 5, 1000, 1, "no tail checkpoint"), (1000, None, 1000, 1, "stale"),
])
def test_evaluate(cp, age, latest, code, word):
    upd = NOW - timedelta(seconds=age) if age is not None else None
    got, line = health.evaluate(cp, upd, latest, max_lag=50, max_age=120, now=NOW)
    assert got == code and word in line


def _health(store, node):
    lines = []
    code = asyncio.run(health.run_health(store.cfg, max_lag=50, max_age=120, out=lines.append,
                                         store_factory=lambda c: store, source_factory=lambda c: node))
    return code, "\n".join(lines)


def test_health_healthy_with_naive_datetime_and_legacy_string(store):
    store.meta.insert_one({"_id": LAST_LEDGER_ID, "index": "999",
                           "updated_at": datetime.now(timezone.utc).replace(tzinfo=None)})
    code, out = _health(store, FakeNode({**INFO, "validated_ledger": {"seq": 1000}}))
    assert code == 0 and out.startswith("healthy: checkpoint 999, node validated 1000 (lag 1")


def test_health_unreachable_and_wrong_network(store):
    store.set_checkpoint(10)
    code, out = _health(store, FakeNode(error=ConnectFailed("down")))
    assert code == 4 and "node unreachable" in out
    from xrpl_ingest.errors import NetworkMismatch
    code, _ = _health(store, FakeNode(error=NetworkMismatch("x")))
    assert code == 2


def test_health_mongo_unreachable_exit_4(dcfg):
    store = make_store(dcfg)
    store.meta = type("Dead", (), {"find_one": lambda *a, **k: (_ for _ in ()).throw(
        ServerSelectionTimeoutError("down"))})()
    code, out = _health(store, FakeNode())
    assert code == 4 and "MongoDB unreachable" in out


def test_fast_clients_use_5s_timeouts(dcfg):
    src = health.fast_source(dcfg)
    assert src.request_timeout == 5 and src.connect_retries == 1
    assert health.fast_source(dcfg, check_network=False).expected_network_id is None
    assert health.FAST_MONGO == {"serverSelectionTimeoutMS": 5000, "connectTimeoutMS": 5000,
                                 "socketTimeoutMS": 5000}


# -- against the real local MongoDB (throwaway DB) --------------------------------------------

@pytest.mark.integration
def test_doctor_against_local_mongodb(tmp_path):
    if os.environ.get("XRPL_INGEST_NO_INTEGRATION") == "1":
        pytest.skip("XRPL_INGEST_NO_INTEGRATION=1")
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
    client = MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=1000)
    try:
        client.admin.command("ping")
    except PyMongoError:
        pytest.skip("no local MongoDB")
    db_name = f"xrpl_ingest_test_{uuid.uuid4().hex}"
    cfg = Config(mongo_uri="mongodb://127.0.0.1:27017", db_name=db_name, base_dir=str(tmp_path),
                 process_status_file=str(tmp_path / "ps.txt"),
                 backfill_range_file=str(tmp_path / "r.txt"))
    try:
        client[db_name][cfg.tx_collection].insert_many([{"hash": "D"}, {"hash": "D"}])
        lines = []
        code = run_doctor(cfg, EnvFile(), {}, str(tmp_path), node=False, out=lines.append)
        out = "\n".join(lines)
        assert "MongoDB" in out and "GB free on MongoDB's filesystem" in out
        assert "duplicates transactions.hash" in out and "'D'" in out
        assert code in (1, 2)  # duplicates warn; old installed deps may warn/fail too
    finally:
        assert db_name.startswith("xrpl_ingest_test_")
        client.drop_database(db_name)
        client.close()
