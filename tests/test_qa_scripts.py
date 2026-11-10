"""Focused, offline tests for the real-data QA harnesses."""
from contextlib import contextmanager
import json

import pytest

from reviews import _clio_driver as driver


@pytest.fixture
def record_qa_result(tmp_path, request):
    @contextmanager
    def record():
        result = {"test": request.node.nodeid}
        try:
            yield result
        except BaseException as exc:
            result.update(status="fail", error=f"{type(exc).__name__}: {exc}")
            raise
        else:
            result["status"] = "pass"
        finally:
            (tmp_path / "qa_result.json").write_text(json.dumps(result, indent=2))
    return record


def test_make_env_uses_all_servers_and_rejects_non_qa_database(record_qa_result, tmp_path):
    with record_qa_result():
        env = driver.make_env(
            "xrpl_ingest_real_fixture", tmp_path, ("wss://one", "wss://two"))
        assert env["SERVERS"] == "wss://one,wss://two"
        assert env["DB_NAME"] == "xrpl_ingest_real_fixture"
        with pytest.raises(ValueError, match="non-QA database"):
            driver.make_env("production", tmp_path, "wss://one")
        assert driver.verified_ledger_rate(8, 2, True) == 4
        assert driver.verified_ledger_rate(8, 2, False) is None


def test_interruption_wait_requires_observed_partial_database_progress(record_qa_result):
    with record_qa_result():
        class RunningProcess:
            returncode = None

            def poll(self):
                return None

        class PartialStore:
            def ledger_numbers(self, _start, _end):
                return [101, 102]

        assert driver.wait_for_partial_progress(
            RunningProcess(), PartialStore(), 101, 140, 40) == 2

        class ExitedProcess:
            returncode = 0

            def poll(self):
                return self.returncode

        with pytest.raises(RuntimeError, match="exited before interruption"):
            driver.wait_for_partial_progress(
                ExitedProcess(), PartialStore(), 101, 140, 40)


def test_public_node_probe_fails_over_after_rate_limit(monkeypatch, record_qa_result):
    with record_qa_result():
        attempted = []

        class FakeSocket:
            def __init__(self, url):
                self.url = url

            async def __aenter__(self):
                attempted.append(self.url)
                return self

            async def __aexit__(self, *_):
                return False

            async def send(self, _):
                pass

            async def recv(self):
                if self.url.endswith("limited"):
                    return json.dumps({"status": "error", "error": "slowDown"})
                return json.dumps({"result": {"info": {
                    "validated_ledger": {"seq": 123}, "complete_ledgers": "1-123"}}})

        monkeypatch.setattr(driver, "SERVERS", ["wss://limited", "wss://healthy"])
        monkeypatch.setattr("websockets.connect", lambda url, **_: FakeSocket(url))
        info = driver.node_snapshot()
        assert info["validated_ledger"]["seq"] == 123
        assert attempted == ["wss://limited", "wss://healthy"]
        assert driver._is_rate_limited("RpcError: slowDown: quota exceeded")


def test_cli_failover_rotates_endpoints_and_waits_out_rate_limit(
        monkeypatch, record_qa_result):
    with record_qa_result():
        attempts = []
        delays = []

        def fake_run_cli(env, *args, timeout):
            attempts.append(env["SERVERS"].split(","))
            if len(attempts) < 3:
                return 1, "RpcError: slowDown"
            return 0, "healthy endpoint"

        monkeypatch.setattr(driver, "run_cli", fake_run_cli)
        code, log = driver.run_cli_with_failover(
            {"SERVERS": "wss://one,wss://two,wss://three"},
            "status",
            sleep=delays.append,
        )
        assert code == 0
        assert attempts == [
            ["wss://one", "wss://two", "wss://three"],
            ["wss://two", "wss://three", "wss://one"],
            ["wss://three", "wss://one", "wss://two"],
        ]
        assert delays == [driver.QUIESCE_SECONDS, driver.QUIESCE_SECONDS]
        assert "healthy endpoint" in log


def test_rate_limited_backfill_retries_recorded_failures_before_resume(
        monkeypatch, record_qa_result):
    with record_qa_result():
        calls = []

        def fake_run_cli(env, *args, timeout):
            calls.append((env["SERVERS"], args))
            if len(calls) == 1:
                return 1, "RpcError: slowDown"
            if args[0] == "retry-failed":
                return 1, "one ledger remained unavailable"
            return 0, "backfill complete"

        monkeypatch.setattr(driver, "run_cli", fake_run_cli)
        code, _ = driver.run_cli_with_failover(
            {"SERVERS": "wss://one,wss://two"}, "backfill", "--start", "1", "--end", "2",
            sleep=lambda _: None,
        )
        assert code == 0
        assert [args[0] for _, args in calls] == ["backfill", "retry-failed", "backfill"]
        assert all(servers.startswith("wss://one,wss://two") for servers, _ in calls)


def test_phase3_runs_deep_verification_and_checks_the_range(
        monkeypatch, record_qa_result, tmp_path):
    with record_qa_result():
        calls = []

        class FakeStore:
            def __init__(self, _cfg):
                pass

            def stats(self):
                return {"failed": 0}

            def ledger_numbers(self, _start, _end):
                return [998, 999]

            def find_gaps(self, _start, _end):
                return []

            def tx_count_mismatches(self, _start, _end):
                return []

            def close(self):
                pass

        monkeypatch.setattr(driver, "EVIDENCE", tmp_path)
        monkeypatch.setattr(driver, "check_mongo", lambda *_: None)
        monkeypatch.setattr(driver, "wait_ready", lambda **_: {
            "validated_ledger": {"seq": 1001, "age": 1}, "server_state": "full",
            "complete_ledgers": "1-1001",
        })
        monkeypatch.setattr(driver, "pick_server", lambda: ("wss://one", "wss://two"))
        monkeypatch.setattr(
            "xrpl_ingest.store.MongoStore.from_config",
            classmethod(lambda _cls, cfg: FakeStore(cfg)),
        )

        def fake_cli(env, *args, timeout):
            calls.append((env, args))
            return 0, "OK"

        monkeypatch.setattr(driver, "run_cli_with_failover", fake_cli)
        result = driver.phase3_confirm_node_and_backfill(window=2)
        assert [args[0] for _, args in calls] == ["backfill", "verify"]
        assert calls[1][1][-1] == "--deep"
        assert result["outcome"] == "passed" and result["verified"] is True
        assert json.loads((tmp_path / "phase3.json").read_text())["outcome"] == "passed"


def test_phase4_clears_stop_file_before_resume_and_verifies_recovery(
        monkeypatch, record_qa_result, tmp_path):
    with record_qa_result():
        class FakeStore:
            count = 1

            def __init__(self, _cfg):
                pass

            def ledger_numbers(self, _start, _end):
                return list(range(self.count))

            def stats(self):
                return {"failed": 0}

            def count_failed(self):
                return 0

            def find_gaps(self, _start, _end):
                return []

            def tx_count_mismatches(self, _start, _end):
                return []

            def close(self):
                pass

        class FakeProcess:
            returncode = None

            def poll(self):
                return self.returncode

            def wait(self, timeout):
                self.returncode = 1
                return self.returncode

        monkeypatch.setattr(driver, "EVIDENCE", tmp_path)
        monkeypatch.setattr(driver, "check_mongo", lambda *_: None)
        monkeypatch.setattr(driver, "wait_ready", lambda **_: {
            "validated_ledger": {"seq": 1001, "age": 1}, "server_state": "full",
            "complete_ledgers": "1-1001",
        })
        monkeypatch.setattr(driver, "pick_server", lambda: ("wss://one", "wss://two"))
        store = FakeStore(None)
        monkeypatch.setattr(
            "xrpl_ingest.store.MongoStore.from_config",
            classmethod(lambda _cls, _cfg: store),
        )
        monkeypatch.setattr(driver.subprocess, "Popen", lambda *_, **__: FakeProcess())
        monkeypatch.setattr(driver.time, "sleep", lambda _: None)
        invoked = []

        def fake_cli(_env, *args, timeout):
            invoked.append(args[0])
            if args[0] == "backfill":
                assert not list(tmp_path.glob("base_p4_*/process_status.txt"))
                store.count = 40
            return 0, "OK"

        monkeypatch.setattr(driver, "run_cli_with_failover", fake_cli)
        result = driver.phase4_interrupt_resume(window=40)
        assert result["confirmed_in_progress"]["ledgers_written"] == 1
        assert result["interrupt_exit"] == 1
        assert result["interrupted_ledger_count"] == 1
        assert result["outcome"] == "passed"
        assert invoked == ["backfill", "verify"]
        assert json.loads((tmp_path / "phase4.json").read_text())["verified"] is True


def test_mongo_unavailability_is_reported_as_structured_failure(
        monkeypatch, record_qa_result, tmp_path):
    with record_qa_result():
        from pymongo.errors import ServerSelectionTimeoutError

        class FakeAdmin:
            def command(self, _):
                raise ServerSelectionTimeoutError("local QA MongoDB unavailable")

        class FakeClient:
            admin = FakeAdmin()
            closed = False

            def __init__(self, *_args, **_kwargs):
                pass

            def close(self):
                self.closed = True

        monkeypatch.setattr("pymongo.MongoClient", FakeClient)
        with pytest.raises(ServerSelectionTimeoutError, match="unavailable"):
            driver.check_mongo()
        monkeypatch.setattr(driver, "EVIDENCE", tmp_path)
        with pytest.raises(ServerSelectionTimeoutError):
            driver.phase3_confirm_node_and_backfill()
        result = json.loads((tmp_path / "phase3.json").read_text())
        assert result["outcome"] == "failed"
        assert "local QA MongoDB unavailable" in result["error"]
