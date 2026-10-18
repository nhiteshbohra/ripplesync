"""CLI data commands: verify --repair/--deep, fetch, tx. .env is never loaded."""
import dataclasses

import pytest

from conftest import NOW, FakeSource, make_result
from xrpl_ingest import cli
from xrpl_ingest.errors import ConnectFailed, RpcError
from xrpl_ingest.transform import parse_ledger_result


@pytest.fixture
def cli_main(monkeypatch, cfg, store, tmp_path):
    import dotenv
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *a, **k: False)
    monkeypatch.setattr(cli.Config, "from_env", staticmethod(
        lambda env=None: dataclasses.replace(cfg, process_status_file=str(tmp_path / "stop.txt"))))
    monkeypatch.setattr(cli, "setup_logging", lambda level: None)
    monkeypatch.setattr(cli.MongoStore, "from_config", lambda _cfg: store)
    monkeypatch.setattr(cli.MongoStore, "close", lambda self: None)
    return cli.main


@pytest.fixture
def node(monkeypatch):
    """The FakeSource every command gets; tests script it."""
    src = FakeSource(latest=100, ranges="1-100")
    seen = []
    monkeypatch.setattr(cli.WsSource, "from_config",
                        lambda _cfg, **kw: seen.append(kw.get("stop_check")) or src)
    src.stop_checks = seen
    return src


class DeadNode:
    async def server_info(self):
        raise ConnectFailed("all servers failed: refused")

    async def latest_validated(self):
        raise ConnectFailed("all servers failed: refused")

    async def get_ledger(self, index):
        raise ConnectFailed("all servers failed: refused")

    async def close(self):
        pass


@pytest.fixture
def dead_node(monkeypatch):
    monkeypatch.setattr(cli.WsSource, "from_config", lambda _cfg, **kw: DeadNode())


def save(store, *indices):
    for i in indices:
        store.save_ledger_bundle(*parse_ledger_result(i, make_result(i), NOW))


# -- verify --repair ------------------------------------------------------------------------

def test_verify_repair_fixes_and_exits_0(cli_main, node, store, capsys):
    save(store, 1, 2, 5)
    assert cli_main(["verify", "--start", "1", "--end", "5", "--repair"]) == 0
    out = capsys.readouterr().out
    assert "missing ledgers 3-4" in out and "repaired 2" in out and out.rstrip().endswith("OK")
    assert sorted(node.calls) == [3, 4] and store.find_gaps(1, 5) == []
    assert store.meta.count_documents({"kind": "backfill_chunk"}) == 0


def test_verify_repair_still_failing_exits_1(cli_main, node, store, capsys):
    save(store, 1, 2)
    node.script[3] = [RpcError("lgrNotFound")]
    assert cli_main(["verify", "--start", "1", "--end", "3", "--repair"]) == 1
    out = capsys.readouterr().out
    assert "ledger 3: repair failed" in out and "1 problem(s) remain" in out


def test_verify_repair_node_unreachable_exits_4(cli_main, dead_node, store, capsys):
    assert cli_main(["verify", "--start", "1", "--end", "3", "--repair"]) == 4
    assert "ConnectFailed" in capsys.readouterr().err


def test_verify_repair_reversed_range_exits_2(cli_main, node):
    assert cli_main(["verify", "--start", "5", "--end", "1", "--repair"]) == 2
    assert node.calls == []


def test_verify_repair_respects_stop_file(cli_main, node, store, tmp_path, capsys):
    (tmp_path / "stop.txt").write_text("current_process=0")
    assert cli_main(["verify", "--start", "1", "--end", "3", "--repair"]) == 1
    assert node.calls == [] and "stopped early" in capsys.readouterr().out
    assert node.stop_checks and node.stop_checks[0]() is True  # source gets the stop check too


def test_plain_verify_unchanged_needs_no_node(cli_main, dead_node, store, capsys):
    save(store, 1, 2, 3)
    assert cli_main(["verify", "--start", "1", "--end", "3"]) == 0
    assert capsys.readouterr().out.strip() == "OK"


# -- verify --deep --------------------------------------------------------------------------

def test_verify_deep_clean_exits_0(cli_main, node, store, capsys):
    save(store, 1, 2, 3)
    assert cli_main(["verify", "--start", "1", "--end", "3", "--deep"]) == 0
    out = capsys.readouterr().out
    assert "chain breaks 0, hash mismatches 0" in out and out.rstrip().endswith("OK")


def test_verify_deep_hash_mismatch_exits_1(cli_main, node, store, capsys):
    save(store, 1, 2, 3)
    store.txs.update_one({"hash": "H2_0"}, {"$set": {"hash": "BOGUS"}})
    assert cli_main(["verify", "--start", "1", "--end", "3", "--deep"]) == 1
    assert "ledger 2: tx hashes differ from node" in capsys.readouterr().out
    assert node.calls == []  # no repair without --repair


def test_verify_deep_without_node_runs_offline_checks(cli_main, dead_node, store, capsys):
    save(store, 1, 2, 3)
    store.ledgers.update_one({"block_number": 3}, {"$set": {"parent_hash": "WRONG"}})
    assert cli_main(["verify", "--start", "1", "--end", "3", "--deep"]) == 1  # problem found
    out = capsys.readouterr().out
    assert "node: error: ConnectFailed" in out and "ledger 3: parent_hash WRONG" in out


def test_verify_deep_without_node_and_offline_ok_exits_4(cli_main, dead_node, store, capsys):
    save(store, 1, 2, 3)
    assert cli_main(["verify", "--start", "1", "--end", "3", "--deep"]) == 4
    assert "offline checks OK; node comparison not done" in capsys.readouterr().out


def test_verify_deep_repair_fixes_hash_mismatch(cli_main, node, store, capsys):
    save(store, 1, 2, 3)
    store.txs.delete_one({"hash": "H2_1"})
    store.ledgers.update_one({"block_number": 2}, {"$set": {"num_transactions": 1}})
    assert cli_main(["verify", "--start", "1", "--end", "3", "--deep", "--repair"]) == 0
    assert node.calls == [2] and store.tx_hashes(2) == {"H2_0", "H2_1"}


def test_verify_deep_repair_without_node_exits_4(cli_main, dead_node, store):
    save(store, 1, 2, 3)
    assert cli_main(["verify", "--start", "1", "--end", "3", "--deep", "--repair"]) == 4


# -- fetch ----------------------------------------------------------------------------------

L = 32570  # FIRST_LEDGER


def test_fetch_number_stores_ledger_without_chunk_and_clears_failure(cli_main, node, store,
                                                                     capsys):
    store.record_failure(L + 5, "earlier")
    assert cli_main(["fetch", "--ledger", str(L + 5)]) == 0
    assert capsys.readouterr().out.strip() == f"block_number {L + 5}: 2 transactions, stored"
    assert node.calls == [L + 5] and store.ledgers.count_documents({"block_number": L + 5}) == 1
    assert store.list_failed() == []
    assert store.meta.count_documents({"kind": "backfill_chunk"}) == 0


def test_fetch_validated_resolves_latest(cli_main, node, store, capsys):
    node.latest = L + 42
    assert cli_main(["fetch", "--ledger", "validated"]) == 0
    assert node.calls == [L + 42] and f"block_number {L + 42}" in capsys.readouterr().out


def test_fetch_failure_exits_1_and_is_recorded(cli_main, node, store, capsys):
    node.script[L] = [RpcError("lgrNotFound")]
    assert cli_main(["fetch", "--ledger", str(L)]) == 1
    assert "failed: RpcError: lgrNotFound" in capsys.readouterr().out
    assert store.list_failed() == [L]


def test_fetch_node_unreachable_exits_4(cli_main, dead_node, capsys):
    assert cli_main(["fetch", "--ledger", "validated"]) == 4
    assert cli_main(["fetch", "--ledger", str(L)]) == 4


@pytest.mark.parametrize("value", ["abc", "-5", str(L - 1), "1.5", ""])
def test_fetch_bad_ledger_argument_exits_2(cli_main, node, capsys, value):
    with pytest.raises(SystemExit) as e:
        cli_main(["fetch", "--ledger", value])
    assert e.value.code == 2 and node.calls == []
    assert "expected a ledger number >= 32570 or 'validated'" in capsys.readouterr().err


@pytest.mark.parametrize("error,recorded,code", [
    (None, True, 0), ("RpcError: lgrNotFound", True, 1), ("StopRequested: stop", False, 1),
    ("NetworkMismatch: network_id 1 != expected 0", True, 2),
    ("ConnectFailed: all servers failed", True, 4), ("ServerSelectionTimeoutError: x", False, 4),
])
def test_fetch_exit_code(error, recorded, code):
    from xrpl_ingest.fetcher import IngestResult
    r = IngestResult(L, error is None, 0, error=error, recorded=recorded)
    assert cli.fetch_exit_code(r) == code
