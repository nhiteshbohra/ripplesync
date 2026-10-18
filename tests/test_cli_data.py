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
    assert "ledger 3: repair failed" in out and "1 problem ledger(s) remain" in out


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
