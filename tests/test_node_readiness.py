"""Startup guard runs before command work and never writes while the node is syncing."""
import asyncio

import pytest

from xrpl_ingest import cli
from xrpl_ingest.errors import ConnectFailed, IngestError, NetworkMismatch


@pytest.mark.parametrize("state,latest,ranges,ready", [
    ("connected", 0, "empty", False),
    ("syncing", 100, "1-100", False),
    ("full", 0, "1-100", False),
    ("full", True, "1-100", False),
    ("full", 100, "empty", False),
    ("full", 100, "1-99", False),
    ("full", 100, "1-100", True),
    ("proposing", 100, "1-100", True),
    ("validating", 100, "1-100", True),
])
def test_ready_requires_synced_state_and_available_validated_ledger(
        monkeypatch, cfg, state, latest, ranges, ready):
    closed = []

    class Node:
        async def server_info(self):
            return {"server_state": state, "validated_ledger": {"seq": latest},
                    "complete_ledgers": ranges}

        async def close(self):
            closed.append(True)

    monkeypatch.setattr(cli.WsSource, "from_config", lambda *a, **kw: Node())
    if ready:
        asyncio.run(cli._check_node_ready(cfg, lambda: False))
    else:
        with pytest.raises(IngestError, match="Please wait a few minutes"):
            asyncio.run(cli._check_node_ready(cfg, lambda: False))
    assert closed == [True]


@pytest.mark.parametrize("args", [
    ["backfill"], ["tail"], ["retry-failed"], ["fetch", "--ledger", "validated"],
    ["tx", "--hash", "A" * 64],
    ["verify", "--start", "32570", "--end", "32571", "--repair"],
])
@pytest.mark.parametrize("outcome,code", [(None, 0), ("syncing", 4),
                                        (ConnectFailed("refused"), 4),
                                        (NetworkMismatch("wrong network"), 2)])
def test_main_checks_node_before_dispatch(monkeypatch, tmp_path, isolated_environ,
                                         args, outcome, code, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MONGO_URI", "mongodb://unused")
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **kw: None)
    calls = []

    class Node:
        async def server_info(self):
            calls.append("check")
            if isinstance(outcome, Exception):
                raise outcome
            return {"server_state": "connected" if outcome else "full",
                    "validated_ledger": {"seq": 100}, "complete_ledgers": "1-100"}

        async def close(self):
            calls.append("close")

    monkeypatch.setattr(cli.WsSource, "from_config", lambda *a, **kw: Node())

    def execute(run):
        calls.append("execute")
        return 0

    original = cli.COMMANDS[args[0]]
    monkeypatch.setitem(cli.COMMANDS, args[0], cli.Command(execute, ingest=original.ingest))
    assert cli.main(args) == code
    assert calls == (["check", "close", "execute"] if code == 0 else ["check", "close"])
    if outcome == "syncing":
        assert "Please wait a few minutes" in capsys.readouterr().err


@pytest.mark.parametrize("args", [["config"], ["doctor", "--node"], ["status"], ["health"],
                                 ["verify", "--start", "32570", "--end", "32571"]])
def test_diagnostic_and_offline_commands_remain_available(
        monkeypatch, tmp_path, isolated_environ, args):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MONGO_URI", "mongodb://unused")
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **kw: None)
    monkeypatch.setitem(cli.COMMANDS, args[0], cli.Command(lambda run: 0))
    monkeypatch.setattr(cli.WsSource, "from_config", lambda *a, **kw: pytest.fail("startup check"))
    assert cli.main(args) == 0
