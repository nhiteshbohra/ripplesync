import os

import pytest
from pymongo.errors import InvalidURI, ServerSelectionTimeoutError

from conftest import FakeSource
from xrpl_ingest import cli
from xrpl_ingest.cli import _verify, backfill_exit_code, build_parser
from xrpl_ingest.errors import ConfigError, ConnectFailed, NetworkMismatch

CLEAN = {"ledgers_failed": 0, "chunks_incomplete": 0, "unavailable_ranges": []}


@pytest.mark.parametrize("changes,code", [
    ({}, 0),
    ({"unavailable_ranges": [(32570, 32599)]}, 3),  # node lacks part of the range
    ({"ledgers_failed": 2}, 1),
    ({"chunks_incomplete": 1}, 1),
    ({"ledgers_failed": 1, "unavailable_ranges": [(1, 2)]}, 1),  # failure takes priority
    ({"chunks_incomplete": 1, "unavailable_ranges": [(1, 2)]}, 1),
])
def test_backfill_exit_code(changes, code):
    assert backfill_exit_code({**CLEAN, **changes}) == code


def test_backfill_help_documents_exit_codes():
    sub = build_parser()._subparsers._group_actions[0].choices["backfill"]
    assert "3" in sub.format_help() and "complete_ledgers" in sub.format_help()


# -- main(): error handling and exit codes. .env is never loaded: load_dotenv is a no-op and
# Config.from_env returns the test config. -------------------------------------------------

PACKAGE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(cli.__file__)))


@pytest.fixture
def env_lookups(monkeypatch, tmp_path):
    """Record what main() asks dotenv to load; never load anything. cwd = tmp_path."""
    import dotenv
    loaded = []
    monkeypatch.setattr(dotenv, "load_dotenv", lambda path=None, **k: loaded.append(path) or False)
    monkeypatch.setattr(cli.Config, "from_env", staticmethod(raising(ConfigError("stop here"))))
    monkeypatch.chdir(tmp_path)
    return loaded


def test_env_file_is_taken_from_cwd(env_lookups, tmp_path):
    (tmp_path / ".env").write_text("MONGO_URI=mongodb://from-cwd\n")
    assert cli.main(["status"]) == 2
    assert env_lookups == [str(tmp_path / ".env")]


def test_env_lookup_does_not_fall_back_to_package_dir(env_lookups):
    assert cli.main(["status"]) == 2
    assert len(env_lookups) == 1 and isinstance(env_lookups[0], str)  # cwd lookup was used
    assert not env_lookups[0].startswith(PACKAGE_ROOT)


@pytest.fixture
def cli_main(monkeypatch, cfg, store, tmp_path):
    import dotenv
    monkeypatch.chdir(tmp_path)  # .env lookup starts here, far from the repo
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *a, **k: False)
    monkeypatch.setattr(cli.Config, "from_env", staticmethod(lambda env=None: cfg))
    monkeypatch.setattr(cli, "setup_logging", lambda level: None)
    monkeypatch.setattr(cli.MongoStore, "from_config", lambda _cfg: store)
    return cli.main


def raising(exc):
    def f(*a, **k):
        raise exc
    return f


@pytest.mark.parametrize("exc,code", [
    (ValueError("start must be >= 32570"), 2),
    (NetworkMismatch("every server is on the wrong network"), 2),
    (InvalidURI("bad uri"), 2),
    (ConnectFailed("all servers failed: refused"), 4),
    (ServerSelectionTimeoutError("127.0.0.1:27017 refused"), 4),
    (KeyboardInterrupt(), 130),
])
def test_backfill_errors_map_to_exit_codes_without_traceback(cli_main, monkeypatch, capsys,
                                                             exc, code):
    monkeypatch.setattr(cli, "run_backfill", raising(exc))
    assert cli_main(["backfill"]) == code
    err = capsys.readouterr().err
    assert "Traceback" not in err
    if code != 130:
        assert err.startswith(f"error: {type(exc).__name__}: ")


def test_backfill_unavailable_ranges_exit_3_via_main(cli_main, monkeypatch):
    summary = {**CLEAN, "unavailable_ranges": [(32570, 32599)]}
    monkeypatch.setattr(cli, "run_backfill", lambda *a, **k: summary)
    assert cli_main(["backfill"]) == 3


def test_tail_on_wrong_network_exits_2(cli_main, monkeypatch, capsys):
    async def wrong_network(*a, **k):
        raise NetworkMismatch("every server is on the wrong network: network_id 1 != expected 0")
    monkeypatch.setattr(cli, "tail", wrong_network)
    assert cli_main(["tail"]) == 2
    assert "NetworkMismatch" in capsys.readouterr().err


class DeadNode:
    async def server_info(self):
        raise ConnectFailed("all servers failed: refused")

    async def close(self):
        pass


def test_status_prints_db_stats_when_node_unreachable(cli_main, monkeypatch, capsys):
    monkeypatch.setattr(cli.WsSource, "from_config", lambda _cfg: DeadNode())
    assert cli_main(["status"]) == 4
    out = capsys.readouterr().out
    assert "node: error: ConnectFailed" in out and "database: {" in out


def test_status_ok(cli_main, monkeypatch, capsys):
    monkeypatch.setattr(cli.WsSource, "from_config", lambda _cfg: FakeSource(latest=100))
    assert cli_main(["status"]) == 0
    out = capsys.readouterr().out
    assert "latest validated: 100" in out and "database: {" in out


def test_main_installs_sigterm_handler(cli_main, monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "install_stop_signal", lambda: calls.append(1) or True)
    monkeypatch.setattr(cli.WsSource, "from_config", lambda _cfg: FakeSource(latest=100))
    assert cli_main(["status"]) == 0 and calls == [1]


def test_main_help_lists_exit_codes():
    text = build_parser().format_help()
    assert "4 = node or MongoDB" in text and "130" in text


def test_verify_rejects_reversed_range(store, capsys):
    assert _verify(store, 200, 100) == 2
    out = capsys.readouterr()
    assert "OK" not in out.out and "must be <=" in out.err


def test_verify_reports_missing_ledgers(store, capsys):
    assert _verify(store, 100, 102) == 1
    assert "missing ledgers 100-102" in capsys.readouterr().out
