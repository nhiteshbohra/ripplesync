import dataclasses
import os

import pytest
from pymongo.errors import InvalidURI, ServerSelectionTimeoutError

from conftest import FakeSource
from xrpl_ingest import __version__, cli
from xrpl_ingest.support import runcontrol as stopflag
from xrpl_ingest.cli import COMMANDS, _verify, backfill_exit_code, build_parser
from xrpl_ingest.errors import ConnectFailed, NetworkMismatch
from xrpl_ingest.support.runcontrol import InstanceLock, lock_path

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


def _sub_help(name):
    return build_parser()._subparsers._group_actions[0].choices[name].format_help()


def test_backfill_help_documents_exit_codes():
    text = _sub_help("backfill")
    assert "3" in text and "complete_ledgers" in text and "--range-file" in text


def test_main_help_lists_every_exit_code_as_a_table():
    text = build_parser().format_help()
    for line in ("  0    ok", "  1    ", "  2    config", "  3    backfill", "  4    node or MongoDB",
                 "  5    another instance", "  6    unexpected internal error", "  130  interrupted"):
        assert line in text, line
    assert "current_process=0" in text and "retry-failed" in text  # stop keys documented


def test_dispatch_is_a_mapping_with_a_parser_for_every_command():
    choices = build_parser()._subparsers._group_actions[0].choices
    assert set(COMMANDS) == set(choices)
    assert {n for n, c in COMMANDS.items() if c.ingest} == {"backfill", "tail", "retry-failed"}


# -- .env lookup: real paths.load_env, synthetic files in tmp_path only --------------

@pytest.fixture
def run_config(isolated_environ, monkeypatch, tmp_path, restore_logging, capsys):
    """Run `config` for real (it never touches the network) and return (code, stdout)."""
    def go(cwd=None, **env):
        monkeypatch.chdir(cwd or tmp_path)
        os.environ.update(env)
        code = cli.main(["config"])
        return code, capsys.readouterr()
    return go


def _row(out, name):
    return next(line for line in out.splitlines() if line.startswith(name + " "))


def test_env_file_is_taken_from_cwd(run_config, tmp_path):
    (tmp_path / ".env").write_text("MONGO_URI=mongodb://from-cwd\n")
    code, cap = run_config()
    assert code == 0
    assert "mongodb://from-cwd" in _row(cap.out, "MONGO_URI") and "[.env]" in _row(cap.out, "MONGO_URI")
    assert str(tmp_path / ".env") in _row(cap.out, "ENV_FILE")


def test_env_lookup_does_not_walk_up_to_parent_dirs(run_config, tmp_path):
    (tmp_path / ".env").write_text("MONGO_URI=mongodb://parent\n")
    sub = tmp_path / "sub"
    sub.mkdir()
    code, cap = run_config(cwd=sub)
    assert code == 2 and "MONGO_URI is required" in cap.out
    assert "parent" not in cap.out


def test_env_file_variable_and_relative_paths_resolve_against_its_dir(run_config, tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    (home / "my.env").write_text("MONGO_URI=mongodb://explicit\nPROCESS_STATUS_FILE=ps.txt\n")
    code, cap = run_config(ENV_FILE=str(home / "my.env"))
    assert code == 0
    assert "mongodb://explicit" in cap.out
    assert str(home / "ps.txt") in _row(cap.out, "PROCESS_STATUS_FILE")
    assert str(home) in _row(cap.out, "XRPL_INGEST_HOME")


def test_missing_env_file_variable_is_a_config_error(run_config, tmp_path):
    code, cap = run_config(ENV_FILE=str(tmp_path / "nope.env"))
    assert code == 2 and "ENV_FILE" in cap.err and "does not exist" in cap.err


def test_xrpl_ingest_home_holds_env_and_is_the_base_dir(run_config, tmp_path):
    home = tmp_path / "srv"
    home.mkdir()
    (home / ".env").write_text("MONGO_URI=mongodb://home\nLOG_FILE=logs/x.log\n")
    code, cap = run_config(XRPL_INGEST_HOME=str(home))
    assert code == 0 and "mongodb://home" in cap.out
    assert str(home / "logs" / "x.log") in _row(cap.out, "LOG_FILE")
    assert str(home / "backfill_range.txt") in _row(cap.out, "BACKFILL_RANGE_FILE")


def test_process_env_overrides_env_file(run_config, tmp_path):
    (tmp_path / ".env").write_text("MONGO_URI=mongodb://file\nDB_NAME=fromfile\n")
    code, cap = run_config(DB_NAME="fromenv")
    assert "fromenv" in _row(cap.out, "DB_NAME") and "[env (overrides .env)]" in _row(cap.out, "DB_NAME")
    assert "[default]" in _row(cap.out, "CHUNK_SIZE")


@pytest.mark.parametrize("label,encode", [
    ("utf-8 BOM (PS 5.1 Set-Content -Encoding utf8)", lambda t: "\ufeff".encode() + t.encode()),
    ("utf-16le BOM (PS 5.1 '>' / Out-File)", lambda t: "\ufeff".encode("utf-16-le") + t.encode("utf-16-le")),
    ("utf-16be BOM", lambda t: "\ufeff".encode("utf-16-be") + t.encode("utf-16-be")),
    ("CRLF", lambda t: t.replace("\n", "\r\n").encode()),
])
def test_env_file_encodings_written_by_windows_tools(run_config, tmp_path, label, encode):
    (tmp_path / ".env").write_bytes(encode("MONGO_URI=mongodb://enc\nDB_NAME=bomdb\n"))
    code, cap = run_config()
    assert code == 0, (label, cap.out, cap.err)
    assert "bomdb" in _row(cap.out, "DB_NAME") and "mongodb://enc" in cap.out, label


def test_undecodable_env_file_is_a_config_error(run_config, tmp_path):
    (tmp_path / ".env").write_bytes(b"MONGO_URI=mongodb://x\nDB_NAME=\xff\xfe\xfa\n")
    code, cap = run_config()
    assert code == 2 and "cannot read .env" in cap.err


def test_mongo_uri_file_is_read_and_never_printed(run_config, tmp_path):
    (tmp_path / "uri.txt").write_bytes("\ufeffmongodb://u:s3cret@db.example:27017/?authSource=admin\r\n"
                                       .encode("utf-8"))
    code, cap = run_config(MONGO_URI_FILE="uri.txt")
    assert code == 0
    assert "s3cret" not in cap.out + cap.err and "authSource" not in cap.out
    assert "db.example" in _row(cap.out, "MONGO_URI") and "[MONGO_URI_FILE]" in _row(cap.out, "MONGO_URI")


def test_config_command_redacts_credentials(run_config):
    code, cap = run_config(MONGO_URI="mongodb://admin:hunter2@h:27017/db?tlsCertificateKeyFilePassword=pw",
                           SERVERS="ws://user:pw123@n:6006")
    assert code == 0
    assert "hunter2" not in cap.out and "pw123" not in cap.out and "=pw" not in cap.out
    assert "mongodb://h:27017/db?<1 option(s) hidden>" in cap.out


def test_config_command_reports_errors_with_exit_2(run_config):
    code, cap = run_config(MONGO_URI="mongodb://admin:hunter2@h", CHUNK_SIZE="0")
    assert code == 2 and "CHUNK_SIZE" in cap.out and "hunter2" not in cap.out


def test_start_line_logs_version_and_resolved_paths(isolated_environ, monkeypatch, tmp_path,
                                                    cfg, store, caplog):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **k: [])
    monkeypatch.setattr(cli.Config, "from_env", staticmethod(lambda env=None, **kw: dataclasses.replace(
        cfg, process_status_file=str(tmp_path / "ps.txt"), base_dir=kw["base_dir"])))
    monkeypatch.setattr(cli.MongoStore, "from_config", lambda _cfg, **kw: store)
    monkeypatch.setattr(cli.WsSource, "from_config", lambda _cfg, **kw: FakeSource(latest=100))
    with caplog.at_level("INFO", logger="xrpl_ingest.cli"):
        assert cli.main(["status"]) == 0
    line = next(m for m in caplog.messages if m.startswith("xrpl-ingest "))
    assert __version__ in line and str(tmp_path) in line and "ps.txt" in line


# -- main(): error handling and exit codes. Config.from_env returns the test config. -----

@pytest.fixture
def cli_main(monkeypatch, cfg, store, tmp_path, isolated_environ):
    monkeypatch.chdir(tmp_path)  # no .env here; lock files go here
    monkeypatch.setattr(cli.Config, "from_env", staticmethod(lambda env=None, **kw: cfg))
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **k: [])
    monkeypatch.setattr(cli.MongoStore, "from_config", lambda _cfg, **kw: store)
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


def test_unexpected_exception_exits_6(cli_main, monkeypatch, capsys):
    monkeypatch.setattr(cli, "run_backfill", raising(ZeroDivisionError("boom")))
    assert cli_main(["backfill"]) == 6
    assert "error: internal error: ZeroDivisionError: boom" in capsys.readouterr().err


def test_unexpected_exception_before_dispatch_exits_6(cli_main, monkeypatch, capsys):
    monkeypatch.setattr(cli.files, "load_env", raising(RuntimeError("weird")))
    assert cli_main(["status"]) == 6
    assert "internal error: RuntimeError: weird" in capsys.readouterr().err


def test_backfill_unavailable_ranges_exit_3_via_main(cli_main, monkeypatch):
    summary = {**CLEAN, "unavailable_ranges": [(32570, 32599)]}
    monkeypatch.setattr(cli, "run_backfill", lambda *a, **k: summary)
    assert cli_main(["backfill"]) == 3


def _capture_backfill(monkeypatch):
    seen = {}

    def fake(cfg, start, end, **kw):
        seen.update(start=start, end=end, stop=kw.get("stop_check"))
        return CLEAN
    monkeypatch.setattr(cli, "run_backfill", fake)
    return seen


def test_backfill_uses_range_file_when_no_cli_range(cli_main, monkeypatch, tmp_path):
    (tmp_path / "backfill_range.txt").write_text("start = 40_000\nend = 50000  # note\n")
    seen = _capture_backfill(monkeypatch)
    assert cli_main(["backfill"]) == 0
    assert (seen["start"], seen["end"]) == (40000, 50000)


def test_backfill_cli_start_or_end_ignores_range_file(cli_main, monkeypatch, tmp_path):
    (tmp_path / "backfill_range.txt").write_text("start = 40000\nend = 50000\n")
    seen = _capture_backfill(monkeypatch)
    assert cli_main(["backfill", "--end", "33000"]) == 0
    assert (seen["start"], seen["end"]) == (32570, 33000)  # file ignored entirely


def test_backfill_no_range_file_flag_and_explicit_file(cli_main, monkeypatch, tmp_path):
    (tmp_path / "backfill_range.txt").write_text("start = 40000\n")
    (tmp_path / "other.txt").write_text("end = 40000\n")
    seen = _capture_backfill(monkeypatch)
    assert cli_main(["backfill", "--no-range-file"]) == 0
    assert (seen["start"], seen["end"]) == (32570, None)
    assert cli_main(["backfill", "--range-file", str(tmp_path / "other.txt")]) == 0
    assert (seen["start"], seen["end"]) == (32570, 40000)


@pytest.mark.parametrize("args,content,msg", [
    (["backfill"], "start = 1\n", "must be >= 32570"),
    (["backfill"], "begin = 40000\n", "unknown key"),
    (["backfill", "--range-file", "missing.txt"], None, "does not exist"),
    (["backfill", "--range-file", "backfill_range.txt", "--start", "40000"], "", "cannot be combined"),
])
def test_backfill_range_file_errors_exit_2(cli_main, monkeypatch, tmp_path, capsys, args, content, msg):
    if content is not None:
        (tmp_path / "backfill_range.txt").write_text(content)
    seen = _capture_backfill(monkeypatch)
    assert cli_main(args) == 2
    assert msg in capsys.readouterr().err and not seen


def test_backfill_range_file_and_no_range_file_are_exclusive(cli_main, capsys):
    with pytest.raises(SystemExit) as e:
        cli_main(["backfill", "--range-file", "x", "--no-range-file"])
    assert e.value.code == 2


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
    monkeypatch.setattr(cli.WsSource, "from_config", lambda _cfg, **kw: DeadNode())
    assert cli_main(["status"]) == 4
    out = capsys.readouterr().out
    assert "node: error: ConnectFailed" in out and "database: {" in out
    assert "end unknown (node unreachable)" in out


def test_status_ok(cli_main, monkeypatch, capsys):
    monkeypatch.setattr(cli.WsSource, "from_config", lambda _cfg, **kw: FakeSource(latest=100))
    assert cli_main(["status"]) == 0
    out = capsys.readouterr().out
    assert "latest validated: 100" in out and "database: {" in out


def test_status_shows_range_file_progress(cli_main, monkeypatch, capsys, store, tmp_path):
    (tmp_path / "backfill_range.txt").write_text("start = 32570\nend = latest\n")
    store.mark_chunk_done(32570, 32579, 10, 0)  # cfg chunk_size = 10
    store.record_failure(32575, "x")
    monkeypatch.setattr(cli.WsSource, "from_config",
                        lambda _cfg, **kw: FakeSource(latest=32609, ranges="32570-32609"))
    assert cli_main(["status"]) == 0
    line = next(m for m in capsys.readouterr().out.splitlines() if m.startswith("backfill range"))
    assert "32570-32609" in line and "chunks 1/4 (25.0%)" in line and "failed ledgers 1" in line


def test_status_with_bad_range_file_exits_2_but_prints_everything(cli_main, monkeypatch, capsys,
                                                                  tmp_path):
    (tmp_path / "backfill_range.txt").write_text("start 40000\n")
    monkeypatch.setattr(cli.WsSource, "from_config", lambda _cfg, **kw: FakeSource(latest=100))
    assert cli_main(["status"]) == 2
    out = capsys.readouterr().out
    assert "database: {" in out and "backfill_range.txt:1" in out


@pytest.mark.parametrize("command,target", [("status", "_status"), ("tail", "tail"),
                                            ("retry-failed", "retry_failed")])
def test_sources_get_the_stop_check(cli_main, monkeypatch, cfg, tmp_path, command, target):
    """The node connection gives up between connect attempts once a stop is requested."""
    monkeypatch.setattr(stopflag, "CHECK_INTERVAL", 0)  # re-read the file on every check
    flag = tmp_path / "status.txt"
    monkeypatch.setattr(cli.Config, "from_env", staticmethod(
        lambda env=None, **kw: dataclasses.replace(cfg, process_status_file=str(flag))))
    seen = []
    monkeypatch.setattr(cli.WsSource, "from_config",
                        lambda _cfg, **kw: seen.append(kw.get("stop_check")) or DeadNode())
    if target != "_status":
        async def done(*a, **k):
            return 0
        monkeypatch.setattr(cli, target, done)
    cli_main([command])
    assert len(seen) == 1 and seen[0]() is False
    flag.write_text("current_process=0")
    assert seen[0]() is True


@pytest.mark.parametrize("command,target", [("tail", "tail"), ("retry-failed", "retry_failed")])
def test_per_command_stop_key_reaches_only_that_command(cli_main, monkeypatch, cfg, tmp_path,
                                                        command, target):
    monkeypatch.setattr(stopflag, "CHECK_INTERVAL", 0)
    flag = tmp_path / "status.txt"
    flag.write_text("backfill=0\n")  # another command's key
    monkeypatch.setattr(cli.Config, "from_env", staticmethod(
        lambda env=None, **kw: dataclasses.replace(cfg, process_status_file=str(flag))))
    seen = []
    monkeypatch.setattr(cli.WsSource, "from_config",
                        lambda _cfg, **kw: seen.append(kw.get("stop_check")) or DeadNode())

    async def done(*a, **k):
        return 0
    monkeypatch.setattr(cli, target, done)
    cli_main([command])
    assert seen[0]() is False
    flag.write_text(f"{command.upper()} = 0\ncurrent_process=1\n")
    assert seen[0]() is True


def test_main_installs_signal_handlers_graceful_ctrl_c_only_for_ingest(cli_main, monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "install_stop_signal", lambda **k: calls.append(k) or True)
    monkeypatch.setattr(cli.WsSource, "from_config", lambda _cfg, **kw: FakeSource(latest=100))
    assert cli_main(["status"]) == 0
    monkeypatch.setattr(cli, "run_backfill", lambda *a, **k: CLEAN)
    assert cli_main(["backfill"]) == 0
    assert calls == [{"graceful_sigint": False}, {"graceful_sigint": True}]


# -- lock -------------------------------------------------------------------------------

def test_second_instance_exits_5_with_holder_info(cli_main, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(cli, "run_backfill", lambda *a, **k: CLEAN)
    with InstanceLock(lock_path(str(tmp_path), "backfill"), "backfill"):
        assert cli_main(["backfill"]) == 5
    err = capsys.readouterr().err
    assert f"pid {os.getpid()}" in err and "xrpl_ingest.backfill.lock" in err
    assert cli_main(["backfill"]) == 0  # released: runs again


def test_lock_is_per_command(cli_main, monkeypatch, tmp_path):
    async def done(*a, **k):
        return 0
    monkeypatch.setattr(cli, "tail", done)
    monkeypatch.setattr(cli.WsSource, "from_config", lambda _cfg, **kw: FakeSource())
    with InstanceLock(lock_path(str(tmp_path), "backfill")):
        assert cli_main(["tail"]) == 0


def test_lock_released_after_failure(cli_main, monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "run_backfill", raising(ConnectFailed("x")))
    assert cli_main(["backfill"]) == 4
    with InstanceLock(lock_path(str(tmp_path), "backfill")):  # would raise LockHeld if leaked
        pass


@pytest.mark.skipif(os.name != "posix" or (hasattr(os, "geteuid") and os.geteuid() == 0),
                    reason="needs POSIX permissions and a non-root user")
def test_unwritable_base_dir_for_lock_exits_2(cli_main, monkeypatch, tmp_path, capsys):
    ro = tmp_path / "ro"
    ro.mkdir()
    ro.chmod(0o500)
    monkeypatch.chdir(ro)
    monkeypatch.setattr(cli, "run_backfill", lambda *a, **k: CLEAN)
    try:
        assert cli_main(["backfill"]) == 2
    finally:
        ro.chmod(0o700)
    assert "cannot create lock file" in capsys.readouterr().err


# -- disk guard -------------------------------------------------------------------------

class FullDisk:
    def __init__(self, store, free_gb):
        self.store, self.free = store, free_gb

    def __call__(self):
        return {"fsTotalSize": 100 * 1024 ** 3, "fsUsedSize": (100 - self.free) * 1024 ** 3}


@pytest.mark.parametrize("command", ["tail", "retry-failed"])
def test_low_disk_refuses_to_start_exit_1(cli_main, monkeypatch, cfg, store, capsys, command):
    monkeypatch.setattr(cli.Config, "from_env", staticmethod(
        lambda env=None, **kw: dataclasses.replace(cfg, min_free_gb=10)))
    monkeypatch.setattr(store, "db_stats", FullDisk(store, 3), raising=False)
    started = []

    async def never(*a, **k):
        started.append(1)
        return 0
    monkeypatch.setattr(cli, "tail", never)
    monkeypatch.setattr(cli, "retry_failed", never)
    assert cli_main([command]) == 1
    assert "LowDiskSpace" in capsys.readouterr().err and not started


def test_disk_dropping_below_limit_stops_tail_with_exit_1(cli_main, monkeypatch, cfg, store):
    monkeypatch.setattr(cli.Config, "from_env", staticmethod(
        lambda env=None, **kw: dataclasses.replace(cfg, min_free_gb=10)))
    disk = FullDisk(store, 50)
    monkeypatch.setattr(store, "db_stats", disk, raising=False)
    monkeypatch.setattr(cli.DiskGuard, "__init__", _guard_init_no_interval)

    async def fake_tail(source, store_, retry, *, stop_check, **kw):
        assert stop_check() is False
        disk.free = 1
        assert stop_check() is True
        return 0
    monkeypatch.setattr(cli, "tail", fake_tail)
    monkeypatch.setattr(cli.WsSource, "from_config", lambda _cfg, **kw: FakeSource())
    assert cli_main(["tail"]) == 1


_real_guard_init = cli.DiskGuard.__init__


def _guard_init_no_interval(self, store, min_free_gb, **kw):
    _real_guard_init(self, store, min_free_gb, interval=0)


# -- stdout -------------------------------------------------------------------------------

def test_stdout_reconfigured_to_never_fail_on_unencodable_text(monkeypatch):
    import io
    import sys
    raw = io.BytesIO()
    out = io.TextIOWrapper(raw, encoding="cp1252", newline="\n")
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", io.TextIOWrapper(io.BytesIO(), encoding="cp1252"))
    cli._reconfigure_streams()
    print("ledger \u4f8b")  # strict cp1252 would raise UnicodeEncodeError
    out.flush()
    assert raw.getvalue() == b"ledger \\u4f8b\n"


# -- verify -------------------------------------------------------------------------------

def test_verify_rejects_reversed_range(store, capsys):
    assert _verify(store, 200, 100) == 2
    out = capsys.readouterr()
    assert "OK" not in out.out and "must be <=" in out.err


def test_verify_reports_missing_ledgers(store, capsys):
    assert _verify(store, 100, 102) == 1
    assert "missing ledgers 100-102" in capsys.readouterr().out
