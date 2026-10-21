"""Failure regressions for chaos findings 012-021; fixtures never load the real .env."""
import asyncio
import codecs
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from conftest import FakeWs
from xrpl_ingest.errors import ConfigError, ConnectionLost, RequestTimeout, TransientError
from xrpl_ingest.source import WsSource
from xrpl_ingest.store import LAST_LEDGER_ID
from xrpl_ingest.support import monitor, runcontrol

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "setup"))
import edit_cfg
import envfile
import xrpld_check


@pytest.mark.parametrize("traffic", [False, True])
def test_blocked_send_expires_and_drops_even_with_other_traffic(traffic):
    async def run():
        entered = asyncio.Event()

        class BlockedWs(FakeWs):
            async def send(self, raw):
                entered.set()
                await asyncio.Event().wait()

        ws = BlockedWs(lambda req: None)

        async def connect(*args, **kwargs):
            return ws

        source = WsSource(["ws://fixture"], connect_fn=connect, expected_network_id=None,
                          request_timeout=0.05)
        task = asyncio.create_task(source.get_ledger(32570))
        await entered.wait()
        conn = source._conn
        sibling = asyncio.get_running_loop().create_future()
        conn.pending[999] = sibling
        if traffic:
            conn.received += 1
        try:
            with pytest.raises(RequestTimeout):
                await asyncio.wait_for(task, 1)
            assert conn.closed and ws.closed and not conn.pending
            with pytest.raises(ConnectionLost):
                await sibling
        finally:
            await source.close()
    asyncio.run(run())


def test_send_and_reply_share_one_deadline():
    async def run():
        class SlowWs(FakeWs):
            async def send(self, raw):
                await asyncio.sleep(0.2)
                await super().send(raw)

        def reply(req):
            response = {"id": req["id"], "result": {"fixture": True}}
            asyncio.get_running_loop().call_later(0.2, ws.queue.put_nowait, json.dumps(response))
            return None

        ws = SlowWs(reply)

        async def connect(*args, **kwargs):
            return ws

        source = WsSource(["ws://fixture"], connect_fn=connect, expected_network_id=None,
                          request_timeout=0.3)
        try:
            # Each stage takes less than the timeout; together they exceed it.
            with pytest.raises(RequestTimeout):
                await source.request("ledger")
        finally:
            await source.close()
    asyncio.run(run())


@pytest.mark.parametrize("info", [[], None, "bad", {"validated_ledger": []},
                                {"validated_ledger": {"seq": True}},
                                {"validated_ledger": {"seq": "12"}}])
@pytest.mark.parametrize("network_check", [False, True])
def test_malformed_node_metadata_raises_retryable_error(info, network_check):
    async def run():
        ws = FakeWs(lambda req: {"id": req["id"], "result": {"info": info}})

        async def connect(*args, **kwargs):
            return ws

        source = WsSource(["ws://fixture"], connect_fn=connect, connect_retries=1,
                          expected_network_id=0 if network_check else None)
        try:
            with pytest.raises(TransientError):
                await source.latest_validated()
        finally:
            await source.close()
    asyncio.run(run())


@pytest.mark.parametrize("value", ["legacy-invalid", None, True, -1, 12.5, float("inf"), float("nan")])
def test_damaged_checkpoint_fails_closed_on_every_reader_and_writer(store, value):
    store.meta.insert_one({"_id": LAST_LEDGER_ID, "index": value, "keep": "fixture"})
    for operation in (store.get_checkpoint, lambda: monitor.read_checkpoint(store),
                      lambda: store.set_checkpoint(100)):
        with pytest.raises(ConfigError, match="No progress was reset"):
            operation()
        saved = store.meta.find_one({"_id": LAST_LEDGER_ID})
        assert saved["keep"] == "fixture" and "updated_at" not in saved
        assert repr(saved["index"]) == repr(value)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "utf-16", "utf-16-be"])
def test_installer_reads_and_updates_bom_configuration(tmp_path, encoding):
    path = tmp_path / ".env"
    text = "# keep comment\nMONGO_URI_FILE=secret-file.txt\nDB_NAME=fixture\n"
    data = text.encode(encoding)
    if encoding == "utf-16-be":
        data = codecs.BOM_UTF16_BE + data
    path.write_bytes(data)
    assert envfile.main([str(path), "--has", "MONGO_URI_FILE"]) == 0
    assert envfile.main([str(path), "DB_NAME=updated"]) == 0
    assert envfile.read_text(str(path)) == text.replace("fixture", "updated")
    assert next(tmp_path.glob(".env.bak.*")).read_bytes() == data


@pytest.mark.parametrize("writer", ["env", "node", "validators"])
def test_repeated_config_edits_keep_all_backups(tmp_path, writer):
    path = tmp_path / "config"
    path.write_text("original", encoding="utf-8")
    if writer == "env":
        envfile.main([str(path), "DB_NAME=first"])
        intermediate = path.read_bytes()
        envfile.main([str(path), "DB_NAME=second"])
    elif writer == "node":
        edit_cfg.main([str(path), "--db-path", "/first"])
        intermediate = path.read_bytes()
        edit_cfg.main([str(path), "--db-path", "/second"])
    else:
        xrpld_check._backup(str(path))
        path.write_text("first", encoding="utf-8")
        intermediate = path.read_bytes()
        xrpld_check._backup(str(path))
    backups = [p.read_bytes() for p in tmp_path.glob("config.bak.*")]
    assert len(backups) == 2 and b"original" in backups and intermediate in backups


@pytest.mark.parametrize("fail_at", [1, 2])
def test_validator_repair_interruption_preserves_config_target(tmp_path, monkeypatch, fail_at):
    cfg, custom, target = (tmp_path / p for p in ("xrpld.cfg", "custom.txt", "validators.txt"))
    cfg.write_text("[validators_file]\ncustom.txt\n", encoding="utf-8")
    custom.write_text("[validator_list_keys]\nwrong\n", encoding="utf-8")
    target.write_text("old target", encoding="utf-8")
    original = edit_cfg.write_atomic
    writes = 0

    def interrupted(path, text):
        nonlocal writes
        writes += 1
        if writes == fail_at:
            raise OSError("fixture disk failure")
        original(path, text)

    monkeypatch.setattr(edit_cfg, "write_atomic", interrupted)
    with pytest.raises(OSError, match="fixture disk failure"):
        xrpld_check.run("fix", str(cfg))
    assert cfg.read_text(encoding="utf-8") == "[validators_file]\ncustom.txt\n"
    assert Path(xrpld_check.validators_path(str(cfg), cfg.read_text())).exists()
    assert next(tmp_path.glob("validators.txt.bak.*")).read_text() == "old target"
    assert next(tmp_path.glob("custom.txt.bak.*")).read_bytes() == custom.read_bytes()


@pytest.mark.parametrize("host,protocol,expected", [("127.0.0.1", "ws", "ws://127.0.0.1:6007"),
    ("0.0.0.0", "ws,http", "ws://127.0.0.1:6007"), ("::", "ws", "ws://[::1]:6007"),
    ("127.0.0.1", "wss", "wss://127.0.0.1:6007")])
def test_installer_reads_custom_node_endpoint_without_editing(tmp_path, capsys, host, protocol, expected):
    path = tmp_path / "xrpld.cfg"
    text = f"[server]\nport_ws_admin_local\n[port_ws_admin_local]\nport=6007\nip={host}\nprotocol={protocol}\n"
    path.write_text(text, encoding="utf-8")
    assert edit_cfg.main([str(path), "--ws-url"]) == 0
    assert capsys.readouterr().out.strip() == expected
    assert path.read_text(encoding="utf-8") == text
    assert not list(tmp_path.glob("*.bak.*"))


def test_doctor_probe_never_acquires_or_writes_ingestion_lock(tmp_path, monkeypatch):
    path = str(tmp_path / "tail.lock")
    Path(path).write_text("pid=42\n", encoding="utf-8")
    with monkeypatch.context() as patch:
        patch.setattr(runcontrol.InstanceLock, "acquire", lambda *a, **kw: pytest.fail("doctor acquired lock"))
        assert runcontrol.probe(path)["held"].startswith("unknown")
    assert Path(path).read_text() == "pid=42\n"
    with runcontrol.InstanceLock(path):
        assert runcontrol.probe(path)["held"].startswith("unknown")
        with pytest.raises(runcontrol.LockHeld):
            runcontrol.InstanceLock(path).acquire()


@pytest.mark.skipif(os.name != "nt", reason="Windows installer discovery")
@pytest.mark.parametrize("candidate", ["venv", "python", "py", "absent"])
def test_windows_python_discovery_without_running_installer(tmp_path, candidate):
    script = tmp_path / "probe.ps1"
    script.write_text(r'''param($Source, $Candidate)
$ErrorActionPreference='Stop'
$script:Wanted=$Candidate
$tokens=$null; $errors=$null
$ast=[System.Management.Automation.Language.Parser]::ParseFile($Source,[ref]$tokens,[ref]$errors)
if ($errors) { throw ($errors | Out-String) }
$fn=$ast.Find({param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'Find-Python'},$true)
. ([scriptblock]::Create($fn.Extent.Text))
$venvPy='fixture-venv.exe'
function Test-Path { param($LiteralPath) return $script:Wanted -eq 'venv' -and $LiteralPath -eq $venvPy }
function Get-Command { param($Name) if ($script:Wanted -eq $Name) { return $Name }; return $null }
function Get-NativeOutput { param($Exe, $Arguments) if ($Arguments -contains '') { throw 'empty Python argument' }; return '3.12' }
$found=Find-Python
if ($Candidate -eq 'absent') { if ($null -ne $found) { throw 'found missing Python' } }
else {
  $expected=if ($Candidate -eq 'venv') { $venvPy } else { $Candidate }
  if ($found[0] -ne $expected -or $found[2] -ne '3.12') { throw 'incorrect Python discovery' }
}
''', encoding="utf-8-sig")
    result = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script),
                             str(ROOT / "install.ps1"), candidate], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("value", [r"\\server\share #folder", " C:\\share\\folder\\", r'''it's "quoted" C:\share #folder'''])
def test_quoted_windows_paths_survive_config_update(tmp_path, value):
    from xrpl_ingest.support.files import load_env
    path = tmp_path / ".env"
    envfile.main([str(path), "MONGO_URI_FILE=" + value])
    assert load_env({}, cwd=str(tmp_path)).values["MONGO_URI_FILE"] == value
    assert envfile.get_value(envfile.read_text(str(path)), "MONGO_URI_FILE") == value


@pytest.mark.parametrize("line,expected", [("MONGO_URI='' # empty\n", 1),
    ('MONGO_URI="" # empty\n', 1), ("MONGO_URI='mongodb://fixture' # configured\n", 0)])
def test_installer_has_handles_comments_after_quoted_values(tmp_path, line, expected):
    path = tmp_path / ".env"
    path.write_text(line, encoding="utf-8")
    assert envfile.main([str(path), "--has", "MONGO_URI"]) == expected


def test_failed_env_publish_keeps_original_and_cleans_temporary_file(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text("DB_NAME=original\n", encoding="utf-8")

    def fail(src, dst):
        if os.name == "posix":
            assert os.stat(src).st_mode & 0o077 == 0
        raise OSError("fixture publish failure")

    monkeypatch.setattr(envfile.os, "replace", fail)
    with pytest.raises(OSError, match="fixture publish failure"):
        envfile.write_private(str(path), "DB_NAME=new\n")
    assert path.read_text() == "DB_NAME=original\n"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("max_lag,max_age", [(-1, 120), (50, -1), (50, float("inf")),
                                           (50, float("nan"))])
def test_health_invalid_limits_fail_before_opening_database(cfg, max_lag, max_age):
    def never(*args):
        pytest.fail("health opened the database before validating limits")
    with pytest.raises(ConfigError, match="finite --max-age"):
        asyncio.run(monitor.run_health(cfg, max_lag=max_lag, max_age=max_age, store_factory=never))


@pytest.mark.skipif(os.name != "nt", reason="Windows installer configuration")
@pytest.mark.parametrize("process_source", [False, True])
def test_windows_installer_preserves_uri_file_mode_in_apostrophe_directory(tmp_path, process_source):
    from xrpl_ingest.support.files import load_env
    from xrpl_ingest.config import Config
    repo = tmp_path / "operator's project"
    (repo / "setup").mkdir(parents=True)
    for name in ("envfile.py", "edit_cfg.py"):
        shutil.copyfile(ROOT / "setup" / name, repo / "setup" / name)
    (repo / ".env.example").write_text("MONGO_URI=mongodb://default\nDB_NAME=fixture\n", encoding="utf-8")
    (repo / "mongo-uri.txt").write_text("mongodb://fixture", encoding="utf-8")
    if not process_source:
        (repo / ".env").write_text("MONGO_URI_FILE=mongo-uri.txt\nDB_NAME=xrpl_test\n", encoding="utf-16")
    source = (ROOT / "install.ps1").read_text(encoding="utf-8")
    block = source[source.index("$envPath = Join-Path"):source.index("# ---------------------------------------------------------------------------------- 6. checks")]
    script = tmp_path / "env-step.ps1"
    script.write_text('''param($repo, $venvPy)
$ErrorActionPreference='Stop'
function Invoke-Native { param($Exe, $Arguments) & $Exe @Arguments | Out-Host; return $LASTEXITCODE }
function Get-NativeOutput { param($Exe, $Arguments) $output=& $Exe @Arguments; if ($LASTEXITCODE -ne 0) { throw 'native read failed' }; return ($output | Out-String).Trim() }
function Say { param($Text) }
function Fail { param($Text) throw $Text }
$localMongo='mongodb://default'; $Mode='connect'; $NodeUrl='ws://fixture:6007'; $cores=4
''' + block, encoding="utf-8-sig")
    environ = {k: v for k, v in os.environ.items() if k not in {"MONGO_URI", "MONGO_URI_FILE", "ENV_FILE", "XRPL_INGEST_HOME"}}
    if process_source:
        environ["MONGO_URI_FILE"] = "mongo-uri.txt"
    result = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
                             str(script), str(repo), sys.executable], env=environ, cwd=ROOT,
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    values = load_env({}, cwd=str(repo)).values
    if process_source:
        values["MONGO_URI_FILE"] = "mongo-uri.txt"
    config = Config.from_env(values, base_dir=str(repo))
    assert config.mongo_uri == "mongodb://fixture" and config.servers == ("ws://fixture:6007",)
    assert values["DB_NAME"] == ("fixture" if process_source else "xrpl_data_collection")


@pytest.mark.parametrize("credentials", ["file", "direct", "process_file"])
@pytest.mark.parametrize("override", [False, True])
def test_linux_installer_configuration_step_with_fixture_files(tmp_path, credentials, override):
    from xrpl_ingest.config import Config
    from xrpl_ingest.support.files import load_env
    git = shutil.which("git")
    bash = str(Path(git).parents[1] / "bin/bash.exe") if os.name == "nt" and git else shutil.which("bash")
    if not bash or not Path(bash).is_file():
        pytest.skip("Bash unavailable for installer function check")
    repo = tmp_path / "operator's project"
    (repo / "setup").mkdir(parents=True)
    (repo / ".venv/bin").mkdir(parents=True)
    for name in ("envfile.py", "edit_cfg.py"):
        shutil.copyfile(ROOT / "setup" / name, repo / "setup" / name)
    (repo / ".env.example").write_text("MONGO_URI=mongodb://default\nDB_NAME=fixture\n", encoding="utf-8")
    (repo / "mongo-uri.txt").write_text("mongodb://fixture", encoding="utf-8")
    if credentials != "process_file":
        line = "MONGO_URI_FILE=mongo-uri.txt" if credentials == "file" else "MONGO_URI=mongodb://fixture"
        (repo / ".env").write_text(line + "\nDB_NAME=xrpl_test\n", encoding="utf-16")
    (repo / "xrpld.cfg").write_text("[server]\nport_ws_admin_local\n[port_ws_admin_local]\nport=6007\nip=127.0.0.1\nprotocol=ws\n", encoding="utf-8")
    python_wrapper = repo / ".venv/bin/python"
    python_wrapper.write_text('#!/usr/bin/env bash\nexec "$FIXTURE_PYTHON" "$@"\n', encoding="utf-8")
    python_wrapper.chmod(0o755)
    source = (ROOT / "install.sh").read_text(encoding="utf-8")
    functions = source[source.index("env_set()"):source.index("step_tests()")]
    script = tmp_path / "env-step.sh"
    script.write_text('''set -euo pipefail
REPO_DIR="$1"; CFG="$REPO_DIR/xrpld.cfg"; SUDO=""; CORES=4; WSL=no
LOCAL_MONGO=mongodb://default; TEST_DB=xrpl_test; SERVER_DB=xrpl_data_collection
python3() { "$FIXTURE_PYTHON" "$@"; }
say() { :; }
die() { printf '%s\\n' "$*" >&2; exit 1; }
port_open() { return 0; }
''' + functions + '\nstep_env server\n', encoding="utf-8")
    environ = {k: v for k, v in os.environ.items() if k not in {"MONGO_URI", "MONGO_URI_FILE", "ENV_FILE", "XRPL_INGEST_HOME"}}
    environ.update(FIXTURE_PYTHON=sys.executable, PYTHONPATH=str(ROOT),
                   NODE_WS="ws://override:7007" if override else "")
    if credentials == "process_file":
        environ["MONGO_URI_FILE"] = "mongo-uri.txt"
    result = subprocess.run([bash, str(script), str(repo)], cwd=ROOT, env=environ,
                            capture_output=True, text=True, timeout=25)
    assert result.returncode == 0, result.stdout + result.stderr
    values = load_env({}, cwd=str(repo)).values
    if credentials == "process_file":
        values["MONGO_URI_FILE"] = "mongo-uri.txt"
    config = Config.from_env(values, base_dir=str(repo))
    assert config.mongo_uri == "mongodb://fixture"
    assert config.servers == (("ws://override:7007" if override else "ws://127.0.0.1:6007"),)
    assert values["DB_NAME"] == ("fixture" if credentials == "process_file" else "xrpl_data_collection")
