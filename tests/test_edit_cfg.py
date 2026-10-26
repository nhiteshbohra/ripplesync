import os
import stat
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "setup"))
import edit_cfg  # noqa: E402

SAMPLE = """[server]
port_rpc_admin_local
port_peer

[node_db]
type=NuDB
path=/old/path
online_delete=2000
advisory_delete=0

[node_size]
medium
"""


def sections(text):
    return dict((n, [b.strip() for b in body if b.strip()]) for n, body in edit_cfg._split(text)[1])


def test_full_history_removes_online_delete():
    out = edit_cfg.apply(SAMPLE, db_path="/data/nudb", history="full", node_size="huge", peers=[])
    s = sections(out)
    assert s["node_db"] == ["type=NuDB", "path=/data/nudb"]
    assert "online_delete=2000" not in out and "advisory_delete" not in out
    assert s["ledger_history"] == ["full"] and s["node_size"] == ["huge"]
    assert "port_ws_admin_local" in s["server"]
    assert s["port_ws_admin_local"][0] == "port = 6006"
    assert "log_level" in s["rpc_startup"][0]


def test_pruned_keeps_online_delete_and_drops_ledger_history():
    text = SAMPLE + "\n[ledger_history]\nfull\n"
    out = edit_cfg.apply(text, db_path="/d", history="pruned", node_size="large", peers=[])
    s = sections(out)
    assert "online_delete=2000" in s["node_db"] and "ledger_history" not in s


def test_idempotent():
    once = edit_cfg.apply(SAMPLE, db_path="/d", history="full", node_size="huge", peers=["h 51235"])
    twice = edit_cfg.apply(once, db_path="/d", history="full", node_size="huge", peers=["h 51235"])
    assert once == twice
    assert once.count("port_ws_admin_local") == 2  # server line + stanza header


@pytest.mark.parametrize("existing,expected", [("", ["0"]), ("[network_id]\n2\n", ["2"])])
def test_mainnet_default_preserves_explicit_network(existing, expected):
    out = edit_cfg.apply(SAMPLE + "\n" + existing, db_path="/d", history="pruned",
                         node_size="medium", peers=[])
    assert sections(out)["network_id"] == expected


def test_new_ws_stanza_matches_example_cfg():
    s = sections(edit_cfg.apply(SAMPLE, db_path="/d", history="full", node_size="huge", peers=[]))
    assert s["port_ws_admin_local"] == [
        "port = 6006", "ip = 127.0.0.1", "admin = 127.0.0.1", "protocol = ws", "send_queue_limit = 500",
    ]


def test_existing_ws_stanza_not_overwritten():
    text = SAMPLE + "\n[port_ws_admin_local]\nport = 7777\nip = 127.0.0.1\nadmin = 127.0.0.1\nprotocol = ws\n"
    s = sections(edit_cfg.apply(text, db_path="/d", history="full", node_size="huge", peers=[]))
    assert s["port_ws_admin_local"][0] == "port = 7777"
    assert "send_queue_limit = 500" not in s["port_ws_admin_local"]  # user's stanza left exactly as is


def test_empty_file_and_peers():
    out = edit_cfg.apply("", db_path="/d", history="full", node_size="huge", peers=["a 1", "b 2"])
    assert sections(out)["ips_fixed"] == ["a 1", "b 2"]


def test_cli_writes_backup(tmp_path):
    cfg = tmp_path / "xrpld.cfg"
    cfg.write_text(SAMPLE)
    assert edit_cfg.main([str(cfg), "--db-path", "/x"]) == 0
    assert any(p.name.startswith("xrpld.cfg.bak.") for p in tmp_path.iterdir())
    assert "path=/x" in cfg.read_text()


def test_cli_rejects_unknown_node_size(tmp_path):
    cfg = tmp_path / "xrpld.cfg"
    cfg.write_text(SAMPLE)
    with pytest.raises(SystemExit) as e:
        edit_cfg.main([str(cfg), "--db-path", "/x", "--node-size", "gigantic"])
    assert e.value.code == 2
    assert cfg.read_text() == SAMPLE
    assert [p.name for p in tmp_path.iterdir()] == ["xrpld.cfg"]  # no backup, no temp file


@pytest.mark.parametrize("size", edit_cfg.NODE_SIZES)
def test_cli_accepts_every_node_size(tmp_path, size):
    cfg = tmp_path / "xrpld.cfg"
    cfg.write_text(SAMPLE)
    assert edit_cfg.main([str(cfg), "--db-path", "/x", "--node-size", size]) == 0
    assert sections(cfg.read_text())["node_size"] == [size]


def test_cli_creates_missing_cfg_without_backup(tmp_path):
    cfg = tmp_path / "sub" / "xrpld.cfg"
    assert edit_cfg.main([str(cfg), "--db-path", "/x"]) == 0
    assert "path=/x" in cfg.read_text()
    assert [p.name for p in cfg.parent.iterdir()] == ["xrpld.cfg"]


def test_failed_write_keeps_original_and_leaves_no_temp(tmp_path, monkeypatch):
    cfg = tmp_path / "xrpld.cfg"
    cfg.write_text(SAMPLE)

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(edit_cfg.os, "replace", boom)
    with pytest.raises(OSError, match="disk full"):
        edit_cfg.write_atomic(str(cfg), "new contents\n")
    assert cfg.read_text() == SAMPLE
    assert [p.name for p in tmp_path.iterdir()] == ["xrpld.cfg"]


def _symlink_or_skip(target, link):
    try:
        os.symlink(target, link)
    except (OSError, NotImplementedError) as e:  # Windows without Developer Mode / admin
        pytest.skip(f"symlinks unavailable here: {e}")


def test_cli_writes_through_symlink_and_keeps_link(tmp_path):
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    target = real_dir / "xrpld.cfg"
    target.write_text(SAMPLE)
    link = tmp_path / "link.cfg"
    _symlink_or_skip(target, link)
    assert edit_cfg.main([str(link), "--db-path", "/x"]) == 0
    assert link.is_symlink() and os.path.samefile(link, target)
    assert "path=/x" in target.read_text()
    assert not [p for p in real_dir.iterdir() if p.name.startswith(".edit_cfg.")]  # no temp left


def test_write_atomic_targets_resolved_path(tmp_path, monkeypatch):
    """Portable stand-in for the symlink test: the write goes to realpath(), not the given path."""
    given, resolved = tmp_path / "given.cfg", tmp_path / "resolved.cfg"
    given.write_text("link stand-in\n")
    resolved.write_text(SAMPLE)
    monkeypatch.setattr(edit_cfg.os.path, "realpath", lambda p: str(resolved))
    edit_cfg.write_atomic(str(given), "new\n")
    assert resolved.read_text() == "new\n" and given.read_text() == "link stand-in\n"


# POSIX permission bits are not meaningful on Windows (chmod only toggles read-only).
posix_only = pytest.mark.skipif(os.name == "nt", reason="file mode bits not meaningful on Windows")


@posix_only
def test_cli_keeps_mode_on_backup_and_cfg(tmp_path):
    cfg = tmp_path / "xrpld.cfg"
    cfg.write_text(SAMPLE)
    os.chmod(cfg, 0o640)
    assert edit_cfg.main([str(cfg), "--db-path", "/x"]) == 0
    (backup,) = [p for p in tmp_path.iterdir() if p.name.startswith("xrpld.cfg.bak.")]
    assert stat.S_IMODE(os.stat(backup).st_mode) == 0o640
    assert stat.S_IMODE(os.stat(cfg).st_mode) == 0o640
    assert backup.read_text() == SAMPLE
