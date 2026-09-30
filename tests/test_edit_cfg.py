import os
import sys

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


def test_existing_ws_stanza_not_overwritten():
    text = SAMPLE + "\n[port_ws_admin_local]\nport = 7777\nip = 127.0.0.1\nadmin = 127.0.0.1\nprotocol = ws\n"
    s = sections(edit_cfg.apply(text, db_path="/d", history="full", node_size="huge", peers=[]))
    assert s["port_ws_admin_local"][0] == "port = 7777"


def test_empty_file_and_peers():
    out = edit_cfg.apply("", db_path="/d", history="full", node_size="huge", peers=["a 1", "b 2"])
    assert sections(out)["ips_fixed"] == ["a 1", "b 2"]


def test_cli_writes_backup(tmp_path):
    cfg = tmp_path / "xrpld.cfg"
    cfg.write_text(SAMPLE)
    assert edit_cfg.main([str(cfg), "--db-path", "/x"]) == 0
    assert any(p.name.startswith("xrpld.cfg.bak.") for p in tmp_path.iterdir())
    assert "path=/x" in cfg.read_text()
