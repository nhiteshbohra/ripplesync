import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "setup"))
import xrpld_check  # noqa: E402

CFG = """[server]
port_rpc_admin_local
port_peer
port_ws_admin_local

[node_db]
type=NuDB
path=/var/lib/xrpld/db/nudb

[validators_file]
validators.txt
"""

# What a user had on 2026-10-20: keys typed into xrpld.cfg, the second one wrong (65 characters).
BROKEN_CFG = CFG + """
[validator_list_keys]
ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734
ED45D1840EE724BE327ABE9146503D5848EFD5F38B6D5FEDE71E80ACCE5E6E738
"""


def write(tmp_path, cfg, validators=xrpld_check.OFFICIAL_VALIDATORS_TXT):
    c = tmp_path / "xrpld.cfg"
    c.write_text(cfg, encoding="utf-8")
    if validators is not None:
        (tmp_path / "validators.txt").write_text(validators, encoding="utf-8")
    return str(c)


def test_official_setup_is_ok(tmp_path):
    assert xrpld_check.run("check", write(tmp_path, CFG)) == (0, [])


def test_broken_user_config_found(tmp_path):
    code, found = xrpld_check.run("check", write(tmp_path, BROKEN_CFG))
    assert code == 1
    text = "\n".join(found)
    assert "xrpld.cfg contains [validator_list_keys]" in text
    assert "not 66 hex characters (has 65)" in text


def test_unofficial_but_wellformed_key_reported(tmp_path):
    other = xrpld_check.OFFICIAL_VALIDATORS_TXT.replace(xrpld_check.OFFICIAL_KEYS[1], "ED" + "A" * 64)
    code, found = xrpld_check.run("check", write(tmp_path, CFG, other))
    assert code == 1 and "not one of the official ones" in found[0]


def test_missing_validators_file_and_stanza(tmp_path):
    code, found = xrpld_check.run("check", write(tmp_path, CFG.replace("[validators_file]\nvalidators.txt\n", ""), None))
    assert code == 1
    assert any("no [validators_file]" in f for f in found) and any("does not exist" in f for f in found)


def test_fix_backs_up_and_restores_official_setup(tmp_path):
    cfg = write(tmp_path, BROKEN_CFG, "[validator_list_keys]\nbad\n")
    code, msgs = xrpld_check.run("fix", cfg)
    assert code == 0
    assert len(list(tmp_path.glob("xrpld.cfg.bak.*"))) == 1 and len(list(tmp_path.glob("validators.txt.bak.*"))) == 1
    fixed = (tmp_path / "xrpld.cfg").read_text(encoding="utf-8")
    assert "validator_list_keys" not in fixed and "path=/var/lib/xrpld/db/nudb" in fixed
    assert (tmp_path / "validators.txt").read_text(encoding="utf-8") == xrpld_check.OFFICIAL_VALIDATORS_TXT
    assert xrpld_check.run("check", cfg) == (0, [])


def test_fix_on_good_setup_changes_nothing(tmp_path):
    cfg = write(tmp_path, CFG)
    assert xrpld_check.run("fix", cfg) == (0, ["nothing to fix"])
    assert not list(tmp_path.glob("*.bak.*"))


def test_missing_cfg_is_exit_2(tmp_path):
    assert xrpld_check.run("check", str(tmp_path / "none.cfg"))[0] == 2


def test_unreadable_cfg_is_exit_2_not_a_traceback(tmp_path, monkeypatch):
    def denied(path):
        raise PermissionError(13, "Permission denied", path)
    monkeypatch.setattr(xrpld_check, "_read", denied)
    code, msgs = xrpld_check.run("check", str(tmp_path / "xrpld.cfg"))
    assert code == 2 and "run with sudo" in msgs[0]


def test_cli(tmp_path, capsys):
    cfg = write(tmp_path, CFG)
    assert xrpld_check.main(["check", "--cfg", cfg]) == 0
    assert "validator settings OK" in capsys.readouterr().out
