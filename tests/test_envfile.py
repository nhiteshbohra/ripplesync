import os
import sys

import pytest
from dotenv import dotenv_values

from xrpl_ingest.errors import ConfigError
from xrpl_ingest.support.files import load_env

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "setup"))
import envfile  # noqa: E402

EXAMPLE = """# Copy to .env
MONGO_URI=mongodb://localhost:27017
DB_NAME=xrpl_data_collection
# Minimum free disk space in GB (0 = check off)
# MIN_FREE_GB=0
SERVERS=ws://127.0.0.1:6006
"""


def test_changes_only_named_keys_and_keeps_comments():
    out = envfile.set_values(EXAMPLE, {"SERVERS": "wss://s2.ripple.com", "DB_NAME": "xrpl_trial"})
    assert out == EXAMPLE.replace("ws://127.0.0.1:6006", "wss://s2.ripple.com").replace(
        "DB_NAME=xrpl_data_collection", "DB_NAME=xrpl_trial")


def test_missing_key_goes_under_its_commented_example_or_at_the_end():
    out = envfile.set_values(EXAMPLE, {"MIN_FREE_GB": "5", "CONCURRENCY": "2"}).splitlines()
    assert out[out.index("# MIN_FREE_GB=0") + 1] == "MIN_FREE_GB=5"
    assert out[-1] == "CONCURRENCY=2"


def test_every_uncommented_copy_is_changed():
    out = envfile.set_values("A=1\nB=2\nA=3\n", {"A": "9"})
    assert out == "A=9\nB=2\nA=9\n"


@pytest.mark.parametrize("value", [
    "mongodb://u:p@h:27017/?authSource=admin", "pa ss #word", " lead", "it's #x", "'q'", "#hash",
])
def test_values_read_back_unchanged_by_python_dotenv(tmp_path, value):
    f = tmp_path / ".env"
    f.write_text(envfile.set_values("", {"MONGO_URI": value}), encoding="utf-8")
    assert dotenv_values(f)["MONGO_URI"] == value
    assert envfile.get_value(f.read_text(encoding="utf-8"), "MONGO_URI") == value


def test_line_break_refused():
    with pytest.raises(ValueError):
        envfile.quote("a\nb")


def test_runtime_loader_rejects_malformed_lines_instead_of_using_defaults(tmp_path):
    (tmp_path / ".env").write_text(
        "MONGO_URI=mongodb://localhost\nMIN_FREE_GB 100\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="malformed line.*2"):
        load_env({}, str(tmp_path))


def test_cli_creates_from_example_then_backs_up_on_change(tmp_path, capsys):
    ex = tmp_path / ".env.example"
    ex.write_text(EXAMPLE, encoding="utf-8")
    env = tmp_path / ".env"
    assert envfile.main([str(env), "--example", str(ex), "MONGO_URI=mongodb://u:secret@h"]) == 0
    assert "secret" not in capsys.readouterr().out  # values are never printed
    assert dotenv_values(env)["MONGO_URI"] == "mongodb://u:secret@h"
    assert not list(tmp_path.glob(".env.bak.*"))
    assert envfile.main([str(env), "MONGO_URI=mongodb://u:secret@h"]) == 0  # no change, no backup
    assert not list(tmp_path.glob(".env.bak.*"))
    assert envfile.main([str(env), "DB_NAME=x"]) == 0
    assert len(list(tmp_path.glob(".env.bak.*"))) == 1


def test_cli_has(tmp_path):
    env = tmp_path / ".env"
    assert envfile.main([str(env), "--has", "MONGO_URI"]) == 1
    env.write_text("MONGO_URI=\nDB_NAME=x\n", encoding="utf-8")
    assert envfile.main([str(env), "--has", "MONGO_URI"]) == 1
    assert envfile.main([str(env), "--has", "DB_NAME"]) == 0


def test_cli_rejects_bad_pairs(tmp_path):
    assert envfile.main([str(tmp_path / ".env"), "NOEQUALS"]) == 2
    assert envfile.main([str(tmp_path / ".env"), "1BAD=x"]) == 2
    assert not (tmp_path / ".env").exists()


@pytest.mark.skipif(os.name == "nt", reason="file mode bits not meaningful on Windows")
def test_cli_file_is_private(tmp_path):
    env = tmp_path / ".env"
    envfile.main([str(env), "A=1"])
    assert oct(os.stat(env).st_mode & 0o777) == "0o600"
