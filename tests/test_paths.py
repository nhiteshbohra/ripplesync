"""paths.py / textfile.py: .env location, base dir, decoding. Synthetic files only."""
import codecs
import os

import pytest

from xrpl_ingest.support import files as paths
from xrpl_ingest.errors import ConfigError
from xrpl_ingest.support.files import TextFileError, decode, read_text

BODY = "MONGO_URI=mongodb://127.0.0.1:1\nDB_NAME=synthetic\n"


@pytest.mark.parametrize("data", [
    BODY.encode(), codecs.BOM_UTF8 + BODY.encode(),
    codecs.BOM_UTF16_LE + BODY.encode("utf-16-le"), codecs.BOM_UTF16_BE + BODY.encode("utf-16-be"),
])
def test_decode_by_bom(data):
    assert decode(data) == BODY


def test_decode_strict_rejects_garbage_replace_does_not():
    with pytest.raises(TextFileError):
        decode(b"\xff\xfa")
    assert decode(b"a\xffb", errors="replace") == "a\ufffdb"


def test_read_text_size_limit(tmp_path):
    f = tmp_path / "f"
    f.write_bytes(b"x" * 11)
    assert read_text(str(f), 11) == "x" * 11
    f.write_bytes(b"x" * 12)
    with pytest.raises(TextFileError, match="larger than 11"):
        read_text(str(f), 11)


def test_load_env_does_not_override_process_env(tmp_path):
    (tmp_path / ".env").write_text(BODY + "LOG_LEVEL=DEBUG\n")
    environ = {"DB_NAME": "fromenv"}
    env = paths.load_env(environ, cwd=str(tmp_path))
    assert environ["DB_NAME"] == "fromenv" and environ["MONGO_URI"] == "mongodb://127.0.0.1:1"
    assert env.path == str(tmp_path / ".env")
    assert env.source("MONGO_URI", environ) == ".env"
    assert env.source("DB_NAME", environ) == "env (overrides .env)"
    assert env.source("CHUNK_SIZE", environ) == "default"


def test_load_env_missing_default_is_fine_missing_explicit_is_error(tmp_path):
    env = paths.load_env({}, cwd=str(tmp_path))
    assert env.path is None and env.looked_for == str(tmp_path / ".env")
    with pytest.raises(ConfigError, match="ENV_FILE"):
        paths.load_env({"ENV_FILE": "nope.env"}, cwd=str(tmp_path))


def test_load_env_directory_named_env_is_not_loaded(tmp_path):
    (tmp_path / ".env").mkdir()
    assert paths.load_env({}, cwd=str(tmp_path)).path is None


def test_relative_env_file_resolves_against_cwd_and_sets_base(tmp_path):
    (tmp_path / "conf").mkdir()
    (tmp_path / "conf" / "a.env").write_text(BODY)
    environ = {"ENV_FILE": os.path.join("conf", "a.env")}
    env = paths.load_env(environ, cwd=str(tmp_path))
    assert env.explicit and env.path == str(tmp_path / "conf" / "a.env")
    assert paths.base_dir(environ, env, cwd=str(tmp_path)) == str(tmp_path / "conf")


def test_home_in_process_env_wins_and_holds_the_env(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    (home / ".env").write_text(BODY)
    environ = {"XRPL_INGEST_HOME": str(home)}
    env = paths.load_env(environ, cwd=str(tmp_path))
    assert env.path == str(home / ".env")
    assert paths.base_dir(environ, env, cwd=str(tmp_path)) == str(home)


def test_home_set_inside_env_file_moves_base_dir(tmp_path):
    (tmp_path / ".env").write_text(BODY + f"XRPL_INGEST_HOME={tmp_path / 'data'}\n")
    environ = {}
    env = paths.load_env(environ, cwd=str(tmp_path))
    assert paths.base_dir(environ, env, cwd=str(tmp_path)) == str(tmp_path / "data")


def test_base_dir_defaults_to_cwd(tmp_path):
    assert paths.base_dir({}, paths.EnvFile(), cwd=str(tmp_path)) == str(tmp_path)


def test_control_chars():
    assert paths.control_chars("/srv/x\r") == ["'\\r'"]
    assert paths.control_chars("C:\\data\\ok") == []
