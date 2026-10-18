import logging

import pytest

from xrpl_ingest.config import DEFAULT_SERVERS, LOG_FORMAT, Config, redact_url, setup_logging
from xrpl_ingest.errors import ConfigError

BASE = {"MONGO_URI": "mongodb://localhost:27017"}


def test_defaults():
    c = Config.from_env(BASE)
    assert c.servers == DEFAULT_SERVERS
    assert c.expected_network_id == 0
    assert c.chunk_size == 1000 and c.num_processes == 4


def test_missing_mongo_uri():
    with pytest.raises(ConfigError, match="MONGO_URI is required"):
        Config.from_env({})


def test_bad_mongo_scheme():
    with pytest.raises(ConfigError, match="mongodb://"):
        Config.from_env({"MONGO_URI": "http://x"})


def test_multiline_quoted_servers_like_legacy_env():
    raw = '"wss://a.example,\n        wss://b.example:443"'
    c = Config.from_env({**BASE, "SERVERS": raw})
    assert c.servers == ("wss://a.example", "wss://b.example:443")


def test_server_bad_scheme_rejected():
    with pytest.raises(ConfigError, match="ws://"):
        Config.from_env({**BASE, "SERVERS": "http://a"})


@pytest.mark.parametrize("name,val", [("MAX_RETRIES", "abc"), ("MAX_RETRIES", "0"),
                                      ("NUM_PROCESSES", "0"), ("CHUNK_SIZE", "-5"),
                                      ("REQUEST_TIMEOUT", "1.5")])
def test_invalid_numbers(name, val):
    with pytest.raises(ConfigError, match=name):
        Config.from_env({**BASE, name: val})


@pytest.mark.parametrize("name,val", [("BACKOFF_MAX", "nan"), ("RECONNECT_DELAY_BASE", "inf"),
                                      ("POLLING_INTERVAL_SECONDS", "Infinity")])
def test_non_finite_floats_rejected(name, val):
    with pytest.raises(ConfigError, match=f"{name}.*finite"):
        Config.from_env({**BASE, name: val})


def test_finite_float_accepted():
    assert Config.from_env({**BASE, "BACKOFF_MAX": "2.5"}).backoff_max == 2.5


def test_all_errors_reported_together():
    with pytest.raises(ConfigError) as e:
        Config.from_env({"MAX_RETRIES": "x", "CHUNK_SIZE": "0"})
    msg = str(e.value)
    assert "MONGO_URI" in msg and "MAX_RETRIES" in msg and "CHUNK_SIZE" in msg


def test_duplicate_collection_names_rejected():
    with pytest.raises(ConfigError, match="different"):
        Config.from_env({**BASE, "TX_COLLECTION": "block_number"})


def test_bad_log_level():
    with pytest.raises(ConfigError, match="LOG_LEVEL"):
        Config.from_env({**BASE, "LOG_LEVEL": "loud"})


def test_setup_logging_uses_shared_format_and_level(restore_logging):
    setup_logging("WARNING")
    root = logging.getLogger()
    assert root.level == logging.WARNING
    ours = [h for h in root.handlers if getattr(h, "_xrpl_ingest_handler", False)]
    assert len(ours) == 1 and ours[0].formatter._fmt == LOG_FORMAT
    assert "%(levelname)s %(name)s" in LOG_FORMAT


def test_repr_hides_mongo_uri():
    c = Config.from_env({"MONGO_URI": "mongodb://admin:hunter2@db.example:27017"})
    assert "hunter2" not in repr(c) and "db.example" not in repr(c)
    assert c.mongo_uri.endswith("@db.example:27017")


def test_repr_redacts_server_credentials():
    c = Config(mongo_uri="mongodb://x", servers=("ws://user:pw123@a:6006", "wss://b"))
    assert "pw123" not in repr(c) and "user" not in repr(c)
    assert "'ws://a:6006', 'wss://b'" in repr(c) and repr(c).startswith("Config(db_name=")
    assert c.servers[0] == "ws://user:pw123@a:6006"  # the real value is untouched


@pytest.mark.parametrize("text,shown", [
    ("ws://u:p@ss@host/x", "ws://host/x"),  # unencoded '@' in the password
    ("all servers failed: cannot reach ws://u:secret@a:6006 (refused)",
     "all servers failed: cannot reach ws://a:6006 (refused)"),
    ("no creds ws://a and mongodb://h", "no creds ws://a and mongodb://h"),
    # Auditor A r13
    ("ws://u:pa/ss@host:6006", "ws://host:6006"),  # unencoded '/' in the password
    ("mongodb://u:p@h1:27017,h2:27017/db?authSource=admin", "mongodb://h1:27017,h2:27017/db?authSource=admin"),
    ("mongodb+srv://u:p@cluster.example.net/db", "mongodb+srv://cluster.example.net/db"),
    ("connect to ws://a:b@h failed: [Errno 111] x@y", "connect to ws://h failed: [Errno 111] x@y"),
    ("ws://host:6006/?token=a@b", "ws://host:6006/?token=a@b"),  # '@' in the query is not userinfo
    ("email admin@example.com", "email admin@example.com"),
    ("ws://u%40x:p%2Fq@host", "ws://host"),
    ("WS://U:P@HOST", "WS://HOST"),
    ("ws://host:6006 (user@host on ssh)", "ws://host:6006 (user@host on ssh)"),
    ("ws://u:p#x@host", "ws://host"),
])
def test_redact_url_in_urls_and_messages(text, shown):
    assert redact_url(text) == shown


def test_network_check_can_be_disabled():
    assert Config.from_env({**BASE, "EXPECTED_NETWORK_ID": "none"}).expected_network_id is None
    assert Config.from_env({**BASE, "EXPECTED_NETWORK_ID": "1"}).expected_network_id == 1


# -- base dir, new variables, MONGO_URI_FILE, Windows process cap, Mongo client options ---

def test_new_defaults_and_paths_resolve_against_base(tmp_path):
    c = Config.from_env(BASE, base_dir=str(tmp_path))
    assert c.base_dir == str(tmp_path)
    assert c.process_status_file == str(tmp_path / "process_status.txt")
    assert c.backfill_range_file == str(tmp_path / "backfill_range.txt")
    assert c.log_file == "" and (c.log_max_mb, c.log_backups) == (100, 10)
    assert (c.mongo_server_selection_timeout_ms, c.mongo_socket_timeout_ms) == (10000, 120000)
    assert c.min_free_gb == 0


def test_absolute_paths_kept(tmp_path):
    other = tmp_path / "elsewhere" / "ps.txt"
    c = Config.from_env({**BASE, "PROCESS_STATUS_FILE": str(other), "LOG_FILE": "logs/a.log"},
                        base_dir=str(tmp_path / "base"))
    assert c.process_status_file == str(other)
    assert c.log_file == str(tmp_path / "base" / "logs" / "a.log")


@pytest.mark.parametrize("name,val", [("LOG_MAX_MB", "0"), ("LOG_BACKUPS", "-1"),
                                      ("MONGO_SERVER_SELECTION_TIMEOUT_MS", "0"),
                                      ("MONGO_SOCKET_TIMEOUT_MS", "x"), ("MIN_FREE_GB", "-1"),
                                      ("MIN_FREE_GB", "nan")])
def test_new_numbers_validated(name, val):
    with pytest.raises(ConfigError, match=name):
        Config.from_env({**BASE, name: val})


def test_new_numbers_accepted():
    c = Config.from_env({**BASE, "LOG_BACKUPS": "0", "MONGO_SOCKET_TIMEOUT_MS": "0",
                         "MIN_FREE_GB": "2.5"})
    assert c.log_backups == 0 and c.mongo_socket_timeout_ms == 0 and c.min_free_gb == 2.5


def test_num_processes_capped_on_windows_only():
    env = {**BASE, "NUM_PROCESSES": "62"}
    with pytest.raises(ConfigError, match="NUM_PROCESSES=62 must be <= 61 on Windows"):
        Config.from_env(env, platform="win32")
    assert Config.from_env(env, platform="linux").num_processes == 62
    assert Config.from_env({**BASE, "NUM_PROCESSES": "61"}, platform="win32").num_processes == 61


def test_mongo_uri_file(tmp_path):
    (tmp_path / "secret").write_bytes(b"\xef\xbb\xbfmongodb://u:p@h:1/db\r\n\r\n")
    c = Config.from_env({"MONGO_URI_FILE": "secret"}, base_dir=str(tmp_path))
    assert c.mongo_uri == "mongodb://u:p@h:1/db" and c.mongo_uri_file == str(tmp_path / "secret")
    assert "u:p" not in repr(c)


@pytest.mark.parametrize("content,msg", [(None, "cannot read"), (b"", "exactly one"),
                                         (b"mongodb://a\nmongodb://b\n", "exactly one"),
                                         (b"http://x\n", "must start with mongodb://")])
def test_mongo_uri_file_errors(tmp_path, content, msg):
    if content is not None:
        (tmp_path / "secret").write_bytes(content)
    with pytest.raises(ConfigError, match=msg):
        Config.from_env({"MONGO_URI_FILE": "secret"}, base_dir=str(tmp_path))


def test_mongo_uri_and_file_both_set_is_error(tmp_path):
    (tmp_path / "secret").write_text("mongodb://h")
    with pytest.raises(ConfigError, match="only one of MONGO_URI and MONGO_URI_FILE"):
        Config.from_env({**BASE, "MONGO_URI_FILE": "secret"}, base_dir=str(tmp_path))


def test_env_vars_registry_covers_everything_from_env_reads():
    from xrpl_ingest.config import ENV_VARS

    class Recording(dict):
        def get(self, k, default=None):
            seen.add(k)
            return super().get(k, default)
    seen = set()
    Config.from_env(Recording(BASE))
    assert seen == {n for n, _ in ENV_VARS}
    import dataclasses
    names = {f.name for f in dataclasses.fields(Config)}
    assert all(f in names for _, f in ENV_VARS)


def test_mongo_client_kwargs_defaults_uri_wins_overrides_win():
    from xrpl_ingest import __version__
    from xrpl_ingest.config import mongo_client_kwargs
    c = Config(mongo_uri="mongodb://h")
    assert mongo_client_kwargs(c) == {"serverSelectionTimeoutMS": 10000, "socketTimeoutMS": 120000,
                                      "appname": f"xrpl-ingest/{__version__}"}
    c = Config(mongo_uri="mongodb://h/?serverSelectionTimeoutMS=5000&APPNAME=mine",
               mongo_socket_timeout_ms=0)
    assert mongo_client_kwargs(c) == {"socketTimeoutMS": None}
    assert mongo_client_kwargs(c, socketTimeoutMS=5000)["socketTimeoutMS"] == 5000


def test_store_from_config_passes_client_options(monkeypatch):
    from xrpl_ingest import store as store_mod
    seen = {}

    class FakeClient(dict):
        def __init__(self, uri, **kw):
            seen.update(uri=uri, **kw)

        def __getitem__(self, name):
            return mongomock.MongoClient()[name]
    import mongomock
    monkeypatch.setattr(store_mod, "MongoClient", FakeClient)
    store_mod.MongoStore.from_config(Config(mongo_uri="mongodb://h"), connectTimeoutMS=5000)
    assert seen["serverSelectionTimeoutMS"] == 10000 and seen["connectTimeoutMS"] == 5000
    assert seen["appname"].startswith("xrpl-ingest/")


def test_redact_mongo_uri_hides_credentials_and_query():
    from xrpl_ingest.config import redact_mongo_uri
    assert redact_mongo_uri("mongodb://u:p@h:1/db?a=1&b=secret") == "mongodb://h:1/db?<2 option(s) hidden>"
    assert redact_mongo_uri("mongodb://h") == "mongodb://h"
