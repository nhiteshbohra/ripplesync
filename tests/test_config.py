import logging

import pytest

from xrpl_ingest.config import DEFAULT_SERVERS, LOG_FORMAT, Config, setup_logging
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


def test_setup_logging_uses_shared_format_and_level(monkeypatch):
    seen = {}
    monkeypatch.setattr(logging, "basicConfig", lambda **kw: seen.update(kw))
    setup_logging("WARNING")
    assert seen["level"] == logging.WARNING and seen["format"] == LOG_FORMAT
    assert "%(levelname)s %(name)s" in LOG_FORMAT


def test_repr_hides_mongo_uri():
    c = Config.from_env({"MONGO_URI": "mongodb://admin:hunter2@db.example:27017"})
    assert "hunter2" not in repr(c) and "db.example" not in repr(c)
    assert c.mongo_uri.endswith("@db.example:27017")


def test_network_check_can_be_disabled():
    assert Config.from_env({**BASE, "EXPECTED_NETWORK_ID": "none"}).expected_network_id is None
    assert Config.from_env({**BASE, "EXPECTED_NETWORK_ID": "1"}).expected_network_id == 1
