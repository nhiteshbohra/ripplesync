"""Configuration from environment variables. Validates everything up front."""
from __future__ import annotations

import math
import os
import re
import sys
from dataclasses import dataclass, field, fields
from typing import Mapping, Optional, Tuple

from . import logsetup
from .errors import ConfigError
from .logsetup import LOG_FORMAT  # noqa: F401  (re-exported)

DEFAULT_SERVERS = ("ws://127.0.0.1:6006",)
WINDOWS_MAX_PROCESSES = 61  # ProcessPoolExecutor refuses max_workers > 61 on Windows
URI_FILE_MAX_BYTES = 4096
# userinfo = everything up to the LAST '@' of the URL token (passwords may hold '/', '@',
# '#'), but never past whitespace (next token) or '?' (an '@' in the query is not userinfo).
# A '@' in a path without '?' over-redacts, which hides text but never leaks it.
_USERINFO = re.compile(r"(\b[a-z][a-z0-9+.-]*://)[^\s?]*@", re.IGNORECASE)


def redact_url(text: str) -> str:
    """Drop `user:pass@` from every URL in `text` (one URL, or a message quoting URLs):
    ws://user:pass@host:6006 -> ws://host:6006. For logs, errors and stored reasons."""
    return _USERINFO.sub(r"\1", text)


def setup_logging(level: str) -> None:
    """stderr-only logging (UTC timestamps). The CLI uses logsetup.setup_logging directly
    (adds LOG_FILE); this stays for callers that only need stderr."""
    logsetup.setup_logging(level)


@dataclass(frozen=True)
class Config:
    mongo_uri: str = field(repr=False)  # may hold credentials; keep it out of logs/tracebacks
    db_name: str = "xrpl_data_collection"

    ledger_collection: str = "block_number"
    tx_collection: str = "transactions"
    meta_memos_collection: str = "internal_data"
    failed_collection: str = "failed_transactions"
    metadata_collection: str = "last_processed_block_no"

    servers: tuple = DEFAULT_SERVERS
    expected_network_id: Optional[int] = 0  # 0 = XRPL mainnet; None disables the check
    max_message_bytes: int = 64 * 1024 * 1024

    max_retries: int = 5
    request_timeout: int = 30
    reconnect_delay_base: float = 1.0
    backoff_max: float = 30.0
    polling_interval_seconds: float = 2.0

    num_processes: int = 4
    concurrency: int = 16  # in-flight ledger requests per process
    chunk_size: int = 1000

    process_status_file: str = "process_status.txt"
    log_level: str = "INFO"

    # base dir, logging, range file, Mongo client, disk guard
    base_dir: str = ""  # from_env resolved the relative paths here against it
    log_file: str = ""  # empty = stderr only
    log_max_mb: int = 100
    log_backups: int = 10
    backfill_range_file: str = "backfill_range.txt"
    mongo_server_selection_timeout_ms: int = 10_000
    mongo_socket_timeout_ms: int = 120_000  # 0 = no timeout
    min_free_gb: float = 0.0  # 0 = disk guard off
    mongo_uri_file: str = ""  # where mongo_uri was read from (display only)

    def __repr__(self) -> str:  # mongo_uri hidden, server credentials redacted
        shown = []
        for f in fields(self):
            if f.repr:
                v = getattr(self, f.name)
                if f.name == "servers":
                    v = tuple(redact_url(s) for s in v)
                shown.append(f"{f.name}={v!r}")
        return f"Config({', '.join(shown)})"

    @staticmethod
    def from_env(env: Optional[Mapping[str, str]] = None, *, base_dir: Optional[str] = None,
                 platform: Optional[str] = None) -> "Config":
        """Build from environment variables. Relative paths resolve against `base_dir`
        (default: cwd); `platform` (default sys.platform) only matters for NUM_PROCESSES."""
        env = os.environ if env is None else env
        base = os.path.abspath(base_dir or os.getcwd())
        platform = platform or sys.platform
        errors: list = []

        def text(name: str, default: str) -> str:
            v = (env.get(name) or "").strip().strip("\"'").strip()
            return v or default

        def num(name: str, default, cast, minimum) -> float:
            raw = (env.get(name) or "").strip()
            if not raw:
                return default
            try:
                v = cast(raw)
            except ValueError:
                errors.append(f"{name}={raw!r} is not a valid {cast.__name__}")
                return default
            if not math.isfinite(v):  # nan passes `< minimum`; sleep(nan) never returns
                errors.append(f"{name}={raw!r} must be a finite number")
                return default
            if v < minimum:
                errors.append(f"{name}={raw} must be >= {minimum}")
                return default
            return v

        def path(name: str, default: str) -> str:
            v = text(name, default)
            return resolve_path(v, base) if v else ""

        mongo_uri = text("MONGO_URI", "")
        uri_file = path("MONGO_URI_FILE", "")
        if uri_file:
            if mongo_uri:
                errors.append("set only one of MONGO_URI and MONGO_URI_FILE")
            else:
                mongo_uri = _read_uri_file(uri_file, errors)
        if not mongo_uri:
            if not uri_file:
                errors.append("MONGO_URI is required (or MONGO_URI_FILE)")
        elif not mongo_uri.startswith(("mongodb://", "mongodb+srv://")):
            errors.append("MONGO_URI must start with mongodb:// or mongodb+srv://")

        servers = DEFAULT_SERVERS
        raw_servers = (env.get("SERVERS") or "").strip().strip("\"'")
        if raw_servers:
            servers = tuple(s for s in re.split(r"[,\s]+", raw_servers.strip("\"' ")) if s)
            bad = [s for s in servers if not s.startswith(("ws://", "wss://"))]
            if not servers or bad:
                errors.append(f"SERVERS entries must start with ws:// or wss:// (bad: {bad or 'none given'})")
                servers = DEFAULT_SERVERS

        net_raw = (env.get("EXPECTED_NETWORK_ID") or "").strip().lower()
        if net_raw in ("none", "off", "any"):
            network_id = None
        else:
            network_id = num("EXPECTED_NETWORK_ID", 0, int, 0)

        cfg = Config(
            mongo_uri=mongo_uri,
            db_name=text("DB_NAME", Config.db_name),
            ledger_collection=text("LEDGER_COLLECTION", Config.ledger_collection),
            tx_collection=text("TX_COLLECTION", Config.tx_collection),
            meta_memos_collection=text("META_MEMOS_COLLECTION", Config.meta_memos_collection),
            failed_collection=text("FAILED_TX_COLLECTION", Config.failed_collection),
            metadata_collection=text("METADATA_COLLECTION", Config.metadata_collection),
            servers=servers,
            expected_network_id=network_id,
            max_message_bytes=num("MAX_MESSAGE_MB", 64, int, 1) * 1024 * 1024,
            max_retries=num("MAX_RETRIES", Config.max_retries, int, 1),
            request_timeout=num("REQUEST_TIMEOUT", Config.request_timeout, int, 1),
            reconnect_delay_base=num("RECONNECT_DELAY_BASE", Config.reconnect_delay_base, float, 0),
            backoff_max=num("BACKOFF_MAX", Config.backoff_max, float, 0),
            polling_interval_seconds=num("POLLING_INTERVAL_SECONDS", Config.polling_interval_seconds, float, 0),
            num_processes=num("NUM_PROCESSES", Config.num_processes, int, 1),
            concurrency=num("CONCURRENCY", Config.concurrency, int, 1),
            chunk_size=num("CHUNK_SIZE", Config.chunk_size, int, 1),
            process_status_file=path("PROCESS_STATUS_FILE", Config.process_status_file),
            log_level=text("LOG_LEVEL", Config.log_level).upper(),
            base_dir=base,
            log_file=path("LOG_FILE", ""),
            log_max_mb=num("LOG_MAX_MB", Config.log_max_mb, int, 1),
            log_backups=num("LOG_BACKUPS", Config.log_backups, int, 0),
            backfill_range_file=path("BACKFILL_RANGE_FILE", Config.backfill_range_file),
            mongo_server_selection_timeout_ms=num(
                "MONGO_SERVER_SELECTION_TIMEOUT_MS", Config.mongo_server_selection_timeout_ms, int, 1),
            mongo_socket_timeout_ms=num("MONGO_SOCKET_TIMEOUT_MS", Config.mongo_socket_timeout_ms,
                                        int, 0),
            min_free_gb=num("MIN_FREE_GB", Config.min_free_gb, float, 0),
            mongo_uri_file=uri_file,
        )
        if platform == "win32" and cfg.num_processes > WINDOWS_MAX_PROCESSES:
            errors.append(f"NUM_PROCESSES={cfg.num_processes} must be <= {WINDOWS_MAX_PROCESSES} "
                          "on Windows (process pool limit)")
        if cfg.log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            errors.append(f"LOG_LEVEL={cfg.log_level} is not a valid level")
        names = [cfg.ledger_collection, cfg.tx_collection, cfg.meta_memos_collection,
                 cfg.failed_collection, cfg.metadata_collection]
        if len(set(names)) != len(names):
            errors.append("collection names must all be different")
        if errors:
            raise ConfigError("; ".join(errors))
        return cfg


def resolve_path(path: str, base: str) -> str:
    """`path` with ~ expanded, made absolute against `base` if relative, normalised."""
    path = os.path.expanduser(path)
    return os.path.normpath(path if os.path.isabs(path) else os.path.join(base, path))


def _read_uri_file(path: str, errors: list) -> str:
    """MONGO_URI from a file (e.g. a Docker/systemd secret): one line, BOM-tolerant."""
    from .textfile import TextFileError, read_text
    try:
        text = read_text(path, URI_FILE_MAX_BYTES)
    except (OSError, TextFileError) as e:
        errors.append(f"MONGO_URI_FILE {path}: cannot read ({type(e).__name__}: {e})")
        return ""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) != 1:
        errors.append(f"MONGO_URI_FILE {path}: must hold exactly one non-empty line, "
                      f"found {len(lines)}")
        return ""
    return lines[0].strip("\"'").strip()


# Every variable from_env reads, with the Config field it sets. The `config` command
# shows each effective value and where it came from.
ENV_VARS: Tuple[Tuple[str, str], ...] = (
    ("MONGO_URI", "mongo_uri"), ("MONGO_URI_FILE", "mongo_uri_file"), ("DB_NAME", "db_name"),
    ("LEDGER_COLLECTION", "ledger_collection"), ("TX_COLLECTION", "tx_collection"),
    ("META_MEMOS_COLLECTION", "meta_memos_collection"),
    ("FAILED_TX_COLLECTION", "failed_collection"), ("METADATA_COLLECTION", "metadata_collection"),
    ("SERVERS", "servers"), ("EXPECTED_NETWORK_ID", "expected_network_id"),
    ("MAX_MESSAGE_MB", "max_message_bytes"), ("MAX_RETRIES", "max_retries"),
    ("REQUEST_TIMEOUT", "request_timeout"), ("RECONNECT_DELAY_BASE", "reconnect_delay_base"),
    ("BACKOFF_MAX", "backoff_max"), ("POLLING_INTERVAL_SECONDS", "polling_interval_seconds"),
    ("NUM_PROCESSES", "num_processes"), ("CONCURRENCY", "concurrency"),
    ("CHUNK_SIZE", "chunk_size"), ("PROCESS_STATUS_FILE", "process_status_file"),
    ("LOG_LEVEL", "log_level"), ("LOG_FILE", "log_file"), ("LOG_MAX_MB", "log_max_mb"),
    ("LOG_BACKUPS", "log_backups"), ("BACKFILL_RANGE_FILE", "backfill_range_file"),
    ("MONGO_SERVER_SELECTION_TIMEOUT_MS", "mongo_server_selection_timeout_ms"),
    ("MONGO_SOCKET_TIMEOUT_MS", "mongo_socket_timeout_ms"), ("MIN_FREE_GB", "min_free_gb"),
)
# Read by paths.py (before a Config exists) to find the .env and the base dir.
PATH_ENV_VARS = ("XRPL_INGEST_HOME", "ENV_FILE")


def redact_mongo_uri(uri: str) -> str:
    """For display: credentials removed and the query string (may hold secrets) hidden."""
    shown = redact_url(uri)
    base, sep, query = shown.partition("?")
    if not sep:
        return shown
    n = len([q for q in query.split("&") if q])
    return f"{base}?<{n} option(s) hidden>"


def display_value(cfg: "Config", field_name: str) -> str:
    """One Config value as `config` / `doctor` show it (credentials redacted)."""
    v = getattr(cfg, field_name)
    if field_name == "mongo_uri":
        return redact_mongo_uri(v)
    if field_name == "servers":
        return ",".join(redact_url(s) for s in v)
    if field_name == "max_message_bytes":
        return str(v // (1024 * 1024))
    if field_name == "expected_network_id":
        return "none" if v is None else str(v)
    return str(v)


def _uri_has_option(uri: str, name: str) -> bool:
    return re.search(rf"[?&]{name}=", uri, re.IGNORECASE) is not None


def mongo_client_kwargs(cfg: "Config", **overrides) -> dict:
    """MongoClient options: MONGO_SERVER_SELECTION_TIMEOUT_MS, MONGO_SOCKET_TIMEOUT_MS
    (0 = none) and appname xrpl-ingest/<version>. The same option written in MONGO_URI
    wins over the env/default; `overrides` win over both."""
    from . import __version__
    kw: dict = {}
    for opt, value in (("serverSelectionTimeoutMS", cfg.mongo_server_selection_timeout_ms),
                       ("socketTimeoutMS", cfg.mongo_socket_timeout_ms or None),
                       ("appname", f"xrpl-ingest/{__version__}")):
        if not _uri_has_option(cfg.mongo_uri, opt):
            kw[opt] = value
    kw.update(overrides)
    return kw
