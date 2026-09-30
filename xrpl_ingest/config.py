"""Configuration from environment variables. Validates everything up front."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Mapping, Optional

from .errors import ConfigError

DEFAULT_SERVERS = ("ws://127.0.0.1:6006",)


@dataclass(frozen=True)
class Config:
    mongo_uri: str
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

    @staticmethod
    def from_env(env: Optional[Mapping[str, str]] = None) -> "Config":
        env = os.environ if env is None else env
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
            if v < minimum:
                errors.append(f"{name}={raw} must be >= {minimum}")
                return default
            return v

        mongo_uri = text("MONGO_URI", "")
        if not mongo_uri:
            errors.append("MONGO_URI is required")
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
            process_status_file=text("PROCESS_STATUS_FILE", Config.process_status_file),
            log_level=text("LOG_LEVEL", Config.log_level).upper(),
        )
        if cfg.log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            errors.append(f"LOG_LEVEL={cfg.log_level} is not a valid level")
        names = [cfg.ledger_collection, cfg.tx_collection, cfg.meta_memos_collection,
                 cfg.failed_collection, cfg.metadata_collection]
        if len(set(names)) != len(names):
            errors.append("collection names must all be different")
        if errors:
            raise ConfigError("; ".join(errors))
        return cfg
