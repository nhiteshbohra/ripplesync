"""Command line: python -m xrpl_ingest <command>"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import List, Optional

from .backfill import discover_node, retry_failed, retry_policy, run_backfill
from .config import Config
from .constants import FIRST_LEDGER
from .errors import ConfigError
from .source import WsSource
from .stopflag import stop_requested
from .store import MongoStore
from .tail import tail

VERIFY_WINDOW = 10_000


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="xrpl_ingest")
    sub = p.add_subparsers(dest="command", required=True)

    b = sub.add_parser("backfill", help="fetch historical ledgers (resumable)")
    b.add_argument("--start", type=int, default=FIRST_LEDGER)
    b.add_argument("--end", type=int, default=None, help="default: node's latest validated ledger")

    sub.add_parser("tail", help="follow new validated ledgers")

    r = sub.add_parser("retry-failed", help="re-fetch ledgers in the failed collection")
    r.add_argument("--max-rounds", type=int, default=5)

    v = sub.add_parser("verify", help="find missing ledgers and tx-count mismatches in a range")
    v.add_argument("--start", type=int, required=True)
    v.add_argument("--end", type=int, required=True)

    sub.add_parser("status", help="show node and database state")
    return p


def _setup_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level), stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        from dotenv import load_dotenv
        load_dotenv()
        cfg = Config.from_env()
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2
    _setup_logging(cfg.log_level)
    stop = lambda: stop_requested(cfg.process_status_file)  # noqa: E731

    try:
        if args.command == "backfill":
            summary = run_backfill(cfg, args.start, args.end)
            return 1 if summary["ledgers_failed"] or summary["chunks_incomplete"] else 0

        store = MongoStore.from_config(cfg)
        try:
            if args.command == "tail":
                store.ensure_indexes()
                return asyncio.run(_run_tail(cfg, store, stop))
            if args.command == "retry-failed":
                remaining = asyncio.run(_run_retry(cfg, store, args.max_rounds, stop))
                print(f"remaining failed: {remaining}")
                return 1 if remaining else 0
            if args.command == "verify":
                return _verify(store, args.start, args.end)
            if args.command == "status":
                return asyncio.run(_status(cfg, store))
        finally:
            store.close()
    except KeyboardInterrupt:
        return 130
    return 2


async def _run_tail(cfg: Config, store: MongoStore, stop) -> int:
    source = WsSource.from_config(cfg)
    try:
        await tail(source, store, retry_policy(cfg), poll_interval=cfg.polling_interval_seconds,
                   stop_check=stop)
    finally:
        await source.close()
    return 0


async def _run_retry(cfg: Config, store: MongoStore, rounds: int, stop) -> int:
    source = WsSource.from_config(cfg)
    try:
        return await retry_failed(source, store, retry_policy(cfg), max_rounds=rounds,
                                  concurrency=cfg.concurrency, stop_check=stop)
    finally:
        await source.close()


def _verify(store: MongoStore, start: int, end: int) -> int:
    if start > end:
        print(f"verify: --start ({start}) must be <= --end ({end})", file=sys.stderr)
        return 2
    bad = 0
    for a in range(start, end + 1, VERIFY_WINDOW):
        b = min(end, a + VERIFY_WINDOW - 1)
        for g in store.find_gaps(a, b):
            print(f"missing ledgers {g[0]}-{g[1]}")
            bad += 1
        for n, exp, got in store.tx_count_mismatches(a, b):
            print(f"ledger {n}: expected {exp} transactions, stored {got}")
            bad += 1
    print("OK" if not bad else f"{bad} problem(s)")
    return 1 if bad else 0


async def _status(cfg: Config, store: MongoStore) -> int:
    source = WsSource.from_config(cfg)
    try:
        ranges, latest = await discover_node(source)
        print(f"node complete_ledgers: {ranges}  latest validated: {latest}")
    finally:
        await source.close()
    print(f"database: {store.stats()}")
    return 0
