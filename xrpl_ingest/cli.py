"""Command line: python -m xrpl_ingest <command>"""
from __future__ import annotations

import argparse
import asyncio
import sys
from typing import List, Optional

from pymongo.errors import ConfigurationError, PyMongoError

from .backfill import discover_node, retry_failed, retry_policy, run_backfill
from .config import Config, setup_logging
from .constants import FIRST_LEDGER
from .errors import ConfigError, IngestError, NetworkMismatch
from .source import WsSource
from .stopflag import install_stop_signal, stop_requested
from .store import MongoStore
from .tail import tail

VERIFY_WINDOW = 10_000
EXIT_CODES = (
    "exit codes: 0 = ok; 1 = some ledgers failed, the run stopped early, or verify found "
    "problems; 2 = config/usage error (incl. node on the wrong network); 3 = backfill had no "
    "failures, but the node lacks some requested ledgers (see its complete_ledgers); "
    "4 = node or MongoDB unreachable / error; 130 = interrupted")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="xrpl_ingest", epilog=EXIT_CODES)
    sub = p.add_subparsers(dest="command", required=True)

    b = sub.add_parser("backfill", help="fetch historical ledgers (resumable)", epilog=EXIT_CODES)
    b.add_argument("--start", type=int, default=FIRST_LEDGER)
    b.add_argument("--end", type=int, default=None, help="default: node's latest validated ledger")

    sub.add_parser("tail", help="follow new validated ledgers")

    r = sub.add_parser("retry-failed", help="re-fetch ledgers in the failed collection")
    r.add_argument("--max-rounds", type=int, default=5)

    v = sub.add_parser("verify", help="find missing ledgers and tx-count mismatches in a range")
    v.add_argument("--start", type=int, required=True)
    v.add_argument("--end", type=int, required=True)
    add_data_parsers(sub, v)

    sub.add_parser("status", help="show node and database state")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        from dotenv import find_dotenv, load_dotenv
        load_dotenv(find_dotenv(usecwd=True))  # .env of the directory we run in, not the package's
        cfg = Config.from_env()
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2
    setup_logging(cfg.log_level)
    install_stop_signal()  # SIGTERM = graceful stop, same as current_process=0 (POSIX)
    stop = lambda: stop_requested(cfg.process_status_file)  # noqa: E731

    try:
        if args.command == "backfill":
            return backfill_exit_code(run_backfill(cfg, args.start, args.end))

        store = MongoStore.from_config(cfg)
        try:
            if args.command == "tail":
                store.ensure_indexes()
                return asyncio.run(_run_tail(cfg, store, stop))
            if args.command == "retry-failed":
                remaining = asyncio.run(_run_retry(cfg, store, args.max_rounds, stop))
                print(f"remaining failed: {remaining}")
                return 1 if remaining else 0
            if args.command in DATA_COMMANDS:  # verify, fetch, tx
                return DATA_COMMANDS[args.command](cfg, store, args, stop)
            if args.command == "status":
                return asyncio.run(_status(cfg, store, stop))
        finally:
            store.close()
    except KeyboardInterrupt:
        return 130
    except (IngestError, PyMongoError, ValueError) as e:  # ValueError: bad --start/--end
        print(f"error: {_describe(e)}", file=sys.stderr)
        return error_exit_code(e)
    return 2


def error_exit_code(e: BaseException) -> int:
    """2 when fixing config/arguments is needed, 4 when the node or MongoDB failed."""
    return 2 if isinstance(e, (NetworkMismatch, ValueError, ConfigurationError)) else 4


def _describe(e: BaseException) -> str:
    return f"{type(e).__name__}: {e}"


def backfill_exit_code(summary: dict) -> int:
    """1 for failed ledgers or incomplete chunks, else 3 if the node lacked part of the range."""
    if summary["ledgers_failed"] or summary["chunks_incomplete"]:
        return 1
    return 3 if summary["unavailable_ranges"] else 0


async def _run_tail(cfg: Config, store: MongoStore, stop) -> int:
    source = WsSource.from_config(cfg, stop_check=stop)
    try:
        await tail(source, store, retry_policy(cfg), poll_interval=cfg.polling_interval_seconds,
                   stop_check=stop)
    finally:
        await source.close()
    return 0


async def _run_retry(cfg: Config, store: MongoStore, rounds: int, stop) -> int:
    source = WsSource.from_config(cfg, stop_check=stop)
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


async def _status(cfg: Config, store: MongoStore, stop=lambda: False) -> int:
    """Node part first; DB stats are printed even when the node cannot be reached."""
    code = 0
    source = WsSource.from_config(cfg, stop_check=stop)
    try:
        ranges, latest = await discover_node(source)
        print(f"node complete_ledgers: {ranges}  latest validated: {latest}")
    except IngestError as e:
        print(f"node: error: {_describe(e)}")
        code = error_exit_code(e)
    finally:
        await source.close()
    print(f"database: {await asyncio.to_thread(store.stats)}")
    return code


# -- data commands: verify --repair/--deep, fetch, tx ----------------------------------------
# Kept together at the end (imports inside functions) so they stay separate from startup code.

def add_data_parsers(sub, verify: argparse.ArgumentParser) -> None:
    verify.add_argument("--repair", action="store_true",
                        help="re-ingest the missing / tx-count-mismatch ledgers found "
                             "(exit 0 if all OK afterwards, 1 if some remain, 4 node unreachable)")


def _verify_cmd(cfg: Config, store: MongoStore, args, stop) -> int:
    if not args.repair:
        return _verify(store, args.start, args.end)
    if args.start > args.end:
        print(f"verify: --start ({args.start}) must be <= --end ({args.end})", file=sys.stderr)
        return 2
    return asyncio.run(_verify_repair(cfg, store, args.start, args.end, stop))


async def _verify_repair(cfg: Config, store: MongoStore, start: int, end: int, stop) -> int:
    from .repair import repair_range
    source = WsSource.from_config(cfg, stop_check=stop)
    try:
        node_ranges, _ = await discover_node(source)  # unreachable => IngestError => exit 4
        s = await repair_range(source, store, start, end, node_ranges, retry=retry_policy(cfg),
                               concurrency=cfg.concurrency, stop_check=stop, emit=print,
                               window=VERIFY_WINDOW)
    finally:
        await source.close()
    print(f"repair: found {s.found}, repaired {s.repaired}, failed {s.failed}, "
          f"node lacks {s.node_lacks}, remaining {s.remaining}")
    if s.stopped:
        print("stopped early: the rest of the range was not verified")
    elif s.remaining:
        print(f"{s.remaining} problem ledger(s) remain")
    else:
        print("OK")
    return 0 if s.ok else 1


DATA_COMMANDS = {"verify": _verify_cmd}
