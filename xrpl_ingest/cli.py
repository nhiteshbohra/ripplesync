"""Command line: `python -m xrpl_ingest <command>`.

Adding a command: write `_cmd_<name>(run: Run) -> int`, add its parser in build_parser()
and an entry in COMMANDS. main() does the shared part: .env + config, logging, signals,
the per-command lock, and mapping errors to exit codes.
"""
from __future__ import annotations

import argparse
import asyncio
import io
import logging
import os
import sys
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from pymongo.errors import ConfigurationError, PyMongoError

from . import __version__
from .support import doctor, files, monitor
from .backfill import discover_node, retry_failed, retry_policy, run_backfill
from .config import Config
from .config import FIRST_LEDGER
from .support.monitor import DiskGuard, LowDiskSpace
from .errors import ConfigError, IngestError, NetworkMismatch
from .support.runcontrol import InstanceLock, LockHeld, lock_path
from .support.logsetup import setup_logging
from .source import WsSource
from .support.runcontrol import StopFlag, any_of, install_stop_signal, stop_requested
from .store import MongoStore
from .tail import tail

log = logging.getLogger(__name__)

VERIFY_WINDOW = 10_000

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_CONFIG = 2
EXIT_UNAVAILABLE = 3
EXIT_UNREACHABLE = 4
EXIT_LOCKED = 5
EXIT_INTERNAL = 6
EXIT_INTERRUPTED = 130

EXIT_CODES = """\
exit codes:
  0    ok
  1    some ledgers failed, the run stopped early (stop file, signal, MIN_FREE_GB),
       verify found problems, or health: lagging / stale; doctor: warnings
  2    config or usage error (incl. node on the wrong network, bad Mongo credentials)
  3    backfill had no failures, but the node lacks some requested ledgers
       (see its complete_ledgers)
  4    node or MongoDB unreachable / error
  5    another instance of this command is already running (lock held)
  6    unexpected internal error (a bug; the log has the traceback)
  130  interrupted (second Ctrl+C, or Ctrl+C in a command without graceful stop)"""

STOP_HELP = """\
graceful stop: a line `current_process=0` (all commands) or `<command>=0` (tail, backfill,
retry-failed) in the stop file (PROCESS_STATUS_FILE); any line that applies and says 0
stops. Or SIGTERM, SIGBREAK (Windows Ctrl+Break), or the first Ctrl+C (second = abort)."""


def build_parser() -> argparse.ArgumentParser:
    fmt = argparse.RawDescriptionHelpFormatter
    p = argparse.ArgumentParser(prog="python -m xrpl_ingest", epilog=f"{STOP_HELP}\n\n{EXIT_CODES}",
                                formatter_class=fmt,
                                description=f"xrpl-ingest {__version__}: XRPL history from a "
                                            "local xrpld node into MongoDB")
    sub = p.add_subparsers(dest="command", required=True)

    def add(name: str, help_: str) -> argparse.ArgumentParser:
        return sub.add_parser(name, help=help_, epilog=EXIT_CODES, formatter_class=fmt)

    b = add("backfill", "fetch historical ledgers (resumable)")
    b.add_argument("--start", type=int, default=None,
                   help="first ledger (default: range file, else 32570)")
    b.add_argument("--end", type=int, default=None,
                   help="last ledger (default: range file, else node's latest validated)")
    g = b.add_mutually_exclusive_group()
    g.add_argument("--range-file", metavar="PATH", default=None,
                   help="read start/end from this file (default: BACKFILL_RANGE_FILE, "
                        "used only when it exists and neither --start nor --end is given)")
    g.add_argument("--no-range-file", action="store_true",
                   help="ignore the range file; defaults 32570..latest")

    add("tail", "follow new validated ledgers")

    r = add("retry-failed", "re-fetch ledgers in the failed collection")
    r.add_argument("--max-rounds", type=int, default=5)

    v = add("verify", "find missing ledgers and tx-count mismatches in a range")
    v.add_argument("--start", type=int, required=True)
    v.add_argument("--end", type=int, required=True)
    add_data_parsers(sub, v)

    add("status", "show node and database state, and backfill progress of the range file")

    d = add("doctor", "check installation, config, paths, MongoDB and the node")
    d.add_argument("--node", action="store_true", help="node checks only (with --mongo: both)")
    d.add_argument("--mongo", action="store_true", help="MongoDB checks only (with --node: both)")
    d.add_argument("--json", action="store_true", help="machine-readable output")

    h = add("health", "is tail keeping up? 0 healthy, 1 lagging/stale, 4 unreachable")
    h.add_argument("--max-lag", type=int, default=monitor.DEFAULT_MAX_LAG, metavar="N",
                   help="ledgers the checkpoint may trail the node (default %(default)s)")
    h.add_argument("--max-age", type=float, default=monitor.DEFAULT_MAX_AGE, metavar="S",
                   help="seconds since the checkpoint was last written (default %(default)s)")

    add("config", "print the effective configuration (redacted) and where each value came from")
    return p


def _never() -> bool:
    return False


# -- per-run context ----------------------------------------------------------------

@dataclass
class Run:
    """What a command function gets: parsed args, config (None only for commands with
    needs_config=False when the config is invalid), and lazily a MongoStore."""
    command: str
    args: argparse.Namespace
    cfg: Optional[Config]
    env: files.EnvFile
    base_dir: str
    config_error: Optional[ConfigError] = None
    stop: Callable[[], bool] = _never  # replaced per run in _main
    _store: Optional[MongoStore] = None

    def store(self) -> MongoStore:
        if self._store is None:
            self._store = MongoStore.from_config(self.cfg)
        return self._store

    def close(self) -> None:
        if self._store is not None:
            store, self._store = self._store, None
            store.close()


@dataclass(frozen=True)
class Command:
    run: Callable[[Run], int]
    ingest: bool = False  # lock file, stop-file key, graceful Ctrl+C
    needs_config: bool = True  # False: runs (and reports) with an invalid config
    quiet_start: bool = False  # start line at DEBUG (monitoring / inspection commands)


# -- commands -----------------------------------------------------------------------

def _cmd_backfill(run: Run) -> int:
    a = run.args
    rng = files.choose_range(a.start, a.end, run.cfg.backfill_range_file,
                           range_file=a.range_file, use_file=not a.no_range_file)
    log.info("backfill range %s", rng.describe())
    return backfill_exit_code(run_backfill(run.cfg, rng.start, rng.end, stop_check=run.stop))


def _disk_guard(run: Run, store: MongoStore) -> DiskGuard:
    guard = DiskGuard(store, run.cfg.min_free_gb)
    guard.check_now()  # LowDiskSpace -> exit 1 before anything starts
    run.stop = any_of(run.stop, guard)
    return guard


def _cmd_tail(run: Run) -> int:
    store = run.store()
    store.ensure_indexes()
    guard = _disk_guard(run, store)
    code = asyncio.run(_run_tail(run.cfg, store, run.stop))
    return EXIT_FAILED if guard.tripped else code


def _cmd_retry_failed(run: Run) -> int:
    store = run.store()
    guard = _disk_guard(run, store)
    remaining = asyncio.run(_run_retry(run.cfg, store, run.args.max_rounds, run.stop))
    print(f"remaining failed: {remaining}")
    return EXIT_FAILED if remaining or guard.tripped else EXIT_OK


def _cmd_data(run: Run) -> int:
    """verify (incl. --repair/--deep), fetch, tx: see DATA_COMMANDS at the end."""
    return DATA_COMMANDS[run.command](run.cfg, run.store(), run.args, run.stop)


def _cmd_status(run: Run) -> int:
    return asyncio.run(_status(run.cfg, run.store(), run.stop))


def _cmd_doctor(run: Run) -> int:
    a = run.args
    both = not a.node and not a.mongo
    return doctor.run_doctor(run.cfg, run.env, os.environ, run.base_dir,
                             config_error=run.config_error, node=both or a.node,
                             mongo=both or a.mongo, as_json=a.json)


def _cmd_health(run: Run) -> int:
    return asyncio.run(monitor.run_health(run.cfg, max_lag=run.args.max_lag,
                                         max_age=run.args.max_age))


def _cmd_config(run: Run) -> int:
    return doctor.run_config(run.cfg, run.env, os.environ, run.base_dir,
                             config_error=run.config_error)


COMMANDS: Dict[str, Command] = {
    "backfill": Command(_cmd_backfill, ingest=True),
    "tail": Command(_cmd_tail, ingest=True),
    "retry-failed": Command(_cmd_retry_failed, ingest=True),
    "verify": Command(_cmd_data),
    "fetch": Command(_cmd_data),
    "tx": Command(_cmd_data),
    "status": Command(_cmd_status),
    "doctor": Command(_cmd_doctor, needs_config=False, quiet_start=True),
    "health": Command(_cmd_health, quiet_start=True),
    "config": Command(_cmd_config, needs_config=False, quiet_start=True),
}


# -- main -----------------------------------------------------------------------------

def _reconfigure_streams() -> None:
    """Never die on printing a character the console/redirect encoding lacks (cp1252...)."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="backslashreplace")
        except (AttributeError, ValueError, io.UnsupportedOperation):
            pass


def main(argv: Optional[List[str]] = None) -> int:
    _reconfigure_streams()
    args = build_parser().parse_args(argv)
    try:
        return _main(args, COMMANDS[args.command])
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED
    except Exception as e:  # noqa: BLE001 - last resort: exit 6, traceback in the log
        log.critical("internal error (please report it with this traceback)", exc_info=True)
        print(f"error: internal error: {_describe(e)}", file=sys.stderr)
        return EXIT_INTERNAL


def _main(args: argparse.Namespace, command: Command) -> int:
    try:
        env = files.load_env()
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        return EXIT_CONFIG
    base = files.base_dir(os.environ, env)
    cfg: Optional[Config] = None
    cfg_error: Optional[ConfigError] = None
    try:
        cfg = Config.from_env(os.environ, base_dir=base)
    except ConfigError as e:
        if command.needs_config:
            print(f"config error: {e}", file=sys.stderr)
            return EXIT_CONFIG
        cfg_error = e
    try:
        if cfg is not None:
            setup_logging(cfg.log_level, cfg.log_file, max_mb=cfg.log_max_mb, backups=cfg.log_backups)
        else:
            setup_logging("INFO")
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        return EXIT_CONFIG

    run = Run(args.command, args, cfg, env, base, cfg_error)
    _log_start(run, command)
    install_stop_signal(graceful_sigint=command.ingest)
    lock: Optional[InstanceLock] = None
    try:
        if cfg is not None:
            path = cfg.process_status_file
            run.stop = (StopFlag(path, args.command) if command.ingest
                        else (lambda: stop_requested(path)))
        if command.ingest:
            lock = _acquire_lock(base, args.command)
        return command.run(run)
    except LockHeld as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_LOCKED
    except OSError as e:
        if lock is None and command.ingest:  # lock file could not be created
            print(f"error: cannot create lock file in {base}: {_describe(e)}", file=sys.stderr)
            return EXIT_CONFIG
        raise
    except (IngestError, PyMongoError, ValueError) as e:  # ValueError: bad --start/--end
        print(f"error: {_describe(e)}", file=sys.stderr)
        return error_exit_code(e)
    finally:
        run.close()
        if lock is not None:
            lock.release()


def _acquire_lock(base: str, command: str) -> InstanceLock:
    lk = InstanceLock(lock_path(base, command), command).acquire()
    log.debug("lock %s acquired", lk.path)
    return lk


def _log_start(run: Run, command: Command) -> None:
    cfg = run.cfg
    level = logging.DEBUG if command.quiet_start else logging.INFO
    log.log(level, "xrpl-ingest %s (Python %s, %s) %s: base dir %s; .env %s; stop file %s; "
            "range file %s; log file %s; lock %s",
            __version__, ".".join(map(str, sys.version_info[:3])), sys.platform, run.command,
            run.base_dir, run.env.path or f"none ({run.env.looked_for})",
            cfg.process_status_file if cfg else "?", cfg.backfill_range_file if cfg else "?",
            (cfg.log_file or "stderr only") if cfg else "?",
            lock_path(run.base_dir, run.command) if command.ingest else "none")


def error_exit_code(e: BaseException) -> int:
    """1 for low disk, 2 when fixing config/arguments is needed, 5 for a held lock,
    4 when the node or MongoDB failed."""
    if isinstance(e, LowDiskSpace):
        return EXIT_FAILED
    if isinstance(e, LockHeld):
        return EXIT_LOCKED
    return 2 if isinstance(e, (NetworkMismatch, ValueError, ConfigurationError, ConfigError)) else 4


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
    latest: Optional[int] = None
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
    range_code = await asyncio.to_thread(_range_status, cfg, store, latest)
    return code or range_code


def _range_status(cfg: Config, store: Any, latest: Optional[int]) -> int:
    """Progress of the range-file range (what a plain `backfill` would do)."""
    try:
        rng = files.load_range(cfg.backfill_range_file)
    except ConfigError as e:
        print(f"backfill range: config error: {e}")
        return EXIT_CONFIG
    end = rng.end if rng.end is not None else latest
    if not end:
        print(f"backfill range {rng.describe()}: end unknown (node unreachable)")
        return 0
    if end < rng.start:
        print(f"backfill range {rng.describe()}: empty (end {end} < start {rng.start})")
        return 0
    p = files.range_progress(store, rng.start, end, cfg.chunk_size)
    print(f"backfill range {rng.start}-{end} ({rng.source}): {files.format_range_progress(p)}")
    return 0


# -- data commands: verify --repair/--deep, fetch, tx ----------------------------------------
# Kept together at the end (imports inside functions) so they stay separate from startup code.

def add_data_parsers(sub, verify: argparse.ArgumentParser) -> None:
    verify.add_argument("--repair", action="store_true",
                        help="re-ingest the missing / tx-count-mismatch (with --deep also "
                             "tx-hash-mismatch) ledgers found; exit 0 if all OK afterwards, "
                             "1 if problems remain, 4 node unreachable")
    verify.add_argument("--deep", action="store_true",
                        help="also check parent_hash links (offline) and compare stored tx "
                             "hashes with the node's ledger hash lists")

    f = sub.add_parser("fetch", help="fetch and store one ledger now (no chunk progress)",
                       epilog="exit codes: 0 stored; 1 failed (recorded in the failed "
                              "collection); 2 usage; 4 node or MongoDB unreachable")
    f.add_argument("--ledger", type=ledger_arg, required=True, metavar="N|validated",
                   help="ledger number, or 'validated' for the node's latest validated ledger")

    t = sub.add_parser("tx", help="spot-check one stored transaction against the node",
                       epilog="exit codes: 0 same; 1 different, not validated, or missing "
                              "(stored or on the node); 2 usage; 4 node or MongoDB unreachable")
    t.add_argument("--hash", type=tx_hash_arg, required=True, dest="tx_hash", metavar="H",
                   help="64-hex-digit transaction hash")


def tx_hash_arg(text: str) -> str:
    from .repair import normalize_tx_hash
    try:
        return normalize_tx_hash(text)
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e)) from None


def ledger_arg(text: str):
    """argparse type for fetch --ledger: 'validated' or a ledger number >= FIRST_LEDGER."""
    t = text.strip().lower()
    if t == "validated":
        return t
    if not t.isdigit() or int(t) < FIRST_LEDGER:
        raise argparse.ArgumentTypeError(
            f"expected a ledger number >= {FIRST_LEDGER} or 'validated', got {text!r}")
    return int(t)


def fetch_exit_code(result) -> int:
    """0 stored; 2 wrong network; 4 node unreachable or failure not recordable (MongoDB);
    1 any other failure, or a stop."""
    if result.ok:
        return 0
    error = result.error or ""
    if error.startswith("StopRequested"):
        return 1
    if error.startswith("NetworkMismatch"):
        return 2
    if error.startswith("ConnectFailed") or not result.recorded:
        return 4
    return 1


def _fetch_cmd(cfg: Config, store: MongoStore, args, stop) -> int:
    return asyncio.run(_fetch(cfg, store, args.ledger, stop))


async def _fetch(cfg: Config, store: MongoStore, ledger, stop) -> int:
    from .fetcher import ingest_ledger
    source = WsSource.from_config(cfg, stop_check=stop)
    try:
        index = await source.latest_validated() if ledger == "validated" else ledger
        await asyncio.to_thread(store.ensure_indexes)
        r = await ingest_ledger(source, store, index, retry_policy(cfg), clear_failure=True)
    finally:
        await source.close()
    outcome = "stored" if r.ok else f"failed: {r.error}"
    print(f"block_number {r.block_number}: {r.num_transactions} transactions, {outcome}")
    return fetch_exit_code(r)


def _verify_cmd(cfg: Config, store: MongoStore, args, stop) -> int:
    if not (args.repair or args.deep):
        return _verify(store, args.start, args.end)
    if args.start > args.end:
        print(f"verify: --start ({args.start}) must be <= --end ({args.end})", file=sys.stderr)
        return 2
    return asyncio.run(_verify_with_node(cfg, store, args.start, args.end, stop,
                                         repair=args.repair, deep=args.deep))


async def _verify_with_node(cfg: Config, store: MongoStore, start: int, end: int, stop, *,
                            repair: bool, deep: bool) -> int:
    """1 if problems remain (or stopped early), else 4 if the node part could not run, else 0.
    Without --repair an unreachable node still lets the offline checks run."""
    from .repair import repair_range
    source = WsSource.from_config(cfg, stop_check=stop)
    node_error: Optional[IngestError] = None
    try:
        try:
            node_ranges: Optional[list] = (await discover_node(source))[0]
        except IngestError as e:
            if repair or isinstance(e, NetworkMismatch):
                raise  # main(): exit 4 (unreachable) or 2 (wrong network)
            node_error, node_ranges = e, None
            print(f"node: error: {_describe(e)}; running the offline checks only")
        if repair:  # writes: same unique indexes as tail/backfill/fetch
            await asyncio.to_thread(store.ensure_indexes)
        s = await repair_range(source if node_ranges is not None else None, store, start, end,
                               node_ranges, retry=retry_policy(cfg), concurrency=cfg.concurrency,
                               stop_check=stop, emit=print, window=VERIFY_WINDOW,
                               repair=repair, deep=deep)
    finally:
        await source.close()
    line = f"verify: bad ledgers {s.found}"
    if deep:
        line += (f", chain breaks {s.chain_breaks}, hash mismatches {s.hash_mismatches}, "
                 f"not compared {s.unchecked}")
    if repair:
        line += (f"; repaired {s.repaired}, failed {s.failed}, node lacks {s.node_lacks}")
    print(line + f"; remaining {s.remaining}")
    if s.stopped:
        print("stopped early: the rest of the range was not verified")
    elif s.remaining:
        print(f"{s.remaining} problem(s) remain")
    elif node_error is not None:
        print("offline checks OK; node comparison not done")
    else:
        print("OK")
    if not s.ok:
        return 1
    return error_exit_code(node_error) if node_error is not None else 0


def _tx_cmd(cfg: Config, store: MongoStore, args, stop) -> int:
    return asyncio.run(_tx(cfg, store, args.tx_hash, stop))


async def _tx(cfg: Config, store: MongoStore, tx_hash: str, stop) -> int:
    from .errors import RpcError
    from .repair import describe_tx, diff_tx_fields, node_tx_fields, stored_tx_fields
    stored = await asyncio.to_thread(store.find_tx, tx_hash)
    source = WsSource.from_config(cfg, stop_check=stop)
    try:
        node: Optional[dict] = await source.get_tx(tx_hash)
    except RpcError as e:
        if e.error != "txnNotFound":
            raise  # main(): exit 4
        node = None
    finally:
        await source.close()
    print(f"tx {tx_hash}")
    if stored is None:
        print("stored: not found")
    if node is None:
        print("node:   not found (txnNotFound)")
    if stored is None or node is None:
        return 1
    mine, theirs = stored_tx_fields(stored), node_tx_fields(node)
    print(f"stored: {describe_tx(mine)}")
    print(f"node:   {describe_tx(theirs)}")
    diffs = diff_tx_fields(mine, theirs)
    for field, a, b in diffs:
        print(f"  {field}: stored {a!r}, node {b!r}")
    validated = node.get("validated") is True
    if not validated:
        print("node reports the transaction as not validated")
    print("same" if not diffs and validated else f"different ({len(diffs)} field(s))")
    return 0 if not diffs and validated else 1


DATA_COMMANDS = {"verify": _verify_cmd, "fetch": _fetch_cmd, "tx": _tx_cmd}
