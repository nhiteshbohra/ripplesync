"""24/72-hour live-ingestion soak with bounded deep checks and JSONL evidence.

Each run uses a generated xrpl_qa_soak_* database; it never writes to the configured DB.
"""
from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from xrpl_ingest.backfill import discover_node, retry_policy
from xrpl_ingest.config import Config, FIRST_LEDGER, redact_url
from xrpl_ingest.ledger_ranges import subtract
from xrpl_ingest.repair import repair_range
from xrpl_ingest.source import WsSource
from xrpl_ingest.store import MongoStore
from xrpl_ingest.support import files
from xrpl_ingest.support.monitor import DiskGuard, mongo_free_bytes
from xrpl_ingest.tail import tail


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, choices=(24, 72), required=True)
    parser.add_argument("--sample-interval", type=int, default=60)
    parser.add_argument("--max-lag", type=int, default=10)
    parser.add_argument("--results", type=Path)
    args = parser.parse_args()
    if args.sample_interval < 1 or args.max_lag < 0:
        parser.error("--sample-interval must be positive and --max-lag non-negative")
    return args


def _config() -> Config:
    env = files.load_env()
    base_dir = files.base_dir(os.environ, env)
    return Config.from_env(os.environ, base_dir=base_dir)


async def _run(args: argparse.Namespace, cfg: Config, result_path: Path) -> int:
    run_id = uuid.uuid4().hex
    qa_cfg = dataclasses.replace(cfg, db_name=f"xrpl_qa_soak_{run_id[:16]}")
    store = None
    source = None
    guard = None
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM, getattr(signal, "SIGBREAK", signal.SIGTERM)):
        try:
            signal.signal(signum, lambda *_: loop.call_soon_threadsafe(stop_event.set))
        except (OSError, ValueError):
            pass

    def record(event: Dict[str, Any]) -> None:
        data = {
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "hours": args.hours,
            "database": qa_cfg.db_name,
            **event,
        }
        result_path.parent.mkdir(parents=True, exist_ok=True)
        with result_path.open("a", encoding="utf-8") as output:
            output.write(json.dumps(data, sort_keys=True, default=str) + "\n")
            output.flush()
            os.fsync(output.fileno())

    started = time.monotonic()
    deadline = started + args.hours * 3600
    record({"event": "start", "status": "running"})
    tail_task = None
    samples = 0
    passed_samples = 0
    checkpoint_advances = 0
    last_checkpoint = 0
    tail_error = None
    try:
        store = MongoStore.from_config(qa_cfg)
        source = WsSource.from_config(qa_cfg)
        guard = DiskGuard(store, qa_cfg.min_free_gb)
        store.ensure_indexes()
        guard.check_now()

        def should_stop() -> bool:
            return stop_event.is_set() or guard()

        tail_task = asyncio.create_task(tail(
            source, store, retry_policy(qa_cfg), poll_interval=qa_cfg.polling_interval_seconds,
            stop_check=should_stop))
        while time.monotonic() < deadline and not should_stop():
            try:
                ranges, latest = await discover_node(source)
                checkpoint = await asyncio.to_thread(store.get_checkpoint)
                failed = await asyncio.to_thread(store.count_failed)
                start = max(FIRST_LEDGER, checkpoint - 19)
                if checkpoint >= start and checkpoint:
                    deep = await repair_range(
                        source, store, start, checkpoint, ranges, retry=retry_policy(qa_cfg),
                        concurrency=1, stop_check=stop_event.is_set, repair=False, deep=True)
                    gaps = await asyncio.to_thread(store.find_gaps, start, checkpoint)
                    count_mismatches = await asyncio.to_thread(
                        store.tx_count_mismatches, start, checkpoint)
                    bundle_mismatches = await asyncio.to_thread(
                        store.bundle_mismatches, start, checkpoint)
                    source_covered = not subtract(start, checkpoint, ranges)
                    progress = await asyncio.to_thread(
                        store.ledger_progress, max(FIRST_LEDGER, checkpoint - 99), checkpoint)
                else:
                    deep = None
                    gaps, count_mismatches, bundle_mismatches = [], [], []
                    source_covered = False
                    progress = None
                lag = max(0, latest - checkpoint)
                ok = bool(checkpoint and source_covered and not failed and not gaps
                          and not count_mismatches and not bundle_mismatches
                          and (deep is None or deep.ok) and lag <= args.max_lag)
                sample = {
                    "event": "sample", "status": "pass" if ok else "fail",
                    "elapsed_seconds": round(time.monotonic() - started, 1),
                    "latest_validated": latest, "checkpoint": checkpoint,
                    "checkpoint_advanced": checkpoint > last_checkpoint,
                    "lag": lag, "failed_ledgers": failed,
                    "checked_range": [start, checkpoint],
                    "source_covered": source_covered,
                    "gaps": gaps, "transaction_count_mismatches": count_mismatches,
                    "bundle_mismatches": bundle_mismatches,
                    "deep_remaining": deep.remaining if deep is not None else None,
                    "deep_unchecked": deep.unchecked if deep is not None else None,
                    "ledger_progress": progress,
                    "mongo_free_bytes": await asyncio.to_thread(mongo_free_bytes, store),
                    "disk_guard_tripped": guard.tripped,
                    "tail_running": not tail_task.done(),
                }
                record(sample)
                samples += 1
                passed_samples += int(ok)
                checkpoint_advances += int(checkpoint > last_checkpoint)
                last_checkpoint = max(last_checkpoint, checkpoint)
            except Exception as e:
                record({"event": "sample", "status": "error",
                        "error": redact_url(f"{type(e).__name__}: {e}")})
                samples += 1

            if tail_task.done():
                error = tail_task.exception()
                if error is not None:
                    tail_error = redact_url(f"{type(error).__name__}: {error}")
                    break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=args.sample_interval)
            except asyncio.TimeoutError:
                pass
        stop_event.set()
        if tail_task is not None:
            result = (await asyncio.gather(tail_task, return_exceptions=True))[0]
            if isinstance(result, BaseException):
                tail_error = redact_url(f"{type(result).__name__}: {result}")
    except Exception as e:
        tail_error = redact_url(f"{type(e).__name__}: {e}")
    finally:
        stop_event.set()
        if tail_task is not None and not tail_task.done():
            result = (await asyncio.gather(tail_task, return_exceptions=True))[0]
            if isinstance(result, BaseException) and tail_error is None:
                tail_error = redact_url(f"{type(result).__name__}: {result}")
        if source is not None:
            try:
                await source.close()
            except Exception as e:
                if tail_error is None:
                    tail_error = redact_url(f"{type(e).__name__}: {e}")
        if store is not None:
            store.close()

    elapsed = time.monotonic() - started
    completed_duration = elapsed >= args.hours * 3600
    clean = tail_task is not None and tail_task.done() and tail_error is None
    status = ("pass" if completed_duration and clean and samples > 0
              and passed_samples == samples and checkpoint_advances > 0 else "fail")
    record({
        "event": "summary", "status": status, "elapsed_seconds": round(elapsed, 1),
        "completed_duration": completed_duration, "clean_termination": clean,
        "tail_error": tail_error, "samples": samples, "passed_samples": passed_samples,
        "checkpoint_advances": checkpoint_advances, "last_checkpoint": last_checkpoint,
    })
    return 0 if status == "pass" else 1


def main() -> int:
    args = _arguments()
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    result_path = args.results or Path("reviews") / f"reliability-soak-{args.hours}h-{stamp}.jsonl"
    return asyncio.run(_run(args, _config(), result_path))


if __name__ == "__main__":
    raise SystemExit(main())
