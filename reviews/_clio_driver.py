"""Real-data QA driver (Phase 3/4/5) against public Clio + live local MongoDB.

Runs the project's real CLI against a real node (public Clio, fully synced) and a
real MongoDB (disposable local DB). Produces evidence under reviews/real_clio_evidence/.
Free public Clio throttle by unit quota (~10000/60s), so this uses modest windows and
low concurrency, waits out quota errors, and rotates across public endpoints.
"""
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
REVIEWS = ROOT / "reviews"
SERVERS = ["wss://xrplcluster.com", "wss://s1.ripple.com", "wss://s2.ripple.com"]
MONGO_URI = "mongodb://127.0.0.1:27017"

EVIDENCE = REVIEWS / "real_clio_evidence"
EVIDENCE.mkdir(exist_ok=True)

QUIESCE_SECONDS = 65  # Clio unit quota resets each 60s window
_server_cursor = 0


class NodeProbeError(RuntimeError):
    def __init__(self, failures):
        self.failures = failures
        self.rate_limited = any(_is_rate_limited(message) for _, message in failures)
        super().__init__("no public Clio endpoint reachable: " + "; ".join(
            f"{server}: {message}" for server, message in failures))


def _is_rate_limited(message):
    text = str(message).lower()
    return any(token in text for token in (
        "slowdown", "slow down", "too busy", "rate limit", "ratelimit",
        "quota", "429", "1008",
    ))


def _assert_qa_database(database):
    if not database.startswith(("xrpl_ingest_real_", "xrpl_ingest_plumb_")):
        raise ValueError(f"refusing non-QA database name: {database}")


def stamp():
    return datetime.now(timezone.utc).isoformat()


def save(name, data):
    (EVIDENCE / name).write_text(json.dumps(data, indent=2) + "\n")


def make_env(database, base, server: str | Sequence[str], **overrides):
    _assert_qa_database(database)
    env = dict(os.environ)
    for var in ("MONGO_URI", "DB_NAME", "SERVERS", "XRPL_INGEST_HOME", "EXPECTED_NETWORK_ID",
                "NUM_PROCESSES", "CONCURRENCY", "CHUNK_SIZE", "MIN_FREE_GB", "LOG_LEVEL",
                "MAX_RETRIES", "BACKOFF_MAX", "REQUEST_TIMEOUT"):
        env.pop(var, None)
    env.update({
        "MONGO_URI": MONGO_URI,
        "DB_NAME": database,
        "SERVERS": ",".join(server) if not isinstance(server, str) else server,
        "XRPL_INGEST_HOME": str(base),
        "EXPECTED_NETWORK_ID": "0",
        "LOG_LEVEL": "INFO",
        "MIN_FREE_GB": "0",
    })
    env.update(overrides)
    return env


def run_cli(env, *args, timeout=240):
    """Run `python -m xrpl_ingest <args>`; return (exit_code, log_text)."""
    proc = subprocess.run([sys.executable, "-m", "xrpl_ingest", *args],
                          cwd=ROOT, env=env, capture_output=True, text=True,
                          timeout=timeout)
    return proc.returncode, proc.stdout + proc.stderr


def node_snapshot(server=None):
    import asyncio
    import websockets

    async def fetch(url):
        async with websockets.connect(url, open_timeout=10) as ws:
            await ws.send(json.dumps({"method": "server_info", "params": [{}]}))
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            message = json.loads(raw)
            error = message.get("error") or message.get("error_message")
            if error or message.get("status") == "error":
                raise RuntimeError(f"{error or 'public node returned an error'}: "
                                   f"{message.get('error_message', '')}".strip())
            info = message["result"]["info"]
            if not isinstance(info, dict):
                raise ValueError("server_info returned no info object")
            return info

    failures = []
    for url in ([server] if server else SERVERS):
        try:
            return asyncio.run(fetch(url))
        except Exception as exc:
            failures.append((url, f"{type(exc).__name__}: {exc}"))
    raise NodeProbeError(failures)


def node_ready(info):
    val = info.get("validated_ledger") or {}
    age = val.get("age")
    state = info.get("server_state")
    if state is None and info.get("clio_version"):
        state = "full"  # Clio omits server_state; a fresh validated ledger implies ready
    return (state in ("full", "proposing", "validating")
            and type(val.get("seq")) is int and val["seq"] > 0
            and type(age) in (int, float) and 0 <= age <= 60)


def wait_ready(max_wait=300, interval=15):
    """Wait until a public Clio is ready AND not currently rate limited."""
    deadline = time.monotonic() + max_wait
    while time.monotonic() < deadline:
        delay = interval
        try:
            info = node_snapshot()
            if node_ready(info):
                return info
        except NodeProbeError as exc:
            if exc.rate_limited:
                delay = max(delay, QUIESCE_SECONDS)
        time.sleep(min(delay, max(0, deadline - time.monotonic())))
    raise RuntimeError("public Clio never became ready in time")


def pick_server():
    """Return all public endpoints with a rotating first choice for source failover."""
    global _server_cursor
    start = _server_cursor
    _server_cursor = (_server_cursor + 1) % len(SERVERS)
    return tuple(SERVERS[start:] + SERVERS[:start])


def run_cli_with_failover(env, *args, timeout=240, sleep=time.sleep):
    """Retry a rate-limited QA CLI operation from the next public endpoint."""
    servers = [entry.strip() for entry in env["SERVERS"].split(",") if entry.strip()]
    if not servers:
        raise ValueError("SERVERS must contain at least one endpoint")
    attempts = []
    for offset in range(len(servers)):
        attempt_env = dict(env)
        ordered = servers[offset:] + servers[:offset]
        attempt_env["SERVERS"] = ",".join(ordered)
        code, log = run_cli(attempt_env, *args, timeout=timeout)
        attempts.append(f"servers={ordered!r}, exit={code}\n{log}")
        if code == 0 or not _is_rate_limited(log) or offset == len(servers) - 1:
            return code, "\n".join(attempts)
        sleep(QUIESCE_SECONDS)
        if args and args[0] == "backfill":
            retry_code, retry_log = run_cli(
                attempt_env, "retry-failed", "--max-rounds", "1", timeout=timeout)
            attempts.append(
                f"retry-failed servers={ordered!r}, exit={retry_code}\n{retry_log}")
            code, log = run_cli(attempt_env, *args, timeout=timeout)
            attempts.append(f"resumed backfill servers={ordered!r}, exit={code}\n{log}")
            if code == 0 or not _is_rate_limited(retry_log + log):
                return code, "\n".join(attempts)
    raise AssertionError("unreachable")


def check_mongo(uri=MONGO_URI):
    """Verify only the configured QA MongoDB endpoint; never reads an environment URI."""
    from pymongo import MongoClient

    client = MongoClient(uri, serverSelectionTimeoutMS=1000)
    try:
        client.admin.command("ping")
    finally:
        client.close()


def verified_ledger_rate(verified_count, elapsed_seconds, verified):
    if not verified or elapsed_seconds <= 0:
        return None
    return round(verified_count / elapsed_seconds, 3)


def wait_for_partial_progress(proc, store, start, end, expected, *, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        written = len(store.ledger_numbers(start, end))
        if proc.poll() is not None:
            raise RuntimeError(
                f"backfill exited before interruption could be confirmed: {proc.returncode}")
        if 0 < written < expected:
            return written
        time.sleep(0.25)
    raise TimeoutError("no partial ledger writes observed while backfill was running")


def phase3_confirm_node_and_backfill(window=40, workers=1, concurrency=1):
    """Phase 3: fresh validated ledgers + accessible ledgers + deep verify."""
    result = {"phase": "3", "started_utc": stamp()}
    try:
        check_mongo()
        info = wait_ready(max_wait=240)
        result["node"] = {
            "server_state": info.get("server_state"),
            "clio_version": info.get("clio_version"),
            "validated_ledger_seq": (info.get("validated_ledger") or {}).get("seq"),
            "validated_age_seconds": (info.get("validated_ledger") or {}).get("age"),
            "complete_ledgers": info.get("complete_ledgers"),
        }
        result["node_ready"] = node_ready(info)
        latest = (info.get("validated_ledger") or {}).get("seq")
        end = latest - 2
        start = end - (window - 1)
        result["backfill_window"] = [start, end]
        result["window_size"] = window
        db = "xrpl_ingest_real_p3_" + uuid.uuid4().hex[:8]
        base = EVIDENCE / ("base_p3_" + db)
        base.mkdir(exist_ok=True)
        servers = pick_server()
        env = make_env(db, base, servers, NUM_PROCESSES=str(workers),
                       CONCURRENCY=str(concurrency), CHUNK_SIZE=str(window))
        result.update(database=db, servers=list(servers), concurrency=[workers, concurrency])
        code, log = run_cli_with_failover(
            env, "backfill", "--start", str(start), "--end", str(end), timeout=240)
        result["backfill_exit"] = code
        result["backfill_log_tail"] = log[-3000:]
        if code == 0:
            verify_code, verify_log = run_cli_with_failover(
                env, "verify", "--start", str(start), "--end", str(end), "--deep",
                timeout=240)
            result["deep_verify_exit"] = verify_code
            result["deep_verify_log_tail"] = verify_log[-3000:]
            from xrpl_ingest.config import Config
            from xrpl_ingest.store import MongoStore
            store = MongoStore.from_config(Config(mongo_uri=MONGO_URI, db_name=db))
            try:
                result["database_stats"] = store.stats()
                result["range_ledgers"] = len(store.ledger_numbers(start, end))
                result["gaps"] = store.find_gaps(start, end)
                result["transaction_mismatches"] = store.tx_count_mismatches(start, end)
                result["verified"] = (verify_code == 0
                    and result["range_ledgers"] == window and not result["gaps"]
                    and not result["transaction_mismatches"]
                    and result["database_stats"]["failed"] == 0)
            finally:
                store.close()
        result["outcome"] = "passed" if result.get("verified") else "failed"
        if result["outcome"] == "failed":
            result["error"] = "Backfill or deep source verification failed"
        return result
    except BaseException as exc:
        result.update(outcome="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        result["finished_utc"] = stamp()
        save("phase3.json", result)


def phase4_interrupt_resume(window=40, workers=1, concurrency=1):
    """Phase 4: interrupt a real backfill, restart safely, deep verify."""
    result = {"phase": "4", "started_utc": stamp()}
    proc = None
    store = None
    log_path = None
    stop_file = None
    try:
        check_mongo()
        info = wait_ready(max_wait=240)
        end = info["validated_ledger"]["seq"] - 2
        start = end - (window - 1)
        result.update(window=[start, end], window_size=window)
        db = "xrpl_ingest_real_p4_" + uuid.uuid4().hex[:8]
        base = EVIDENCE / ("base_p4_" + db)
        base.mkdir(exist_ok=True)
        servers = pick_server()
        env = make_env(db, base, servers, NUM_PROCESSES=str(workers),
                       CONCURRENCY=str(concurrency), CHUNK_SIZE="1")
        result.update(database=db, servers=list(servers), concurrency=[workers, concurrency])
        stop_file = base / "process_status.txt"
        from xrpl_ingest.config import Config
        from xrpl_ingest.store import MongoStore
        store = MongoStore.from_config(Config(mongo_uri=MONGO_URI, db_name=db))
        log_path = EVIDENCE / f"phase4_{db}_interrupt.log"
        with log_path.open("w", encoding="utf-8") as output:
            proc = subprocess.Popen(
                [sys.executable, "-m", "xrpl_ingest", "backfill",
                 "--start", str(start), "--end", str(end)],
                cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT)
            written = wait_for_partial_progress(proc, store, start, end, window)
            result["confirmed_in_progress"] = {
                "ledgers_written": written,
                "requested_ledgers": window,
                "process_running": proc.poll() is None,
            }
            stop_file.write_text("backfill=0\n", encoding="utf-8")
            result["interruption_method"] = "backfill stop file"
            result["interrupt_exit"] = proc.wait(timeout=60)
        result["interrupt_log_tail"] = log_path.read_text(encoding="utf-8")[-2500:]
        result["interrupted_ledger_count"] = len(store.ledger_numbers(start, end))
        if result["interrupt_exit"] == 0 or result["interrupted_ledger_count"] >= window:
            raise AssertionError("interruption did not leave confirmed unfinished work")
        stop_file.unlink(missing_ok=True)

        time.sleep(QUIESCE_SECONDS)
        code, log = run_cli_with_failover(
            env, "backfill", "--start", str(start), "--end", str(end), timeout=240)
        result.update(resume_exit=code, resume_log_tail=log[-2500:])
        if code != 0:
            raise RuntimeError("backfill resume did not complete successfully")
        if store.count_failed():
            code, log = run_cli_with_failover(
                env, "retry-failed", "--max-rounds", "3", timeout=240)
            result.update(retry_failed_exit=code, retry_failed_log_tail=log[-2500:])
            if code != 0:
                raise RuntimeError("recovery left failed ledgers after retry-failed")
        time.sleep(QUIESCE_SECONDS)
        code, log = run_cli_with_failover(
            env, "verify", "--start", str(start), "--end", str(end), "--deep", timeout=240)
        result.update(deep_verify_exit=code, deep_verify_log_tail=log[-2500:])
        result["database_stats"] = store.stats()
        result["recovered_ledger_count"] = len(store.ledger_numbers(start, end))
        result["gaps"] = store.find_gaps(start, end)
        result["transaction_mismatches"] = store.tx_count_mismatches(start, end)
        result["verified"] = (code == 0 and result["recovered_ledger_count"] == window
                              and not result["gaps"] and not result["transaction_mismatches"]
                              and result["database_stats"]["failed"] == 0)
        result["outcome"] = "passed" if result["verified"] else "failed"
        if not result["verified"]:
            result["error"] = "recovery did not pass source and database correctness checks"
        return result
    except BaseException as exc:
        result.update(outcome="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        if stop_file is not None and proc is not None and proc.poll() is None:
            stop_file.write_text("backfill=0\n", encoding="utf-8")
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
                result["cleanup_warning"] = "interrupted backfill required forced termination"
        if stop_file is not None:
            stop_file.unlink(missing_ok=True)
        if log_path is not None and log_path.exists():
            result.setdefault("interrupt_log_tail", log_path.read_text(encoding="utf-8")[-2500:])
        if store is not None:
            store.close()
        result["finished_utc"] = stamp()
        save("phase4.json", result)


def phase5_throughput(window=30):
    """Phase 5: throughput at three concurrency levels on a modest window."""
    result = {"phase": "5-throughput", "started_utc": stamp(), "profiles": []}
    try:
        check_mongo()
        info = wait_ready(max_wait=240)
        end = info["validated_ledger"]["seq"] - 2
        start = end - (window - 1)
        result.update(window=[start, end], window_size=window)
        from xrpl_ingest.config import Config
        from xrpl_ingest.store import MongoStore
        for workers, concurrency in ((1, 1), (1, 2), (2, 4)):
            db = "xrpl_ingest_real_p5_" + uuid.uuid4().hex[:8]
            base = EVIDENCE / ("base_p5_" + db)
            base.mkdir(exist_ok=True)
            servers = pick_server()
            env = make_env(db, base, servers, NUM_PROCESSES=str(workers),
                           CONCURRENCY=str(concurrency), CHUNK_SIZE=str(window))
            began = time.perf_counter()
            code, log = run_cli_with_failover(
                env, "backfill", "--start", str(start), "--end", str(end), timeout=240)
            elapsed = time.perf_counter() - began
            profile = {"processes": workers, "concurrency": concurrency,
                       "servers": list(servers), "exit": code,
                       "seconds": round(elapsed, 3), "log_tail": log[-1500:]}
            result["profiles"].append(profile)
            if code == 0:
                time.sleep(QUIESCE_SECONDS)
                verify_code, verify_log = run_cli_with_failover(
                    env, "verify", "--start", str(start), "--end", str(end), "--deep",
                    timeout=240)
                store = MongoStore.from_config(Config(mongo_uri=MONGO_URI, db_name=db))
                try:
                    stats = store.stats()
                    verified_count = len(store.ledger_numbers(start, end))
                    profile.update(verify_exit=verify_code, verify_log_tail=verify_log[-1200:],
                                   database_stats=stats, verified_ledger_count=verified_count,
                                   gaps=store.find_gaps(start, end),
                                   transaction_mismatches=store.tx_count_mismatches(start, end))
                    profile["verified"] = (verify_code == 0 and verified_count == window
                                           and stats["failed"] == 0 and not profile["gaps"]
                                           and not profile["transaction_mismatches"])
                    profile["ledgers_per_second"] = verified_ledger_rate(
                        verified_count, elapsed, profile["verified"])
                finally:
                    store.close()
            else:
                profile.update(verified=False, verified_ledger_count=0, ledgers_per_second=None)
            save("phase5_throughput.json", result)
            if not profile["verified"]:
                result.update(outcome="failed", error="profile failed backfill or deep verification")
                return result
            time.sleep(QUIESCE_SECONDS)
        result["outcome"] = "passed"
        return result
    except BaseException as exc:
        result.update(outcome="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        result["finished_utc"] = stamp()
        save("phase5_throughput.json", result)


def phase5_live_hour(seconds=120):
    """Phase 5 (live): tail the live validated tip, monitoring checkpoint progress."""
    result = {"phase": "5-live", "started_utc": stamp(), "target_seconds": seconds,
              "samples": []}
    proc = None
    store = None
    stop_file = None
    try:
        check_mongo()
        from xrpl_ingest.config import Config
        from xrpl_ingest.store import MongoStore
        db = "xrpl_ingest_real_live_" + uuid.uuid4().hex[:8]
        base = EVIDENCE / ("base_live_" + db)
        base.mkdir(exist_ok=True)
        servers = pick_server()
        env = make_env(db, base, servers, CHUNK_SIZE="10", POLLING_INTERVAL_SECONDS="1")
        result.update(database=db, servers=list(servers), failovers=[])
        store = MongoStore.from_config(Config(mongo_uri=MONGO_URI, db_name=db))
        log_path = EVIDENCE / f"phase5_live_{db}.log"
        stop_file = base / "process_status.txt"
        with log_path.open("w", encoding="utf-8") as output:
            proc = subprocess.Popen([sys.executable, "-m", "xrpl_ingest", "tail"],
                                    cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT)
            startup = time.monotonic()
            while not store.get_checkpoint():
                if proc.poll() is not None:
                    raise RuntimeError(f"tail exited during startup with {proc.returncode}")
                if time.monotonic() - startup > 90:
                    raise TimeoutError("live tail produced no initial checkpoint in 90 seconds")
                time.sleep(1)
            initial_checkpoint = store.get_checkpoint()
            result["checkpoint_before"] = initial_checkpoint
            began = time.monotonic()
            while time.monotonic() - began < seconds:
                if proc.poll() is not None:
                    raise RuntimeError(f"tail exited before the live test ended: {proc.returncode}")
                info = node_snapshot()
                latest = (info.get("validated_ledger") or {}).get("seq")
                checkpoint = store.get_checkpoint()
                lag = max(0, latest - checkpoint) if isinstance(latest, int) else None
                first = store.ledgers.find_one(sort=[("block_number", 1)])
                failed_count = store.count_failed()
                if first and checkpoint >= first["block_number"]:
                    gaps = store.find_gaps(first["block_number"], checkpoint)
                    mismatches = store.tx_count_mismatches(first["block_number"], checkpoint)
                    complete = not gaps and not mismatches
                else:
                    gaps, mismatches, complete = [], [], False
                sample = {"utc": stamp(), "node_latest": latest,
                          "checkpoint": checkpoint, "lag_ledgers": lag,
                          "failed_ledger_count": failed_count,
                          "data_complete_locally": complete,
                          "gaps": gaps, "transaction_mismatches": mismatches,
                          "stored_ledgers": store.ledgers.count_documents(
                              {"block_number": {"$lte": latest}}) if latest else 0}
                sample["healthy"] = (isinstance(lag, int) and lag <= 20
                                     and checkpoint > 0 and failed_count == 0 and complete)
                result["samples"].append(sample)
                result["latest_sample"] = sample
                save("phase5_live.json", result)
                rate_limited = any(_is_rate_limited(reason)
                                   for _, reason in store.list_failed_with_reasons())
                if rate_limited:
                    if len(result["failovers"]) >= len(servers):
                        raise RuntimeError("public endpoint failover limit reached")
                    stop_file.write_text("tail=0\n", encoding="utf-8")
                    tail_code = proc.wait(timeout=45)
                    if tail_code not in (0, 1):
                        raise RuntimeError(f"tail exited unexpectedly during failover: {tail_code}")
                    result["failovers"].append({
                        "from_servers": env["SERVERS"].split(","),
                        "tail_stop_exit": tail_code,
                        "failed_ledgers_before_retry": failed_count,
                    })
                    proc = None
                    stop_file.unlink(missing_ok=True)
                    time.sleep(QUIESCE_SECONDS)
                    retry_code, retry_log = run_cli_with_failover(
                        env, "retry-failed", "--max-rounds", "3", timeout=600)
                    result["failovers"][-1].update(
                        retry_exit=retry_code, retry_log_tail=retry_log[-2000:])
                    if retry_code != 0:
                        raise RuntimeError("rate-limited live ledgers were not recovered")
                    next_index = len(result["failovers"]) % len(servers)
                    ordered = servers[next_index:] + servers[:next_index]
                    env["SERVERS"] = ",".join(ordered)
                    result["failovers"][-1]["to_servers"] = list(ordered)
                    proc = subprocess.Popen([sys.executable, "-m", "xrpl_ingest", "tail"],
                                            cwd=ROOT, env=env, stdout=output,
                                            stderr=subprocess.STDOUT)
                    result["failovers"][-1]["tail_restarted"] = True
                    save("phase5_live.json", result)
                time.sleep(min(20, max(0, seconds - (time.monotonic() - began))))
        proc.send_signal(__import__("signal").SIGTERM)
        result["tail_exit"] = proc.wait(timeout=45)
        result["tail_terminated_cleanly"] = result["tail_exit"] in (0, 1)
        first = store.ledgers.find_one(sort=[("block_number", 1)])
        last = store.highest_ledger()
        result.update(checkpoint_after=store.get_checkpoint(), database_stats=store.stats(),
                      first_ledger=first["block_number"] if first else None,
                      last_ledger=last)
        if first and last >= first["block_number"]:
            start = first["block_number"]
            result["gaps"] = store.find_gaps(start, last)
            result["transaction_mismatches"] = store.tx_count_mismatches(start, last)
            code, log = run_cli_with_failover(
                env, "verify", "--start", str(start), "--end", str(last), "--deep",
                timeout=600)
            result.update(deep_verify_exit=code, deep_verify_log_tail=log[-2500:])
        else:
            result["deep_verify_exit"] = 1
            result["error"] = "no live ledger range was stored"
        result["checkpoint_progressed"] = result["checkpoint_after"] > initial_checkpoint
        result["outcome"] = "passed" if (
            result["tail_terminated_cleanly"] and result["checkpoint_progressed"]
            and result["deep_verify_exit"] == 0 and not result.get("gaps")
            and not result.get("transaction_mismatches")
            and result["database_stats"]["failed"] == 0
            and result["latest_sample"]["healthy"]
        ) else "failed"
        if result["outcome"] == "failed":
            result.setdefault("error", "live ingestion health or completeness checks failed")
        return result
    except BaseException as exc:
        result.update(outcome="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        if proc is not None and proc.poll() is None:
            proc.send_signal(__import__("signal").SIGTERM)
            try:
                proc.wait(timeout=45)
                result["tail_exit"] = proc.returncode
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
                result["cleanup_warning"] = "tail required forced termination"
        if stop_file is not None:
            stop_file.unlink(missing_ok=True)
        if store is not None:
            store.close()
        result["finished_utc"] = stamp()
        save("phase5_live.json", result)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--phase", required=True, choices=["3", "4", "5-throughput", "5-live"])
    p.add_argument("--seconds", type=int, default=120)
    p.add_argument("--window", type=int, default=40)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--concurrency", type=int, default=1)
    a = p.parse_args()
    if a.phase == "3":
        r = phase3_confirm_node_and_backfill(a.window, a.workers, a.concurrency)
    elif a.phase == "4":
        r = phase4_interrupt_resume(a.window, a.workers, a.concurrency)
    elif a.phase == "5-throughput":
        r = phase5_throughput(a.window)
    else:
        r = phase5_live_hour(a.seconds)
    print(json.dumps(r, indent=2))
    raise SystemExit(0 if r.get("outcome") == "passed" else 1)
