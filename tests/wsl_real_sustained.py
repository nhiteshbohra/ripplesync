"""Remaining checks: one-hour local live ingestion/stability, then real throughput."""
import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path('/home/ubuntu/ripplesync')
ROOT = Path(__file__).resolve().parents[1] / 'reviews'
sys.path.insert(0, str(PROJECT))
from xrpl_ingest.config import Config, ENV_VARS, PATH_ENV_VARS
from xrpl_ingest.ledger_ranges import parse_complete_ledgers
from xrpl_ingest.source import WsSource
from xrpl_ingest.store import MongoStore


def benchmark_range(info):
    latest = info['validated_ledger']['seq']
    ranges = [(a, min(b, latest - 2)) for a, b in parse_complete_ledgers(info['complete_ledgers'])
              if b >= latest - 1000]
    usable = [(a, b) for a, b in ranges if b - a + 1 >= 20]
    if not usable:
        raise RuntimeError('No recent contiguous 20-ledger benchmark range')
    a, b = max(usable, key=lambda item: item[1])
    return max(a, b - 199), b


def enough_memory(available, swap_free, windows_mb, chunk_bytes, workers):
    # ponytail: 24x JSON bytes estimates decoded/batched objects, not a hard bound;
    # use measured worker RSS and byte-bounded queues for tighter capacity control.
    estimate = workers * chunk_bytes * 24
    return (available >= estimate + 2 * 1024**3 and swap_free >= 1024**3
            and windows_mb is not None and windows_mb >= 1024)


def node_ready(info):
    ledger = info.get('validated_ledger', {})
    age = ledger.get('age')
    return (info.get('server_state') in ('full', 'proposing', 'validating')
            and type(ledger.get('seq')) is int and ledger['seq'] > 0
            and type(age) in (int, float) and 0 <= age <= 30)


if '--self-check' in sys.argv:
    assert benchmark_range({'validated_ledger': {'seq': 1000}, 'complete_ledgers': '1-500,900-1000'}) == (900, 998)
    assert enough_memory(8 * 1024**3, 2 * 1024**3, 3000, 1024**2, 4)
    assert not enough_memory(2 * 1024**3, 2 * 1024**3, 3000, 1024**2, 4)
    assert not enough_memory(8 * 1024**3, 2 * 1024**3, None, 1024**2, 4)
    assert node_ready({'server_state': 'full', 'validated_ledger': {'seq': 1000, 'age': 2}})
    assert not node_ready({'server_state': 'tracking', 'validated_ledger': {'seq': 1000, 'age': 2}})
    assert not node_ready({'server_state': 'full', 'validated_ledger': {'seq': 1000, 'age': 300}})
    print('PASS real-throughput range selection and resource preconditions')
    raise SystemExit(0)


def terminate(process):
    if process is not None and process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            process.wait()
            return
        try:
            process.wait(timeout=45)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
            raise RuntimeError('Owned QA process required SIGKILL')


def host_resources():
    script = Path(__file__).resolve().with_name('windows_resource_snapshot.ps1')
    windows_script = subprocess.check_output(['wslpath', '-w', str(script)], text=True).strip()
    command = ['/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe', '-NoProfile', '-File',
               windows_script]
    try:
        run = subprocess.run(command, capture_output=True, text=True, timeout=30)
        run.check_returncode()
        return json.loads(run.stdout.lstrip('\ufeff'))
    except (OSError, ValueError, subprocess.SubprocessError):
        return {}


def main(mode):
    def interrupted(signum, frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    ROOT.mkdir(exist_ok=True)
    token = uuid.uuid4().hex
    evidence = ROOT / ('real_sustained_' + token)
    evidence.mkdir()
    database = 'xrpl_ingest_test_soak_' + token
    result = dict(outcome='running', phase=('throughput' if mode == 'throughput' else 'live_stability'), mode=mode, database=database,
                  server='ws://127.0.0.1:6006', target_seconds=3600, profiles=[])

    def save():
        for path in (evidence / 'results.json', ROOT / ('real_sustained_latest.json' if mode == 'all' else 'real_' + mode + '_latest.json')):
            temporary = path.with_suffix('.tmp')
            temporary.write_text(json.dumps(result, indent=2))
            temporary.replace(path)

    cfg = Config(mongo_uri='mongodb://192.168.1.39:27017', db_name=database)
    store = MongoStore.from_config(cfg)
    process = None
    source = WsSource(['ws://127.0.0.1:6006'])
    loop = asyncio.new_event_loop()

    async def node_info():
        node = WsSource([result['server']], request_timeout=10, connect_retries=1)
        try:
            return await node.server_info()
        finally:
            await node.close()
    with tempfile.TemporaryDirectory(prefix='xrpl-real-soak-') as base:
        env = dict(os.environ)
        for name in [name for name, _ in ENV_VARS] + list(PATH_ENV_VARS):
            env.pop(name, None)
        env.update(XRPL_INGEST_HOME=base, MONGO_URI=cfg.mongo_uri, DB_NAME=database,
                   SERVERS=result['server'], NUM_PROCESSES='2', CONCURRENCY='4',
                   CHUNK_SIZE='10', POLLING_INTERVAL_SECONDS='1', MIN_FREE_GB='5')
        prefix = [sys.executable, '-m', 'xrpl_ingest']

        def check_host():
            nonlocal process
            host = host_resources()
            drive = next((drive for drive in host.get('drives', [])
                          if drive['Name'] == host.get('wsl_backing_drive')), None)
            result['host_resources'] = dict(utc=host.get('utc'),
                available_ram_mb=host.get('available_ram_counter_mb'), backing_drive=drive)
            if drive is None:
                result.update(outcome='blocked_resources', error='Cannot verify physical Windows backing-drive space')
                raise RuntimeError(result['error'])
            if drive['free_gb'] < 5:
                result.update(outcome='blocked_resources', error=f"Physical {drive['Name']}: free space below 5 GiB")
                terminate(process)
                process = None
                # Stop only this test node before its separate storage writes fill the host drive.
                stopped = subprocess.run(['/mnt/c/Windows/System32/wsl.exe', '-d', 'Ubuntu-24.04',
                    '-u', 'root', '--', 'systemctl', 'stop', 'xrpld'], capture_output=True, timeout=360)
                result['node_stop_for_disk_exit'] = stopped.returncode
                save()
                raise RuntimeError(result['error'])
            return host

        def command(label, args, command_env=env, timeout=600):
            nonlocal process
            with (evidence / (label + '.log')).open('w') as output:
                process = subprocess.Popen(prefix + args, cwd=PROJECT, env=command_env,
                    stdout=output, stderr=output, start_new_session=True)
                try:
                    code = process.wait(timeout=timeout)
                    assert code == 0, (label, code)
                finally:
                    terminate(process)
                    process = None

        try:
            save()
            check_host()
            if mode != 'throughput':
                with (evidence / 'tail.log').open('w') as output:
                    process = subprocess.Popen(prefix + ['tail'], cwd=PROJECT, env=env,
                        stdout=output, stderr=output, start_new_session=True)
                    startup = time.monotonic()
                    while not store.get_checkpoint():
                        assert process.poll() is None, 'Live tail exited during startup'
                        assert time.monotonic() - startup < 90, 'Live tail produced no checkpoint'
                        time.sleep(1)
                    began = time.monotonic()
                    started = datetime.now(timezone.utc)
                    result['started_utc'] = started.isoformat()
                    bad_since = None
                    unhealthy = 0
                    samples = 0
                    next_host_check = time.monotonic() + 60
                    while (time.monotonic() - began < 3600 or
                           (datetime.now(timezone.utc) - started).total_seconds() < 3600):
                        assert process.poll() is None, 'Live tail exited before the hour completed'
                        assert time.monotonic() - began < 5400, 'Clock/duration bound exceeded'
                        if time.monotonic() >= next_host_check:
                            check_host()
                            next_host_check = time.monotonic() + 60
                        sample = dict(utc=datetime.now(timezone.utc).isoformat())
                        try:
                            info = loop.run_until_complete(node_info())
                            ledger = info.get('validated_ledger', {})
                            checkpoint = store.get_checkpoint()
                            sample.update(state=info.get('server_state'), latest=ledger.get('seq'),
                                          age=ledger.get('age'), checkpoint=checkpoint)
                            healthy = (sample['state'] in ('full', 'proposing', 'validating')
                                       and type(sample['age']) in (int, float) and 0 <= sample['age'] <= 30
                                       # The checkpoint can advance after the node-info snapshot.
                                       and max(0, sample['latest'] - checkpoint) <= 20)
                        except Exception as exc:
                            healthy = False
                            sample['error'] = f'{type(exc).__name__}: {exc}'
                        sample['healthy'] = healthy
                        samples += 1
                        unhealthy += not healthy
                        with (evidence / 'samples.jsonl').open('a') as samples_file:
                            samples_file.write(json.dumps(sample) + '\n')
                        result.update(elapsed_seconds=round(time.monotonic() - began),
                                      samples=samples, unhealthy_samples=unhealthy, latest_sample=sample)
                        save()
                        if healthy:
                            bad_since = None
                        elif bad_since is None:
                            bad_since = time.monotonic()
                        elif time.monotonic() - bad_since >= 60:
                            raise AssertionError('Local node/live follower unhealthy for at least 60 seconds')
                        time.sleep(15)
                    terminate(process)
                    result['tail_stop_exit'] = process.returncode
                    assert process.returncode in (0, 1)
                    process = None
                result['duration_wall_seconds'] = (datetime.now(timezone.utc) - started).total_seconds()
                first = store.ledgers.find_one(sort=[('block_number', 1)])['block_number']
                last = store.highest_ledger()
                assert last > first and store.count_failed() == 0
                assert not store.find_gaps(first, last) and not store.tx_count_mismatches(first, last)
                result.update(phase='deep_verify_hour', live_stats=store.stats(), first=first, last=last)
                save()
                command('hour-deep-verify', ['verify', '--start', str(first), '--end', str(last), '--deep'])
                assert unhealthy == 0, 'Hour completed, but sampled node stability was not clean'
                if mode == 'live':
                    result.update(outcome='passed', phase='complete', finished_utc=datetime.now(timezone.utc).isoformat())
                    return 0
            result['phase'] = 'throughput'
            save()
            info = loop.run_until_complete(node_info())
            assert node_ready(info), 'Node is not ready with a fresh validated ledger; wait before throughput'
            a, b = benchmark_range(info)
            sizes = []
            preflight_deadline = time.monotonic() + 300
            for index in range(a, b + 1):
                assert time.monotonic() < preflight_deadline, 'Size preflight exceeded five minutes'
                raw = loop.run_until_complete(source.get_ledger(index))
                assert raw.get('validated') is True
                sizes.append(len(json.dumps(raw).encode()))
            loop.run_until_complete(source.close())
            chunk_bytes = max(sum(sizes[i:i + 25]) for i in range(len(sizes)))
            result.update(benchmark_start=a, benchmark_end=b, largest_json_bytes=max(sizes),
                          maximum_25_ledger_json_bytes=chunk_bytes,
                          benchmark_cache='node warmed by a serial size preflight')
            for workers, concurrency in ((1, 2), (2, 4), (4, 16)):
                assert node_ready(loop.run_until_complete(node_info())), 'Node lost readiness before profile'
                memory = dict(line.split()[:2] for line in Path('/proc/meminfo').read_text().splitlines())
                windows_mb = check_host().get('available_ram_counter_mb')
                if not enough_memory(int(memory['MemAvailable:']) * 1024,
                                     int(memory['SwapFree:']) * 1024, windows_mb, chunk_bytes, workers):
                    result.update(outcome='blocked_resources', blocked_profile=[workers, concurrency],
                                  windows_available_mb=windows_mb, guest_memory_kb=memory['MemAvailable:'])
                    save()
                    return 3
                bench_database = 'xrpl_ingest_test_speed_' + uuid.uuid4().hex
                bench_env = dict(env, DB_NAME=bench_database, NUM_PROCESSES=str(workers),
                                 CONCURRENCY=str(concurrency), CHUNK_SIZE='25')
                label = f'backfill-{workers}-{concurrency}'
                began_profile = time.perf_counter()
                command(label, ['backfill', '--start', str(a), '--end', str(b)], bench_env)
                elapsed = time.perf_counter() - began_profile
                bench = MongoStore.from_config(Config(mongo_uri=cfg.mongo_uri, db_name=bench_database))
                try:
                    stats = bench.stats()
                    assert stats['ledgers'] == b - a + 1 and stats['failed'] == 0
                    command(label + '-verify', ['verify', '--start', str(a), '--end', str(b), '--deep'], bench_env)
                    after = loop.run_until_complete(node_info())
                    result['profiles'].append(dict(database=bench_database, processes=workers,
                        concurrency=concurrency, chunk_size=25, seconds=elapsed, stats=stats,
                        ledgers_per_second=(b - a + 1) / elapsed,
                        transactions_per_second=stats['transactions'] / elapsed,
                        node_after={'state': after.get('server_state'),
                                    'validated': after.get('validated_ledger')}))
                    save()
                    assert node_ready(after), 'Node lost readiness/freshness under measured load'
                finally:
                    bench.close()
            result.update(outcome='passed', phase='complete', finished_utc=datetime.now(timezone.utc).isoformat())
            return 0
        except BaseException as exc:
            if result['outcome'] != 'blocked_resources':
                result.update(outcome='failed', error=f'{type(exc).__name__}: {exc}')
            raise
        finally:
            try:
                terminate(process)
            finally:
                loop.run_until_complete(source.close())
                loop.close()
                store.close()
                save()


if __name__ == '__main__':
    if '--check-host' in sys.argv:
        available = host_resources().get('available_ram_counter_mb')
        print(json.dumps({'windows_available_mb': available, 'readable': available is not None}))
        raise SystemExit(0 if available is not None else 1)
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['all', 'live', 'throughput'], default='all')
    raise SystemExit(main(parser.parse_args().mode))
