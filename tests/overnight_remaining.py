"""Bounded overnight readiness polling and independent real-data QA attempts."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.request

from run_remaining_tests import ROOT, PYTHON, REVIEWS, save, stamp
from wsl_real_sustained import benchmark_range, host_resources, node_ready


def pending_modes(attempts, passed):
    return sorted((mode for mode in ('throughput', 'live')
                   if mode not in passed and attempts[mode] < 3), key=attempts.get)


def snapshot():
    request = urllib.request.Request('http://127.0.0.1:5005/',
        data=b'{"method":"server_info","params":[{}]}',
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=15) as response:
        info = json.load(response)['result']['info']
    fields = ('server_state', 'uptime', 'validated_ledger', 'complete_ledgers', 'peers')
    return info, {key: info.get(key) for key in fields}


def main(hours):
    REVIEWS.mkdir(exist_ok=True)
    with open('/tmp/xrpl-overnight-remaining.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        deadline = time.monotonic() + hours * 3600
        state = dict(status='waiting_for_sync', started_utc=stamp(), hours=hours,
                     interval_seconds=600, pid=os.getpid(), passed=[], attempts=[], checks=0)
        attempts = {'throughput': 0, 'live': 0}
        path = REVIEWS / 'overnight_remaining_status.json'
        process = None

        def persist():
            state['updated_utc'] = stamp()
            save(path, state)

        def interrupted(signum, frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, interrupted)
        try:
            while time.monotonic() < deadline:
                modes = pending_modes(attempts, state['passed'])
                if not modes:
                    state['status'] = 'passed' if len(state['passed']) == 2 else 'attempt_limit_reached'
                    return 0 if len(state['passed']) == 2 else 1
                state.update(status='waiting_for_sync', checks=state['checks'] + 1)
                ready = False
                try:
                    info, summary = snapshot()
                    state['node'] = summary
                    ready = node_ready(info)
                    if ready:
                        benchmark_range(info)
                    host = host_resources()
                    drive = next((d for d in host.get('drives', [])
                                  if d['Name'] == host.get('wsl_backing_drive')), None)
                    state['resources'] = dict(backing_drive=drive,
                        available_ram_mb=host.get('available_ram_counter_mb'))
                    if drive is not None and drive['free_gb'] < 5:
                        state.update(status='blocked_disk', error='Backing drive has less than 5 GiB free')
                        state['node_stop_exit'] = subprocess.run(
                            ['systemctl', 'stop', 'xrpld'], timeout=360).returncode
                        return 3
                    ready = (ready and drive is not None and drive['free_gb'] >= 8
                             and (host.get('available_ram_counter_mb') or 0) >= 1024)
                    state.pop('check_error', None)
                except Exception as error:
                    ready = False
                    state['check_error'] = f'{type(error).__name__}: {error}'
                persist()
                print(json.dumps({'utc': stamp(), 'ready': ready, 'node': state.get('node'),
                                  'resources': state.get('resources'),
                                  'error': state.get('check_error')}), flush=True)
                if ready:
                    mode = modes[0]
                    # Reserve enough time for an honest full-hour run and verification.
                    needed = 7200 if mode == 'live' else 2400
                    if deadline - time.monotonic() < needed:
                        state['status'] = 'insufficient_time_remaining'
                        return 3
                    attempts[mode] += 1
                    log = REVIEWS / f'overnight_{mode}_{time.time_ns()}.log'
                    attempt = dict(mode=mode, number=attempts[mode], started_utc=stamp(), log=str(log))
                    state['attempts'].append(attempt)
                    state['status'] = 'running_' + mode
                    persist()
                    command = [str(PYTHON),
                               str(ROOT / 'tests/run_remaining_tests.py'), mode]
                    with log.open('w') as output:
                        process = subprocess.Popen(command, stdout=output, stderr=output,
                                                   start_new_session=True)
                        code = process.wait(timeout=min(10800, deadline - time.monotonic()))
                    process = None
                    attempt.update(exit_code=code, finished_utc=stamp())
                    result_path = REVIEWS / f'real_{mode}_latest.json'
                    if result_path.exists():
                        attempt['result'] = json.loads(result_path.read_text())
                    if code == 0 and attempt.get('result', {}).get('outcome') == 'passed':
                        state['passed'].append(mode)
                    persist()
                    if len(state['passed']) == 2:
                        state['status'] = 'passed'
                        return 0
                # Polling belongs to this independent background process, not the chat.
                time.sleep(min(600, max(0, deadline - time.monotonic())))
            state['status'] = 'deadline_expired'
            return 3
        except KeyboardInterrupt:
            state['status'] = 'interrupted'
            return 130
        except Exception as error:
            state.update(status='failed', error=f'{type(error).__name__}: {error}')
            raise
        finally:
            if process is not None and process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGINT)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    state['cleanup_warning'] = 'Forced coordinator stop; inspect QA processes'
            state.update(pid=None, finished_utc=stamp())
            persist()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--hours', type=float, default=12)
    parser.add_argument('--self-check', action='store_true')
    args = parser.parse_args()
    if args.self_check:
        assert pending_modes({'throughput': 0, 'live': 0}, []) == ['throughput', 'live']
        assert pending_modes({'throughput': 3, 'live': 0}, []) == ['live']
        assert pending_modes({'throughput': 1, 'live': 0}, []) == ['live', 'throughput']
        assert pending_modes({'throughput': 1, 'live': 0}, ['throughput']) == ['live']
        assert pending_modes({'throughput': 0, 'live': 0}, ['throughput', 'live']) == []
        print('PASS overnight selection, completion and attempt cap; no real tests run')
    else:
        if not 0 < args.hours <= 24:
            parser.error('--hours must be between 0 and 24')
        raise SystemExit(main(args.hours))
