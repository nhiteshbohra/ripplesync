"""Independent runner for this workspace's retained real-data WSL checks.

Run in a WSL terminal; no chat session is required. This deliberately reuses the
local QA harnesses, not synthetic benchmarks or a fresh installer deployment.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
REVIEWS = ROOT / 'reviews'
STATE = REVIEWS / 'remaining_tests_status.json'
HOME = Path('/home/ubuntu/ripplesync')
PYTHON = HOME / '.venv/bin/python'


def save(path, data):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, indent=2) + '\n', encoding='utf-8')
    temporary.replace(path)


def stages(wait, mode='all'):
    if mode != 'all':
        return [(mode, 'wsl_real_sustained.py', ['--mode', mode], 10800, False)]
    return [
        ('sync-and-real-cli', 'wsl_wait_real_node.py', [str(wait)], wait + 1800, False),
        ('systemd-backfill-tail-shutdown', 'wsl_real_services.py', ['300'], 900, True),
        ('installer-real-data-trial', 'wsl_real_installer_trial.py', [], 2400, False),
        ('worker-stop-sigterm-recovery', 'wsl_real_backfill_interruptions.py',
         ['worker-death', 'stop-file', 'sigterm'], 2400, False),
        ('one-hour-live-and-three-throughput-profiles', 'wsl_real_sustained.py', [], 10800, False),
    ]


def stamp():
    return datetime.now(timezone.utc).isoformat()


def harness_path(script):
    return ROOT / 'tests' / script if script == 'wsl_real_sustained.py' else REVIEWS / script


def run(args):
    import fcntl
    import pwd
    if os.geteuid() != 0:
        raise RuntimeError('Run with sudo: the transient systemd checks need root.')
    REVIEWS.mkdir(exist_ok=True)
    account = pwd.getpwnam('ubuntu')
    if account.pw_uid != 1000:
        raise RuntimeError('Existing service harness requires ubuntu UID 1000.')
    plan = stages(args.wait_seconds, getattr(args, 'mode', 'all'))
    for _, script, _, _, _ in plan:
        if not harness_path(script).is_file():
            raise RuntimeError(f'Missing retained QA harness: {script}; use this workspace copy.')
    if not PYTHON.is_file():
        raise RuntimeError(f'Missing existing WSL virtual environment: {PYTHON}')
    # Harnesses run the installed home copy: prevent accidentally testing stale code.
    for source in (ROOT / 'xrpl_ingest').rglob('*.py'):
        target = HOME / source.relative_to(ROOT)
        if not target.is_file() or target.read_bytes() != source.read_bytes():
            raise RuntimeError(f'WSL home code differs: {source.relative_to(ROOT)}; reconcile before testing.')
    if (ROOT / 'install.sh').read_bytes() != (HOME / 'install.sh').read_bytes():
        raise RuntimeError('WSL home install.sh differs; reconcile before testing.')
    inputs = sorted((ROOT / 'xrpl_ingest').rglob('*.py')) + [ROOT / 'install.sh', Path(__file__)]
    inputs += [harness_path(script) for _, script, _, _, _ in plan]
    fingerprint = hashlib.sha256(b''.join(path.read_bytes() for path in inputs)).hexdigest()
    with (REVIEWS / 'remaining_tests.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('A remaining-tests runner is already active.') from None
        if args.command == 'resume':
            state = json.loads(STATE.read_text(encoding='utf-8'))
            if state.get('code_fingerprint') != fingerprint:
                raise RuntimeError('Code changed since this run. Use run to test the new code from the beginning.')
            if state.get('cleanup_warning'):
                raise RuntimeError('Previous cleanup was forced. Inspect QA jobs, then use run for a fresh suite.')
            if state.get('status') == 'passed':
                print('All runner stages already passed. Use run for a fresh suite.')
                return 0
        else:
            if STATE.exists():
                archive = REVIEWS / ('remaining_tests_status.' + str(time.time_ns()) + '.json')
                archive.write_bytes(STATE.read_bytes())
            state = {'started_utc': stamp(), 'completed': [], 'attempts': [],
                     'code_fingerprint': fingerprint}
        state.update(status='running', pid=os.getpid(), updated_utc=stamp())
        save(STATE, state)
        env = dict(os.environ, REAL_TEST_SERVER='ws://127.0.0.1:6006')
        child = None

        def interrupted(signum, frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, interrupted)
        try:
            for name, script, extra, timeout, root in plan:
                if name in state['completed']:
                    continue
                log_path = REVIEWS / f'remaining_{name}_{time.time_ns()}.log'
                attempt = {'stage': name, 'started_utc': stamp(), 'log': str(log_path)}
                state['attempts'].append(attempt)
                state.update(stage=name, updated_utc=stamp())
                save(STATE, state)
                print(f'RUN {name}\nLog: {log_path}', flush=True)
                command = [str(PYTHON), str(harness_path(script)), *extra]
                if not root:
                    command = ['runuser', '-u', 'ubuntu', '--', *command]
                with log_path.open('w', encoding='utf-8') as log:
                    child = subprocess.Popen(command, env=env, stdout=log, stderr=log,
                                             start_new_session=True)
                    code = child.wait(timeout=timeout)
                child = None
                attempt.update(exit_code=code, finished_utc=stamp())
                if code:
                    state['status'] = 'blocked' if code == 3 else 'failed'
                    print(f'{state["status"].upper()}: {name}; see {log_path}', flush=True)
                    return code
                state['completed'].append(name)
                save(STATE, state)
            state['status'] = 'passed'
            print(f'PASS: selected {len(plan)} runner stage(s) completed. Detailed evidence remains in reviews/.')
            return 0
        except KeyboardInterrupt:
            state['status'] = 'interrupted'
            return 130
        except Exception as error:
            state.update(status='failed', error=f'{type(error).__name__}: {error}')
            print(state['error'], file=sys.stderr)
            return 1
        finally:
            if child is not None and child.poll() is None:
                # Python harnesses use finally blocks to clean up their own QA jobs.
                try:
                    os.killpg(child.pid, signal.SIGINT)
                except ProcessLookupError:
                    pass
                try:
                    child.wait(timeout=90)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                    state['cleanup_warning'] = 'Forced harness stop; inspect owned QA jobs before resume.'
            state.update(updated_utc=stamp(), pid=None)
            save(STATE, state)


def main():
    global STATE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['run', 'resume', 'status', 'self-check', 'throughput', 'live'])
    parser.add_argument('--mode', choices=['all', 'live', 'throughput'], default='all',
                        help='Select separate progress for status/resume; default all.')
    parser.add_argument('--wait-seconds', type=int, default=3600,
                        help='Maximum initial local-node sync wait (default 3600).')
    args = parser.parse_args()
    if args.command in ('throughput', 'live'):
        args.mode = args.command
    if args.mode != 'all':
        STATE = REVIEWS / f'remaining_{args.mode}_status.json'
    if args.wait_seconds <= 0:
        parser.error('--wait-seconds must be positive')
    if args.command == 'self-check':
        import tempfile
        from types import SimpleNamespace
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / 'state.json'
            save(path, {'completed': ['first'], 'status': 'running'})
            save(path, {'completed': ['first'], 'status': 'failed'})
            assert json.loads(path.read_text()) == {'completed': ['first'], 'status': 'failed'}
            assert not path.with_suffix('.tmp').exists()
            (root / 'xrpl_ingest').mkdir()
            (root / 'tests').mkdir()
            (root / 'install.sh').write_text('# QA placeholder for orchestration self-check\n')
            python = root / 'python'
            python.touch()
            for _, script, _, _, _ in stages(120):
                (root / 'tests' / script if script == 'wsl_real_sustained.py' else root / script).touch()
            modules = {'fcntl': SimpleNamespace(LOCK_EX=1, LOCK_NB=2, flock=lambda *a: None),
                       'pwd': SimpleNamespace(getpwnam=lambda user: SimpleNamespace(pw_uid=1000))}
            # Exercise actual fail-fast and resume flow without spawning any QA command.
            with patch.dict(sys.modules, modules), \
                    patch.multiple(sys.modules[__name__], ROOT=root, HOME=root, REVIEWS=root,
                                   STATE=path, PYTHON=python), \
                    patch.object(os, 'geteuid', return_value=0, create=True), \
                    patch.object(subprocess, 'Popen') as spawn:
                spawn.return_value.wait.side_effect = [0, 1]
                assert run(SimpleNamespace(command='run', wait_seconds=120)) == 1
                state = json.loads(path.read_text())
                assert state['status'] == 'failed' and len(state['completed']) == 1
                assert spawn.call_count == 2
                spawn.reset_mock()
                spawn.return_value.wait.side_effect = [0, 0, 0, 0]
                assert run(SimpleNamespace(command='resume', wait_seconds=120)) == 0
                assert spawn.call_count == 4
                assert len(json.loads(path.read_text())['completed']) == 5
                spawn.reset_mock()
                spawn.return_value.wait.side_effect = [0]
                assert run(SimpleNamespace(command='throughput', mode='throughput', wait_seconds=120)) == 0
                assert json.loads(path.read_text())['completed'] == ['throughput']
                assert spawn.call_count == 1
                assert spawn.call_args.args[0][-2:] == ['--mode', 'throughput']
                (root / 'install.sh').write_text('# changed\n')
                try:
                    run(SimpleNamespace(command='resume', wait_seconds=120))
                except RuntimeError as error:
                    assert 'Code changed' in str(error)
                else:
                    raise AssertionError('Resume accepted changed code')
        planned = stages(120)
        assert len(planned) == 5 and planned[0][3] > 120
        assert planned[-1][3] >= 3600
        assert planned[3][2] == ['worker-death', 'stop-file', 'sigterm']
        for mode in ('live', 'throughput'):
            selected = stages(120, mode)
            assert len(selected) == 1 and selected[0][2] == ['--mode', mode]
        print('PASS self-check: fail-fast, resume, code-change guard, state persistence and plan. No real-data tests run.')
        return 0
    if args.command == 'status':
        if not STATE.exists():
            print('Not started.'); return 0
        state = json.loads(STATE.read_text(encoding='utf-8'))
        if state.get('pid') and sys.platform == 'linux':
            import fcntl
            with (REVIEWS / 'remaining_tests.lock').open('r') as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    state['status'] = 'interrupted (runner is no longer active)'
                except BlockingIOError:
                    pass
        print(json.dumps(state, indent=2))
        return 0
    if sys.platform != 'linux':
        parser.error('Run real-data tests inside WSL, not Windows Python.')
    return run(args)


if __name__ == '__main__':
    raise SystemExit(main())
