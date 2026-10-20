import os
import subprocess
import sys
import textwrap
import time

import pytest

from xrpl_ingest.support.runcontrol import InstanceLock, LockHeld, lock_path, parse_info, probe, read_info

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_lock_path_is_per_command(tmp_path):
    assert lock_path(str(tmp_path), "retry-failed") == str(tmp_path / "xrpl_ingest.retry-failed.lock")


def test_second_lock_refused_with_holder_info_then_free_after_release(tmp_path):
    p = lock_path(str(tmp_path), "tail")
    first = InstanceLock(p, "tail").acquire()
    with pytest.raises(LockHeld) as e:
        InstanceLock(p, "tail").acquire()
    assert e.value.holder["pid"] == str(os.getpid()) and e.value.holder["command"] == "tail"
    assert f"pid {os.getpid()}" in str(e.value) and "started 20" in str(e.value) and p in str(e.value)
    first.release()
    with InstanceLock(p, "tail") as again:
        assert again.held
    assert not again.held and os.path.exists(p)  # file stays; only the OS lock matters


def test_release_is_idempotent(tmp_path):
    lk = InstanceLock(lock_path(str(tmp_path), "tail")).acquire()
    lk.release()
    lk.release()


def test_lock_held_by_another_process_and_freed_when_it_dies(tmp_path):
    p = lock_path(str(tmp_path), "backfill")
    code = textwrap.dedent(f"""
        import os, sys, time
        sys.path.insert(0, {ROOT!r})
        from xrpl_ingest.support.runcontrol import InstanceLock
        InstanceLock({p!r}, "backfill").acquire()
        print("locked", os.getpid(), flush=True)
        time.sleep(60)
    """)
    child = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
    try:
        word, pid = child.stdout.readline().split()
        assert word == "locked"
        with pytest.raises(LockHeld) as e:
            InstanceLock(p).acquire()
        # The interpreter's own pid: in a Windows venv, child.pid is the venv launcher, which
        # starts the real python.exe as its child.
        assert e.value.holder["pid"] == pid
        assert probe(p)["held"] == "yes"
    finally:
        child.kill()  # like a crash: no cleanup code runs
        child.wait(30)
    # The OS releases the lock when the process is gone. In a Windows venv the real python.exe
    # is ended a moment after its launcher, so allow a few seconds.
    deadline = time.monotonic() + 10
    while True:
        try:
            with InstanceLock(p):  # the OS released it
                break
        except LockHeld:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)
    info = probe(p)
    assert info["held"] == "no" and info["pid"] == str(os.getpid())  # last holder: us


def test_probe_does_not_overwrite_holder_info(tmp_path):
    p = lock_path(str(tmp_path), "tail")
    with open(p, "w") as f:
        f.write("pid=123\nhost=old\nstarted=2026-01-01T00:00:00Z\n")
    info = probe(p)
    assert info == {"pid": "123", "host": "old", "started": "2026-01-01T00:00:00Z", "held": "no"}
    assert read_info(p)["pid"] == "123"
    assert probe(str(tmp_path / "none.lock")) is None


def test_parse_info_ignores_junk():
    assert parse_info("pid=1\njunk\nhost = h\n") == {"pid": "1", "host": "h"}
