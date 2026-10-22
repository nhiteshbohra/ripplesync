"""Run control: graceful stop (stop file + signals) and one instance per command (lock).

stopflag.py (merged): Graceful stop: the stop file (process_status.txt) and signals.

Stop file: any line whose key matters to this command and whose value is 0 stops it; keys
are case-insensitive. `current_process=0` stops every command, `tail=0`, `backfill=0` and
`retry-failed=0` stop only that command; any other value (e.g. `=1`) means keep running.
Blank lines, `#` comments and other keys are skipped. A missing or unreadable file means
keep running (legacy behaviour).

Signals: SIGTERM (e.g. `systemctl stop`) and SIGBREAK (Windows Ctrl+Break) act like the
stop file. With install_stop_signal(graceful_sigint=True) the first Ctrl+C (SIGINT) does too
and the second one aborts at once (KeyboardInterrupt, exit 130).

lock.py (merged): One running instance per command: an OS file lock on <base>/xrpl_ingest.<command>.lock.

POSIX: fcntl.flock(LOCK_EX | LOCK_NB) on the whole file. Windows: msvcrt.locking(LK_NBLCK)
on one byte far past the end, so the holder info written at the start stays readable for
the process that is refused. The OS drops the lock when the holder exits or dies, so a
leftover file is harmless (it only keeps the last holder's info); it is never deleted.
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional

from ..errors import IngestError
from .files import decode

log = logging.getLogger(__name__)


# ======================================================================================
# stopflag: Graceful stop: the stop file (process_status.txt) and signals.
# ======================================================================================


MAX_READ_BYTES = 1024 * 1024  # bounded, but large enough for files edited by external tools
CHECK_INTERVAL = 1.0  # StopFlag reads the file at most this often (seconds)
ALL_KEY = "current_process"
COMMAND_KEYS = ("tail", "backfill", "retry-failed")

_signalled = False  # set by the signal handlers; per process
_interrupts = 0  # SIGINTs seen by the graceful handler


def request_stop(signum: int, frame: Any) -> None:
    """SIGTERM / SIGBREAK handler: finish in-flight work, start nothing new."""
    global _signalled
    _signalled = True
    log.warning("signal %s received; stopping after in-flight work", signum)


def _graceful_sigint(signum: int, frame: Any) -> None:
    """First Ctrl+C = graceful stop; the second one aborts (KeyboardInterrupt)."""
    global _signalled, _interrupts
    _interrupts += 1
    if _interrupts > 1:
        raise KeyboardInterrupt
    _signalled = True
    log.warning("Ctrl+C: stopping after in-flight work; press Ctrl+C again to abort now")


def stop_signalled() -> bool:
    return _signalled


def install_stop_signal(*, graceful_sigint: bool = False) -> bool:
    """Route SIGTERM and SIGBREAK (Windows) to request_stop, and SIGINT to the graceful
    two-step handler if asked. Main thread only. On Windows `kill`/TerminateProcess never
    runs a handler: use the stop file or Ctrl+C / Ctrl+Break there."""
    installed = False
    for name in ("SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            signal.signal(sig, request_stop)
            installed = True
    if graceful_sigint:
        signal.signal(signal.SIGINT, _graceful_sigint)
    return installed


def ignore_sigint() -> None:
    """Pool workers: Ctrl+C reaches the whole console/process group; only the parent acts
    on it and tells the workers through the shared stop event."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def text_lines(data: bytes) -> List[str]:
    """Lines of a small text file in UTF-8 (with or without BOM) or UTF-16 with BOM."""
    return decode(data, errors="replace").replace("\x00", "").splitlines()


def first_line(data: bytes) -> str:
    lines = text_lines(data)
    return lines[0].strip() if lines else ""


def _keys(command: Optional[str]) -> set:
    keys = {ALL_KEY}
    if command:
        keys.add(command.lower())
    return keys


def says_stop(lines: Iterable[str], command: Optional[str] = None) -> bool:
    """True if any line with a key that applies (current_process, or `command`) is `=0`.

    Any-zero-wins, so a leftover `current_process=1` line cannot hide a `tail=0` below it."""
    keys = _keys(command)
    for raw in lines:
        line = raw.split(" #", 1)[0].split("\t#", 1)[0].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key.strip().lower() in keys and value.strip() == "0":
            return True
    return False


def _file_says_stop(path: str, command: Optional[str]) -> bool:
    try:
        if not os.path.exists(path):
            return False
        with open(path, "rb") as f:
            return says_stop(text_lines(f.read(MAX_READ_BYTES)), command)
    except OSError:
        return False


def stop_requested(path: str, command: Optional[str] = None) -> bool:
    """True after a stop signal, or when the file says stop (for `command`, if given)."""
    if _signalled:
        return True
    return _file_says_stop(path, command)


class StopFlag:
    """Callable stop check for one command: signals and `event` every call, the file at
    most once per `interval` seconds (called per ledger, i.e. thousands of times a second).
    Once it said stop it keeps saying stop."""

    def __init__(self, path: str, command: Optional[str] = None, *,
                 interval: Optional[float] = None, event: Any = None,
                 clock: Callable[[], float] = time.monotonic):
        self.path = path
        self.command = command
        self.interval = CHECK_INTERVAL if interval is None else interval
        self.event = event  # e.g. multiprocessing.Event shared with pool workers
        self._clock = clock
        self._next_read: Optional[float] = None
        self._stopped = False

    def __call__(self) -> bool:
        if self._stopped:
            return True
        if _signalled or (self.event is not None and self.event.is_set()):
            self._stopped = True
            return True
        now = self._clock()
        if self._next_read is None or now >= self._next_read:
            self._next_read = now + self.interval
            if _file_says_stop(self.path, self.command):
                self._stopped = True
                log.warning("stop file %s says stop%s", self.path,
                            f" ({self.command})" if self.command else "")
        return self._stopped


def any_of(*checks: Callable[[], bool]) -> Callable[[], bool]:
    """A stop check that is true when any of `checks` is (evaluated in order)."""
    def check() -> bool:
        return any(c() for c in checks)
    return check


# ======================================================================================
# lock: One running instance per command: an OS file lock on <base>/xrpl_ingest.<command>.lock.
# ======================================================================================

_WIN_LOCK_OFFSET = 1 << 30  # byte locked on Windows; beyond any holder-info text
INFO_MAX_BYTES = 4096


class LockHeld(IngestError):
    """Another process holds the lock (CLI exit code 5)."""

    def __init__(self, path: str, holder: Dict[str, str]):
        self.path = path
        self.holder = holder
        who = ", ".join(f"{k} {holder[k]}" for k in ("pid", "host", "started") if holder.get(k))
        super().__init__(f"another instance is already running ({who or 'holder unknown'}); "
                         f"lock file {path}")


def lock_path(base_dir: str, command: str) -> str:
    return os.path.join(base_dir, f"xrpl_ingest.{command}.lock")


def _try_lock(fd: int) -> bool:
    if sys.platform == "win32":
        import msvcrt
        os.lseek(fd, _WIN_LOCK_OFFSET, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True
    import fcntl
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:  # BlockingIOError: EWOULDBLOCK / EAGAIN
        return False
    return True


def _unlock(fd: int) -> None:
    if sys.platform == "win32":
        import msvcrt
        os.lseek(fd, _WIN_LOCK_OFFSET, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        import fcntl
        fcntl.flock(fd, fcntl.LOCK_UN)


def parse_info(text: str) -> Dict[str, str]:
    out = {}
    for line in text.splitlines():
        k, sep, v = line.partition("=")
        if sep:
            out[k.strip()] = v.strip()
    return out


def read_info(path: str) -> Dict[str, str]:
    """Holder info of a lock file ({} when missing/unreadable)."""
    try:
        with open(path, "rb") as f:
            return parse_info(f.read(INFO_MAX_BYTES).decode("utf-8", errors="replace"))
    except OSError:
        return {}


class InstanceLock:
    """Non-blocking exclusive lock. acquire() raises LockHeld if another process has it;
    OSError (e.g. directory not writable) is passed through."""

    def __init__(self, path: str, command: str = ""):
        self.path = path
        self.command = command
        self._fd: Optional[int] = None

    def acquire(self, *, write_info: bool = True) -> "InstanceLock":
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        if not _try_lock(fd):
            os.close(fd)
            raise LockHeld(self.path, read_info(self.path))
        self._fd = fd
        if not write_info:
            return self
        info = (f"pid={os.getpid()}\nhost={socket.gethostname()}\n"
                f"started={datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
                f"command={self.command}\n")
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            os.ftruncate(fd, 0)
            os.write(fd, info.encode("utf-8"))
        except OSError:
            pass  # the lock itself is what matters; info is only for the message
        return self

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            _unlock(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

    @property
    def held(self) -> bool:
        return self._fd is not None

    def __enter__(self) -> "InstanceLock":
        return self.acquire()

    def __exit__(self, *exc) -> None:
        self.release()


def probe(path: str) -> Optional[Dict[str, str]]:
    """Read last-holder metadata without competing for the ingestion lock.

    File presence and recorded PID cannot establish current OS lock ownership.
    """
    if not os.path.exists(path):
        return None
    return {**read_info(path), "held": "unknown (read-only inspection)"}
