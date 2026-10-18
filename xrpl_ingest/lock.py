"""One running instance per command: an OS file lock on <base>/xrpl_ingest.<command>.lock.

POSIX: fcntl.flock(LOCK_EX | LOCK_NB) on the whole file. Windows: msvcrt.locking(LK_NBLCK)
on one byte far past the end, so the holder info written at the start stays readable for
the process that is refused. The OS drops the lock when the holder exits or dies, so a
leftover file is harmless (it only keeps the last holder's info); it is never deleted.
"""
from __future__ import annotations

import os
import socket
import sys
from datetime import datetime, timezone
from typing import Dict, Optional

from .errors import IngestError

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
    """None if no lock file; else holder info plus held='yes'/'no' (doctor). Taking the
    lock for a moment can make a command starting in that instant exit 5; rerun it."""
    if not os.path.exists(path):
        return None
    info = read_info(path)
    lk = InstanceLock(path)
    try:
        lk.acquire(write_info=False)
    except LockHeld:
        return {**info, "held": "yes"}
    except OSError as e:
        return {**info, "held": f"unknown ({e})"}
    lk.release()
    return {**info, "held": "no"}
