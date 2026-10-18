"""Graceful stop: the stop file (process_status.txt) and signals.

Stop file: the first line whose key matters to this command decides; keys are
case-insensitive. `current_process=0` stops every command, `tail=0`, `backfill=0` and
`retry-failed=0` stop only that command; any other value (e.g. `=1`) means keep running.
Blank lines, `#` comments and other keys are skipped. A missing or unreadable file means
keep running (legacy behaviour).

Signals: SIGTERM (e.g. `systemctl stop`) and SIGBREAK (Windows Ctrl+Break) act like the
stop file. With install_stop_signal(graceful_sigint=True) the first Ctrl+C (SIGINT) does too
and the second one aborts at once (KeyboardInterrupt, exit 130).
"""
from __future__ import annotations

import logging
import os
import signal
import time
from typing import Any, Callable, Iterable, List, Optional

from .textfile import decode

log = logging.getLogger(__name__)

MAX_READ_BYTES = 4096  # the switch is a few short lines; never read a huge file
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
    """First line with a key that applies (current_process, or `command`) decides."""
    keys = _keys(command)
    for raw in lines:
        line = raw.split(" #", 1)[0].split("\t#", 1)[0].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key.strip().lower() in keys:
            return value.strip() == "0"
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
