"""Stop switch: a text file whose first line is 'current_process=1' (run) or '=0' (stop),
or SIGTERM (e.g. `systemctl stop`), which this process then treats exactly like the file."""
from __future__ import annotations

import codecs
import logging
import os
import signal
from typing import Any

log = logging.getLogger(__name__)

MAX_READ_BYTES = 4096  # the switch is one short line; never read a huge file

_signalled = False  # set by the SIGTERM handler; per process


def request_stop(signum: int, frame: Any) -> None:
    """SIGTERM handler: finish in-flight work, start nothing new (like current_process=0)."""
    global _signalled
    _signalled = True
    log.warning("signal %s received; stopping after in-flight work", signum)


def stop_signalled() -> bool:
    return _signalled


def install_stop_signal() -> bool:
    """Route SIGTERM to request_stop. Call from the main thread. On Windows the handler can
    be installed, but `kill`/TerminateProcess never runs it: use the stop file there."""
    if not hasattr(signal, "SIGTERM"):
        return False
    signal.signal(signal.SIGTERM, request_stop)
    return True


def first_line(data: bytes) -> str:
    """First line of a small text file in UTF-8 (with or without BOM) or UTF-16 with BOM.

    Windows PowerShell 5.1 writes UTF-16LE for `>` and UTF-8 with BOM for
    `Set-Content -Encoding utf8`; both must read the same as a plain file.
    """
    if data[:2] in (codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE):
        text = data.decode("utf-16", errors="replace")
    else:
        text = data.decode("utf-8-sig", errors="replace")
    lines = text.replace("\x00", "").splitlines()
    return lines[0].strip() if lines else ""


def stop_requested(path: str) -> bool:
    """True after SIGTERM, or when the file explicitly says current_process=0.

    Missing, unreadable or malformed files mean "keep running" (same as legacy behaviour).
    """
    if _signalled:
        return True
    try:
        if not os.path.exists(path):
            return False
        with open(path, "rb") as f:
            line = first_line(f.read(MAX_READ_BYTES))
    except OSError:
        return False
    return line.replace(" ", "") == "current_process=0"
