"""Stop switch: a text file whose first line is 'current_process=1' (run) or '=0' (stop)."""
from __future__ import annotations

import os


def stop_requested(path: str) -> bool:
    """True only when the file explicitly says current_process=0.

    Missing, unreadable or malformed files mean "keep running" (same as legacy behaviour).
    """
    try:
        if not os.path.exists(path):
            return False
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            line = f.readline().strip()
    except OSError:
        return False
    return line.replace(" ", "") == "current_process=0"
