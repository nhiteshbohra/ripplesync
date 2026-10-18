"""backfill_range.txt: the default range for `backfill` (and `status` progress).

    # used by `backfill` when neither --start nor --end is given
    start = 32570
    end   = latest      # or a ledger number

One `key = value` per line; blank and `#` lines ignored, inline ` #` comments allowed; keys
case-insensitive, only `start` and `end`. start: digits (underscores allowed) or `first`
(= 32570), >= 32570, default 32570. end: digits or `latest` (default), >= start. Decoded like
.env (UTF-8 / UTF-8-BOM / UTF-16-BOM), at most 4096 bytes. Read once; never written.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .constants import FIRST_LEDGER
from .errors import ConfigError
from .ledger_ranges import grid_chunks
from .textfile import TextFileError, read_text

log = logging.getLogger(__name__)

MAX_BYTES = 4096
_NUMBER = re.compile(r"\d+(?:_\d+)*")
RATE_WINDOW = 200  # most recent chunk completions used for the status rate


@dataclass(frozen=True)
class BackfillRange:
    start: int = FIRST_LEDGER
    end: Optional[int] = None  # None = node's latest validated ledger
    source: str = "defaults"  # "command line", "file <path>", "defaults"

    def describe(self) -> str:
        return f"{self.start}-{'latest' if self.end is None else self.end} ({self.source})"


def _number(value: str, words: Dict[str, Optional[int]], where: str, key: str) -> Optional[int]:
    low = value.lower()
    if low in words:
        return words[low]
    if not _NUMBER.fullmatch(value):
        allowed = " or ".join(f"'{w}'" for w in words)
        raise ConfigError(f"{where}: {key} = {value!r} must be a ledger number or {allowed}")
    return int(value.replace("_", ""))


def parse(text: str, path: str) -> BackfillRange:
    """Parse range-file text; errors name `path` and the line number."""
    seen: Dict[str, Any] = {}
    for no, raw in enumerate(text.splitlines(), 1):
        line = re.split(r"\s#", raw, maxsplit=1)[0].strip()
        if not line or line.startswith("#"):
            continue
        where = f"{path}:{no}"
        if "=" not in line:
            raise ConfigError(f"{where}: expected 'key = value', got {raw.strip()!r}")
        key, _, value = (p.strip() for p in line.partition("="))
        key = key.lower()
        if key not in ("start", "end"):
            raise ConfigError(f"{where}: unknown key {key!r} (only 'start' and 'end')")
        if key in seen:
            raise ConfigError(f"{where}: duplicate key {key!r} (first on line {seen[key][1]})")
        if key == "start":
            v = _number(value, {"first": FIRST_LEDGER}, where, key)
            if v < FIRST_LEDGER:
                raise ConfigError(f"{where}: start = {v} must be >= {FIRST_LEDGER} (first ledger)")
        else:
            v = _number(value, {"latest": None}, where, key)
        seen[key] = (v, no)
    start = seen.get("start", (FIRST_LEDGER, 0))[0]
    end = seen.get("end", (None, 0))[0]
    if end is not None and end < start:
        raise ConfigError(f"{path}:{seen['end'][1]}: end = {end} must be >= start ({start})")
    return BackfillRange(start, end, f"file {path}")


def load(path: str, *, required: bool = False) -> BackfillRange:
    """The file's range; defaults if it does not exist (unless `required`)."""
    if not os.path.exists(path):
        if required:
            raise ConfigError(f"range file {path} does not exist")
        log.info("range file %s not found; using defaults (start %d, end latest)", path, FIRST_LEDGER)
        return BackfillRange()
    try:
        text = read_text(path, MAX_BYTES)
    except (OSError, TextFileError) as e:
        raise ConfigError(f"range file {path}: cannot read ({type(e).__name__}: {e})") from None
    return parse(text, path)


def choose(cli_start: Optional[int], cli_end: Optional[int], default_path: str, *,
           range_file: Optional[str] = None, use_file: bool = True) -> BackfillRange:
    """Command line (either --start or --end given: file ignored entirely) > file > defaults.
    `range_file` is an explicit --range-file (must exist); else `default_path` (optional)."""
    if cli_start is not None or cli_end is not None:
        if range_file:
            raise ConfigError("--range-file cannot be combined with --start/--end")
        if use_file:
            log.info("--start/--end given; range file ignored")
        return BackfillRange(FIRST_LEDGER if cli_start is None else cli_start, cli_end,
                             "command line")
    if not use_file:
        return BackfillRange(source="defaults (--no-range-file)")
    if range_file:
        return load(range_file, required=True)
    return load(default_path)


# -- status: progress of the range ------------------------------------------------

def _utc(dt: Any) -> Optional[datetime]:
    if not isinstance(dt, datetime):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)  # pymongo: naive = UTC


def progress(store: Any, start: int, end: int, chunk_size: int,
             now: Optional[datetime] = None) -> Dict[str, Any]:
    """Chunk progress of [start, end] from the chunk docs (done = marked chunk docs inside
    the range). Rate from the completed_at of the most recent chunks."""
    from .store import CHUNK_KIND
    now = now or datetime.now(timezone.utc)
    total_chunks = len(grid_chunks(start, end, chunk_size)) if end >= start else 0
    q = {"kind": CHUNK_KIND, "start": {"$gte": start}, "end": {"$lte": end}}
    docs = list(store.meta.find(q, {"start": 1, "end": 1, "completed_at": 1, "_id": 0}))
    done_ledgers = sum(d["end"] - d["start"] + 1 for d in docs)
    failed = store.failed.count_documents({"block_number": {"$gte": start, "$lte": end}})
    out: Dict[str, Any] = {
        "start": start, "end": end, "chunks_done": len(docs), "chunks_total": total_chunks,
        "ledgers_done": done_ledgers, "ledgers_total": max(0, end - start + 1),
        "failed_ledgers": failed, "rate": None, "eta_seconds": None, "last_completed": None,
    }
    timed = sorted((t, d["end"] - d["start"] + 1) for d in docs
                   if (t := _utc(d.get("completed_at"))) is not None)
    if timed:
        out["last_completed"] = timed[-1][0]
    recent = timed[-RATE_WINDOW:]
    if len(recent) >= 2:
        span = (recent[-1][0] - recent[0][0]).total_seconds()
        if span > 0:
            out["rate"] = sum(n for _, n in recent[1:]) / span
    left = out["ledgers_total"] - done_ledgers
    if out["rate"] and left > 0:
        out["eta_seconds"] = left / out["rate"]
    return out


def format_progress(p: Dict[str, Any], now: Optional[datetime] = None) -> str:
    from .logsetup import format_duration
    now = now or datetime.now(timezone.utc)
    pct = 100.0 * p["chunks_done"] / p["chunks_total"] if p["chunks_total"] else 100.0
    parts: List[str] = [f"chunks {p['chunks_done']}/{p['chunks_total']} ({pct:.1f}%)",
                        f"failed ledgers {p['failed_ledgers']}"]
    if p["rate"]:
        parts.append(f"{p['rate']:.1f} ledgers/s" if p["rate"] < 10 else f"{p['rate']:.0f} ledgers/s")
    if p["last_completed"] is not None:
        ago = (now - p["last_completed"]).total_seconds()
        parts.append(f"last chunk {format_duration(ago)} ago")
    if p["eta_seconds"] is not None:
        parts.append(f"ETA {format_duration(p['eta_seconds'])} (at that rate)")
    return ", ".join(parts)
