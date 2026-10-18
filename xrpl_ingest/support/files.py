"""Files people edit: .env lookup + base dir, BOM/UTF-16-safe decoding, and
backfill_range.txt (the default backfill range).

textfile.py (merged): Small text files written by people and Windows tools: .env, stop file, range file.

Windows PowerShell 5.1 writes UTF-16LE with BOM for `>` / Out-File and UTF-8 with BOM for
`Set-Content -Encoding utf8`; Notepad may write either. All of them must read the same as
a plain UTF-8 file.

paths.py (merged): Where things are: the base directory, the .env file, and relative paths.

Rules (see README):
- `.env` is `ENV_FILE` if set, else `<XRPL_INGEST_HOME or cwd>/.env`. No walking up the tree.
- base dir = `XRPL_INGEST_HOME` (process env, or set inside the .env), else the directory of
  the loaded .env, else the current directory. Relative paths in the config (stop file, range
  file, log file, lock files, MONGO_URI_FILE) resolve against it.
- Process environment variables win over values in the .env file.

rangefile.py (merged): backfill_range.txt: the default range for `backfill` (and `status` progress).

    # used by `backfill` when neither --start nor --end is given
    start = 32570
    end   = latest      # or a ledger number

One `key = value` per line; blank and `#` lines ignored, inline ` #` comments allowed; keys
case-insensitive, only `start` and `end`. start: digits (underscores allowed) or `first`
(= 32570), >= 32570, default 32570. end: digits or `latest` (default), >= start. Decoded like
.env (UTF-8 / UTF-8-BOM / UTF-16-BOM), at most 4096 bytes. Read once; never written.
"""
from __future__ import annotations

import codecs
import io
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, FrozenSet, List, MutableMapping, Optional

from ..config import FIRST_LEDGER, resolve_path
from ..errors import ConfigError
from ..ledger_ranges import grid_chunks

log = logging.getLogger(__name__)


# ======================================================================================
# textfile: Small text files written by people and Windows tools: .env, stop file, range file.
# ======================================================================================

class TextFileError(ValueError):
    """File too large, or not decodable as UTF-8 / UTF-8-BOM / UTF-16-BOM."""


def decode(data: bytes, *, errors: str = "strict") -> str:
    """Decode by BOM: UTF-16 (LE/BE) with BOM, else UTF-8 with or without BOM.

    The BOM is removed. With errors="strict" undecodable bytes raise TextFileError."""
    try:
        if data[:2] in (codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE):
            return data.decode("utf-16", errors=errors)
        return data.decode("utf-8-sig", errors=errors)
    except UnicodeDecodeError as e:
        raise TextFileError(f"not UTF-8 or UTF-16 with BOM ({e.reason} at byte {e.start})") from None


def read_text(path: str, max_bytes: int) -> str:
    """Whole file decoded with decode(). OSError (missing, directory, permission) is passed
    through; a file over `max_bytes` or with undecodable bytes raises TextFileError."""
    with open(path, "rb") as f:
        data = f.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise TextFileError(f"larger than {max_bytes} bytes")
    return decode(data)


# ======================================================================================
# paths: Where things are: the base directory, the .env file, and relative paths.
# ======================================================================================


ENV_MAX_BYTES = 1024 * 1024


@dataclass(frozen=True)
class EnvFile:
    """Outcome of loading the .env file."""
    path: Optional[str] = None  # absolute path of the file that was loaded; None = none found
    looked_for: str = ""  # where we looked (for messages when nothing was found)
    from_file: FrozenSet[str] = frozenset()  # keys whose effective value came from the file
    overridden: FrozenSet[str] = frozenset()  # keys in the file that the process env overrode
    explicit: bool = False  # path came from ENV_FILE
    values: Dict[str, str] = field(default_factory=dict, repr=False, compare=False)

    def source(self, key: str, environ) -> str:
        """'.env', 'env' (process environment) or 'default' for one variable."""
        if key in self.from_file:
            return ".env"
        if key in environ:
            return "env (overrides .env)" if key in self.overridden else "env"
        return "default"


def _clean(value: Optional[str]) -> str:
    return (value or "").strip().strip("\"'").strip()


def find_env_file(environ, cwd: str) -> tuple:
    """(path to try, explicit). ENV_FILE wins (relative to cwd); else <home or cwd>/.env."""
    explicit = _clean(environ.get("ENV_FILE"))
    if explicit:
        return resolve_path(explicit, cwd), True
    home = _clean(environ.get("XRPL_INGEST_HOME"))
    return resolve_path(os.path.join(home or cwd, ".env"), cwd), False


def load_env(environ: Optional[MutableMapping[str, str]] = None,
             cwd: Optional[str] = None) -> EnvFile:
    """Load the .env into `environ` (default os.environ) without overriding existing keys.

    The file is decoded by BOM (UTF-8, UTF-8-BOM, UTF-16-BOM) and parsed by python-dotenv
    from a text stream. A missing default `.env` is fine; a missing ENV_FILE, an unreadable
    or undecodable file is a ConfigError."""
    from dotenv import dotenv_values  # lazy: config-free imports stay cheap
    environ = os.environ if environ is None else environ
    cwd = cwd or os.getcwd()
    path, explicit = find_env_file(environ, cwd)
    if not os.path.isfile(path):
        if explicit:
            raise ConfigError(f"ENV_FILE {path} does not exist or is not a file")
        return EnvFile(looked_for=path)
    try:
        text = read_text(path, ENV_MAX_BYTES)
    except (OSError, TextFileError) as e:
        raise ConfigError(f"cannot read .env file {path}: {e}") from None
    raw = dotenv_values(stream=io.StringIO(text))
    values = {k: v for k, v in raw.items() if v is not None}
    applied = set()
    for k, v in values.items():
        if k not in environ:
            environ[k] = v
            applied.add(k)
    return EnvFile(path=path, looked_for=path, from_file=frozenset(applied),
                   overridden=frozenset(set(values) - applied), explicit=explicit, values=values)


def base_dir(environ, env_file: EnvFile, cwd: Optional[str] = None) -> str:
    """XRPL_INGEST_HOME, else the loaded .env's directory, else cwd (absolute)."""
    cwd = cwd or os.getcwd()
    home = _clean(environ.get("XRPL_INGEST_HOME"))
    if home:
        return resolve_path(home, cwd)
    if env_file.path:
        return os.path.dirname(env_file.path)
    return os.path.normpath(os.path.abspath(cwd))


def control_chars(text: str) -> List[str]:
    """Control characters in a path (e.g. a stray '\\r' or tab from an edited .env)."""
    return sorted({repr(c) for c in text if ord(c) < 0x20 or ord(c) == 0x7F})


# ======================================================================================
# rangefile: backfill_range.txt: the default range for `backfill` (and `status` progress).
# ======================================================================================


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


def parse_range(text: str, path: str) -> BackfillRange:
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


def load_range(path: str, *, required: bool = False) -> BackfillRange:
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
    return parse_range(text, path)


def choose_range(cli_start: Optional[int], cli_end: Optional[int], default_path: str, *,
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
        return load_range(range_file, required=True)
    return load_range(default_path)


# -- status: progress of the range ------------------------------------------------

def _utc(dt: Any) -> Optional[datetime]:
    if not isinstance(dt, datetime):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)  # pymongo: naive = UTC


def range_progress(store: Any, start: int, end: int, chunk_size: int,
             now: Optional[datetime] = None) -> Dict[str, Any]:
    """Chunk progress of [start, end] from the chunk docs (done = marked chunk docs inside
    the range). Rate from the completed_at of the most recent chunks."""
    from ..store import CHUNK_KIND
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


def format_range_progress(p: Dict[str, Any], now: Optional[datetime] = None) -> str:
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
