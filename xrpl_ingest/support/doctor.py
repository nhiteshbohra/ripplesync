"""`doctor` and `config`: check the installation, configuration, MongoDB and the node.

doctor exit codes: 0 all ok · 1 warnings · 2 config problem (incl. wrong network, bad
credentials) · 4 node or MongoDB unreachable. The worst one wins (2 > 4 > 1 > 0).
Every network operation uses a short (5 s) timeout.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import stat
import sys
from dataclasses import asdict, dataclass, field
from importlib import metadata
from typing import Any, Callable, Dict, List, Optional, Tuple

from pymongo.errors import ExecutionTimeout, OperationFailure, PyMongoError

from .. import __version__
from ..config import ENV_VARS, WINDOWS_MAX_PROCESSES, Config, display_value, redact_mongo_uri
from .monitor import GB, local_free_bytes, mongo_free_bytes
from ..errors import IngestError, NetworkMismatch
from .monitor import FAST_TIMEOUT_S, fast_source, fast_store
from .runcontrol import lock_path, probe
from .files import EnvFile, control_chars
from .runcontrol import COMMAND_KEYS
from ..store import CHUNK_KIND

# Keep in step with requirements.txt / pyproject.toml (a test checks pyproject).
MIN_PYTHON = (3, 10)
REQUIREMENTS: Tuple[Tuple[str, str], ...] = (
    ("pymongo", ">=4.9,<4.11"),
    ("python-dotenv", ">=1.2.2,<2"),
    ("websockets", ">=13,<18"),
)
SUPPORTED_MONGO_MAJORS = (7, 8)  # 9.0 is not supported (pymongo <4.11, see DECISIONS)
SYNCED_STATES = ("full", "proposing", "validating")
FAST_MS = FAST_TIMEOUT_S * 1000

# exit code implied by a status; "fail" carries its own (2 config / 4 unreachable)
_RANK = {0: 0, 1: 1, 4: 2, 2: 3}


@dataclass
class Check:
    name: str
    status: str  # ok | info | warn | fail | skip
    detail: str
    code: int = 0  # 0, 1 (warn), 2 (config) or 4 (unreachable)


@dataclass
class Report:
    checks: List[Check] = field(default_factory=list)

    def add(self, name: str, status: str, detail: str, code: Optional[int] = None) -> Check:
        if code is None:
            code = 1 if status == "warn" else (2 if status == "fail" else 0)
        c = Check(name, status, detail, code)
        self.checks.append(c)
        return c

    def exit_code(self) -> int:
        return max((c.code for c in self.checks), key=lambda c: _RANK[c], default=0)


# -- versions ---------------------------------------------------------------------

def version_tuple(v: str) -> Tuple[int, ...]:
    """'4.10.1' -> (4, 10, 1); '16.0rc1' -> (16, 0); non-numeric tails are ignored."""
    out = []
    for part in v.split("."):
        m = re.match(r"\d+", part)
        if not m:
            break
        out.append(int(m.group()))
        if m.end() != len(part):
            break
    return tuple(out)


def satisfies(version: str, spec: str) -> bool:
    """Tiny subset of PEP 440 specifiers: comma-separated >=, >, <=, <, ==."""
    v = version_tuple(version)
    for clause in (c.strip() for c in spec.split(",") if c.strip()):
        m = re.fullmatch(r"(>=|<=|==|>|<)\s*([\w.]+)", clause)
        if not m:
            continue
        op, w = m.group(1), version_tuple(m.group(2))
        n = max(len(v), len(w))
        a, b = v + (0,) * (n - len(v)), w + (0,) * (n - len(w))
        ok = {">=": a >= b, "<=": a <= b, "==": a == b, ">": a > b, "<": a < b}[op]
        if not ok:
            return False
    return True


def check_versions(r: Report, version_of: Callable[[str], str] = metadata.version) -> None:
    py = ".".join(map(str, sys.version_info[:3]))
    if sys.version_info[:2] >= MIN_PYTHON:
        r.add("python", "ok", f"{py} (requires >={'.'.join(map(str, MIN_PYTHON))})")
    else:
        r.add("python", "fail", f"{py}; requires >={'.'.join(map(str, MIN_PYTHON))}", 2)
    for name, spec in REQUIREMENTS:
        try:
            v = version_of(name)
        except metadata.PackageNotFoundError:
            r.add(name, "fail", f"not installed; requires {spec} (pip install -r requirements.txt)", 2)
            continue
        if satisfies(v, spec):
            r.add(name, "ok", f"{v} ({spec})")
        else:
            r.add(name, "warn", f"{v} installed; requires {spec} (pip install -r requirements.txt)")


# -- configuration and paths ------------------------------------------------------

def config_rows(cfg: Optional[Config], env: EnvFile, environ: Any, base_dir: str,
                ) -> List[Tuple[str, str, str]]:
    """(variable, effective value, source) for every variable; credentials redacted.
    Without a valid cfg the raw values that are set are shown."""
    rows = [("XRPL_INGEST_HOME", base_dir, env.source("XRPL_INGEST_HOME", environ)
             if "XRPL_INGEST_HOME" in environ else "default (.env dir, else cwd)"),
            ("ENV_FILE", env.path or f"(none; looked for {env.looked_for})",
             "env" if env.explicit else "default")]
    for name, fld in ENV_VARS:
        src = env.source(name, environ)
        if cfg is not None:
            value = display_value(cfg, fld)
            if fld == "mongo_uri" and cfg.mongo_uri_file:
                src = "MONGO_URI_FILE"
        else:
            if name not in environ:
                continue
            value = str(environ[name])
            if name == "MONGO_URI":
                value = redact_mongo_uri(value)
        if control_chars(value):
            value = repr(value)
        rows.append((name, value, src))
    return rows


def format_rows(rows: List[Tuple[str, str, str]]) -> List[str]:
    width = max(len(n) for n, _, _ in rows)
    return [f"{n:<{width}} = {v}    [{s}]" for n, v, s in rows]


def check_paths(r: Report, cfg: Config, env: EnvFile, command_locks=COMMAND_KEYS) -> None:
    named = [("base dir", cfg.base_dir), (".env", env.path or ""),
             ("stop file", cfg.process_status_file), ("range file", cfg.backfill_range_file),
             ("MONGO_URI_FILE", cfg.mongo_uri_file)]
    if cfg.log_file:
        named.append(("log file", cfg.log_file))
    for label, p in named:
        if not p:
            continue
        bad = control_chars(p)
        if bad:
            r.add(f"path {label}", "warn", f"{p!r} contains control characters {', '.join(bad)} "
                  "(stray \\r or tab in the .env?)")
        else:
            r.add(f"path {label}", "info", p)
    if env.path is None:
        r.add("path .env", "info", f"none (looked for {env.looked_for}); using process environment")
    _check_env_perms(r, env.path)
    for cmd in command_locks:
        info = probe(lock_path(cfg.base_dir, cmd))
        if info is None:
            continue
        who = f"pid {info.get('pid', '?')} on {info.get('host', '?')} since {info.get('started', '?')}"
        if info["held"] == "yes":
            r.add(f"lock {cmd}", "info", f"{cmd} is running ({who})")
        elif info["held"] == "no":
            r.add(f"lock {cmd}", "info", f"stale lock file, not held (last: {who}); harmless")
        else:
            r.add(f"lock {cmd}", "warn", f"cannot probe {lock_path(cfg.base_dir, cmd)}: {info['held']}")


def _check_env_perms(r: Report, path: Optional[str]) -> None:
    if not path:
        return
    if os.name != "posix":
        r.add(".env permissions", "info", "not checked on Windows (restrict access with NTFS ACLs)")
        return
    try:
        mode = stat.S_IMODE(os.stat(path).st_mode)
    except OSError as e:
        r.add(".env permissions", "warn", f"cannot stat {path}: {e}")
        return
    if mode & 0o077:
        r.add(".env permissions", "warn", f"{oct(mode)}: readable by group/others; run chmod 600 {path}")
    else:
        r.add(".env permissions", "ok", oct(mode))


# -- host -------------------------------------------------------------------------

def ram_bytes() -> Tuple[Optional[int], Optional[int]]:
    """(total, available) bytes; None where the platform does not tell."""
    try:
        if sys.platform.startswith("linux"):
            vals = {}
            with open("/proc/meminfo", encoding="ascii", errors="replace") as f:
                for line in f:
                    k, _, v = line.partition(":")
                    parts = v.split()
                    if parts and parts[0].isdigit():
                        vals[k] = int(parts[0]) * 1024
            return vals.get("MemTotal"), vals.get("MemAvailable")
        if sys.platform == "win32":
            import ctypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong),
                            ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong),
                            ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
            m = MEMORYSTATUSEX()
            m.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m)):
                return int(m.ullTotalPhys), int(m.ullAvailPhys)
            return None, None
        total = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")  # macOS, BSD
        return (int(total) if total > 0 else None), None
    except (OSError, ValueError, AttributeError):
        return None, None


def check_host(r: Report, cfg: Config) -> None:
    free = local_free_bytes(cfg.base_dir)
    r.add("disk (base dir)", "info", "unknown" if free is None else f"{free / GB:.1f} GB free")
    total, avail = ram_bytes()
    if total is None:
        r.add("ram", "info", "unknown on this platform")
    else:
        text = f"{total / GB:.1f} GB total"
        if avail is not None:
            text += f", {avail / GB:.1f} GB available"
        r.add("ram", "info", text)
    cpus = os.cpu_count() or 1
    detail = f"NUM_PROCESSES={cfg.num_processes}, CONCURRENCY={cfg.concurrency}, CPUs {cpus}"
    if sys.platform == "win32":
        detail += f" (Windows limit {WINDOWS_MAX_PROCESSES})"
    r.add("processes", "ok", detail)


# -- MongoDB ----------------------------------------------------------------------

class _Recorder:
    """Stands in for a pymongo client/db/collection to learn which indexes
    MongoStore.ensure_indexes creates (no server involved)."""

    def __init__(self, sink: List[Tuple[str, Any, Dict[str, Any]]], name: str = ""):
        self._sink, self._name = sink, name

    def __getitem__(self, name: str) -> "_Recorder":
        return _Recorder(self._sink, name)

    def create_index(self, keys: Any, **opts: Any) -> str:
        self._sink.append((self._name, keys, opts))
        return ""

    def __getattr__(self, name: str) -> Callable[..., None]:
        return lambda *a, **k: None


def _key_list(keys: Any) -> List[Tuple[str, int]]:
    if isinstance(keys, str):
        return [(keys, 1)]
    out = []
    for k in keys:
        if isinstance(k, str):
            out.append((k, 1))
        else:
            name, direction = k
            out.append((name, int(direction) if isinstance(direction, (int, float)) else direction))
    return out


def wanted_indexes(cfg: Config) -> List[Tuple[str, List[Tuple[str, Any]], Dict[str, Any]]]:
    """(collection, key, options) for every index ensure_indexes creates."""
    from ..store import MongoStore
    sink: List[Tuple[str, Any, Dict[str, Any]]] = []
    MongoStore(_Recorder(sink), cfg).ensure_indexes()
    return [(coll, _key_list(keys), opts) for coll, keys, opts in sink]


def _index_name(key: List[Tuple[str, Any]]) -> str:
    return "_".join(f"{k}_{d}" for k, d in key)


def check_indexes(r: Report, db: Any, cfg: Config) -> None:
    """Conflicts that would make ensure_indexes fail on an existing (legacy) database:
    an index with the same name/key but other options, or duplicates under a unique index."""
    problems = 0
    for coll_name, key, opts in wanted_indexes(cfg):
        coll = db[coll_name]
        existing = {n: {**i, "key": _key_list(i["key"])} for n, i in coll.index_information().items()}
        name = opts.get("name") or _index_name(key)
        want = (bool(opts.get("unique")), bool(opts.get("sparse")))
        label = f"{coll_name}.{name}"
        same_key = [n for n, i in existing.items() if i["key"] == key]
        if name in existing:
            have = existing[name]
            got = (bool(have.get("unique")), bool(have.get("sparse")))
            if have["key"] != key or got != want:
                problems += 1
                r.add(f"index {label}", "warn",
                      f"exists as key={have['key']} unique={got[0]} sparse={got[1]}, code wants "
                      f"key={key} unique={want[0]} sparse={want[1]}: ensure_indexes will fail "
                      "(IndexKeySpecsConflict); drop or rename that index")
            continue
        if same_key:
            problems += 1
            r.add(f"index {label}", "warn",
                  f"same key already indexed as {same_key[0]!r}: ensure_indexes will fail "
                  "(IndexOptionsConflict, different name); drop that index or rename it")
            continue
        if want[0]:
            problems += _check_duplicates(r, coll, coll_name, key, sparse=want[1])
    if not problems:
        r.add("indexes", "ok", "no conflicts with the indexes this version creates")


def _check_duplicates(r: Report, coll: Any, coll_name: str, key: List[Tuple[str, Any]],
                      sparse: bool) -> int:
    fields = [k for k, _ in key]
    match = {f: {"$exists": True} for f in fields} if sparse else {}
    group_id = {f: f"${f}" for f in fields} if len(fields) > 1 else f"${fields[0]}"
    pipeline = [{"$match": match}, {"$group": {"_id": group_id, "n": {"$sum": 1}}},
                {"$match": {"n": {"$gt": 1}}}, {"$limit": 3}]
    label = f"{coll_name}.{'+'.join(fields)}"
    try:
        dups = list(coll.aggregate(pipeline, maxTimeMS=FAST_MS, allowDiskUse=True))
    except ExecutionTimeout:
        r.add(f"duplicates {label}", "info", f"not checked: more than {FAST_TIMEOUT_S}s on this "
              "collection (ensure_indexes reports duplicates as DuplicateKeyError)")
        return 0
    if dups:
        sample = ", ".join(repr(d["_id"]) for d in dups)
        r.add(f"duplicates {label}", "warn",
              f"duplicate values (e.g. {sample}) block the unique index ensure_indexes creates "
              "(legacy insert_many data?); remove the duplicates first")
        return 1
    return 0


def check_chunk_docs(r: Report, meta: Any, chunk_size: int) -> None:
    """Chunk docs written with another CHUNK_SIZE are never matched by backfill: those ranges
    are fetched again (harmless but slow)."""
    sizes: Dict[int, int] = {}
    misaligned = partial = good = 0
    cur = meta.find({"kind": CHUNK_KIND}, {"start": 1, "end": 1, "_id": 0})
    try:
        cur = cur.max_time_ms(FAST_MS)
    except AttributeError:
        pass
    for d in cur:
        a, b = d.get("start"), d.get("end")
        if not isinstance(a, int) or not isinstance(b, int):
            continue
        n = b - a + 1
        if n == chunk_size and a % chunk_size == 0:
            good += 1
        elif a // chunk_size == b // chunk_size:
            partial += 1  # clipped at a range edge, or a smaller old CHUNK_SIZE
        else:
            misaligned += 1
            sizes[n] = sizes.get(n, 0) + 1
    if misaligned:
        seen = ", ".join(f"{n} ledgers x{c}" for n, c in sorted(sizes.items())[:5])
        r.add("chunk docs", "warn",
              f"{misaligned} chunk docs do not fit CHUNK_SIZE={chunk_size} ({seen}); backfill "
              "fetches those ranges again. Did CHUNK_SIZE change? Set it back to skip them.")
    else:
        r.add("chunk docs", "ok", f"{good} full, {partial} partial (range edges) for "
              f"CHUNK_SIZE={chunk_size}")


def check_mongo(r: Report, cfg: Config, store_factory: Callable[[Config], Any] = fast_store) -> None:
    try:
        store = store_factory(cfg)
    except PyMongoError as e:  # e.g. ConfigurationError for a bad URI option
        r.add("mongo", "fail", f"{type(e).__name__}: {e}", 2)
        return
    try:
        try:
            store.client.admin.command("ping")
        except OperationFailure as e:
            r.add("mongo", "fail", f"{redact_mongo_uri(cfg.mongo_uri)}: {e} (credentials?)", 2)
            return
        except PyMongoError as e:
            r.add("mongo", "fail", f"{redact_mongo_uri(cfg.mongo_uri)} unreachable: "
                  f"{type(e).__name__}: {e}", 4)
            return
        version = str(store.client.server_info().get("version", "?"))
        major = version_tuple(version)[:1]
        if major and major[0] in SUPPORTED_MONGO_MAJORS:
            r.add("mongo", "ok", f"{redact_mongo_uri(cfg.mongo_uri)}: MongoDB {version}")
        else:
            r.add("mongo", "warn", f"MongoDB {version}: supported are 7.0 and 8.0")
        users = (store.client.admin.command("connectionStatus").get("authInfo", {})
                 .get("authenticatedUsers", []))
        if users:
            r.add("mongo auth", "ok", ", ".join(f"{u.get('user')}@{u.get('db')}" for u in users))
        else:
            r.add("mongo auth", "info", "not authenticated (fine only if MongoDB listens on "
                  "localhost / a private network)")
        _check_mongo_disk(r, store, cfg)
        db = store.ledgers.database
        check_indexes(r, db, cfg)
        check_chunk_docs(r, store.meta, cfg.chunk_size)
    except OperationFailure as e:
        r.add("mongo", "fail", f"{type(e).__name__}: {e} (missing privileges?)", 2)
    except PyMongoError as e:
        r.add("mongo", "fail", f"{type(e).__name__}: {e}", 4)
    finally:
        store.close()


def _check_mongo_disk(r: Report, store: Any, cfg: Config) -> None:
    free = mongo_free_bytes(store)
    if free is None:
        r.add("mongo disk", "info", "MongoDB reports no filesystem size")
        return
    detail = f"{free / GB:.1f} GB free on MongoDB's filesystem"
    if cfg.min_free_gb and free < cfg.min_free_gb * GB:
        r.add("mongo disk", "warn", f"{detail}, below MIN_FREE_GB={cfg.min_free_gb:g}: "
              "backfill/tail will refuse to run")
    else:
        r.add("mongo disk", "ok", detail + (f" (MIN_FREE_GB={cfg.min_free_gb:g})"
                                            if cfg.min_free_gb else " (MIN_FREE_GB off)"))


# -- node -------------------------------------------------------------------------

async def check_node(r: Report, cfg: Config,
                     source_factory: Callable[..., Any] = fast_source) -> None:
    source = source_factory(cfg, check_network=False)
    try:
        info = await source.server_info()
    except IngestError as e:
        r.add("node", "fail", f"{type(e).__name__}: {e}", 4)
        return
    finally:
        await source.close()
    state = info.get("server_state", "?")
    r.add("node state", "ok" if state in SYNCED_STATES else "warn",
          f"{state}" + ("" if state in SYNCED_STATES else " (not synced yet)"))
    nid = info.get("network_id")
    expected = cfg.expected_network_id
    if expected is None:
        r.add("node network", "info", f"network_id {nid} (check disabled)")
    elif nid is None:
        r.add("node network", "warn", "network_id not reported; cannot verify mainnet")
    elif nid != expected:
        r.add("node network", "fail", f"network_id {nid} != EXPECTED_NETWORK_ID {expected}", 2)
    else:
        r.add("node network", "ok", f"network_id {nid}")
    complete = info.get("complete_ledgers", "")
    r.add("node ledgers", "warn" if complete in ("", "empty") else "info",
          f"complete_ledgers {complete or 'empty'}")
    seq = (info.get("validated_ledger") or {}).get("seq")
    r.add("node validated", "ok" if seq else "warn", f"seq {seq}" if seq else "no validated ledger")
    peers = info.get("peers")
    r.add("node peers", "warn" if peers == 0 else "info", f"{peers}")
    r.add("node build", "info", str(info.get("build_version", "?")))


# -- entry ------------------------------------------------------------------------

def run_doctor(cfg: Optional[Config], env: EnvFile, environ: Any, base_dir: str, *,
               config_error: Optional[BaseException] = None, node: bool = True,
               mongo: bool = True, as_json: bool = False, out: Callable[[str], None] = print,
               store_factory: Callable[[Config], Any] = fast_store,
               source_factory: Callable[..., Any] = fast_source,
               version_of: Callable[[str], str] = metadata.version) -> int:
    r = Report()
    check_versions(r, version_of)
    if cfg is None:
        r.add("config", "fail", f"{config_error}", 2)
    else:
        r.add("config", "ok", "valid")
        check_paths(r, cfg, env)
        check_host(r, cfg)
        if mongo:
            check_mongo(r, cfg, store_factory)
        if node:
            try:
                asyncio.run(check_node(r, cfg, source_factory))
            except NetworkMismatch as e:  # not expected with check_network=False
                r.add("node", "fail", str(e), 2)
    code = r.exit_code()
    rows = config_rows(cfg, env, environ, base_dir)
    if as_json:
        out(json.dumps({"version": __version__, "exit_code": code,
                        "checks": [asdict(c) for c in r.checks],
                        "config": [{"name": n, "value": v, "source": s} for n, v, s in rows]},
                       indent=2))
        return code
    out(f"xrpl-ingest {__version__} doctor")
    width = max(len(c.name) for c in r.checks)
    for c in r.checks:
        out(f"{c.status.upper():<5} {c.name:<{width}}  {c.detail}")
    out("")
    out("effective configuration:")
    for line in format_rows(rows):
        out(f"  {line}")
    warns = sum(c.status == "warn" for c in r.checks)
    fails = sum(c.status == "fail" for c in r.checks)
    out("")
    out(f"result: {fails} failure(s), {warns} warning(s) -> exit {code}")
    return code


def run_config(cfg: Optional[Config], env: EnvFile, environ: Any, base_dir: str, *,
               config_error: Optional[BaseException] = None,
               out: Callable[[str], None] = print) -> int:
    for line in format_rows(config_rows(cfg, env, environ, base_dir)):
        out(line)
    if config_error is not None:
        out(f"config error: {config_error}")
        return 2
    return 0
