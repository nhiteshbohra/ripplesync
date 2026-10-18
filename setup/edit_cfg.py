#!/usr/bin/env python3
"""Idempotently edit xrpld.cfg stanzas. Backs the file up first, writes atomically.

usage: edit_cfg.py CFG --db-path PATH [--history full|pruned]
                       [--node-size tiny|small|medium|large|huge]
                       [--peer "host port"]...
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import tempfile
import time
from typing import List

HEADER = re.compile(r"^\[([A-Za-z0-9_]+)\]\s*$")

NODE_SIZES = ("tiny", "small", "medium", "large", "huge")

# Same as the [port_ws_admin_local] stanza of the xrpld 3.4.1 example cfg (send_queue_limit
# included); only used when the stanza is missing, an existing one is never changed.
WS_ADMIN = ["port = 6006", "ip = 127.0.0.1", "admin = 127.0.0.1", "protocol = ws",
            "send_queue_limit = 500"]


def _split(text: str):
    """-> (preamble_lines, [(name, body_lines)])"""
    pre: List[str] = []
    sections = []
    for line in text.splitlines():
        m = HEADER.match(line.strip())
        if m:
            sections.append([m.group(1), []])
        elif sections:
            sections[-1][1].append(line)
        else:
            pre.append(line)
    return pre, sections


def _join(pre, sections) -> str:
    out = list(pre)
    for name, body in sections:
        while body and not body[-1].strip():
            body = body[:-1]
        out += [f"[{name}]", *body, ""]
    return "\n".join(out).rstrip("\n") + "\n"


def set_stanza(text: str, name: str, body: List[str]) -> str:
    pre, sections = _split(text)
    for s in sections:
        if s[0] == name:
            s[1] = list(body)
            return _join(pre, sections)
    sections.append([name, list(body)])
    return _join(pre, sections)


def remove_stanza(text: str, name: str) -> str:
    pre, sections = _split(text)
    return _join(pre, [s for s in sections if s[0] != name])


def ensure_stanza(text: str, name: str, body: List[str]) -> str:
    _, sections = _split(text)
    if any(s[0] == name for s in sections):
        return text
    return set_stanza(text, name, body)


def add_line(text: str, name: str, line: str) -> str:
    """Add `line` to stanza `name` (created if missing) unless already present."""
    pre, sections = _split(text)
    for s in sections:
        if s[0] == name:
            if line not in [b.strip() for b in s[1]]:
                s[1] = [b for b in s[1] if b.strip()] + [line]
            return _join(pre, sections)
    sections.append([name, [line]])
    return _join(pre, sections)


def apply(text: str, *, db_path: str, history: str, node_size: str, peers: List[str]) -> str:
    node_db = ["type=NuDB", f"path={db_path}"]
    if history == "pruned":
        node_db += ["online_delete=2000", "advisory_delete=0"]  # keeps ~2000 ledgers only
        text = remove_stanza(text, "ledger_history")
    else:
        text = set_stanza(text, "ledger_history", ["full"])  # no online_delete: keep everything
    text = set_stanza(text, "node_db", node_db)
    text = set_stanza(text, "node_size", [node_size])
    text = set_stanza(text, "rpc_startup", ['{ "command": "log_level", "severity": "warning" }'])
    text = ensure_stanza(text, "port_ws_admin_local", WS_ADMIN)
    text = add_line(text, "server", "port_ws_admin_local")
    for p in peers:
        text = add_line(text, "ips_fixed", p)
    return text


def write_atomic(path: str, text: str) -> None:
    """Write via a temp file in the same dir + os.replace; keep the original mode and owner.

    A symlinked cfg is written through to its target, so the link itself survives.
    """
    path = os.path.realpath(path)
    try:
        st = os.stat(path)
    except FileNotFoundError:
        st = None
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(path)), prefix=".edit_cfg.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, (st.st_mode & 0o7777) if st else 0o644)
        if st and hasattr(os, "chown"):
            try:
                os.chown(tmp, st.st_uid, st.st_gid)
            except PermissionError:
                pass  # not root: the file stays owned by us
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cfg")
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--history", choices=("full", "pruned"), default="full")
    ap.add_argument("--node-size", choices=NODE_SIZES, default="huge")
    ap.add_argument("--peer", action="append", default=[])
    a = ap.parse_args(argv)

    try:
        with open(a.cfg, encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        text = ""
    backup = f"{a.cfg}.bak.{int(time.time())}"
    if text:
        shutil.copy2(a.cfg, backup)  # keeps mode/timestamps
        print(f"backup: {backup}")
    new = apply(text, db_path=a.db_path, history=a.history, node_size=a.node_size, peers=a.peer)
    os.makedirs(os.path.dirname(os.path.abspath(a.cfg)), exist_ok=True)
    write_atomic(a.cfg, new)
    print(f"updated {a.cfg} (history={a.history}, node_size={a.node_size}, db={a.db_path})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
