#!/usr/bin/env python3
"""Idempotently edit xrpld.cfg stanzas. Backs the file up first.

usage: edit_cfg.py CFG --db-path PATH [--history full|pruned] [--node-size huge]
                       [--peer "host port"]...
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
import time
from typing import List

HEADER = re.compile(r"^\[([A-Za-z0-9_]+)\]\s*$")

WS_ADMIN = ["port = 6006", "ip = 127.0.0.1", "admin = 127.0.0.1", "protocol = ws"]


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
    pre, sections = _split(text)
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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cfg")
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--history", choices=("full", "pruned"), default="full")
    ap.add_argument("--node-size", default="huge")
    ap.add_argument("--peer", action="append", default=[])
    a = ap.parse_args(argv)

    try:
        with open(a.cfg, encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        text = ""
    backup = f"{a.cfg}.bak.{int(time.time())}"
    if text:
        shutil.copyfile(a.cfg, backup)
        print(f"backup: {backup}")
    new = apply(text, db_path=a.db_path, history=a.history, node_size=a.node_size, peers=a.peer)
    with open(a.cfg, "w", encoding="utf-8") as f:
        f.write(new)
    print(f"updated {a.cfg} (history={a.history}, node_size={a.node_size}, db={a.db_path})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
