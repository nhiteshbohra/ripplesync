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


def ws_url(text: str) -> str:
    """Read the local WebSocket stanza managed by this installer without changing it."""
    stanzas = dict(_split(text)[1])
    if "port_ws_admin_local" not in [s.strip() for s in stanzas.get("server", [])]:
        raise ValueError("[server] does not enable port_ws_admin_local; set NODE_WS explicitly")
    values = {}
    for line in stanzas.get("port_ws_admin_local", []):
        key, sep, value = line.split("#", 1)[0].partition("=")
        if sep:
            values[key.strip()] = value.strip()
    try:
        port = int(values.get("port", ""))
        if not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        raise ValueError("invalid WebSocket port; set NODE_WS explicitly") from None
    protocols = [p.strip() for p in values.get("protocol", "").split(",")]
    scheme = "ws" if "ws" in protocols else "wss" if "wss" in protocols else None
    host = values.get("ip", "")
    if not scheme or not host:
        raise ValueError("missing WebSocket protocol/ip; set NODE_WS explicitly")
    host = {"0.0.0.0": "127.0.0.1", "::": "::1"}.get(host, host)
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"{scheme}://{host}:{port}"


def backup(path: str, *, private: bool = False):
    """Reserve a unique backup name, including for repeated edits in the same second."""
    if not os.path.exists(path):
        return None
    fd, dst = tempfile.mkstemp(prefix=os.path.basename(path) + ".bak.",
                               dir=os.path.dirname(os.path.abspath(path)))
    os.close(fd)
    try:
        shutil.copyfile(path, dst)
        if private:
            os.chmod(dst, 0o600)
        else:
            shutil.copystat(path, dst)
        return dst
    except BaseException:
        os.unlink(dst)
        raise


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
    ap.add_argument("--db-path")
    ap.add_argument("--ws-url", action="store_true", help="only print the configured WebSocket URL")
    ap.add_argument("--history", choices=("full", "pruned"), default="full")
    ap.add_argument("--node-size", choices=NODE_SIZES, default="huge")
    ap.add_argument("--peer", action="append", default=[])
    a = ap.parse_args(argv)

    if not a.ws_url and a.db_path is None:
        ap.error("--db-path is required when editing")

    try:
        with open(a.cfg, encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        text = ""
    if a.ws_url:
        try:
            print(ws_url(text))
        except ValueError as exc:
            ap.error(str(exc))
        return 0
    saved = backup(a.cfg)
    if saved:
        print(f"backup: {saved}")
    new = apply(text, db_path=a.db_path, history=a.history, node_size=a.node_size, peers=a.peer)
    os.makedirs(os.path.dirname(os.path.abspath(a.cfg)), exist_ok=True)
    write_atomic(a.cfg, new)
    print(f"updated {a.cfg} (history={a.history}, node_size={a.node_size}, db={a.db_path})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
