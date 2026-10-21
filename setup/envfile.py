#!/usr/bin/env python3
"""Set KEY=VALUE lines in a .env file without touching anything else. Used by install.sh / install.ps1.

usage: envfile.py ENV_PATH [--example .env.example] KEY=VALUE ...    set values (file created from the example)
       envfile.py ENV_PATH --has KEY                                  exit 0 if KEY has a non-empty value

Comments, other keys and their order stay as they are. An existing key is changed in place
(every uncommented copy, so the last one - the one python-dotenv uses - is right too);
a missing key goes under its commented example line (`# KEY=...`) or at the end.
Before a changed existing file is rewritten, a unique ENV_PATH.bak.* copy is kept.
Values are never printed (MONGO_URI may hold a password). POSIX: the file is chmod 600.
"""
from __future__ import annotations

import argparse
import codecs
import os
import re
import sys
import tempfile
from typing import Dict, List, Optional, Tuple

from edit_cfg import backup

KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def read_text(path: str) -> str:
    # Standalone bootstrap helper: no application/dependency imports before installation.
    with open(path, "rb") as f:
        data = f.read()
    encoding = "utf-16" if data[:2] in (codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE) else "utf-8-sig"
    return data.decode(encoding)


def quote(value: str) -> str:
    """Value as python-dotenv reads it back unchanged; quoted only when needed."""
    if "\n" in value or "\r" in value:
        raise ValueError("a value cannot contain a line break")
    plain = value == value.strip() and " #" not in value and not value.startswith(("'", '"', "#"))
    if plain:
        return value
    if "'" not in value:
        return "'" + value.replace("\\", "\\\\") + "'"
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _line_key(line: str) -> Tuple[Optional[str], bool]:
    """(key, commented) for `KEY=...`, `export KEY=...` or `# KEY=...`; (None, False) otherwise."""
    s = line.strip()
    commented = s.startswith("#")
    if commented:
        s = s[1:].strip()
    if s.startswith("export "):
        s = s[len("export "):].strip()
    key, sep, _ = s.partition("=")
    key = key.strip()
    if not sep or not KEY_RE.match(key):
        return None, False
    return key, commented


def set_values(text: str, values: Dict[str, str]) -> str:
    """Return text with every key in values set; everything else unchanged."""
    lines = text.splitlines()
    done = set()
    for i, line in enumerate(lines):
        key, commented = _line_key(line)
        if key in values and not commented:
            lines[i] = f"{key}={quote(values[key])}"
            done.add(key)
    for key, value in values.items():
        if key in done:
            continue
        new = f"{key}={quote(value)}"
        at = None
        for i, line in enumerate(lines):
            k, commented = _line_key(line)
            if k == key and commented:
                at = i + 1
                break
        if at is None:
            lines.append(new)
        else:
            lines.insert(at, new)
    return "\n".join(lines) + "\n"


def get_value(text: str, key: str) -> Optional[str]:
    """Last uncommented value of key (as python-dotenv would use it), quotes removed."""
    found = None
    for line in text.splitlines():
        k, commented = _line_key(line)
        if k == key and not commented:
            v = line.split("=", 1)[1].strip()
            quoted = re.fullmatch(r'''('(?:\\'|[^'])*'|"(?:\\.|[^"\\])*")\s*(?:#.*)?''', v)
            if quoted:
                value = quoted[1][1:-1]
                if v[0] == "'":
                    v = re.sub(r"\\([\\'])", r"\1", value)
                else:
                    escapes = {"a": "\a", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "v": "\v"}
                    v = re.sub(r'''\\([\\'"abfnrtv])''', lambda m: escapes.get(m[1], m[1]), value)
            else:
                v = v.split(" #", 1)[0].strip()
            found = v
    return found


def parse_pairs(pairs: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for p in pairs:
        key, sep, value = p.partition("=")
        if not sep or not KEY_RE.match(key):
            raise ValueError(f"expected KEY=VALUE, got {p.split('=', 1)[0]!r}")
        out[key] = value
    return out


def write_private(path: str, text: str) -> None:
    fd, tmp = tempfile.mkstemp(prefix=".envfile.", dir=os.path.dirname(os.path.abspath(path)))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("env_path")
    ap.add_argument("--example", help="copy this file first when ENV_PATH does not exist")
    ap.add_argument("--has", metavar="KEY", help="exit 0 if KEY has a non-empty value, 1 if not")
    ap.add_argument("pairs", nargs="*", metavar="KEY=VALUE")
    a = ap.parse_intermixed_args(argv)

    exists = os.path.isfile(a.env_path)
    if a.has:
        if not exists:
            return 1
        return 0 if get_value(read_text(a.env_path), a.has) else 1

    try:
        values = parse_pairs(a.pairs)
        for v in values.values():
            quote(v)
    except ValueError as e:
        print(f"envfile: {e}", file=sys.stderr)
        return 2

    if exists:
        old = read_text(a.env_path)
    elif a.example and os.path.isfile(a.example):
        old = read_text(a.example)
    else:
        old = ""
    new = set_values(old, values) if values else old
    if exists and new == old:
        print(f"{a.env_path}: unchanged")
        return 0
    if exists:
        saved = backup(a.env_path, private=True)
        print(f"{a.env_path}: backup {saved}")
    write_private(a.env_path, new)
    print(f"{a.env_path}: {'updated' if exists else 'created'}; set {', '.join(values) or 'nothing'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
