#!/usr/bin/env python3
"""Check (and on request repair) the validator settings of an xrpld node. Used by install.sh.

usage: xrpld_check.py check [--cfg /etc/xrpld/xrpld.cfg]   exit 0 OK, 1 problems found (listed)
       xrpld_check.py fix   [--cfg /etc/xrpld/xrpld.cfg]   back up, then restore the official setup

A wrong validator list key stops xrpld right after start ("Manifest counts" is the last log line,
then exit 255). In the xrpld 3.4.1 example config the validator settings live in validators.txt
([validators_file] in xrpld.cfg), with the two official publisher keys below.

`fix` keeps copies (<file>.bak.<unix time>), removes validator stanzas from xrpld.cfg, points
[validators_file] at validators.txt and writes the official validators.txt. Run it only if you do
not use your own validator list on purpose.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import time
from typing import List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edit_cfg  # noqa: E402

# Source: cfg/validators-example.txt of XRPLF/rippled tag 3.4.1 (checked 2026-10-20).
OFFICIAL_SITES = ["https://vl.ripple.com", "https://unl.xrplf.org"]
OFFICIAL_KEYS = [
    "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734",
    "ED42AEC58B701EEBB77356FFFEC26F83C1F0407263530F068C7C73D392C7E06FD1",
]
OFFICIAL_VALIDATORS_TXT = (
    "[validator_list_sites]\n" + "\n".join(OFFICIAL_SITES) + "\n\n"
    "[validator_list_keys]\n" + "\n".join(OFFICIAL_KEYS) + "\n\n"
    "[validator_list_threshold]\n0\n"
)
VALIDATOR_STANZAS = ("validators", "validator_list_sites", "validator_list_keys", "validator_list_threshold")
KEY_RE = re.compile(r"^[0-9A-Fa-f]{66}$")


def sections(text: str) -> dict:
    return {name: [b.strip() for b in body if b.strip() and not b.strip().startswith("#")]
            for name, body in edit_cfg._split(text)[1]}


def validators_path(cfg_path: str, cfg_text: str) -> str:
    vals = sections(cfg_text).get("validators_file") or ["validators.txt"]
    p = vals[0]
    return p if os.path.isabs(p) else os.path.join(os.path.dirname(os.path.abspath(cfg_path)), p)


def problems(cfg_text: str, validators_text: Optional[str], vfile: str) -> List[str]:
    """Plain-language problems; empty list = looks right."""
    out: List[str] = []
    cfg = sections(cfg_text)
    in_cfg = [s for s in VALIDATOR_STANZAS if s in cfg]
    if in_cfg:
        out.append("xrpld.cfg contains " + ", ".join(f"[{s}]" for s in in_cfg)
                   + f"; xrpld keeps validator settings in {os.path.basename(vfile)} ([validators_file])")
    if "validators_file" not in cfg:
        out.append("xrpld.cfg has no [validators_file] stanza")
    if validators_text is None:
        out.append(f"{vfile} does not exist")
        return out
    val = sections(validators_text)
    keys = val.get("validator_list_keys", []) + cfg.get("validator_list_keys", [])
    if not keys and not val.get("validators"):
        out.append(f"{vfile} has no [validator_list_keys] (the node would trust no validators)")
    for k in keys:
        if not KEY_RE.match(k):
            out.append(f"validator list key is not 66 hex characters (has {len(k)}): {k}")
        elif k.upper() not in OFFICIAL_KEYS:
            out.append(f"validator list key is not one of the official ones: {k}")
    sites = val.get("validator_list_sites", []) + cfg.get("validator_list_sites", [])
    for s in sites:
        if not s.startswith("https://"):
            out.append(f"validator list site does not start with https://: {s}")
    return out


def fixed_cfg(cfg_text: str) -> str:
    text = cfg_text
    for s in VALIDATOR_STANZAS:
        text = edit_cfg.remove_stanza(text, s)
    return edit_cfg.set_stanza(text, "validators_file", ["validators.txt"])


def _read(path: str) -> Optional[str]:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return None


def _backup(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    dst = f"{path}.bak.{int(time.time())}"
    shutil.copy2(path, dst)
    return dst


def run(cmd: str, cfg_path: str) -> Tuple[int, List[str]]:
    try:
        cfg_text = _read(cfg_path)
        if cfg_text is None:
            return 2, [f"{cfg_path} does not exist (is xrpld installed?)"]
        vfile = validators_path(cfg_path, cfg_text)
        vtext = _read(vfile)
    except PermissionError as e:
        return 2, [f"cannot read {e.filename} (run with sudo)"]
    found = problems(cfg_text, vtext, vfile)
    if cmd == "check":
        return (1 if found else 0), found
    if not found:
        return 0, ["nothing to fix"]
    msgs = []
    for p in (cfg_path, vfile):
        b = _backup(p)
        if b:
            msgs.append(f"backup: {b}")
    new_cfg = fixed_cfg(cfg_text)
    new_vfile = os.path.join(os.path.dirname(os.path.abspath(cfg_path)), "validators.txt")
    edit_cfg.write_atomic(cfg_path, new_cfg)
    edit_cfg.write_atomic(new_vfile, OFFICIAL_VALIDATORS_TXT)
    msgs.append(f"wrote {cfg_path} (validator stanzas removed, [validators_file] validators.txt)")
    msgs.append(f"wrote {new_vfile} (official xrpld 3.4.1 validator list)")
    return 0, msgs


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("command", choices=("check", "fix"))
    ap.add_argument("--cfg", default="/etc/xrpld/xrpld.cfg")
    a = ap.parse_args(argv)
    code, lines = run(a.command, a.cfg)
    for line in lines:
        print(line)
    if a.command == "check" and code == 0:
        print("validator settings OK")
    return code


if __name__ == "__main__":
    sys.exit(main())
