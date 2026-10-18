"""Where things are: the base directory, the .env file, and relative paths.

Rules (see README):
- `.env` is `ENV_FILE` if set, else `<XRPL_INGEST_HOME or cwd>/.env`. No walking up the tree.
- base dir = `XRPL_INGEST_HOME` (process env, or set inside the .env), else the directory of
  the loaded .env, else the current directory. Relative paths in the config (stop file, range
  file, log file, lock files, MONGO_URI_FILE) resolve against it.
- Process environment variables win over values in the .env file.
"""
from __future__ import annotations

import io
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, MutableMapping, Optional

from .config import resolve_path
from .errors import ConfigError
from .textfile import TextFileError, read_text

log = logging.getLogger(__name__)

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
