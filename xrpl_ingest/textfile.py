"""Small text files written by people and Windows tools: .env, stop file, range file.

Windows PowerShell 5.1 writes UTF-16LE with BOM for `>` / Out-File and UTF-8 with BOM for
`Set-Content -Encoding utf8`; Notepad may write either. All of them must read the same as
a plain UTF-8 file.
"""
from __future__ import annotations

import codecs


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
