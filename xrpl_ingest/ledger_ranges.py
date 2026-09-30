"""Pure helpers for ledger-range arithmetic."""
from __future__ import annotations

from typing import Iterable, List, Set, Tuple

Range = Tuple[int, int]  # inclusive


def parse_complete_ledgers(text: str) -> List[Range]:
    """Parse server_info's complete_ledgers, e.g. '32570-100,105,200-300' or 'empty'."""
    text = (text or "").strip()
    if not text or text.lower() == "empty":
        return []
    ranges: List[Range] = []
    for part in text.split(","):
        part = part.strip()
        try:
            if "-" in part:
                a, b = (int(x) for x in part.split("-", 1))
            else:
                a = b = int(part)
        except ValueError:
            raise ValueError(f"bad complete_ledgers segment {part!r} in {text!r}") from None
        if a > b:
            raise ValueError(f"reversed range {part!r} in {text!r}")
        ranges.append((a, b))
    return sorted(ranges)


def intersect(ranges: Iterable[Range], start: int, end: int) -> List[Range]:
    out = []
    for a, b in ranges:
        lo, hi = max(a, start), min(b, end)
        if lo <= hi:
            out.append((lo, hi))
    return out


def subtract(start: int, end: int, ranges: Iterable[Range]) -> List[Range]:
    """Parts of [start, end] not covered by `ranges`."""
    gaps: List[Range] = []
    cursor = start
    for a, b in sorted(intersect(ranges, start, end)):
        if a > cursor:
            gaps.append((cursor, a - 1))
        cursor = max(cursor, b + 1)
    if cursor <= end:
        gaps.append((cursor, end))
    return gaps


def grid_chunks(start: int, end: int, size: int) -> List[Range]:
    """Split [start, end] on multiples of `size`, so chunk ids stay stable across runs."""
    if size < 1:
        raise ValueError("chunk size must be >= 1")
    chunks = []
    s = start
    while s <= end:
        e = min(end, (s // size + 1) * size - 1)
        chunks.append((s, e))
        s = e + 1
    return chunks


def plan_chunks(start: int, end: int, node_ranges: List[Range], size: int,
                done: Set[Range]) -> Tuple[List[Range], List[Range]]:
    """Return (chunks_to_run, unavailable_ranges) for a requested [start, end]."""
    if start > end:
        raise ValueError("start must be <= end")
    available = intersect(node_ranges, start, end)
    unavailable = subtract(start, end, node_ranges)
    chunks = [c for a, b in available for c in grid_chunks(a, b, size) if c not in done]
    return chunks, unavailable
