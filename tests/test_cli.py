import pytest

from xrpl_ingest.cli import _verify, backfill_exit_code, build_parser

CLEAN = {"ledgers_failed": 0, "chunks_incomplete": 0, "unavailable_ranges": []}


@pytest.mark.parametrize("changes,code", [
    ({}, 0),
    ({"unavailable_ranges": [(32570, 32599)]}, 3),  # node lacks part of the range
    ({"ledgers_failed": 2}, 1),
    ({"chunks_incomplete": 1}, 1),
    ({"ledgers_failed": 1, "unavailable_ranges": [(1, 2)]}, 1),  # failure takes priority
    ({"chunks_incomplete": 1, "unavailable_ranges": [(1, 2)]}, 1),
])
def test_backfill_exit_code(changes, code):
    assert backfill_exit_code({**CLEAN, **changes}) == code


def test_backfill_help_documents_exit_codes():
    sub = build_parser()._subparsers._group_actions[0].choices["backfill"]
    assert "3" in sub.format_help() and "complete_ledgers" in sub.format_help()


def test_verify_rejects_reversed_range(store, capsys):
    assert _verify(store, 200, 100) == 2
    out = capsys.readouterr()
    assert "OK" not in out.out and "must be <=" in out.err


def test_verify_reports_missing_ledgers(store, capsys):
    assert _verify(store, 100, 102) == 1
    assert "missing ledgers 100-102" in capsys.readouterr().out
