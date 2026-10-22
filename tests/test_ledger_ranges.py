import pytest

from xrpl_ingest.ledger_ranges import (grid_chunks, intersect, parse_complete_ledgers,
                                       plan_chunks, subtract)


@pytest.mark.parametrize("text,expected", [
    ("", []), ("empty", []), ("EMPTY", []), (None, []),
    ("5-10", [(5, 10)]),
    ("7", [(7, 7)]),
    ("32570-100000, 100002-200000,300000", [(32570, 100000), (100002, 200000), (300000, 300000)]),
    ("200-300,5-10", [(5, 10), (200, 300)]),
])
def test_parse_ok(text, expected):
    assert parse_complete_ledgers(text) == expected


@pytest.mark.parametrize("text", ["abc", "5-x", "10-5", "1-2-3", "1,,2"])
def test_parse_malformed(text):
    with pytest.raises(ValueError):
        parse_complete_ledgers(text)


def test_grid_chunks_aligned_and_clipped():
    assert grid_chunks(5, 27, 10) == [(5, 9), (10, 19), (20, 27)]
    assert grid_chunks(10, 19, 10) == [(10, 19)]
    assert grid_chunks(3, 3, 10) == [(3, 3)]


def test_grid_chunks_bad_size():
    with pytest.raises(ValueError):
        grid_chunks(1, 5, 0)


def test_grid_chunks_stable_across_starts():
    a = set(grid_chunks(0, 99, 10))
    b = set(grid_chunks(20, 99, 10))
    assert b <= a


def test_intersect_and_subtract():
    assert intersect([(1, 10), (20, 30)], 5, 25) == [(5, 10), (20, 25)]
    assert subtract(1, 30, [(5, 10), (20, 25)]) == [(1, 4), (11, 19), (26, 30)]
    assert subtract(1, 5, [(1, 5)]) == []
    assert subtract(1, 5, []) == [(1, 5)]


def test_plan_skips_done_and_reports_unavailable():
    chunks, unavailable = plan_chunks(1, 40, [(1, 25)], 10, done={(10, 19)})
    assert chunks == [(1, 9), (20, 25)]
    assert unavailable == [(26, 40)]


def test_plan_node_empty():
    chunks, unavailable = plan_chunks(1, 10, [], 10, set())
    assert chunks == [] and unavailable == [(1, 10)]


def test_plan_start_after_end():
    with pytest.raises(ValueError):
        plan_chunks(10, 1, [(1, 10)], 5, set())


def test_plan_gap_in_node_history():
    chunks, unavailable = plan_chunks(1, 30, [(1, 9), (21, 30)], 10, set())
    assert chunks == [(1, 9), (21, 29), (30, 30)]  # grid-aligned, never spans the gap
    assert unavailable == [(10, 20)]


def test_plan_rejects_a_chunk_grid_too_large_to_materialize():
    with pytest.raises(ValueError, match="increase CHUNK_SIZE"):
        plan_chunks(1, 100_001, [(1, 100_001)], 1, set())
