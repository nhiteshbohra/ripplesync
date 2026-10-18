import codecs
from datetime import datetime, timedelta, timezone

import pytest

from xrpl_ingest.errors import ConfigError
from xrpl_ingest.support.files import BackfillRange, choose_range as choose, format_range_progress as format_progress, load_range as load, parse_range as parse, range_progress as progress

EXAMPLE = """# used by `backfill` when neither --start nor --end is given
start = 32570
end   = latest      # or a ledger number
"""


def test_example_file():
    r = parse(EXAMPLE, "f.txt")
    assert (r.start, r.end) == (32570, None) and r.source == "file f.txt"


@pytest.mark.parametrize("text,start,end", [
    ("", 32570, None),
    ("START = first\nEnd = 100_000_000\n", 32570, 100000000),
    ("end=40000", 32570, 40000),
    ("\n  # only comments\n\nstart = 50000 # inline\n", 50000, None),
    ("start = 40000\nend = 40000\n", 40000, 40000),
    ("end = LATEST\r\nstart = 33000\r\n", 33000, None),
])
def test_valid(text, start, end):
    r = parse(text, "f")
    assert (r.start, r.end) == (start, end)


@pytest.mark.parametrize("text,msg", [
    ("start 40000", "f:1: expected 'key = value'"),
    ("# x\nbegin = 1", "f:2: unknown key 'begin'"),
    ("start = 40000\nstart = 50000", "f:2: duplicate key 'start' (first on line 1)"),
    ("start = 100", "f:1: start = 100 must be >= 32570"),
    ("start = 50000\nend = 40000", "f:2: end = 40000 must be >= start (50000)"),
    ("start = -1", "must be a ledger number or 'first'"),
    ("end = 1e6", "must be a ledger number or 'latest'"),
    ("start = 40__000", "must be a ledger number"),
    ("start = ", "must be a ledger number"),
    ("end = first", "or 'latest'"),
])
def test_invalid_names_file_and_line(text, msg):
    with pytest.raises(ConfigError, match=msg.replace("(", r"\(").replace(")", r"\)")):
        parse(text, "f")


def test_load_missing_is_defaults_and_logged(tmp_path, caplog):
    with caplog.at_level("INFO", logger="xrpl_ingest.support.files"):
        r = load(str(tmp_path / "none.txt"))
    assert r == BackfillRange() and "not found" in caplog.text


def test_load_required_missing_is_error(tmp_path):
    with pytest.raises(ConfigError, match="does not exist"):
        load(str(tmp_path / "none.txt"), required=True)


@pytest.mark.parametrize("data", [
    codecs.BOM_UTF8 + b"start = 40000\n",
    codecs.BOM_UTF16_LE + "start = 40000\r\n".encode("utf-16-le"),
])
def test_load_decodes_like_env(tmp_path, data):
    f = tmp_path / "r.txt"
    f.write_bytes(data)
    assert load(str(f)).start == 40000


def test_load_too_big_or_directory_is_error(tmp_path):
    f = tmp_path / "r.txt"
    f.write_bytes(b"#" * 4097)
    with pytest.raises(ConfigError, match="larger than 4096"):
        load(str(f))
    with pytest.raises(ConfigError, match="cannot read"):
        load(str(tmp_path))


def test_choose_precedence(tmp_path):
    f = tmp_path / "r.txt"
    f.write_text("start = 40000\nend = 50000\n")
    assert choose(None, None, str(f)) == BackfillRange(40000, 50000, f"file {f}")
    assert choose(None, 45000, str(f)) == BackfillRange(32570, 45000, "command line")
    assert choose(33000, None, str(f)) == BackfillRange(33000, None, "command line")
    assert choose(None, None, str(f), use_file=False).start == 32570
    with pytest.raises(ConfigError, match="cannot be combined"):
        choose(33000, None, str(f), range_file=str(f))


def test_file_is_never_written(tmp_path):
    f = tmp_path / "r.txt"
    f.write_text(EXAMPLE)
    before = f.stat().st_mtime_ns
    choose(None, None, str(f))
    assert f.read_text() == EXAMPLE and f.stat().st_mtime_ns == before


# -- status progress ----------------------------------------------------------------------

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_progress_counts_rate_and_eta(store):
    # chunk_size 10, range 32570-32669 = 10 chunks / 100 ledgers; 3 done, 60 s apart
    for i, (a, b) in enumerate([(32570, 32579), (32580, 32589), (32590, 32599)]):
        store.mark_chunk_done(a, b, 10, 0)
        store.meta.update_one({"_id": f"backfill_chunk:{a}-{b}"},
                              {"$set": {"completed_at": (T0 + timedelta(seconds=60 * i)).replace(tzinfo=None)}})
    store.mark_chunk_done(40000, 40009, 10, 0)  # outside the range
    store.record_failure(32571, "x")
    store.record_failure(50000, "x")
    p = progress(store, 32570, 32669, 10, now=T0 + timedelta(seconds=180))
    assert (p["chunks_done"], p["chunks_total"], p["failed_ledgers"]) == (3, 10, 1)
    assert p["rate"] == pytest.approx(20 / 120)  # 2 chunks after the first, over 120 s
    assert p["eta_seconds"] == pytest.approx(70 / (20 / 120))
    line = format_progress(p, now=T0 + timedelta(seconds=180))
    assert line.startswith("chunks 3/10 (30.0%), failed ledgers 1, 0.2 ledgers/s")
    assert "last chunk 1m00s ago" in line and "ETA 7m00s" in line


def test_progress_without_completions_has_no_rate(store):
    p = progress(store, 32570, 32579, 10)
    assert p["rate"] is None and p["eta_seconds"] is None
    assert format_progress(p) == "chunks 0/1 (0.0%), failed ledgers 0"
