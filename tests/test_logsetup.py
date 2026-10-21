import io
import logging
import logging.handlers
import queue
import re

import pytest

from xrpl_ingest.support import logsetup
from xrpl_ingest.errors import ConfigError
from xrpl_ingest.support.logsetup import ChunkProgress, format_duration, setup_logging

UTC_Z = r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z"


def test_stderr_line_has_utc_z_timestamp(restore_logging):
    buf = io.StringIO()
    setup_logging("INFO", stream=buf)
    logging.getLogger("xrpl_ingest.x").info("hello %d", 5)
    assert re.fullmatch(UTC_Z + r" INFO xrpl_ingest.x: hello 5\n", buf.getvalue())


def test_formatter_is_utc_not_local():
    rec = logging.LogRecord("n", logging.INFO, "", 0, "m", (), None)
    rec.created, rec.msecs = 0.0, 5
    assert logsetup.UtcFormatter(logsetup.LOG_FORMAT).format(rec).startswith("1970-01-01T00:00:00.005Z")


def test_log_file_rotates(restore_logging, tmp_path):
    f = tmp_path / "logs" / "x.log"  # parent dir is created
    handlers = setup_logging("INFO", str(f), max_mb=1, backups=2, stream=io.StringIO())
    rot = [h for h in handlers if isinstance(h, logging.handlers.RotatingFileHandler)][0]
    assert rot.maxBytes == 1024 * 1024 and rot.backupCount == 2
    rot.maxBytes = 200  # rotate fast for the test
    for i in range(20):
        logging.getLogger("xrpl_ingest.t").info("line %d %s", i, "x" * 40)
    names = sorted(p.name for p in f.parent.iterdir())
    assert names == ["x.log", "x.log.1", "x.log.2"]
    assert re.match(UTC_Z, f.read_text(encoding="utf-8"))


def test_unwritable_log_file_is_config_error(restore_logging, tmp_path):
    with pytest.raises(ConfigError, match="LOG_FILE"):
        setup_logging("INFO", str(tmp_path), stream=io.StringIO())  # a directory


def test_repeated_setup_replaces_only_our_handlers(restore_logging):
    foreign = logging.NullHandler()
    logging.getLogger().addHandler(foreign)
    setup_logging("INFO", stream=io.StringIO())
    setup_logging("INFO", stream=io.StringIO())
    root = logging.getLogger()
    assert foreign in root.handlers
    assert sum(getattr(h, "_xrpl_ingest_handler", False) for h in root.handlers) == 1


def test_worker_records_reach_parent_handlers_through_queue(restore_logging):
    buf = io.StringIO()
    setup_logging("INFO", stream=buf)
    q = queue.Queue()
    listener = logsetup.start_queue_listener(q)
    try:
        # what a worker's QueueHandler puts on the queue
        rec = logging.LogRecord("xrpl_ingest.source", logging.WARNING, "", 0, "from %s", ("worker",), None)
        logging.handlers.QueueHandler(q).handle(rec)
        debug = logging.LogRecord("xrpl_ingest.source", logging.DEBUG, "", 0, "hidden", (), None)
        logging.handlers.QueueHandler(q).handle(debug)
    finally:
        listener.stop()
    out = buf.getvalue()
    assert "WARNING xrpl_ingest.source: from worker" in out and "hidden" not in out


def test_stop_queue_listener_normal_and_stuck(restore_logging, caplog):
    q = queue.Queue()
    listener = logsetup.start_queue_listener(q)
    assert logsetup.stop_queue_listener(listener, timeout=5) is True

    class NeverDelivers(queue.Queue):  # like a queue whose write lock a killed worker holds
        def put_nowait(self, item):
            pass

    stuck = logsetup.start_queue_listener(NeverDelivers())
    with caplog.at_level("WARNING", logger="xrpl_ingest.support.logsetup"):
        assert logsetup.stop_queue_listener(stuck, timeout=0.2) is False  # returns instead of hanging
    assert "did not stop within" in caplog.text


def test_setup_worker_logging_sends_everything_to_queue(restore_logging):
    q = queue.Queue()
    logsetup.setup_worker_logging(q, "INFO")
    logging.getLogger("xrpl_ingest.w").info("hi")
    assert q.get_nowait().getMessage() == "hi"
    root = logging.getLogger()
    assert len(root.handlers) == 1 and isinstance(root.handlers[0], logging.handlers.QueueHandler)


@pytest.mark.parametrize("s,text", [(0, "0s"), (42, "42s"), (125, "2m05s"), (3 * 3600 + 7 * 60, "3h07m"),
                                    (2 * 86400 + 4 * 3600 + 59, "2d04h"), (-5, "0s")])
def test_format_duration(s, text):
    assert format_duration(s) == text


def test_chunk_progress_line_uses_monotonic_clock():
    t = [100.0]
    p = ChunkProgress(1230, [(0, 999), (1000, 1999), (2000, 2999), (3000, 3999)], clock=lambda: t[0])
    t[0] = 102.0
    assert p.update((0, 999)) == "chunks 1231/1234 (99.8%), 500 ledgers/s, ETA 6s"
    t[0] = 104.0
    assert p.update((1000, 1999)) == "chunks 1232/1234 (99.8%), 500 ledgers/s, ETA 4s"


def test_chunk_progress_spec_example_shape():
    t = [0.0]
    p = ChunkProgress(1233, [(0, 511)] + [(i, i + 999) for i in range(512, 512 + 96766 * 1000, 1000)],
                      clock=lambda: t[0])
    t[0] = 1.0
    line = p.update((0, 511))
    assert re.fullmatch(r"chunks 1234/98000 \(1\.3%\), 512 ledgers/s, ETA \d+d\d\dh", line), line
