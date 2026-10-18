import os
import signal
import sys

import pytest

from xrpl_ingest.support import runcontrol as stopflag
from xrpl_ingest.support.runcontrol import stop_requested


# conftest's autouse fixture resets the stop flag and restores the SIGTERM handler.

def test_sigterm_handler_acts_like_stop_file(tmp_path):
    missing = str(tmp_path / "nope.txt")
    assert stop_requested(missing) is False
    stopflag.request_stop(signal.SIGTERM, None)  # what the installed handler runs
    assert stop_requested(missing) is True and stopflag.stop_signalled() is True


def test_install_stop_signal_registers_sigterm():
    assert stopflag.install_stop_signal() is True
    assert signal.getsignal(signal.SIGTERM) is stopflag.request_stop


@pytest.mark.skipif(sys.platform == "win32", reason="Windows os.kill(SIGTERM) terminates, no handler")
def test_real_sigterm_sets_stop(tmp_path):
    stopflag.install_stop_signal()
    os.kill(os.getpid(), signal.SIGTERM)
    assert stop_requested(str(tmp_path / "nope.txt")) is True


def test_missing_file_means_run(tmp_path):
    assert stop_requested(str(tmp_path / "nope.txt")) is False


def test_stop_and_run_values(tmp_path):
    f = tmp_path / "s.txt"
    f.write_text("current_process=0\n# comment")
    assert stop_requested(str(f)) is True
    f.write_text("current_process=1\n")
    assert stop_requested(str(f)) is False


def test_spaces_tolerated(tmp_path):
    f = tmp_path / "s.txt"
    f.write_text("current_process = 0")
    assert stop_requested(str(f)) is True


def test_garbage_and_empty_mean_run(tmp_path):
    f = tmp_path / "s.txt"
    for content in ("", "hello", "current_process=maybe", "current_process=2"):
        f.write_text(content)
        assert stop_requested(str(f)) is False


def test_directory_instead_of_file_means_run(tmp_path):
    assert stop_requested(str(tmp_path)) is False


@pytest.mark.parametrize("label,data", [
    ("plain utf-8", b"current_process=0\n"),
    ("utf-8 BOM (PS 5.1 Set-Content -Encoding utf8)", "﻿current_process=0\r\n".encode("utf-8")),
    ("utf-16le BOM (PS 5.1 '>' / Out-File)", "﻿current_process=0\r\n".encode("utf-16-le")),
    ("utf-16be BOM", "﻿current_process=0\r\n".encode("utf-16-be")),
])
def test_stop_file_encodings_written_by_windows_tools(tmp_path, label, data):
    f = tmp_path / "s.txt"
    f.write_bytes(data)
    assert stop_requested(str(f)) is True, label


def test_utf16_run_value_and_truncated_utf16_mean_run(tmp_path):
    f = tmp_path / "s.txt"
    f.write_bytes("﻿current_process=1\r\n".encode("utf-16-le"))
    assert stop_requested(str(f)) is False
    f.write_bytes("﻿current_process=0".encode("utf-16-le")[:-1])  # odd byte count
    assert stop_requested(str(f)) is False


def test_binary_garbage_means_run(tmp_path):
    f = tmp_path / "s.txt"
    f.write_bytes(b"\xff\xfe\x00\x01")
    assert stop_requested(str(f)) is False


# -- per-command keys, any matching `=0` line stops, case-insensitive -------------------------

@pytest.mark.parametrize("content,command,stop", [
    ("CURRENT_PROCESS=0", None, True),  # legacy uppercase
    ("Current_Process = 0", "tail", True),
    ("tail=0", "tail", True),
    ("TAIL = 0", "tail", True),
    ("tail=0", "backfill", False),  # another command's key
    ("tail=0", None, False),
    ("backfill=0", "backfill", True),
    ("retry-failed=0", "retry-failed", True),
    ("retry-failed=0", "tail", False),
    ("current_process=1\ntail=0", "tail", True),  # any matching =0 wins (leftover =1 line)
    ("tail=0\ncurrent_process=1", "tail", True),
    ("tail=1\ncurrent_process=0", "tail", True),
    ("current_process=1\ntail=1", "tail", False),
    ("backfill=0\ncurrent_process=0", "tail", True),  # backfill line does not match tail
    ("# tail=0\n\nhello\ntail=0", "tail", True),  # comments, blanks, junk skipped
    ("tail=0  # paused for maintenance", "tail", True),
    ("tail=00", "tail", False),
])
def test_stop_file_keys(tmp_path, content, command, stop):
    f = tmp_path / "s.txt"
    f.write_text(content)
    assert stop_requested(str(f), command) is stop


def test_command_key_in_utf16_file(tmp_path):
    f = tmp_path / "s.txt"
    f.write_bytes("﻿current_process=1\r\nbackfill=0\r\n".encode("utf-16-le"))
    assert stop_requested(str(f), "backfill") is True  # current_process=1 cannot hide backfill=0
    f.write_bytes("﻿backfill=0\r\ncurrent_process=1\r\n".encode("utf-16-le"))
    assert stop_requested(str(f), "backfill") is True


# -- StopFlag: file read at most once per interval, latches -------------------------------

class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_stopflag_reads_file_at_most_once_per_interval(tmp_path, monkeypatch):
    f = tmp_path / "s.txt"
    f.write_text("current_process=1")
    reads = []
    real = stopflag._file_says_stop
    monkeypatch.setattr(stopflag, "_file_says_stop", lambda p, c: reads.append(1) or real(p, c))
    clock = Clock()
    flag = stopflag.StopFlag(str(f), "tail", clock=clock)
    for _ in range(1000):
        assert flag() is False
    assert len(reads) == 1  # old stop_requested read the file on every call
    f.write_text("tail=0")
    clock.t = 0.99
    assert flag() is False and len(reads) == 1
    clock.t = 1.0
    assert flag() is True and len(reads) == 2
    f.write_text("current_process=1")
    clock.t = 5
    assert flag() is True  # a stop is final


def test_stopflag_sees_signal_and_event_on_every_call(tmp_path):
    class Event:
        flag = False

        def is_set(self):
            return self.flag
    ev = Event()
    flag = stopflag.StopFlag(str(tmp_path / "none"), "backfill", event=ev, clock=Clock())
    assert flag() is False
    ev.flag = True
    assert flag() is True
    other = stopflag.StopFlag(str(tmp_path / "none"), clock=Clock())
    assert other() is False
    stopflag.request_stop(15, None)
    assert other() is True


def test_any_of():
    assert stopflag.any_of(lambda: False, lambda: True)() is True
    assert stopflag.any_of(lambda: False)() is False


# -- signals --------------------------------------------------------------------------------

def test_graceful_sigint_first_stops_second_aborts():
    stopflag.install_stop_signal(graceful_sigint=True)
    assert signal.getsignal(signal.SIGINT) is stopflag._graceful_sigint
    signal.raise_signal(signal.SIGINT)  # in-process, works on Windows too
    assert stopflag.stop_signalled() is True
    with pytest.raises(KeyboardInterrupt):
        signal.raise_signal(signal.SIGINT)


def test_sigint_untouched_without_graceful():
    before = signal.getsignal(signal.SIGINT)
    stopflag.install_stop_signal()
    assert signal.getsignal(signal.SIGINT) is before


@pytest.mark.skipif(not hasattr(signal, "SIGBREAK"), reason="SIGBREAK exists only on Windows")
def test_sigbreak_is_a_graceful_stop(tmp_path):
    stopflag.install_stop_signal()
    assert signal.getsignal(signal.SIGBREAK) is stopflag.request_stop
    signal.raise_signal(signal.SIGBREAK)
    assert stop_requested(str(tmp_path / "nope.txt")) is True


def test_raised_sigterm_sets_stop(tmp_path):
    stopflag.install_stop_signal()
    signal.raise_signal(signal.SIGTERM)  # C raise(): runs the handler on Windows as well
    assert stop_requested(str(tmp_path / "nope.txt")) is True


def test_ignore_sigint_for_workers():
    stopflag.ignore_sigint()
    assert signal.getsignal(signal.SIGINT) is signal.SIG_IGN
    signal.raise_signal(signal.SIGINT)  # ignored: no KeyboardInterrupt, no stop
    assert stopflag.stop_signalled() is False
