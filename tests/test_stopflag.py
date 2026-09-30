from xrpl_ingest.stopflag import stop_requested


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


def test_binary_garbage_means_run(tmp_path):
    f = tmp_path / "s.txt"
    f.write_bytes(b"\xff\xfe\x00\x01")
    assert stop_requested(str(f)) is False
