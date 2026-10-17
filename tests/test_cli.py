from xrpl_ingest.cli import _verify


def test_verify_rejects_reversed_range(store, capsys):
    assert _verify(store, 200, 100) == 2
    out = capsys.readouterr()
    assert "OK" not in out.out and "must be <=" in out.err


def test_verify_reports_missing_ledgers(store, capsys):
    assert _verify(store, 100, 102) == 1
    assert "missing ledgers 100-102" in capsys.readouterr().out
