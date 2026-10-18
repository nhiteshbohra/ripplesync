import pytest
from pymongo.errors import AutoReconnect

from xrpl_ingest.diskspace import GB, DiskGuard, LowDiskSpace, mongo_free_bytes


class Stats:
    def __init__(self, db, admin=None):
        self.db, self.admin_stats, self.calls = db, admin, 0
        self.client = self
        self.admin = self

    def db_stats(self):
        self.calls += 1
        if isinstance(self.db, Exception):
            raise self.db
        return self.db

    def command(self, name):
        assert name == "dbStats"
        return self.admin_stats or {}


def fs(free_gb, total_gb=100):
    return {"fsTotalSize": total_gb * GB, "fsUsedSize": (total_gb - free_gb) * GB}


def test_free_bytes_from_db_or_admin_when_db_does_not_exist_yet():
    assert mongo_free_bytes(Stats(fs(40))) == 40 * GB
    assert mongo_free_bytes(Stats({"fsTotalSize": 0, "fsUsedSize": 0}, admin=fs(7))) == 7 * GB
    assert mongo_free_bytes(Stats({}, admin={})) is None


def test_guard_off_by_default_never_queries():
    s = Stats(fs(0))
    g = DiskGuard(s, 0)
    g.check_now()
    assert g() is False and s.calls == 0


def test_guard_trips_and_checks_at_most_once_per_interval():
    t = [0.0]
    s = Stats(fs(50))
    g = DiskGuard(s, 10, interval=60, clock=lambda: t[0])
    assert g() is False and g() is False and s.calls == 1
    s.db = fs(5)
    t[0] = 59
    assert g() is False and s.calls == 1
    t[0] = 60
    assert g() is True and g.tripped and s.calls == 2
    s.db = fs(90)
    t[0] = 500
    assert g() is True  # stays stopped


def test_check_now_raises_below_limit():
    with pytest.raises(LowDiskSpace, match="5.0 GB free, below MIN_FREE_GB=10"):
        DiskGuard(Stats(fs(5)), 10).check_now()
    DiskGuard(Stats(fs(15)), 10).check_now()


def test_failing_or_unknown_check_never_stops(caplog):
    g = DiskGuard(Stats(AutoReconnect("down")), 10, interval=0)
    assert g() is False and "disk space check failed" in caplog.text
    g = DiskGuard(Stats({}, admin={}), 10, interval=0)
    assert g() is False and g() is False
    assert caplog.text.count("cannot be enforced") == 1
