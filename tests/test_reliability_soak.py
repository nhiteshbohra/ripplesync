import asyncio
import json
from types import SimpleNamespace

from reliability_soak import _run
from xrpl_ingest.store import MongoStore


def test_soak_records_mongodb_initialization_failure(cfg, monkeypatch, tmp_path):
    def unavailable(_cls, _cfg):
        raise RuntimeError("MongoDB unavailable")

    monkeypatch.setattr(MongoStore, "from_config", classmethod(unavailable))
    result_path = tmp_path / "soak.jsonl"

    code = asyncio.run(_run(
        SimpleNamespace(hours=24, sample_interval=1, max_lag=10),
        cfg,
        result_path,
    ))

    events = [json.loads(line) for line in result_path.read_text(encoding="utf-8").splitlines()]
    assert code == 1
    assert [event["event"] for event in events] == ["start", "summary"]
    assert events[-1]["status"] == "fail"
    assert events[-1]["tail_error"] == "RuntimeError: MongoDB unavailable"
    assert events[-1]["checkpoint_advances"] == 0
