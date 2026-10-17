import asyncio
import os
import signal
import sys
from datetime import datetime, timezone

import mongomock
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from xrpl_ingest import stopflag  # noqa: E402
from xrpl_ingest.config import Config  # noqa: E402
from xrpl_ingest.errors import ConnectionLost  # noqa: E402
from xrpl_ingest.fetcher import RetryPolicy  # noqa: E402
from xrpl_ingest.store import MongoStore  # noqa: E402

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAST_RETRY = RetryPolicy(attempts=3, base=0, max_delay=0)


async def no_sleep(_seconds):
    return None


@pytest.fixture(autouse=True)
def isolated_stop_signal(monkeypatch):
    """Every test starts un-stopped, and the SIGTERM handler of the pytest process is put
    back afterwards (cli.main and pool-worker code install one)."""
    monkeypatch.setattr(stopflag, "_signalled", False)
    previous = signal.getsignal(signal.SIGTERM)
    yield
    signal.signal(signal.SIGTERM, previous)


@pytest.fixture
def cfg():
    return Config(mongo_uri="mongodb://x", chunk_size=10, concurrency=3, max_retries=3,
                  reconnect_delay_base=0, backoff_max=0, num_processes=1)


@pytest.fixture
def store(cfg):
    s = MongoStore(mongomock.MongoClient(), cfg)
    s.ensure_indexes()
    return s


def make_tx(n, ledger=100, result="tesSUCCESS"):
    return {
        "hash": f"H{ledger}_{n}",
        "meta": {"TransactionResult": result, "TransactionIndex": n},
        "tx_json": {"Account": f"rA{n}", "Destination": f"rB{n}", "TransactionType": "Payment",
                    "Amount": "1000000", "Fee": "12", "Sequence": n},
    }


def make_result(index, n_tx=2, validated=True, **overrides):
    ledger = {
        "ledger_index": str(index), "ledger_hash": f"LH{index}", "parent_hash": f"LH{index-1}",
        "account_hash": "AH", "transaction_hash": "TH", "close_time": 800000000,
        "close_time_human": "x", "total_coins": "99985687626634189",
        "transactions": [make_tx(i, index) for i in range(n_tx)],
    }
    ledger.update(overrides)
    return {"ledger": ledger, "ledger_index": index, "validated": validated}


class FakeSource:
    """Scripted ledger source. `script[index]` is a list of outcomes consumed per call;
    an outcome is a result dict or an Exception instance to raise. Default: valid ledger."""

    def __init__(self, script=None, latest=100, ranges="1-100"):
        self.script = {k: list(v) for k, v in (script or {}).items()}
        self.calls = []
        self.latest = latest
        self.complete = ranges
        self.latest_errors = []

    async def get_ledger(self, index):
        self.calls.append(index)
        queue = self.script.get(index)
        outcome = queue.pop(0) if queue else make_result(index)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def server_info(self):
        return {"complete_ledgers": self.complete, "validated_ledger": {"seq": self.latest}}

    async def latest_validated(self):
        if self.latest_errors:
            raise self.latest_errors.pop(0)
        return self.latest

    async def close(self):
        pass


class FakeWs:
    """Fake websocket. `handler(request_dict)` returns a response dict, None (never answer),
    or raises ConnectionLost to simulate a drop."""

    def __init__(self, handler):
        self.handler = handler
        self.queue = asyncio.Queue()
        self.sent = []
        self.closed = False

    async def send(self, raw):
        import json
        if self.closed:
            raise ConnectionError("closed")
        req = json.loads(raw)
        self.sent.append(req)
        try:
            resp = self.handler(req)
        except ConnectionLost:
            await self.close()
            return
        if resp is not None:
            self.queue.put_nowait(json.dumps(resp))

    def push(self, resp):
        import json
        self.queue.put_nowait(json.dumps(resp))

    async def close(self):
        if not self.closed:
            self.closed = True
            self.queue.put_nowait(None)

    def fail(self, exc):
        """Make the reader's `async for` raise `exc` (e.g. websockets ConnectionClosedError)."""
        self.closed = True
        self.queue.put_nowait(exc)

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.queue.get()
        if item is None:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return item
