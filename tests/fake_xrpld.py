"""Fake xrpld: a real `websockets` server on 127.0.0.1:<random port> in a background thread.

Trimmed from the audit dry-run harness. Answers `server_info` and `ledger` (API v2 shape,
`tx_json` + `meta`). Runs in the test (parent) process only; spawn-pool workers just connect
to `url`. Settings (`latest`, `big`) may be changed between runs.
"""
from __future__ import annotations

import asyncio
import json
import threading
from typing import Dict, Optional

from websockets.asyncio.server import serve


def _h(n: int) -> str:
    return f"{n:064X}"


def make_tx(idx: int, i: int) -> dict:
    return {
        "hash": _h(idx * 100000 + i + 1),
        "meta": {"TransactionIndex": i, "TransactionResult": "tesSUCCESS",
                 "AffectedNodes": [{"ModifiedNode": {"LedgerEntryType": "AccountRoot",
                                                     "LedgerIndex": _h(idx + i),
                                                     "FinalFields": {"Balance": "1000"}}}],
                 "delivered_amount": "1000000"},
        "tx_json": {"Account": f"rAcct{i}", "Destination": f"rDest{i}",
                    "TransactionType": "Payment", "Amount": "1000000", "Fee": "10",
                    "Sequence": i + 1, "SigningPubKey": "02" + "AB" * 32,
                    "TxnSignature": "30" + "CD" * 70},
    }


def make_ledger(idx: int, n_tx: int) -> dict:
    return {
        "ledger": {
            "account_hash": _h(idx * 3), "close_time": 410000000 + idx * 10,
            "close_time_human": "2013-Jan-01 00:00:00.000000000 UTC", "closed": True,
            "ledger_hash": _h(idx), "ledger_index": idx, "parent_hash": _h(idx - 1),
            "total_coins": "99999999999996320", "transaction_hash": _h(idx * 7),
            "transactions": [make_tx(idx, i) for i in range(n_tx)],
        },
        "ledger_hash": _h(idx), "ledger_index": idx, "validated": True,
    }


def tx_count(idx: int) -> int:
    return idx % 5


class FakeXrpld:
    """`with FakeXrpld(first, latest, big={idx: n_tx}) as node: ... node.url`"""

    def __init__(self, first: int, latest: int, big: Optional[Dict[int, int]] = None):
        self.first, self.latest = first, latest
        self.big = dict(big or {})
        self.port = 0
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stopped: Optional[asyncio.Event] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    def __enter__(self) -> "FakeXrpld":
        ready = threading.Event()
        self._thread = threading.Thread(target=lambda: asyncio.run(self._main(ready)), daemon=True)
        self._thread.start()
        if not ready.wait(10):
            raise RuntimeError("fake xrpld did not start")
        return self

    def __exit__(self, *exc) -> None:
        if self._loop is not None and self._stopped is not None:
            self._loop.call_soon_threadsafe(self._stopped.set)
        if self._thread is not None:
            self._thread.join(10)

    async def _main(self, ready: threading.Event) -> None:
        self._loop = asyncio.get_running_loop()
        self._stopped = asyncio.Event()
        async with serve(self._handle, "127.0.0.1", 0, max_size=None, compression=None) as server:
            self.port = server.sockets[0].getsockname()[1]
            ready.set()
            await self._stopped.wait()

    async def _handle(self, ws) -> None:
        tasks = set()
        try:
            async for raw in ws:  # answer concurrently, like a real node
                t = asyncio.create_task(self._respond(ws, json.loads(raw)))
                tasks.add(t)
                t.add_done_callback(tasks.discard)
        except Exception:  # noqa: BLE001 - client went away
            pass

    def _result(self, req: dict) -> dict:
        if req.get("command") == "server_info":
            return {"status": "success", "result": {"info": {
                "complete_ledgers": f"{self.first}-{self.latest}", "server_state": "full",
                "validated_ledger": {"seq": self.latest, "hash": _h(self.latest)}}}}
        idx = int(req.get("ledger_index", 0))
        if req.get("command") != "ledger" or not self.first <= idx <= self.latest:
            return {"status": "error", "error": "lgrNotFound", "error_message": "ledgerNotFound"}
        return {"status": "success", "result": make_ledger(idx, self.big.get(idx, tx_count(idx)))}

    async def _respond(self, ws, req: dict) -> None:
        try:
            await ws.send(json.dumps({"id": req.get("id"), "type": "response", **self._result(req)}))
        except Exception:  # noqa: BLE001 - client went away (e.g. closed with 1009)
            pass
