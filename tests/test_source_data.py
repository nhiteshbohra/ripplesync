"""WsSource helpers used by the data commands (verify --deep, tx)."""
import asyncio

from conftest import FakeWs, no_sleep
from xrpl_ingest.source import WsSource


def ok(req, result):
    return {"id": req["id"], "status": "success", "type": "response", "result": result}


def handler(req):
    if req["command"] == "server_info":
        return ok(req, {"info": {"network_id": 0, "validated_ledger": {"seq": 9}}})
    if req["command"] == "ledger":
        return ok(req, {"validated": True,
                        "ledger": {"ledger_index": req["ledger_index"], "transactions": ["H"]}})
    return None


def run_with(call):
    sent = []

    async def connect(url, **_):
        ws = FakeWs(handler)
        sent.append(ws)
        return ws

    async def go():
        s = WsSource(["ws://a"], connect_fn=connect, sleep=no_sleep, request_timeout=2.0)
        try:
            return await call(s)
        finally:
            await s.close()
    result = asyncio.run(go())
    return result, [r for ws in sent for r in ws.sent if r["command"] != "server_info"]


def test_get_ledger_hashes_requests_hash_list_only():
    result, reqs = run_with(lambda s: s.get_ledger_hashes(7))
    assert result["ledger"]["transactions"] == ["H"]
    assert reqs == [{"id": reqs[0]["id"], "command": "ledger", "api_version": 2,
                     "ledger_index": 7, "transactions": True, "expand": False}]
