import asyncio
import json

import pytest

from conftest import FakeWs, no_sleep
from xrpl_ingest.errors import (ConnectFailed, ConnectionLost, RequestTimeout, RpcError,
                                TransientError)
from xrpl_ingest.source import WsSource


def ok(req, result):
    return {"id": req["id"], "status": "success", "type": "response", "result": result}


def info_reply(req, network_id=0, seq=100):
    info = {"complete_ledgers": "1-100", "validated_ledger": {"seq": seq}}
    if network_id is not None:
        info["network_id"] = network_id
    return ok(req, {"info": info})


def default_handler(req):
    if req["command"] == "server_info":
        return info_reply(req)
    if req["command"] == "ledger":
        return ok(req, {"validated": True, "ledger": {"ledger_index": str(req["ledger_index"])}})
    return None


def make_source(connect_fn, servers=("ws://a", "ws://b"), **kw):
    kw.setdefault("request_timeout", 0.2)
    kw.setdefault("connect_retries", 2)
    return WsSource(servers, connect_fn=connect_fn, sleep=no_sleep, **kw)


def run(coro):
    return asyncio.run(coro)


def single(handler=default_handler):
    made = []

    async def connect(url, **_):
        ws = FakeWs(handler)
        made.append((url, ws))
        return ws
    return connect, made


def test_request_success_and_api_version():
    connect, made = single()

    async def go():
        s = make_source(connect)
        res = await s.get_ledger(5)
        await s.close()
        return res
    res = run(go())
    assert res["validated"] is True
    ledger_req = [r for r in made[0][1].sent if r["command"] == "ledger"][0]
    assert ledger_req["api_version"] == 2 and ledger_req["transactions"] and ledger_req["expand"]
    assert ledger_req["ledger_index"] == 5


def test_reconnect_backoff_for_huge_attempt_is_capped_not_overflow():
    s = WsSource(["ws://a"], backoff_base=1.0, backoff_max=30.0)
    assert s._backoff(5000) == 30.0


def test_error_response_raises_rpcerror():
    def handler(req):
        if req["command"] == "server_info":
            return info_reply(req)
        return {"id": req["id"], "status": "error", "error": "lgrNotFound", "error_message": "nope"}
    connect, _ = single(handler)

    async def go():
        s = make_source(connect)
        try:
            await s.get_ledger(1)
        finally:
            await s.close()
    with pytest.raises(RpcError) as e:
        run(go())
    assert e.value.error == "lgrNotFound" and not e.value.retryable


def test_slowdown_is_retryable():
    assert RpcError("slowDown").retryable and RpcError("noNetwork").retryable
    assert not RpcError("invalidParams").retryable


def test_timeout_raises_and_pending_cleaned():
    def handler(req):
        return info_reply(req) if req["command"] == "server_info" else None  # never answer ledger
    connect, _ = single(handler)

    holder = {}

    async def go():
        s = make_source(connect)
        await s.server_info()
        holder["conn"] = s._conn
        try:
            await s.get_ledger(1)
        finally:
            await s.close()
    with pytest.raises(RequestTimeout):
        run(go())
    assert holder["conn"].pending == {}


def test_hung_connection_dropped_after_repeated_timeouts_and_reconnects():
    made = []

    async def connect(url, **_):
        def handler(req):
            if req["command"] == "server_info":
                return info_reply(req)
            return None if len(made) == 1 else default_handler(req)
        ws = FakeWs(handler)
        made.append(ws)
        return ws

    async def go():
        s = make_source(connect)
        for _ in range(3):
            with pytest.raises(RequestTimeout):
                await s.get_ledger(1)
        res = await s.get_ledger(2)  # reconnects to a healthy socket
        await s.close()
        return res
    assert run(go())["validated"] is True
    assert len(made) == 2 and made[0].closed


def test_connection_drop_fails_inflight_then_reconnects():
    made = []

    async def connect(url, **_):
        first = len(made) == 0

        def handler(req):
            if req["command"] == "ledger" and first:
                raise ConnectionLost("drop")
            return default_handler(req)
        ws = FakeWs(handler)
        made.append(ws)
        return ws

    async def go():
        s = make_source(connect)
        with pytest.raises(ConnectionLost):
            await s.get_ledger(1)
        res = await s.get_ledger(1)
        await s.close()
        return res
    assert run(go())["validated"] is True
    assert len(made) == 2


def test_send_on_closed_socket_becomes_connection_lost():
    connect, made = single()

    async def go():
        s = make_source(connect)
        await s.server_info()
        made[0][1].closed = True  # socket dies silently before the reader notices
        with pytest.raises(TransientError):
            await s.get_ledger(1)
        await s.close()
    run(go())


def test_failover_to_second_server():
    tried = []

    async def connect(url, **_):
        tried.append(url)
        if url == "ws://a":
            raise OSError("refused")
        return FakeWs(default_handler)

    async def go():
        s = make_source(connect)
        await s.server_info()
        await s.close()
    run(go())
    assert tried == ["ws://a", "ws://a", "ws://b"]


def test_all_servers_fail():
    async def connect(url, **_):
        raise OSError("refused")

    async def go():
        await make_source(connect).server_info()
    with pytest.raises(ConnectFailed, match="refused"):
        run(go())


def test_testnet_server_rejected_by_network_check():
    async def connect(url, **_):
        return FakeWs(lambda req: info_reply(req, network_id=1))

    async def go():
        await make_source(connect, servers=("ws://testnet",)).server_info()
    with pytest.raises(ConnectFailed, match="network_id 1"):
        run(go())


def test_network_mismatch_skips_to_next_server_without_retrying_same():
    tried = []

    async def connect(url, **_):
        tried.append(url)
        return FakeWs(lambda req: info_reply(req, network_id=1 if url == "ws://a" else 0))

    async def go():
        s = make_source(connect)
        await s.server_info()
        await s.close()
    run(go())
    assert tried == ["ws://a", "ws://b"]


def test_missing_network_id_accepted_as_mainnet():
    async def connect(url, **_):
        return FakeWs(lambda req: info_reply(req, network_id=None))

    async def go():
        s = make_source(connect)
        await s.server_info()
        await s.close()
    run(go())


def test_network_check_disabled():
    async def connect(url, **_):
        return FakeWs(lambda req: info_reply(req, network_id=7))

    async def go():
        s = make_source(connect, expected_network_id=None)
        await s.server_info()
        await s.close()
    run(go())


def test_out_of_order_responses_matched_by_id():
    connect, made = single(lambda req: info_reply(req) if req["command"] == "server_info" else None)

    async def go():
        s = make_source(connect, request_timeout=2)
        await s.server_info()  # establish connection
        ws = made[0][1]
        t1 = asyncio.ensure_future(s.request("ledger", ledger_index=1))
        t2 = asyncio.ensure_future(s.request("ledger", ledger_index=2))
        while len(ws.sent) < 3:
            await asyncio.sleep(0)
        ids = [r["id"] for r in ws.sent if r["command"] == "ledger"]
        ws.push(ok({"id": ids[1]}, {"which": "second"}))
        ws.push(ok({"id": ids[0]}, {"which": "first"}))
        r = await asyncio.gather(t1, t2)
        await s.close()
        return r
    r = run(go())
    assert r[0]["which"] == "first" and r[1]["which"] == "second"


def test_garbage_and_unknown_id_messages_ignored():
    def handler(req):
        return info_reply(req)
    connect, made = single(handler)

    async def go():
        s = make_source(connect)
        await s.server_info()
        ws = made[0][1]
        ws.queue.put_nowait("not json")
        ws.push({"id": 99999, "result": {}})
        ws.push([1, 2, 3])
        res = await s.server_info()
        await s.close()
        return res
    assert run(go())["validated_ledger"]["seq"] == 100


def test_latest_validated_and_missing_ledger():
    connect, _ = single()

    async def go():
        s = make_source(connect)
        v = await s.latest_validated()
        await s.close()
        return v
    assert run(go()) == 100

    def handler(req):
        return ok(req, {"info": {"network_id": 0}})
    connect2, _ = single(handler)

    async def go2():
        s = make_source(connect2)
        try:
            await s.latest_validated()
        finally:
            await s.close()
    with pytest.raises(TransientError, match="no validated ledger"):
        run(go2())


def test_concurrent_requests_share_one_connection():
    connect, made = single()

    async def go():
        s = make_source(connect)
        await asyncio.gather(*(s.get_ledger(i) for i in range(1, 8)))
        await s.close()
    run(go())
    assert len(made) == 1


def test_no_servers_rejected():
    with pytest.raises(ValueError):
        WsSource([])
