import asyncio
import gc
import json

import pytest
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from conftest import FakeWs, no_sleep
from xrpl_ingest.errors import (ConnectFailed, ConnectionLost, NetworkMismatch, RequestTimeout,
                                RpcError, TransientError, is_retryable)
from xrpl_ingest.fetcher import RetryPolicy, ingest_ledger
from xrpl_ingest.source import WsSource, redact_url


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
    # generous: success paths must not depend on scheduling speed under load; tests
    # that expect a timeout pass a short request_timeout themselves
    kw.setdefault("request_timeout", 2.0)
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
    s = WsSource(["ws://a"], backoff_base=1.0, backoff_max=30.0, jitter=False)
    assert s._backoff(5000) == 30.0


def test_reconnect_backoff_is_jittered_by_default():
    s = WsSource(["ws://a"], backoff_base=1.0, backoff_max=30.0)
    samples = [s._backoff(2) for _ in range(200)]
    assert all(2.0 <= d <= 4.0 for d in samples) and len(set(samples)) > 1


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
        s = make_source(connect, request_timeout=0.5)  # also bounds the server_info step
        await s.server_info()
        holder["conn"] = s._conn
        try:
            await s.get_ledger(1)
        finally:
            await s.close()
    with pytest.raises(RequestTimeout):
        run(go())
    assert holder["conn"].pending == {}


def test_silent_connection_dropped_on_timeout_and_reconnects():
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
        s = make_source(connect, request_timeout=0.5)  # also bounds the server_info step
        with pytest.raises(RequestTimeout):
            await s.get_ledger(1)  # nothing at all arrived for request_timeout: hung socket
        res = await s.get_ledger(2)  # reconnects to a healthy socket
        await s.close()
        return res
    assert run(go())["validated"] is True
    assert len(made) == 2 and made[0].closed


def test_silence_decision_does_not_depend_on_clock_precision(monkeypatch):
    """Found under parallel load: asyncio fires timers up to one clock tick (15.6 ms on
    Windows) early, so 'now - last message >= request_timeout' could come out just short
    and a hung socket survived its first timeout. Freezing the clock source.py sees makes
    that boundary deterministic: the drop must not depend on measured durations."""
    import types
    from xrpl_ingest import source as source_mod
    monkeypatch.setattr(source_mod, "time", types.SimpleNamespace(monotonic=lambda: 1000.0))
    made = []

    async def connect(url, **_):
        def handler(req):
            if req["command"] == "server_info":
                return info_reply(req)
            return None if len(made) == 1 else default_handler(req)
        made.append(FakeWs(handler))
        return made[-1]

    async def go():
        s = make_source(connect, request_timeout=0.5)  # also bounds the server_info step
        with pytest.raises(RequestTimeout):
            await s.get_ledger(1)
        res = await s.get_ledger(2)
        await s.close()
        return res
    assert run(go())["validated"] is True
    assert len(made) == 2 and made[0].closed


def test_slow_requests_time_out_alone_while_connection_is_active():
    """Auditor r06: a few slow ledgers timing out together must not drop a connection that
    is still delivering other responses (the old 3-consecutive-timeouts rule did)."""
    def handler(req):
        if req["command"] == "server_info":
            return info_reply(req)
        if req["ledger_index"] < 10:
            return None  # slow ledgers: never answered in time
        asyncio.get_running_loop().call_later(0.08, made[0][1].push, default_handler(req))
        return None
    connect, made = single(handler)

    async def go():
        s = make_source(connect, request_timeout=1.0)
        await s.server_info()
        slow = [asyncio.create_task(s.get_ledger(i)) for i in (1, 2, 3)]
        healthy, i = [], 10
        # keep a healthy request in flight until the slow ones have timed out (whatever the
        # machine speed), then one more: none may be killed by the slow ones' timeouts
        while not all(t.done() for t in slow):
            healthy.append(await s.get_ledger(i))
            i += 1
        healthy.append(await s.get_ledger(i))
        results = await asyncio.gather(*slow, return_exceptions=True)
        await s.close()
        return results, healthy
    results, healthy = run(go())
    assert all(isinstance(r, RequestTimeout) for r in results)
    assert len(healthy) >= 2 and all(h["validated"] for h in healthy)
    assert len(made) == 1  # never dropped


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


def reader_dies_on_ledger(end_reader):
    """connect fn whose socket ends the reader (via end_reader(ws)) when a ledger is requested."""
    made = []

    def handler(req):
        if req["command"] == "ledger":
            end_reader(made[-1])
            return None
        return default_handler(req)

    async def connect(url, **_):
        made.append(FakeWs(handler))
        return made[-1]
    return connect


def test_reader_error_reason_reaches_pending_request_and_log(caplog):
    too_big = ConnectionClosedError(None, Close(1009, "frame exceeds limit of 1000 bytes"))
    connect = reader_dies_on_ledger(lambda ws: ws.fail(too_big))

    async def go():
        s = make_source(connect, request_timeout=5)
        with pytest.raises(ConnectionLost, match=r"1009 \(message too big\)"):
            await s.get_ledger(1)
        await s.close()
    with caplog.at_level("WARNING", logger="xrpl_ingest.source"):
        run(go())
    assert any("ws://a" in m and "message too big" in m for m in caplog.messages)


def test_peer_close_reason_reaches_pending_request_and_log(caplog):
    connect = reader_dies_on_ledger(lambda ws: ws.queue.put_nowait(None))  # clean close by server

    async def go():
        s = make_source(connect, request_timeout=5)
        with pytest.raises(ConnectionLost, match="closed by peer"):
            await s.get_ledger(1)
        await s.close()
    with caplog.at_level("WARNING", logger="xrpl_ingest.source"):
        run(go())
    assert any("ws://a" in m and "closed by peer" in m for m in caplog.messages)


def test_send_failure_reason_reaches_other_pending_requests():
    connect, made = single(lambda req: None if req["command"] == "ledger" else default_handler(req))

    async def go():
        s = make_source(connect, request_timeout=5)
        await s.server_info()
        inflight = asyncio.create_task(s.get_ledger(1))  # never answered
        await asyncio.sleep(0)
        made[0][1].closed = True  # socket dies; the next send raises ConnectionError("closed")
        with pytest.raises(ConnectionLost):
            await s.get_ledger(2)
        with pytest.raises(ConnectionLost, match="ConnectionError: closed"):
            await inflight
        await s.close()
    run(go())


class ReaderDiesDuringSendWs(FakeWs):
    """The socket dies while a ledger request is being sent: the reader fails the pending
    future first, then send() raises. That future's exception must still be retrieved."""

    async def send(self, raw):
        if json.loads(raw)["command"] != "ledger":
            return await super().send(raw)
        self.fail(ConnectionClosedError(None, Close(1011, "gone")))
        await asyncio.sleep(0)
        await asyncio.sleep(0)  # reader runs and fails every pending future
        raise ConnectionError("closed")


def test_send_failure_after_reader_failed_future_leaves_nothing_unretrieved():
    loop_errors = []

    async def connect(url, **_):
        return ReaderDiesDuringSendWs(default_handler)

    async def go():
        asyncio.get_running_loop().set_exception_handler(
            lambda _loop, ctx: loop_errors.append(ctx.get("message", "")))
        s = make_source(connect)
        try:
            await s.get_ledger(1)
        except ConnectionLost:
            pass
        await s.close()
        gc.collect()  # an unretrieved future reports itself when collected
    run(go())
    assert not [m for m in loop_errors if "never retrieved" in m]


@pytest.mark.parametrize("url,shown", [
    ("ws://user:secret@node:6006/path", "ws://node:6006/path"),
    ("wss://token@node", "wss://node"),
    ("ws://127.0.0.1:6006", "ws://127.0.0.1:6006"),
])
def test_redact_url_strips_userinfo(url, shown):
    assert redact_url(url) == shown


def test_server_credentials_never_logged_or_stored(caplog):
    attempts = []

    def end_reader(ws):
        ws.fail(ConnectionClosedError(None, Close(1011, "gone")))

    dies = reader_dies_on_ledger(end_reader)

    async def connect(url, **kw):
        attempts.append(url)
        if len(attempts) == 1:
            raise OSError("refused")  # exercise the connect-failure log line too
        return await dies(url, **kw)

    async def go():
        s = make_source(connect, servers=("ws://user:secret@a",), request_timeout=5)
        with pytest.raises(ConnectionLost) as e:
            await s.get_ledger(1)
        await s.close()
        return str(e.value)
    with caplog.at_level("DEBUG", logger="xrpl_ingest.source"):
        msg = run(go())
    assert attempts[0] == "ws://user:secret@a"  # the real URL is still used to connect
    assert "ws://a" in msg and "secret" not in msg
    assert caplog.messages and not [m for m in caplog.messages if "secret" in m]


def test_connect_failed_text_and_logs_carry_no_credentials(caplog):
    async def connect(url, **_):
        raise OSError(f"cannot connect to {url}")  # e.g. websockets InvalidURI quotes the URL

    async def go():
        await make_source(connect, servers=("ws://user:secret@a",)).server_info()
    with caplog.at_level("DEBUG", logger="xrpl_ingest.source"):
        with pytest.raises(ConnectFailed) as e:
            run(go())
    assert "ws://a" in str(e.value) and "secret" not in str(e.value)
    assert caplog.messages and not [m for m in caplog.messages if "secret" in m]


def test_unhashable_response_id_ignored_not_fatal():
    def handler(req):
        if req["command"] == "ledger":
            made[0][1].push({"id": [req["id"]], "status": "success", "result": {}})
            made[0][1].push({"id": True, "status": "success", "result": {}})  # bool is not an id
        return default_handler(req)
    connect, made = single(handler)

    async def go():
        s = make_source(connect)
        res = await s.get_ledger(1)
        await s.close()
        return res
    assert run(go())["validated"] is True


def test_own_close_does_not_log_warning(caplog):
    connect, _ = single()

    async def go():
        s = make_source(connect)
        await s.server_info()
        await s.close()
    with caplog.at_level("WARNING", logger="xrpl_ingest.source"):
        run(go())
    assert caplog.records == []


def test_send_on_closed_socket_becomes_connection_lost():
    connect, made = single()

    async def go():
        s = make_source(connect)
        await s.server_info()
        made[0][1].closed = True  # socket dies silently before the reader notices
        with pytest.raises(ConnectionLost, match="ledger failed: ConnectionError: closed"):
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
    with pytest.raises(NetworkMismatch, match="network_id 1") as e:
        run(go())
    assert not is_retryable(e.value)  # wrong network will not fix itself; do not retry


def test_every_server_on_wrong_network_is_permanent_not_retried(store):
    opened = []

    async def connect(url, **_):
        opened.append(url)
        return FakeWs(lambda req: info_reply(req, network_id=1))

    src = make_source(connect, connect_retries=5)
    res = run(ingest_ledger(src, store, 5, RetryPolicy(5, 0, 0), sleep=no_sleep))
    assert not res.ok and res.error.startswith("NetworkMismatch")
    assert opened == ["ws://a", "ws://b"]  # one connect per server, no retry rounds


def test_mixed_failures_stay_connect_failed():
    async def connect(url, **_):
        if url == "ws://a":
            raise OSError("refused")
        return FakeWs(lambda req: info_reply(req, network_id=1))

    async def go():
        await make_source(connect).server_info()
    with pytest.raises(ConnectFailed):
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
