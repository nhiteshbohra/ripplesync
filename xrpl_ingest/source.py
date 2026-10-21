"""Websocket JSON-RPC client for xrpld with multiplexed requests, failover and reconnect."""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

from .config import Config, redact_url
from .errors import (ConnectFailed, ConnectionLost, NetworkMismatch, RequestTimeout,
                     RpcError, StopRequested, TransientError)
from .fetcher import RetryPolicy

log = logging.getLogger(__name__)

API_VERSION = 2
_warned_no_network_id = False  # "network_id not reported" is logged once per process

ConnectFn = Callable[..., Awaitable[Any]]


async def _default_connect(url: str, *, timeout: float, max_size: int):
    from websockets.asyncio.client import connect  # lazy: tests inject a fake
    return await connect(url, open_timeout=timeout, max_size=max_size)


class _Conn:
    """One live websocket plus its in-flight requests."""

    def __init__(self, ws: Any, url: str):
        self.ws = ws
        self.url = redact_url(url)  # only used in messages; never carries credentials
        self.pending: Dict[int, asyncio.Future] = {}
        self.received = 0  # messages of any kind; unchanged over a timeout => hung
        self.last_rx = time.monotonic()  # for the log line only
        self.closed = False
        self.reader: Optional[asyncio.Task] = None

    def fail_pending(self, exc: Exception) -> None:
        for fut in self.pending.values():
            if not fut.done():
                fut.set_exception(exc)
        self.pending.clear()


def _forget(conn: _Conn, rid: int, fut: asyncio.Future) -> None:
    """Request finished (any outcome): unregister it and mark the future's exception as
    retrieved. The reader may have failed it while send() was still raising; asyncio would
    then log 'Future exception was never retrieved'. The caller already raised its own error."""
    conn.pending.pop(rid, None)
    if not fut.done():
        fut.cancel()
    elif not fut.cancelled():
        fut.exception()


def _server_info(result: Dict[str, Any]) -> Dict[str, Any]:
    info = result.get("info", {})
    if not isinstance(info, dict):
        raise TransientError("node returned malformed server_info.info (expected object)")
    validated = info.get("validated_ledger", {})
    if not isinstance(validated, dict):
        raise TransientError("node returned malformed server_info.validated_ledger (expected object)")
    seq = validated.get("seq")
    if seq is not None and (type(seq) is not int or seq < 0):
        raise TransientError("node returned malformed validated ledger sequence")
    return info


class WsSource:
    def __init__(self, servers: Sequence[str], *, request_timeout: float = 30,
                 connect_retries: int = 5, backoff_base: float = 1.0, backoff_max: float = 30.0,
                 expected_network_id: Optional[int] = 0, max_message_bytes: int = 64 * 1024 * 1024,
                 connect_fn: Optional[ConnectFn] = None,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep, jitter: bool = True,
                 stop_check: Callable[[], bool] = lambda: False):
        if not servers:
            raise ValueError("at least one server is required")
        self.servers = tuple(servers)
        self.request_timeout = request_timeout
        self.connect_retries = connect_retries
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self._reconnect = RetryPolicy(connect_retries, backoff_base, backoff_max, jitter=jitter)
        self.expected_network_id = expected_network_id
        self.max_message_bytes = max_message_bytes
        self._connect_fn = connect_fn or _default_connect
        self._sleep = sleep
        self._stop_check = stop_check  # checked between connect attempts (stop file / SIGTERM)
        self._conn: Optional[_Conn] = None
        self._lock: Optional[asyncio.Lock] = None
        self._ids = itertools.count(1)

    @classmethod
    def from_config(cls, cfg: Config, **kw) -> "WsSource":
        return cls(cfg.servers, request_timeout=cfg.request_timeout, connect_retries=cfg.max_retries,
                   backoff_base=cfg.reconnect_delay_base, backoff_max=cfg.backoff_max,
                   expected_network_id=cfg.expected_network_id,
                   max_message_bytes=cfg.max_message_bytes, **kw)

    # -- connection management ------------------------------------------------

    def _backoff(self, attempt: int) -> float:
        return self._reconnect.delay(attempt)  # same capped, jittered formula as ledger retries

    async def _get_conn(self) -> _Conn:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:  # one reconnect at a time, others wait and reuse it
            if self._conn is not None and not self._conn.closed:
                return self._conn
            self._conn = await self._open()
            return self._conn

    async def _open(self) -> _Conn:
        last: Optional[BaseException] = None
        mismatched = 0
        for url in self.servers:
            for attempt in range(self.connect_retries):
                if self._stop_check():  # before every attempt, so also right after a backoff
                    raise StopRequested("stopped: stop requested while connecting")
                conn = None
                try:
                    ws = await self._connect_fn(url, timeout=self.request_timeout,
                                                max_size=self.max_message_bytes)
                    conn = self._start(ws, url)
                    await self._check_network(conn)
                    log.info("connected to %s", redact_url(url))
                    return conn
                except asyncio.CancelledError:
                    # _get_conn has not published this connection yet, so close() cannot
                    # find it if cancellation interrupts the initial network check.
                    await self._drop(conn)
                    raise
                except NetworkMismatch as e:
                    last = e
                    mismatched += 1
                    log.error("%s: %s", redact_url(url), e)
                    await self._drop(conn)
                    break  # wrong network will not fix itself; next server
                except Exception as e:  # noqa: BLE001 - any connect failure means try again
                    last = e
                    log.warning("connect to %s failed (attempt %d/%d): %s",
                                redact_url(url), attempt + 1, self.connect_retries, redact_url(str(e)))
                    await self._drop(conn)
                    if attempt < self.connect_retries - 1:
                        await self._sleep(self._backoff(attempt))
        if mismatched == len(self.servers):  # permanent: retrying cannot change the network
            raise NetworkMismatch(redact_url(f"every server is on the wrong network: {last}"))
        raise ConnectFailed(redact_url(f"all servers failed: {last}"))  # e may quote the URL

    def _start(self, ws: Any, url: str) -> _Conn:
        conn = _Conn(ws, url)
        conn.reader = asyncio.get_running_loop().create_task(self._read_loop(conn))
        return conn

    async def _read_loop(self, conn: _Conn) -> None:
        reason = "closed by peer"
        try:
            async for raw in conn.ws:
                conn.received += 1
                conn.last_rx = time.monotonic()
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                rid = msg.get("id") if isinstance(msg, dict) else None
                # only our own int ids: an unhashable id (list/dict) would kill the reader
                fut = conn.pending.pop(rid, None) if type(rid) is int else None
                if fut is not None and not fut.done():
                    fut.set_result(msg)
        except Exception as e:  # noqa: BLE001 - e.g. websockets ConnectionClosedError
            reason = f"{type(e).__name__}: {e}"  # websockets puts the close code/reason in str(e)
        finally:
            if not conn.closed:  # closed=True means our own _drop/close; nothing to report
                log.warning("connection to %s lost: %s", conn.url, reason)
            conn.closed = True
            conn.fail_pending(ConnectionLost(f"connection to {conn.url} closed: {reason}"))

    async def _drop(self, conn: Optional[_Conn], reason: str = "connection dropped") -> None:
        if conn is None:
            return
        conn.closed = True
        conn.fail_pending(ConnectionLost(reason))
        try:
            await conn.ws.close()
        except Exception:  # noqa: BLE001
            pass
        if conn.reader is not None:
            conn.reader.cancel()

    async def close(self) -> None:
        conn, self._conn = self._conn, None
        await self._drop(conn)

    async def _check_network(self, conn: _Conn) -> None:
        if self.expected_network_id is None:
            return
        info = _server_info(await self._request_on(conn, "server_info"))
        reported = info.get("network_id")
        global _warned_no_network_id
        if reported is None and not _warned_no_network_id:
            _warned_no_network_id = True  # once per process: backfill opens a source per chunk
            log.warning("%s: network_id not reported; network guard cannot verify mainnet",
                        conn.url)
        if reported is not None and reported != self.expected_network_id:
            raise NetworkMismatch(
                f"network_id {reported} != expected {self.expected_network_id}")

    # -- requests ---------------------------------------------------------------

    async def _request_on(self, conn: _Conn, command: str, **params: Any) -> Dict[str, Any]:
        rid = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        conn.pending[rid] = fut
        payload = {"id": rid, "command": command, "api_version": API_VERSION, **params}
        seen = conn.received
        sent = False

        async def exchange():
            nonlocal sent
            await conn.ws.send(json.dumps(payload))
            sent = True
            return await fut

        try:
            msg = await asyncio.wait_for(exchange(), timeout=self.request_timeout)
        except asyncio.TimeoutError:
            conn.pending.pop(rid, None)
            # Nothing at all arrived during this request's whole timeout: the socket is hung.
            # Decided by message count, not by measuring durations: asyncio fires timers up
            # to one clock tick early (15.6 ms on Windows), so 'silent >= timeout' could miss.
            if not sent:
                log.warning("send to %s timed out; dropping connection", conn.url)
                await self._drop(conn, f"connection to {conn.url} dropped: send timed out")
            elif conn.received == seen:
                silent = time.monotonic() - conn.last_rx
                log.warning("no message from %s for %.1fs; dropping connection", conn.url, silent)
                await self._drop(conn, f"connection to {conn.url} dropped: silent for {silent:.1f}s")
            # else: the connection is alive, only this request is slow; the others keep going
            raise RequestTimeout(f"{command} timed out after {self.request_timeout}s") from None
        except asyncio.CancelledError:
            if not sent:
                await self._drop(conn, f"connection to {conn.url} dropped: send cancelled")
            raise
        except TransientError:
            raise
        except Exception as e:  # noqa: BLE001 - send on a closed socket etc.
            conn.pending.pop(rid, None)
            reason = f"{type(e).__name__}: {e}"  # e.g. websockets 1009 message too big
            await self._drop(conn, f"connection to {conn.url} dropped: {reason}")
            raise ConnectionLost(f"{command} failed: {reason}") from e
        finally:
            _forget(conn, rid, fut)

        result = msg.get("result") if isinstance(msg.get("result"), dict) else {}
        if msg.get("status") == "error" or "error" in msg or result.get("status") == "error":
            err = msg.get("error") or result.get("error") or "unknown"
            raise RpcError(str(err), str(msg.get("error_message") or result.get("error_message") or ""))
        if "warnings" in msg:  # message-level node warnings: keep them with the result
            if "warnings" not in result:
                result["warnings"] = msg["warnings"]
            elif result["warnings"] != msg["warnings"]:
                result["node_warnings"] = msg["warnings"]
        return result

    async def request(self, command: str, **params: Any) -> Dict[str, Any]:
        conn = await self._get_conn()
        return await self._request_on(conn, command, **params)

    # -- typed helpers -----------------------------------------------------------

    async def get_ledger(self, index: int) -> Dict[str, Any]:
        return await self.request("ledger", ledger_index=index, transactions=True, expand=True)

    async def server_info(self) -> Dict[str, Any]:
        return _server_info(await self.request("server_info"))

    async def latest_validated(self) -> int:
        seq = (await self.server_info()).get("validated_ledger", {}).get("seq")
        if not isinstance(seq, int) or seq <= 0:
            raise TransientError("node has no validated ledger yet")
        return seq

    # -- data-command helpers (verify --deep, tx) ------------------------------------------

    async def get_ledger_hashes(self, index: int) -> Dict[str, Any]:
        """`ledger` with the transaction hash list only (expand=false)."""
        return await self.request("ledger", ledger_index=index, transactions=True, expand=False)

    async def get_tx(self, tx_hash: str) -> Dict[str, Any]:
        """`tx` lookup by hash, JSON (not binary)."""
        return await self.request("tx", transaction=tx_hash, binary=False)
