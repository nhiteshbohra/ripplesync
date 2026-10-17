"""Websocket JSON-RPC client for xrpld with multiplexed requests, failover and reconnect."""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

from .config import Config
from .constants import MAX_BACKOFF_EXPONENT
from .errors import (ConnectFailed, ConnectionLost, NetworkMismatch, RequestTimeout,
                     RpcError, TransientError)

log = logging.getLogger(__name__)

API_VERSION = 2
MAX_CONSECUTIVE_TIMEOUTS = 3  # then the connection is presumed hung and dropped

ConnectFn = Callable[..., Awaitable[Any]]


async def _default_connect(url: str, *, timeout: float, max_size: int):
    from websockets.asyncio.client import connect  # lazy: tests inject a fake
    return await connect(url, open_timeout=timeout, max_size=max_size)


class _Conn:
    """One live websocket plus its in-flight requests."""

    def __init__(self, ws: Any, url: str):
        self.ws = ws
        self.url = url
        self.pending: Dict[int, asyncio.Future] = {}
        self.timeouts = 0
        self.closed = False
        self.reader: Optional[asyncio.Task] = None

    def fail_pending(self, exc: Exception) -> None:
        for fut in self.pending.values():
            if not fut.done():
                fut.set_exception(exc)
        self.pending.clear()


class WsSource:
    def __init__(self, servers: Sequence[str], *, request_timeout: float = 30,
                 connect_retries: int = 5, backoff_base: float = 1.0, backoff_max: float = 30.0,
                 expected_network_id: Optional[int] = 0, max_message_bytes: int = 64 * 1024 * 1024,
                 connect_fn: Optional[ConnectFn] = None,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        if not servers:
            raise ValueError("at least one server is required")
        self.servers = tuple(servers)
        self.request_timeout = request_timeout
        self.connect_retries = connect_retries
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.expected_network_id = expected_network_id
        self.max_message_bytes = max_message_bytes
        self._connect_fn = connect_fn or _default_connect
        self._sleep = sleep
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
        return min(self.backoff_max, self.backoff_base * (2 ** min(attempt, MAX_BACKOFF_EXPONENT)))

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
        for url in self.servers:
            for attempt in range(self.connect_retries):
                conn = None
                try:
                    ws = await self._connect_fn(url, timeout=self.request_timeout,
                                                max_size=self.max_message_bytes)
                    conn = self._start(ws, url)
                    await self._check_network(conn)
                    log.info("connected to %s", url)
                    return conn
                except NetworkMismatch as e:
                    last = e
                    log.error("%s: %s", url, e)
                    await self._drop(conn)
                    break  # wrong network will not fix itself; next server
                except Exception as e:  # noqa: BLE001 - any connect failure means try again
                    last = e
                    log.warning("connect to %s failed (attempt %d/%d): %s",
                                url, attempt + 1, self.connect_retries, e)
                    await self._drop(conn)
                    if attempt < self.connect_retries - 1:
                        await self._sleep(self._backoff(attempt))
        raise ConnectFailed(f"all servers failed: {last}")

    def _start(self, ws: Any, url: str) -> _Conn:
        conn = _Conn(ws, url)
        conn.reader = asyncio.get_running_loop().create_task(self._read_loop(conn))
        return conn

    async def _read_loop(self, conn: _Conn) -> None:
        try:
            async for raw in conn.ws:
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                fut = conn.pending.pop(msg.get("id"), None) if isinstance(msg, dict) else None
                if fut is not None and not fut.done():
                    fut.set_result(msg)
        except Exception as e:  # noqa: BLE001
            log.debug("reader for %s ended: %s", conn.url, e)
        finally:
            conn.closed = True
            conn.fail_pending(ConnectionLost(f"connection to {conn.url} closed"))

    async def _drop(self, conn: Optional[_Conn]) -> None:
        if conn is None:
            return
        conn.closed = True
        conn.fail_pending(ConnectionLost("connection dropped"))
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
        info = (await self._request_on(conn, "server_info")).get("info", {})
        reported = info.get("network_id")
        if reported is not None and reported != self.expected_network_id:
            raise NetworkMismatch(
                f"network_id {reported} != expected {self.expected_network_id}")

    # -- requests ---------------------------------------------------------------

    async def _request_on(self, conn: _Conn, command: str, **params: Any) -> Dict[str, Any]:
        rid = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        conn.pending[rid] = fut
        payload = {"id": rid, "command": command, "api_version": API_VERSION, **params}
        try:
            await conn.ws.send(json.dumps(payload))
            msg = await asyncio.wait_for(fut, timeout=self.request_timeout)
        except asyncio.TimeoutError:
            conn.pending.pop(rid, None)
            conn.timeouts += 1
            if conn.timeouts >= MAX_CONSECUTIVE_TIMEOUTS:
                log.warning("%d consecutive timeouts on %s; dropping connection", conn.timeouts, conn.url)
                await self._drop(conn)
            raise RequestTimeout(f"{command} timed out after {self.request_timeout}s") from None
        except TransientError:
            raise
        except Exception as e:  # noqa: BLE001 - send on a closed socket etc.
            conn.pending.pop(rid, None)
            await self._drop(conn)
            raise ConnectionLost(f"{command} failed: {e}") from e

        conn.timeouts = 0
        result = msg.get("result") if isinstance(msg.get("result"), dict) else {}
        if msg.get("status") == "error" or "error" in msg or result.get("status") == "error":
            err = msg.get("error") or result.get("error") or "unknown"
            raise RpcError(str(err), str(msg.get("error_message") or result.get("error_message") or ""))
        return result

    async def request(self, command: str, **params: Any) -> Dict[str, Any]:
        conn = await self._get_conn()
        return await self._request_on(conn, command, **params)

    # -- typed helpers -----------------------------------------------------------

    async def get_ledger(self, index: int) -> Dict[str, Any]:
        return await self.request("ledger", ledger_index=index, transactions=True, expand=True)

    async def server_info(self) -> Dict[str, Any]:
        return (await self.request("server_info")).get("info", {})

    async def latest_validated(self) -> int:
        seq = (await self.server_info()).get("validated_ledger", {}).get("seq")
        if not isinstance(seq, int) or seq <= 0:
            raise TransientError("node has no validated ledger yet")
        return seq
