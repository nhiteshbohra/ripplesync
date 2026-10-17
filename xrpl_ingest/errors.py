"""Exception hierarchy. Retry decisions hinge on these types (see is_retryable)."""
from __future__ import annotations

from pymongo.errors import PyMongoError


class IngestError(Exception):
    """Base for all ingestion errors."""


class ConfigError(IngestError):
    """Invalid or missing configuration."""


class TransientError(IngestError):
    """Worth retrying: network hiccup, timeout, node not ready."""


class ConnectFailed(TransientError):
    """No configured server could be reached. (All on the wrong network => NetworkMismatch.)"""


class StopRequested(ConnectFailed):
    """Connecting was abandoned because a stop was requested (stop file / SIGTERM).
    Not retried and not recorded as a ledger failure."""


class ConnectionLost(TransientError):
    """Websocket dropped while a request was in flight."""


class RequestTimeout(TransientError):
    """Server did not answer within REQUEST_TIMEOUT."""


class NotValidated(TransientError):
    """Node returned a ledger that is not (yet) validated."""


class NetworkMismatch(IngestError):
    """Server reports a different network_id than expected (e.g. testnet). Not retried."""


class MalformedLedger(IngestError):
    """Ledger payload is missing required fields. Retrying will not help."""


# Errors from the node that are worth retrying. Everything else (lgrNotFound,
# invalidParams, ...) is permanent for that request.
RETRYABLE_RPC_ERRORS = frozenset({"slowDown", "noNetwork", "noCurrent", "noClosed", "tooBusy"})


class RpcError(IngestError):
    def __init__(self, error: str, message: str = ""):
        super().__init__(f"{error}: {message}" if message else error)
        self.error = error
        self.message = message

    @property
    def retryable(self) -> bool:
        return self.error in RETRYABLE_RPC_ERRORS


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, StopRequested):
        return False
    if isinstance(exc, TransientError):
        return True
    if isinstance(exc, RpcError):
        return exc.retryable
    # Mongo hiccups (failover, duplicate-key upsert race) resolve on retry.
    return isinstance(exc, PyMongoError)
