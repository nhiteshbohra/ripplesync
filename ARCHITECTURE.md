# ARCHITECTURE

## Flow
```
xrpld (local, full history)
  │ ws JSON-RPC api_version 2: `ledger {transactions:true, expand:true}` (ONE request per ledger)
  ▼
WsSource ──▶ ingest_ledger (retry/backoff) ──▶ transform.parse_ledger_result
                                                   │ ledger doc + tx docs + meta/memo docs
                                                   ▼
                                            MongoStore.save_ledger_bundle
                                       (txs → metas → ledger doc LAST, all upserts)
```
Modes:
- **backfill**: `server_info.complete_ledgers` ∩ requested range → grid-aligned chunks (default 1000 ledgers) → spawn `ProcessPoolExecutor` of N processes, one chunk in flight per process; each runs asyncio with `CONCURRENCY` in-flight ledgers on one ws connection. Finished chunks stored in metadata collection (`backfill_chunk:<s>-<e>`); reruns skip them. A stopped chunk (stop file) ends submission; a dead worker (OOM kill) breaks the pool, backfill stops and counts unfinished chunks as incomplete. Workers set up their own logging (spawn children inherit none). CLI exit codes (all commands): 0 ok, 1 failures/incomplete/verify problems, 2 config/usage error or wrong network, 3 node lacks part of the backfill range, 4 node or MongoDB unreachable/error, 130 Ctrl+C; errors print `error: <Type>: <msg>` without a traceback.
- **tail**: poll `server_info` validated seq, ingest `checkpoint+1..latest`, store checkpoint per ledger. A ledger that failed all retries advances the checkpoint only if it was recorded in the failed collection; otherwise tail holds the checkpoint, backs off and retries it.
- **retry-failed**: re-ingest ledgers listed in the failed collection, bounded rounds.
- **verify**: gaps in ledger collection, and tx count ≠ `num_transactions`.

## Key components
`WsSource` multiplexes requests by id over one socket, reconnects lazily (single lock), fails over across `SERVERS`, rejects servers whose `network_id` ≠ expected (default 0 = mainnet; all servers wrong ⇒ `NetworkMismatch`, not retried), drops a socket after 3 consecutive timeouts, logs why a socket died (websockets close code) and puts it in the `ConnectionLost` message. Backoff exponents are capped (`MAX_BACKOFF_EXPONENT`). `errors.is_retryable` decides retry: `TransientError`, retryable `RpcError` codes, any `PyMongoError`. Everything else (e.g. `lgrNotFound`, `MalformedLedger`) fails fast into the failed collection.

## Why
- Own node removes rate limits, so the legacy 2-phase (ledger list, then tx) design collapses to 1 request/ledger.
- Ledger doc last + upserts ⇒ crash-safe and idempotent; ledger presence = "tx complete". `verify` cross-checks.
- Chunks on a fixed grid ⇒ resume works regardless of `--start`.

## Constraints
- Full-history sizing (disk, RAM, NVMe) comes from user-supplied notes, **not verified** against docs; get current numbers before buying hardware.
- Mongo doc mixes types for `Amount`/`TakerGets`/`TakerPays` (int drops vs object) — inherited from legacy schema.
- ws messages for big ledgers are several MB; `MAX_MESSAGE_MB` (default 64) must stay above the largest ledger.
- Untested against a real xrpld. `tests/test_integration.py` (marker `integration`) runs the real spawn pool against a fake websockets node (`tests/fake_xrpld.py`) and a local MongoDB throwaway DB; it skips when MongoDB is absent or `XRPL_INGEST_NO_INTEGRATION=1`.
