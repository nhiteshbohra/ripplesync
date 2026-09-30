# CLAUDE.md — project map & rules

## Purpose
Ingest the full XRP Ledger history (plus live ledgers) from our **own local xrpld node** into MongoDB. Replaces legacy scripts that hit public nodes and lost data to rate limits.

## Stack
Python 3.12 · `websockets` (raw JSON-RPC, API v2) · `pymongo` 4.x · `python-dotenv`. Dev: `pytest`, `mongomock`. (`xrpl-py` intentionally not used — see DECISIONS.md.)

## Layout
- `xrpl_ingest/` — the package (new, current code)
  - `config.py` env → frozen `Config`, validates all at once
  - `source.py` `WsSource`: multiplexed ws client, failover, reconnect, network_id guard
  - `transform.py` pure raw-ledger → Mongo docs (v1 + v2 tx shapes)
  - `store.py` `MongoStore`: idempotent upserts, checkpoint, chunk progress, failed ledgers, verify
  - `fetcher.py` `ingest_ledger`: retry/backoff, records permanent failures
  - `backfill.py` chunk planning, multi-process backfill, `retry_failed`
  - `tail.py` live follow with checkpoint · `stopflag.py` `process_status.txt` switch
  - `ledger_ranges.py` pure range math · `errors.py` retry classification · `cli.py`
- `setup/` — Ubuntu bootstrap (`setup_ubuntu.sh`, `edit_cfg.py`, `check_node.sh`); see `SETUP.md`
- `tests/` — pytest, no network or real Mongo needed
- `ripple blockchain/`, `ripple block chain/` — **legacy** scripts, kept for reference, not maintained

## Run
```
pip install -r requirements-dev.txt
python -m pytest -q
python -m xrpl_ingest status | backfill [--start N --end M] | tail | retry-failed | verify --start N --end M
```
Config: copy `.env.example` → `.env`. Stop any running command gracefully: first line of `process_status.txt` = `current_process=0`.

## Conventions
Match existing style: type hints, small pure functions, `log = logging.getLogger(__name__)`, no import-time side effects (no env reads at import). Blocking pymongo calls go through `asyncio.to_thread`. New behaviour needs a test, including the failure path.

## Do NOT
- Read, print, copy or commit `.env` values (legacy `.env` holds a real Mongo URI). Never put secrets in docs/tests.
- Add dependencies, change collection/field names, or change the write order in `MongoStore.save_ledger_bundle` (ledger doc last = commit marker) without asking.
- Add testnet/altnet servers to `SERVERS` (legacy bug: mixed networks).
- Edit legacy folders, or "fix" anything under TODO.md "Intentional".
- Claim node behaviour (ports, config keys, sizes) as verified — none of it has been tested against a real node yet.
