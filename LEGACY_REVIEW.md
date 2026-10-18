# Legacy code review (`ripple blockchain pastcode/`)

Review date: 2026-10-17/18. Scope: all 31 legacy files on disk plus the 48 files deleted in commit 96568da (still in git history at 03df819). Legacy code is kept for reference only and is not maintained.

## What the legacy code did

| Area | Legacy behaviour (best version) | Now in `xrpl_ingest` |
|---|---|---|
| Source | Public servers (`s2.ripple.com`, `xrplcluster.com`) + **testnet fallback** (mixed-network bug) | Own node over local admin websocket; testnet never in defaults; `network_id` guard |
| Fetch | Two phases: ledger headers, then transactions (2 requests/ledger) | One `ledger` request per ledger (`expand=True`) |
| Range input | `input()` prompts, start ≥ 32570 | `--start/--end` or `backfill_range.txt`; validated |
| Resume (tail) | `_metadata` doc `last_processed_ledger` | Same doc; never moves back (`$max`); smart start after backfill |
| Resume (bulk) | None (static range split, no progress) | Grid chunks marked done; rerun skips them |
| Failed ledgers | `failed_transactions` upsert; **tail version never saved** (`$set`+`$inc` conflict); bulk retried forever | Every failure recorded with reason; `retry-failed` bounded rounds; poison-ledger isolation |
| Missing-data checks | Count mismatch only logged | `verify` (gaps + tx count), `--deep` (tx hashes vs node, parent-hash chain), `--repair` |
| Duplicates | `insert_many` (bulk: no unique index → duplicates on rerun) | Upserts on unique indexes; idempotent |
| Write order | Ledger doc first (crash = "complete-looking" ledger) | Ledger doc last = commit marker |
| Stop switch | `process_status.txt` / `status.txt` (`CURRENT_PROCESS=0`) | Same file, case-insensitive, per-command keys, signals, BOM/UTF-16 safe |
| Logging | File in mode `"w"` (overwritten each run) or fully disabled | stderr or rotating `LOG_FILE`, UTC timestamps, progress + ETA |
| Single ledger / tx | Separate scripts (one websocket per tx) | `fetch --ledger N|validated`, `tx --hash H` |
| Data fields | Ledger/tx/meta_memos schema with renames (`from_address`, `to_address`, `block_number`) | Same names kept + lossless storage of every node field + new fields (below) |
| Tests | **None** | 638 automated tests incl. integration test with real MongoDB |

## Added on top of legacy (user-approved)

- **No data loss:** every field the node sends is stored (unmapped fields under their own name, collisions as `raw_<key>`, unsafe key characters escaped reversibly).
- `Amount` filled from API v2 `DeliverMax` (v2 Payments have no `Amount`).
- `delivered_amount`, `affected_accounts` (indexed), readable `amount_*` / `delivered_*` (exact Decimal128).
- `doctor`, `health`, `config` commands; single-instance lock; Linux systemd and Windows Task Scheduler templates.

## Not ported (with reason)

| Legacy item | Reason |
|---|---|
| Investigation Address Pool (`Auto Investigation Address Pool/`) | BTC/ETH/TRX case-management consumer of another system's graph data; no XRPL logic. Possible separate tool later. |
| zcash script | `main()` never called; other chain. |
| Account history (`account_tx`) fetcher | Read API v1 shape (skipped every v2 entry). Same data is queryable from ingested txs (`from_address`, `to_address`, `affected_accounts`). |
| Float XRP amounts, `_currency` helpers | Lossy floats; replaced by exact Decimal128 `amount_*` fields. |
| Two-phase fetch, infinite retry loop, dropping empty failed collection, interactive prompts, UUID `_id` | Design bugs (see below). |

## Legacy bugs not to repeat (selection)

1. Testnet server in the server list → testnet data in mainnet DB.
2. Tail failure record rejected by MongoDB (`$set` and `$inc` on `retry_count`) → failed ledgers never recorded.
3. `UnboundLocalError` in the failure path when every attempt timed out.
4. Exceptions swallowed on save → checkpoint advanced past unsaved data.
5. Ledger written before its transactions → crash leaves a ledger that looks complete.
6. Phase-1 failures only logged → permanent silent gaps.
7. Fetch failure returned `[]` → same as an empty ledger.
8. Unbounded `while True` retry → never ends on a permanently missing ledger.
9. `FileHandler(..., "w")` at module level → every worker truncated the log.
10. `sys.exit` inside the Pool initializer → pool respawns forever.
11. `if db:` on a pymongo Database → crash.
12. `requirements` pinned PyPI `asyncio==3.4.3` / `logging==0.4.9.6` (shadow stdlib); files saved as UTF-16.

Full per-file catalogs (with file:line) were produced during the review; ask to regenerate if needed.
