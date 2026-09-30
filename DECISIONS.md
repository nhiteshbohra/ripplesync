# DECISIONS (append-only)

## 2026-09-30 — Rebuild as new package, leave legacy untouched
Alternatives: edit `multiprocess 9 complete.py` / `current transaction fetch 3 no log print.py` in place. Chosen: new `xrpl_ingest/`; ~45 legacy files were near-duplicate iterations with hard-coded config and no tests. Legacy kept for reference.

## 2026-09-30 — Drop xrpl-py, use `websockets` + raw JSON
Alternative: keep xrpl-py 4.1.0. Reason: need explicit `api_version 2`, request multiplexing and full control of errors; fewer deps (xrpl-py was not installed here anyway). Reversible.

## 2026-09-30 — One request per ledger (expand=True), single phase
Legacy fetched ledger list first, transactions second, to soften public-node limits. Own node has none. `num_transactions = len(txs)`.

## 2026-09-30 — Ledger doc written last; all writes are upserts
Replaces legacy `insert_many(ordered=False)` + swallowing duplicate errors. Crash-safe, idempotent, `verify` can detect gaps.

## 2026-09-30 — Removed testnet server from defaults + network_id guard
Legacy SERVERS included `wss://s.altnet.rippletest.net:51233` as fallback → could silently write testnet ledgers into mainnet DB.

## 2026-09-30 — Failed ledger does not stall tail
After retries the ledger goes to the failed collection and the checkpoint advances. Legacy looped/reconnected on the same ledger. `retry-failed` recovers later. Failed collection is no longer auto-dropped when empty.

## 2026-09-30 — Logging enabled (legacy "no log print" version disabled all logging)
Silent failures made data loss invisible. `LOG_LEVEL` env, default INFO, to stderr.

## 2026-09-30 — Defaults for collection names = legacy `.env` names
`block_number`, `transactions`, `internal_data`, `failed_transactions`, `last_processed_block_no`, DB `xrpl_data_collection`. Bulk script used different names (`ledgers`, `meta_memos`); unconfirmed which the live DB uses (see TODO).

## 2026-09-30 — Tx `status` "unknown" when meta missing
Legacy labelled anything but tesSUCCESS "failed". Also added `result`, `close_time`, `transaction_index` fields.

## 2026-09-30 — CLI args replace interactive `input()` prompts
Scriptable/supervisable. Stop-file (`current_process=0`) kept for compatibility.

## 2026-09-30 — Setup script edits xrpld.cfg via stanza-level rewrite, with backup
Alternative: overwrite cfg with a template or `sed`. Packaged default cfg has its own `[node_db]` (with online_delete) so duplicates/overwrites are risky; `edit_cfg.py` replaces only managed stanzas, is idempotent, keeps other stanzas. No peers hard-coded (unverifiable/stale).
