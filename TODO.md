# TODO / KNOWN ISSUES

## Open
- [ ] Smoke-test against the real node: `python -m xrpl_ingest status`, then `backfill --start 32570 --end 33570`. Confirm ws port/URL, that `network_id` is reported (or absent) as expected, and the response shape of `ledger` with api_version 2 (assumed `tx_json` + `meta`).
- [ ] Confirm which collection names the live DB uses (legacy scripts disagree) — set in `.env`.
- [ ] Integration test against a real MongoDB (only `mongomock` used so far).
- [ ] Test the spawn `Pool` path end-to-end (needs a node or a local fake ws server).
- [ ] Run `setup/setup_ubuntu.sh` on the real Ubuntu box (never executed; only `bash -n` + edit_cfg unit tests). Confirm package default `xrpld.cfg` port stanza names; script adds `port_ws_admin_local` if missing.
- [ ] Set `[ips_fixed]` full-history peers (docs example IPs may be stale; not hard-coded).
- [ ] Automatic xrpld updates (docs 'Update Automatically on Linux') not scripted.
- [ ] Full history ≈39 TB (docs, Jan 2026) + XFS/ZFS needed; confirm user's disk can hold it (WSL ext4 cannot).
- [ ] Decide deployment: systemd unit / supervisor for `tail`, and how `process_status.txt` is used there.
- [ ] Optional: cap Mongo write pressure (many processes × bulk_write) after measuring.

## Intentional (do not "fix" without asking)
- Legacy folders untouched and unmaintained.
- Mixed int/object types for `Amount`, `TakerGets`, `TakerPays` (legacy schema).
- Chunks with failed ledgers are marked done; failures live in the failed collection until `retry-failed`.
- Tail advances checkpoint past a ledger that failed all retries.
- `xrpl-py` not used.
