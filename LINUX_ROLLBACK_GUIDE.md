# Linux Rollback Guide

## Status

There is **no tested automated rollback** for the current Linux installer. The active Python environment is upgraded in place, package transactions span multiple steps, and the checked-in dependency lock is Windows-specific. This document is a conservative incident procedure, not evidence that package rollback has been qualified.

Do not perform upgrade or rollback drills against production XRPL/MongoDB data. Test first with a disposable host and database.

## Immediate containment

If ingestion is behaving incorrectly or producing discrepancies:

1. Stop only the RippleSync ingestion units:

   ```bash
   sudo systemctl stop xrpl-tail xrpl-backfill
   sudo systemctl status xrpl-tail xrpl-backfill
   ```

2. Leave `mongod`, `xrpld`, their data directories, and MongoDB documents/checkpoints untouched while diagnosing.
3. Capture unit status and relevant journal output. Redact credentials and private endpoints; never include `.env` contents in logs, tickets, or artifacts.
4. Do not run `verify --repair`, `recover-gaps`, retry commands, or cleanup scripts as a rollback substitute. Those are data-changing operations.

## Code and unit rollback

Use only a previously tested release artifact or filesystem snapshot that is known to match the currently installed Python/runtime requirements. Prefer versioned release directories so the previous application tree remains intact.

1. Keep the ingestion units stopped.
2. Restore the prior application source and dependency environment using the host's approved snapshot/release procedure. Do not overwrite or expose `.env`; keep the current operator-managed config path and permissions intact.
3. If unit files changed, compare the rendered units with the saved, known-good copies before restoring them. The installer intentionally refuses to overwrite a differing unit.
4. Reload systemd and validate the restored templates:

   ```bash
   sudo systemctl daemon-reload
   sudo systemd-analyze verify /etc/systemd/system/xrpl-tail.service
   sudo systemd-analyze verify /etc/systemd/system/xrpl-backfill.service
   ```

5. From the restored application directory, run `doctor --mongo --node`, read-only `status`, and a bounded read-only `verify` on a known test range. Do not start automatic repair until the incident owner approves it.
6. Start `xrpl-tail` first and inspect logs/progress. Start backfill only after its configured range and checkpoint state have been reviewed.

If the host has no known-good source/dependency snapshot, keep the services stopped and rebuild a clean tested environment on a disposable host. Do not improvise a downgrade against the live environment.

## Package rollback

- Do not use `apt purge`, remove `xrpld`, remove MongoDB packages, delete package keyrings/source files, or delete NuDB/WiredTiger directories as an initial rollback action.
- Package downgrades must use a vendor-supported version and a separately qualified procedure. The XRPL node database may require an explicit compatibility migration; package rollback can make it unusable.
- MongoDB server-version rollback must follow MongoDB's downgrade compatibility procedure and must use a tested backup/snapshot. Do not downgrade against the only copy of production data.
- Existing apt source/key files are deliberately not overwritten by this installer. Do not remove them until package ownership and other consumers have been checked.

## Recovery and readiness

After rollback, production readiness is not restored merely because services start. Confirm source network identity, fresh validated-ledger progress, stored and fully verified progress, failed/recovery job counts, transaction/metadata reconciliation, and absence of stale worker leases. Retain all evidence.

Until a Linux dependency lock, versioned environment strategy, and native rollback drill pass, package rollback remains a release blocker. See [`LINUX_BUG_REGISTER.md`](./LINUX_BUG_REGISTER.md) and [`PRODUCTION_READINESS.md`](./PRODUCTION_READINESS.md).
