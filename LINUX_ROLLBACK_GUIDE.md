# Linux Rollback Guide

## Status

The installer now builds and checks a candidate Python environment before switching, and preserves the previous environment in `.venv-backup.*`. The lock pins CPython 3.12 x86_64 versions and wheel hashes. **Neither the replacement nor rollback path has been tested on native Linux**; the rest of the system/package installation is not transactional. This remains a conservative incident procedure, not certification.

Do not perform upgrade or rollback drills against production XRPL/MongoDB data. Test first with a disposable host and database.

## Immediate containment

If ingestion is behaving incorrectly or producing discrepancies:

1. Stop only the RippleSync ingestion units:

   ```bash
   sudo systemctl stop xrpl-reconcile.timer xrpl-monitor.timer \
     xrpl-tail xrpl-backfill xrpl-recover xrpl-reconcile
   sudo systemctl status xrpl-tail xrpl-backfill xrpl-recover xrpl-reconcile
   ```

2. Leave `mongod`, `xrpld`, their data directories, and MongoDB documents/checkpoints untouched while diagnosing.
3. Capture unit status and relevant journal output. Redact credentials and private endpoints; never include `.env` contents in logs, tickets, or artifacts.
4. Do not run `verify --repair`, `recover-gaps`, retry commands, or cleanup scripts as a rollback substitute. Those are data-changing operations.

## Code and unit rollback

Use only a previously tested release artifact or filesystem snapshot that is known to match the currently installed Python/runtime requirements. Prefer versioned release directories so the previous application tree remains intact.

1. Keep the ingestion services and timers stopped.
2. Restore the prior application source using the host's approved snapshot/release procedure. Do not overwrite or expose `.env`; keep the operator-managed config path and permissions intact.
3. If only the Python environment needs rollback, confirm the selected backup belongs to the current repo and interpreter, then swap it back while services/timers remain stopped:

   ```bash
   mv .venv .venv.failed
   mv .venv-backup.SELECTED/.venv .venv
   ```

   Keep `.venv.failed` and the selected backup until the rollback is validated. Do not delete either as part of the initial rollback.

4. If unit files changed, compare the rendered units with the saved, known-good copies before restoring them. The installer intentionally refuses to overwrite a differing unit.
5. Reload systemd and validate the restored templates:

   ```bash
   sudo systemctl daemon-reload
   sudo systemd-analyze verify /etc/systemd/system/xrpl-tail.service
   sudo systemd-analyze verify /etc/systemd/system/xrpl-backfill.service
   sudo systemd-analyze verify /etc/systemd/system/xrpl-recover.service
   sudo systemd-analyze verify /etc/systemd/system/xrpl-reconcile.service
   ```

6. From the restored application directory, run `doctor --mongo --node`, read-only `status`, and a bounded read-only `verify` on a known test range. Do not start automatic repair until the incident owner approves it.
7. Start only the reviewed services and inspect logs/progress. Enable scheduled reconciliation only after verifying its fixed range file.

If the host has no known-good source/dependency snapshot, keep the services stopped and rebuild a clean tested environment on a disposable host. Do not improvise a downgrade against the live environment.

## Package rollback

- Do not use `apt purge`, remove `xrpld`, remove MongoDB packages, delete package keyrings/source files, or delete NuDB/WiredTiger directories as an initial rollback action.
- Package downgrades must use a vendor-supported version and a separately qualified procedure. The XRPL node database may require an explicit compatibility migration; package rollback can make it unusable.
- MongoDB server-version rollback must follow MongoDB's downgrade compatibility procedure and must use a tested backup/snapshot. Do not downgrade against the only copy of production data.
- Existing apt source/key files are deliberately not overwritten by this installer. Do not remove them until package ownership and other consumers have been checked.

## Recovery and readiness

After rollback, production readiness is not restored merely because services start. Confirm source network identity, fresh validated-ledger progress, stored and fully verified progress, failed/recovery job counts, transaction/metadata reconciliation, and absence of stale worker leases. Retain all evidence.

The environment backup is a directory under the repository, not a fully versioned release tree; application source, apt packages, configs, and systemd units still need a separately tested snapshot/rollback design. See [`LINUX_BUG_REGISTER.md`](./LINUX_BUG_REGISTER.md) and [`PRODUCTION_READINESS.md`](./PRODUCTION_READINESS.md).
