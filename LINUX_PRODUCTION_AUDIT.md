# Linux Production Audit

## Executive status

**Not production-certified.** The current implementation is a hardening increment, not a Linux production release. The repository and this session run on Windows. The existing XRPL data trials were performed on Windows/WSL and are development evidence only. No fresh native Ubuntu/Debian install, reboot, SIGKILL recovery, firewall/authentication validation, or 24/72-hour native Linux run has been executed.

The ingestion package retains the existing MongoDB collections, document formats, checkpoints, and CLI. The changes in this audit do not alter ingestion writes or database contents.

## Scope inspected

- Linux entry point: [`install.sh`](./install.sh)
- Shared install logic: [`setup/common.sh`](./setup/common.sh)
- Database path guard: [`setup/db_path_guard.sh`](./setup/db_path_guard.sh)
- Node diagnostic wrapper: [`setup/check_node.sh`](./setup/check_node.sh)
- XRPL config editor and validator repair: [`setup/edit_cfg.py`](./setup/edit_cfg.py), [`setup/xrpld_check.py`](./setup/xrpld_check.py)
- Environment writer (read only by inspection; no `.env` values accessed): [`setup/envfile.py`](./setup/envfile.py)
- Ingest service templates: [`deploy/systemd/xrpl-tail.service`](./deploy/systemd/xrpl-tail.service), [`deploy/systemd/xrpl-backfill.service`](./deploy/systemd/xrpl-backfill.service)
- Runtime readiness and stop/locking code: [`xrpl_ingest/cli.py`](./xrpl_ingest/cli.py), [`xrpl_ingest/support/runcontrol.py`](./xrpl_ingest/support/runcontrol.py)
- Dependency declarations: [`requirements.txt`](./requirements.txt), [`requirements-lock.txt`](./requirements-lock.txt)

Windows setup scripts and WSL-specific QA coordinators are not production deployment surfaces. They were not used to certify Linux behavior.

## Compatibility matrix

This is the installer's reviewed scope, not an execution-certified matrix.

| Platform | XRPL package constraint | MongoDB path | Status |
|---|---|---|---|
| Ubuntu 20.04, 22.04, 24.04, x86_64 | XRPLF package; glibc >= 2.31 | Installer supports MongoDB 8.0 apt repository on these Ubuntu codenames | Candidate; native tests pending |
| Debian 11, 12, x86_64 | XRPLF package; glibc >= 2.31 | Existing local MongoDB or external MongoDB only; this installer does not install it | Candidate with external/preinstalled MongoDB; native tests pending |
| Other distributions, later releases, ARM64 | Outside the reviewed automatic-installer matrix | Not covered by automatic installer | Unsupported unless separately qualified |
| WSL | Development only | MongoDB itself does not support WSL | Never production certification |

`FORCE=1` bypasses selected guards; it does not certify an unsupported platform.

## Findings

### Installer and configuration safety

- `install.sh` performs system changes in stages. The XRPL config editor backs up and atomically writes config, but the prior editor replaced managed stanzas without requiring a distinct confirmation. The editor now refuses conflicting or duplicate managed stanzas by default; `--overwrite-existing` is an explicit manual override. `step_node` checks compatibility and validator settings before stopping `xrpld` or changing database ownership. Validator repair also fails closed unless its explicit override is supplied.
- XRPLF and MongoDB apt key/source files are now installed only when absent or already matching. Existing differing files cause a failure instead of replacement. The installer does not edit an existing `mongod.conf`; a leftover config without an installed/running `mongod` blocks MongoDB package installation.
- Ingest service units are compared before any unit is installed. A differing local unit is left untouched. Existing unit files are reviewed as a pair before writes.
- `setup/envfile.py` preserves unrelated lines and backups changed files, but changing deployment database settings is still an intentional installer operation. `.env` values were not read or printed during this audit.
- Failure recovery is not transactional. Package installs, XRPL service stop/configure/start, directory ownership changes, Python environment upgrades, and unit installs can span multiple stages. Re-running is partly idempotent, but there is no complete rollback journal or tested rollback command.

### systemd, readiness, and permissions

- The ingestion units now run `doctor --mongo --node` as `ExecStartPre`, use a 30-second restart delay and a five-start/one-minute limit, and apply a non-root identity, restrictive umask, no-new-privileges, private temporary/device namespaces, read-only system directories, an empty capability set, and restricted address families.
- Unit installation verifies that the selected non-root account exists and can traverse/read the application, read `.env` when present, and write the repo for existing lock/control/log paths. By default this is the installer user. A separate service account can be selected with `XRPL_INGEST_SERVICE_USER`, but the installer does not create that account or configure ACLs.
- `ProtectHome` is intentionally not enabled: the current application reads `.env` and writes lock/control/log files under its base directory, which commonly resides in a home directory. Moving mutable state outside the repo is a separate migration.
- `After=` expresses ordering, not service health. The new pre-start doctor is the readiness gate for the configured MongoDB and node. It does not configure or validate package-managed `mongod`/`xrpld` unit restart policies.
- SIGTERM has application-level graceful-stop handling and the backfill unit uses `KillMode=control-group`. SIGKILL, reboot recovery, failed pre-start behavior, and restart-limit behavior have not been tested under native systemd.

### Network, security, and monitoring

- `doctor --node` checks source reachability, network identity, peer count, validated-ledger freshness, and available ranges. The ingest units run this before each process start. The XRPL config preflight now refuses an existing admin WebSocket bound outside loopback rather than rewriting the operator's config.
- `install.sh` still prints a firewall note rather than configuring or validating host firewall policy. Operators must restrict incoming peer traffic to the configured XRPL peer listener and keep admin WebSocket listeners loopback-only.
- MongoDB authentication and bind/firewall settings are not enabled or changed by the installer. `doctor` can report connection/authentication state but does not remediate it.
- The MongoDB apt key is still Trust-On-First-Use; its fingerprint is displayed but not pinned. This remains a supply-chain hardening item.
- CLI health/status and durable gap/recovery/reconciliation logic exist in the ingestion package. No Linux monitoring agent, alert routing, independent supervisor, or always-on reconciliation service is installed by these units.

### Data correctness and concurrency

- Existing ingestion transforms, MongoDB collection/field formats, ledger-last bundle commit marker, and CLI contracts are preserved.
- Durable gap jobs, attempted/stored/verified progress, integrity checks, deep reconciliation, and fenced recovery leases exist from the earlier reliability work. This does not establish zero missing ledgers for a production history.
- Local instance locks are OS file locks and do not coordinate separate hosts. Recovery-job leases are database-coordinated, but cross-host tail/backfill ownership and contention have not been demonstrated.
- No production database was accessed or modified during this audit.

### Dependencies and upgrades

- `requirements.txt` uses version ranges. `requirements-lock.txt` is explicitly resolved on Windows CPython 3.12 and is not a Linux lock.
- `install.sh` installs/upgrades packages in the active `.venv`; dependency installation is not an atomic environment swap. A failed pip run may leave a partially changed environment.
- There is no tested Linux package pinning, upgrade canary, rollback environment, or package downgrade procedure.

## Implemented in this increment

1. Strict installer matrix checks for Ubuntu 20.04/22.04/24.04 and Debian 11/12 on x86_64; Debian local MongoDB auto-install is explicitly unsupported.
2. Fail-closed XRPL stanza and validator config/list conflict checks, with explicit manual overwrite opt-in and preflight before stopping the node or changing database ownership.
3. Non-overwriting package key/source and systemd-unit file installation.
4. MongoDB config collision guard.
5. Ingest service readiness checks, restart limits, restrictive systemd sandbox directives, and service-account access checks.
6. Automated regression coverage for config safety and systemd template invariants.

These are source-level and Windows/WSL-development validations only. They are not native Linux certification.

## Decision

**No-Go for production.** See [`PRODUCTION_READINESS.md`](./PRODUCTION_READINESS.md), [`LINUX_BUG_REGISTER.md`](./LINUX_BUG_REGISTER.md), and [`LINUX_TEST_RESULTS.md`](./LINUX_TEST_RESULTS.md).
