# Production Readiness

## Decision: NO-GO

RippleSync is **not certified or approved for unattended native Linux production deployment**. New recovery, reconciliation, monitoring, ownership, and dependency-deployment paths have source-level/unit coverage, but native Ubuntu service and data qualification has not run.

## Acceptance criteria

| Criterion | Current status |
|---|---|
| Continuous durable gap recovery | **Implemented in code/templates; source-level tests pass.** Dedicated `xrpl-recover.service` added. Native service operation is blocked/unverified. |
| Scheduled bounded reconciliation with coverage | **Implemented and unit tested.** Timer runs an explicit `reconcile_range.txt` range; Mongo metadata records checked/uncovered intervals. Native scheduler and real-data evidence are blocked. |
| Never report recovery success for unavailable/unchecked data | **Implemented and unit tested.** Completion requires the current lease, full requested-range coverage, deep node checks, and zero remaining issues. Generic verification no longer marks recovery jobs complete. |
| Stale-worker write ownership | **Partially implemented.** Recovery/tail check lease before writes; tail checkpoint is monotonic. The lease check and multi-document writes are not transactionally atomic, so an already-started write can cross takeover. Strict fencing is not proven. |
| Single-owner/multi-host live tail | **Partially implemented.** A MongoDB singleton tail lease coordinates ordinary hosts sharing the same database. Pause/partition and takeover behavior is not native-tested; operate one owner until qualified. Backfill is not globally single-owner. |
| Failed-unit/stalled-checkpoint monitoring | **Implemented in source/templates; unit assertions pass.** A one-minute timer checks service failure and tail health. Failures are journal-only, with no external notification. Native systemd behavior is blocked. |
| Reproducible Linux dependencies and rollback-safe Python environment | **Implemented in source; partially verified.** CPython 3.12 x86_64 versions/wheel hashes are pinned; candidate venv is checked before switch and prior venv retained. Stub tests and target-wheel download passed; native install/rollback are blocked. |
| Firewall restrictions and MongoDB least privilege | **Blocked.** Installer does not configure either control; no disposable Linux network/auth testbed was available. |
| Clean Ubuntu 24.04 install and reboot | **Blocked.** No native Ubuntu host/VM is available. |
| Bounded real-mainnet ingestion/interruption/reconciliation on native Linux | **Blocked.** No native Linux XRPL+Mongo test environment. Windows/WSL results are not substituted. No production database was used. |
| 24-hour and 72-hour Linux endurance with evidence | **Blocked.** Requires a dedicated native Linux runner. |

## Blocking items

1. Run clean Ubuntu 24.04 installation, reboot, unit validation, and upgrade/rollback in an isolated disposable VM; then qualify the remaining supported Ubuntu/Debian versions.
2. Verify SIGTERM/SIGKILL, service failure/restart limits, MongoDB restart, XRPL reconnect, recovery lease takeover, and tail-owner takeover on native Linux.
3. Configure and test MongoDB authentication/least privilege and firewall allowlists on a disposable network; do not use production credentials or data.
4. Run bounded real-mainnet ingestion, interruption, recovery, and deep reconciliation against a dedicated local test database on native Linux.
5. Demonstrate the stale-writer pause/partition boundary and decide whether MongoDB transactions or a stricter ownership design are required before multi-host use.
6. Complete 24-hour and 72-hour endurance runs with structured evidence and independent ledger/transaction/metadata reconciliation.

See [`LINUX_BUG_REGISTER.md`](./LINUX_BUG_REGISTER.md), [`LINUX_TEST_RESULTS.md`](./LINUX_TEST_RESULTS.md), and [`LINUX_IMPLEMENTATION_PLAN.md`](./LINUX_IMPLEMENTATION_PLAN.md).

## Production data safety

No production database was used or modified. Code tests used isolated in-memory/mock data; no real XRPL ingestion or live MongoDB instance was used for this increment. Do not use production data for installer, interruption, repair, rollback, or endurance tests. A future release review must record the isolated database name and prove destructive actions are scoped to that test database before execution.
