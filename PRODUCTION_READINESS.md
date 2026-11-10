# Production Readiness

## Decision: NO-GO

RippleSync is **not certified or approved for unattended native Linux production deployment** by this work. The core ingestion functionality and the prior bounded data trials do not satisfy the Linux success criteria without native Linux recovery and correctness evidence.

## Required acceptance criteria

| Criterion | Current status |
|---|---|
| Zero unexplained missing validated ledgers | **Unverified.** Bounded verification exists; full-history Linux reconciliation has not run. |
| Zero unexplained transaction-hash/count/metadata mismatches | **Unverified.** Deep reconciliation logic exists; no production-sized Linux evidence exists. |
| Repeatable Ubuntu/Debian install and safe conflict handling | **Partially implemented.** Code guards and template tests added; clean native installs and repeats not run. |
| Recover after reboot, SIGTERM, SIGKILL, process crash | **Unverified on native Linux.** WSL trials are not production evidence. |
| Recover after XRPL source/network and MongoDB restart | **Unverified on native Linux.** |
| Simultaneous historical backfill and live tail | **Unit coverage exists; native production-like test pending.** |
| Multi-host lease/fencing, if hosts share a database | **Partial/unverified.** Recovery-job leases exist; general cross-host tail/backfill ownership is not established. |
| Firewall, MongoDB authentication, network identity | **Partial.** Network identity/readiness checks exist; installer does not configure firewall or MongoDB authentication. |
| Reproducible Linux dependencies and tested rollback | **Not met.** Current lock is Windows-specific; no tested rollback environment. |
| 24-hour and 72-hour Linux endurance with automatic evidence | **Not run.** |

## Blocking items

1. Run the native Ubuntu 20.04/22.04/24.04 and Debian 11/12 qualification matrix in disposable hosts.
2. Verify systemd syntax, readiness behavior, restart limits, reboot, SIGTERM, SIGKILL, package/service failure, MongoDB restart, and XRPL source reconnect.
3. Configure and validate a dedicated service identity, MongoDB auth/least privilege, and host firewall policy.
4. Produce a Linux dependency lock and demonstrate a repeatable upgrade/rollback without touching database contents.
5. Demonstrate bounded concurrent tail/backfill and multi-host behavior where applicable; prove leases/fencing and stale-worker rejection.
6. Complete 24-hour and 72-hour native Linux runs with structured evidence and independent ledger/transaction/metadata reconciliation.

See [`LINUX_BUG_REGISTER.md`](./LINUX_BUG_REGISTER.md), [`LINUX_TEST_RESULTS.md`](./LINUX_TEST_RESULTS.md), and [`LINUX_IMPLEMENTATION_PLAN.md`](./LINUX_IMPLEMENTATION_PLAN.md).

## Production data safety

No production database was used or modified in this work. Do not use production data for installer, interruption, repair, rollback, or endurance tests. A future release review must record the isolated database name and prove that destructive actions are scoped to that test database before execution.
