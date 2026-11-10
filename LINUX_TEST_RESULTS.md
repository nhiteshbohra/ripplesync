# Linux Test Results

## Summary

**Native Linux qualification: NOT RUN.** The current host is Windows. The available Windows Bash launcher reports a WSL virtualization initialization warning; WSL is not acceptable for Linux production certification. No fresh native Ubuntu/Debian VM or systemd host was available.

No production MongoDB was accessed or modified. The MongoDB integration run below used the test suite's local, uniquely named `xrpl_ingest_test_<uuid>` database and fake XRPL server; the fixture drops only that generated database.

## Executed tests

| Test | Result | Environment / evidence |
|---|---|---|
| Full pytest suite, integration disabled | **PASS** — 936 passed, 15 skipped, 50.70s | Windows CPython 3.12; `XRPL_INGEST_NO_INTEGRATION=1 python -m pytest -q`. Includes recovery fencing, tail ownership, bounded reconciliation coverage, deployment, and installer tests. No MongoDB was accessed by this run. |
| Isolated MongoDB integration suite (prior evidence) | **PASS** — 3 passed, 7.30s | Windows, local MongoDB and fake XRPL node; `python -m pytest -m integration -q tests/test_integration.py`. Database name generated with `xrpl_ingest_test_` prefix and removed by the fixture. This is not native Linux evidence and does not use a production database. |
| Installer shell self-check | **PASS (development-only)** | `bash tests/check_python.sh` exited 0 through the Windows Bash/WSL shim. The script runs `bash -n` over installer scripts and mocked installer/venv/rollback checks. WSL emitted a virtualization warning; this is not a native Linux or systemd result. |
| Pylance diagnostics | **PASS** | No diagnostics for the changed `cli.py`, `store.py`, `tail.py`, or `recovery.py`. |
| Linux wheel lock resolution | **PASS (artifact check only)** | `pip download --require-hashes` resolved and validated all 13 pinned wheels for CPython 3.12 x86_64 using `manylinux_2_17_x86_64`. This does not verify installation or runtime behavior on Linux. |
| Whitespace validation | **PASS** | `git diff --check` returned exit 0. |

### Iteration record

The latest complete no-integration run includes the reliability and deployment changes recorded in this report. Native Linux-specific behavior remains unverified; see the mandatory tests below.

## Mandatory native Linux tests

| Required test | Result | Reason |
|---|---|---|
| Fresh Ubuntu installation | **NOT RUN** | No native Ubuntu host/VM available. |
| Fresh Debian installation | **NOT RUN** | No native Debian host/VM available. |
| Repeated installer execution | **NOT RUN natively** | Shell helper test is not a full installer repeatability test. |
| Installer failure and rollback | **NOT RUN** | No disposable native Linux host or rollback environment. |
| Service startup dependency/readiness | **NOT RUN natively** | No native systemd host; unit files were only checked by source-level tests. |
| Linux reboot recovery | **NOT RUN** | No native Linux host. |
| SIGTERM and SIGKILL interruption | **NOT RUN natively** | Prior WSL trials are development-only and predate current unit changes. |
| XRPL node disconnect/reconnect | **NOT RUN natively** | No native Linux node/source failure test. |
| MongoDB service restart | **NOT RUN natively** | The Windows integration suite is not a Linux service-restart test. |
| Firewall and authentication | **NOT RUN** | Installer does not configure these controls; no disposable Linux network testbed. |
| Missing historical ledger recovery | **NOT RUN in this Linux task** | Existing bounded reliability tests are not native Linux evidence. |
| Simultaneous live and historical ingestion | **NOT RUN natively** | Prior unit coverage is not a Linux service/system test. |
| Multi-server contention | **NOT RUN** | No multi-host Linux environment. |
| Bounded real-mainnet ingestion, interruption, recovery, and reconciliation | **NOT RUN** | No native Linux XRPL+Mongo test environment; no production database was used. |
| 24-hour endurance | **NOT RUN** | No native Linux runner. |
| 72-hour endurance | **NOT RUN** | No native Linux runner. |

## Evidence limitations

- The Windows pytest suite proves regression behavior on Windows only.
- The integration suite uses a fake XRPL node and does not validate network peer behavior or systemd.
- The shell self-check uses mocked interpreters and command outputs; it does not validate package-manager semantics, real filesystem permissions, or native rollback.
- The new dependency lock targets CPython 3.12 x86_64 Linux wheels, but has only been resolved and hash-checked from Windows; native installation is unverified.
- No bounded real XRPL backfill/deep verification, interruption run, or sustained live-ingestion run was executed as part of this Linux hardening increment.
- The Linux-only systemd units have not been validated with `systemd-analyze verify` or exercised by a native systemd manager.
- Tail ownership uses a MongoDB singleton lease and recovery writes are fenced, but lease checking and subsequent multi-collection writes are not atomic; strict exclusion of an already-started stale-worker write is not proven. Backfill is not globally single-owner.
- Service monitoring reports failed units and stalled checkpoints to journald only; external alert delivery is not configured.

Production status remains **No-Go**. See [`PRODUCTION_READINESS.md`](./PRODUCTION_READINESS.md).
