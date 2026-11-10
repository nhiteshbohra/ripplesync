# Linux Test Results

## Summary

**Native Linux qualification: NOT RUN.** The current host is Windows. The available Windows Bash launcher reports a WSL virtualization initialization warning; WSL is not acceptable for Linux production certification. No fresh native Ubuntu/Debian VM or systemd host was available.

No production MongoDB was accessed or modified. The MongoDB integration run below used the test suite's local, uniquely named `xrpl_ingest_test_<uuid>` database and fake XRPL server; the fixture drops only that generated database.

## Executed tests

| Test | Result | Environment / evidence |
|---|---|---|
| Full pytest suite after hardening changes | **PASS** — 923 passed, 11 skipped, 56.04s | Windows CPython 3.12; `python -m pytest -q`. This includes the new config-safety and systemd-template tests. |
| Isolated MongoDB integration suite | **PASS** — 3 passed, 7.30s | Windows, local MongoDB and fake XRPL node; `python -m pytest -m integration -q tests/test_integration.py`. Database name generated with `xrpl_ingest_test_` prefix and removed by the fixture. |
| Bash syntax and safe-file helper | **PASS (development-only)** — exit 0 | Invoked through the Windows Bash/WSL shim for `bash -n` on Linux shell files and `tests/test_linux_install_safety.sh`. WSL emitted a virtualization warning. This is not a native Linux or systemd result. |
| Pylance syntax validation | **PASS** | No syntax errors in `setup/edit_cfg.py` or `setup/xrpld_check.py`. |
| Pylance diagnostics | **PASS** | No diagnostics for `setup/edit_cfg.py`. |
| Whitespace validation | **PASS** | `git diff --check` returned exit 0; Git noted a pre-existing CRLF normalization warning for `FINAL_QA_REPORT.md`. |

### Iteration record

The first full-suite run after adding the config guard found one failing test: the repeated-edit backup test invoked the XRPL config editor without its new explicit overwrite opt-in. The test was corrected to exercise the intentional overwrite path; the final full-suite run passed. No production code was weakened to make that test pass.

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
| 24-hour endurance | **NOT RUN** | No native Linux runner. |
| 72-hour endurance | **NOT RUN** | No native Linux runner. |

## Evidence limitations

- The Windows pytest suite proves regression behavior on Windows only.
- The integration suite uses a fake XRPL node and does not validate network peer behavior or systemd.
- The Bash helper checks one safe-install function, not package-manager semantics, permissions across a real filesystem, or rollback.
- The current dependency lock is Windows-specific.
- No bounded real XRPL backfill/deep verification, interruption run, or sustained live-ingestion run was executed as part of this Linux hardening increment.

Production status remains **No-Go**. See [`PRODUCTION_READINESS.md`](./PRODUCTION_READINESS.md).
