# Linux Implementation Plan

## Guardrails

- Keep native Linux production as the only certification target; Windows and WSL are development-only.
- Preserve MongoDB collection/document formats, checkpoints, write order, and CLI behavior.
- Do not use production MongoDB data for destructive tests.
- Do not change host XRPL/MongoDB configurations during QA without an explicit operator-approved maintenance window.
- Do not declare readiness until evidence from the required native Linux matrix is collected.

## Phase 0 — Audit and baseline

**Status: Complete for source review; native environment audit remains pending.**

- Inspected Linux installer, shared shell helpers, config editor, node diagnostic wrapper, env writer, database path guard, systemd templates, runtime readiness/lock behavior, and requirements.
- Identified compatibility overclaim, config overwrite risk, unrestricted service replacement, absent pre-start readiness, unbounded restart policy, and missing Linux rollback evidence.
- The initial full regression baseline passed: 910 passed, 10 skipped. That baseline preceded the Linux source changes in this plan.

## Phase 1 — Fail-closed installation and service templates

**Status: Implemented in the working tree; Windows/WSL-development checks only.**

- Gate automatic setup to Ubuntu 20.04/22.04/24.04 and Debian 11/12 on x86_64; require external/preinstalled MongoDB on Debian.
- Refuse conflicting/duplicate managed XRPL config stanzas by default; expose deliberate manual override and validate before stopping `xrpld` or changing DB ownership.
- Never replace differing package key/source files, existing MongoDB config, or local systemd units.
- Add `doctor --mongo --node` service preflight, restart delay/burst cap, non-root account access checks, and systemd sandbox restrictions.
- Add focused tests for config refusal, preflight behavior, service template invariants, and a shell helper test.

**Exit gate:** full unit suite and config/service tests pass; no production DB touched. Native Linux syntax/config validation is still required before installation.

## Phase 2 — Native installer qualification

**Status: Not started.**

1. Provision clean disposable Ubuntu 20.04, 22.04, 24.04 and Debian 11/12 VMs, all x86_64. Capture OS release, package sources, systemd version, and Python version.
2. Verify fresh install, second install, interrupted package install, conflicting user config, conflicting apt source, conflicting systemd unit, and failure recovery. Confirm no existing config is overwritten.
3. Use throwaway MongoDB data only. Test Debian with preinstalled local MongoDB and an external MongoDB URI; test Ubuntu local MongoDB install.
4. Run `systemd-analyze verify` on rendered units; confirm the selected account's file access and `doctor` gating.
5. Confirm package source signing and key rotation behavior. Pin MongoDB signing identity only after independently verifying an official fingerprint source.

**Exit gate:** all supported release combinations pass repeatability and conflict-preservation tests; unsupported combinations stop with a clear error; evidence is archived.

## Phase 3 — Security, readiness, and service recovery

**Status: Partially implemented; native tests not started.**

1. Provision a dedicated unprivileged service account and define ownership/ACLs for the app, `.venv`, `.env`, logs, locks, stop file, and range file.
2. Verify outbound access is limited to configured MongoDB and XRPL source endpoints; verify the node admin WebSocket binds to loopback; configure/verify inbound peer rules from the actual node config.
3. Verify MongoDB authentication and least-privilege write access using a throwaway database. Do not have the installer rewrite `mongod.conf`.
4. Test `ExecStartPre` under source/Mongo outages and recovery. Verify restart rate limits and reset procedure without a storm.
5. Test SIGTERM, SIGKILL, process crash, reboot, MongoDB restart, xrpld disconnect/reconnect, and a stopped/recovered worker. Confirm backfill resumes and tail catches up.

**Exit gate:** all transitions produce explicit logs/status and bounded recovery without data loss or duplicate/incomplete commits.

## Phase 4 — Ingestion correctness and cross-host behavior

**Status: Existing reliability features present; Linux qualification not started.**

1. Use isolated databases and bounded real ledgers. Run historical backfill and live tail concurrently.
2. Create and repair known missing historical ranges; independently verify ledger hashes, transaction hashes/counts, metadata, and contiguous verified progress.
3. Test lease expiry, fencing, stale worker rejection, and concurrent multi-host claims if more than one Linux host will share the database. If multi-host mode is unsupported, fail closed/document single-host ownership.
4. Exercise source range gaps, disconnect/reconnect, and MongoDB errors while collecting structured evidence.

**Exit gate:** zero unexplained missing validated ledgers, transaction mismatches, incomplete bundles, or unfenced stale writes in the bounded test.

## Phase 5 — Upgrade, rollback, and endurance

**Status: Not started.**

1. Produce platform-appropriate, reproducible Linux dependency constraints/lock data and test clean environment creation.
2. Implement/test an upgrade workflow that keeps a known-good code/dependency environment and restores it without touching MongoDB or XRPL data.
3. Run 24-hour and 72-hour native Linux endurance tests with automatic JSONL evidence for source errors, checkpoints, verified progress, lag, failed ledgers, restarts, CPU/memory, and termination.

**Exit gate:** successful rollback drill and both endurance runs with no unexplained data discrepancies or stalled progress.

## Release gate

Production remains **No-Go** until all critical findings in [`LINUX_BUG_REGISTER.md`](./LINUX_BUG_REGISTER.md) are closed and [`PRODUCTION_READINESS.md`](./PRODUCTION_READINESS.md) is supported by native Linux execution evidence. Windows/WSL results do not satisfy any native Linux release gate.
