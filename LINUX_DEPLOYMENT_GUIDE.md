# Linux Deployment Guide

## Status and supported scope

This is a **candidate deployment procedure**, not a production certification. Native installation and recovery tests are pending; Windows/WSL results do not count as Linux production evidence.

Automatic installer matrix:

- Ubuntu 20.04, 22.04, or 24.04 on x86_64. The installer can install MongoDB 8.0 from the supported Ubuntu apt repository.
- Debian 11 or 12 on x86_64. The installer supports the XRPL node package but does not install MongoDB; provide an existing local MongoDB or external MongoDB URI.
- Other Linux releases, ARM64, RHEL-family systems, and WSL are outside automatic-installer certification.

See [`LINUX_PRODUCTION_AUDIT.md`](./LINUX_PRODUCTION_AUDIT.md) and [`PRODUCTION_READINESS.md`](./PRODUCTION_READINESS.md) before any production use.
The recovery, scheduled reconciliation, monitoring, database tail-ownership lease, and replacement-environment paths have source-level/unit coverage only. None has passed native Ubuntu qualification.

## Before installation

1. Use a disposable host/database for qualification. Do not run installer or failure testing against production MongoDB.
2. Record the OS release, systemd version, XRPL package version, MongoDB version, and current service state.
3. Keep the existing XRPL node and MongoDB configurations under operator control. This installer does not configure MongoDB authentication or firewall rules; least-privilege and firewall validation remain operator/testbed work.
4. Ensure an XRPL node WebSocket admin endpoint is loopback-only and that `EXPECTED_NETWORK_ID` is set to the intended network. Installer preflight refuses a non-loopback or ambiguous existing admin WebSocket stanza without changing it. Never mix testnet/altnet endpoints with mainnet.
5. On Debian, configure `MONGO_URI` or `MONGO_URI_FILE` for a usable existing/external MongoDB before installation.
6. Use an unprivileged user. The installer invokes sudo for system operations.

## Candidate install procedure

```bash
bash install.sh --check
bash install.sh --mode test
```

The `test` mode writes to the isolated `xrpl_test` database. Confirm `doctor`, then run bounded backfill and source-based verification before considering server configuration.

For a full-history node, first prepare and validate the node database storage separately, then run:

```bash
bash install.sh --mode server
```

The installer asks before proceeding. `--yes` accepts defaults but does **not** bypass config conflict checks. If an XRPL managed stanza conflicts or is duplicated, the installer stops before shutting down `xrpld` or changing the node database directory ownership. Review the stanza manually. `setup/edit_cfg.py --overwrite-existing` is a deliberate manual operation, not an unattended setup option.

The installer preserves existing differing XRPL/MongoDB apt source/key files and local systemd units. It does not overwrite them; resolve the conflict explicitly and rerun. MongoDB key trust is currently Trust-On-First-Use and needs a separate supply-chain review.

### Service account

By default the units run as the user who invoked the installer. To select an existing dedicated non-root account, set `XRPL_INGEST_SERVICE_USER` for the install command. The installer verifies that the account exists and can traverse/read the repo, read `.env` if present, and write the repo for existing locks, stop control, and optional logs. It does not create the account or ACLs.

Before installing units, validate account access without displaying configuration contents:

```bash
sudo -u xrpl-ingest test -r /path/to/ripplesync/.venv/bin/python
sudo -u xrpl-ingest test -r /path/to/ripplesync/.env
sudo -u xrpl-ingest test -w /path/to/ripplesync
```

Do not weaken `.env` permissions globally to make a service account work. Use an operator-approved ownership/ACL design and rerun the installer. Do not print or copy `.env` values into tickets or test artifacts.

## Readiness and operation

Before enabling services, verify:

```bash
cd /path/to/ripplesync
.venv/bin/python -m xrpl_ingest doctor --mongo --node
.venv/bin/python -m xrpl_ingest status
.venv/bin/python -m xrpl_ingest health
```

The service units call `doctor --mongo --node` in `ExecStartPre` on every start. A preflight failure prevents ingestion from starting. A five-start/one-minute systemd limit and a 30-second restart delay bound repeated failures while allowing longer outages to be retried after the rate window resets.

After a reviewed test run:

```bash
sudo systemctl enable --now xrpl-tail xrpl-recover
sudo systemctl start xrpl-backfill
sudo systemctl enable --now xrpl-monitor.timer
```

For independent reconciliation, configure a fixed inclusive range before enabling its timer:

```bash
cp reconcile_range.txt.example reconcile_range.txt
# Edit start/end; keep this range within the source node's retained ledgers.
sudo systemctl enable --now xrpl-reconcile.timer
sudo systemctl status xrpl-tail xrpl-recover xrpl-backfill xrpl-monitor.timer
journalctl -u xrpl-tail -f
```

`xrpl-backfill` is a one-shot service; it is not enabled at boot by default. `xrpl-recover` continuously claims and verifies persisted recovery jobs. `xrpl-reconcile.timer` runs the explicit bounded range once every 15 minutes. Its last pass is recorded in existing Mongo metadata under `kind=reconciliation_range`, including checked and uncovered intervals; `reconcile` exits nonzero if the requested interval is not fully source-covered or verified. Do not change collection names, chunk sizing, or checkpoint formats during deployment.

The one-minute `xrpl-monitor.timer` checks for failed tail/backfill/recovery/reconciliation units and runs the tail `health` check. Alerts are written to the system journal only; no external notification route is configured. Review with `journalctl -u xrpl-monitor.service`.

The live tail also claims a MongoDB-backed singleton lease, renewed while it runs, to prevent ordinary cross-host duplicate owners. Recovery jobs use fencing tokens and check lease ownership before writes. Tail ledger upserts are idempotent and checkpoints are monotonic, but the lease check and each individual write are not one MongoDB transaction. A paused stale process may finish an already-started write after lease takeover; cross-host pause/partition races have not been qualified. Run one tail owner only until native multi-host tests demonstrate the desired failure guarantees.

For graceful stop and diagnosis:

```bash
sudo systemctl stop xrpl-reconcile.timer xrpl-backfill xrpl-recover xrpl-tail
sudo systemctl status xrpl-tail xrpl-backfill xrpl-recover xrpl-reconcile.timer
journalctl -u xrpl-tail -n 200 --no-pager
journalctl -u xrpl-backfill -n 200 --no-pager
journalctl -u xrpl-recover -n 200 --no-pager
```

The stop command sends SIGTERM. SIGKILL and reboot recovery are not yet qualified natively. `health`, `status`, `recover-gaps`, `reconcile`, and `xrpl-monitor.timer` provide application/local-journal checks; external monitoring and alert delivery are not installed.

## Network and MongoDB controls

- Permit only required outbound access from the ingestion host to the configured MongoDB endpoint and XRPL source endpoints.
- Configure inbound XRPL peer traffic only if this node is expected to accept peers; derive the port from the actual node config.
- Keep the node's admin WebSocket bound to loopback or a separately authenticated, restricted network.
- Enable MongoDB authentication and restrict its bind/firewall rules using the MongoDB operator's approved procedure. The RippleSync installer deliberately does not edit `mongod.conf`.
- Verify credentials/authorization and expected network identity with `doctor`; do not rely on port-open checks alone.

## Upgrade and rollback

On Linux with CPython 3.12 x86_64, the installer uses [`requirements-linux-py312-x86_64.lock`](./requirements-linux-py312-x86_64.lock), which pins versions and wheel hashes. The lock's wheel set was resolved for manylinux 2.17 from this Windows environment; it has not yet been installed on native Ubuntu. Other explicitly accepted Python versions use the range-based `requirements.txt` and are not covered by that Linux lock.

The installer builds a candidate `.venv` and checks package consistency before replacing the active environment. It refuses to switch while an ingestion/monitor service or scheduled timer is active and preserves the previous environment in `.venv-backup.*`. Stop services and timers before upgrades. If a candidate fails, the active environment is left untouched; after a successful switch, rollback by stopping services/timers, moving `.venv` aside, and moving the selected backup's `.venv` back into place. The candidate/backup flow has stub-based source tests only; interruption and rollback have not been qualified on native Linux. See [`LINUX_ROLLBACK_GUIDE.md`](./LINUX_ROLLBACK_GUIDE.md).
