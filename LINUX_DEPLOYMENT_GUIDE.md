# Linux Deployment Guide

## Status and supported scope

This is a **candidate deployment procedure**, not a production certification. Native installation and recovery tests are pending; Windows/WSL results do not count as Linux production evidence.

Automatic installer matrix:

- Ubuntu 20.04, 22.04, or 24.04 on x86_64. The installer can install MongoDB 8.0 from the supported Ubuntu apt repository.
- Debian 11 or 12 on x86_64. The installer supports the XRPL node package but does not install MongoDB; provide an existing local MongoDB or external MongoDB URI.
- Other Linux releases, ARM64, RHEL-family systems, and WSL are outside automatic-installer certification.

See [`LINUX_PRODUCTION_AUDIT.md`](./LINUX_PRODUCTION_AUDIT.md) and [`PRODUCTION_READINESS.md`](./PRODUCTION_READINESS.md) before any production use.

## Before installation

1. Use a disposable host/database for qualification. Do not run installer or failure testing against production MongoDB.
2. Record the OS release, systemd version, XRPL package version, MongoDB version, and current service state.
3. Keep the existing XRPL node and MongoDB configurations under operator control. This installer does not configure MongoDB authentication or firewall rules.
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
sudo systemctl enable --now xrpl-tail
sudo systemctl start xrpl-backfill
sudo systemctl status xrpl-tail xrpl-backfill
journalctl -u xrpl-tail -f
```

`xrpl-backfill` is a one-shot service; it is not enabled at boot by default. Backfill range configuration and MongoDB checkpoint state determine the next run. Do not change collection names, chunk sizing, or checkpoint formats during deployment.

For graceful stop and diagnosis:

```bash
sudo systemctl stop xrpl-backfill xrpl-tail
sudo systemctl status xrpl-tail xrpl-backfill
journalctl -u xrpl-tail -n 200 --no-pager
journalctl -u xrpl-backfill -n 200 --no-pager
```

The stop command sends SIGTERM. SIGKILL and reboot recovery are not yet qualified natively. `health`, `status`, `recover-gaps`, and `reconcile` provide application-level checks; an external monitor and alert route are not installed.

## Network and MongoDB controls

- Permit only required outbound access from the ingestion host to the configured MongoDB endpoint and XRPL source endpoints.
- Configure inbound XRPL peer traffic only if this node is expected to accept peers; derive the port from the actual node config.
- Keep the node's admin WebSocket bound to loopback or a separately authenticated, restricted network.
- Enable MongoDB authentication and restrict its bind/firewall rules using the MongoDB operator's approved procedure. The RippleSync installer deliberately does not edit `mongod.conf`.
- Verify credentials/authorization and expected network identity with `doctor`; do not rely on port-open checks alone.

## Upgrade cautions

The current dependency lock is Windows-specific and `install.sh` upgrades the active virtual environment in place. Until a Linux lock and tested rollback workflow exist, do not treat in-place `pip install -r requirements.txt` as a production-safe upgrade. See [`LINUX_ROLLBACK_GUIDE.md`](./LINUX_ROLLBACK_GUIDE.md).
