# SETUP

The setup guide is now part of **[README.md](README.md)**. This file only points to the right sections.

| Topic | Section in README.md |
|---|---|
| Hardware, versions (xrpld 3.4.x, MongoDB 7.0/8.0, Python 3.10+) | [Requirements](README.md#requirements) |
| Linux / macOS / Windows / WSL2: what runs where | [Which operating system for what](README.md#which-operating-system-for-what) |
| Install xrpld (apt, dnf/yum, `setup/setup_ubuntu.sh`) | [Step 1](README.md#step-1-install-the-node-xrpld) |
| Full history config (NuDB, XFS, `[ledger_history]`, `[ips_fixed]`, port 6006, firewall) | [Step 2](README.md#step-2-configure-the-node-for-full-history) |
| Wait for sync, `server_state`, `complete_ledgers` | [Step 3](README.md#step-3-wait-for-the-node-and-check-it) |
| MongoDB install, authentication, storage | [Step 4](README.md#step-4-install-mongodb) |
| Python, venv, `.env`, all settings, `doctor` | [Step 5](README.md#step-5-install-this-program) |
| Start order, range file, commands, exit codes, stopping, logs | [Step 6](README.md#step-6-start-ingesting) |
| systemd units (`deploy/systemd/`), Windows Task Scheduler (`deploy/windows/`) | [Step 7](README.md#step-7-run-as-a-service) |
| Collections, fields, "nothing is dropped" | [Data model](README.md#data-model-nothing-is-dropped) |
| verify / repair / retry-failed / health | [Checking that data is complete](README.md#checking-that-data-is-complete) |
| Common errors | [Troubleshooting](README.md#troubleshooting) |
| Upgrades and backups | [Upgrading and backups](README.md#upgrading-and-backups) |
| What is not tested on a real node yet | [Not verified yet](README.md#not-verified-yet) |
