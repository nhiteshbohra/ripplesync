# XRPL ingest: full XRP Ledger history into MongoDB

This program copies every ledger and every transaction of the XRP Ledger (XRPL) into MongoDB.
It reads from **your own xrpld node**, not from public servers.
It can copy the old history ("backfill") and follow new ledgers live ("tail").

This README is the complete guide: hardware, install, configuration, running, services, data model and troubleshooting.
Read the [Requirements](#requirements) first. Full history is very large (about 39 TB for the node alone).

> **Status:** the code is tested with unit tests, a fake node and a local MongoDB.
> It has **not yet run against a real xrpld node**. See [Not verified yet](#not-verified-yet).
> Text marked **UNVERIFIED** comes from documentation or reasoning, not from a test on real hardware.

## Table of contents

1. [What it does](#what-it-does)
2. [How it works](#how-it-works)
3. [Quick start](#quick-start)
4. [Requirements](#requirements)
5. [Which operating system for what](#which-operating-system-for-what)
6. [Step 1: Install the node (xrpld)](#step-1-install-the-node-xrpld)
7. [Step 2: Configure the node for full history](#step-2-configure-the-node-for-full-history)
8. [Step 3: Wait for the node and check it](#step-3-wait-for-the-node-and-check-it)
9. [Step 4: Install MongoDB](#step-4-install-mongodb)
10. [Step 5: Install this program](#step-5-install-this-program)
11. [Step 6: Start ingesting](#step-6-start-ingesting)
12. [Step 7: Run as a service](#step-7-run-as-a-service)
13. [Data model: nothing is dropped](#data-model-nothing-is-dropped)
14. [Checking that data is complete](#checking-that-data-is-complete)
15. [Troubleshooting](#troubleshooting)
16. [Upgrading and backups](#upgrading-and-backups)
17. [Not verified yet](#not-verified-yet)
18. [Official sources](#official-sources)

---

## What it does

- **backfill**: copies old ledgers (from ledger 32570, the first ledger that exists on mainnet) into MongoDB. It works in chunks and can be stopped and restarted. Finished chunks are not fetched again.
- **tail**: follows new validated ledgers as they appear, and stores a checkpoint so it can continue after a restart.
- **verify / retry-failed / fetch**: find missing data and fetch it again.
- **doctor / health / status / config**: check the setup and watch progress.

Every field the node sends is stored. Nothing is dropped. See [Data model](#data-model-nothing-is-dropped).

Why a new program? The old scripts (no longer in this repository; still in its git history) used **public** XRPL servers.
Public servers limit how many requests one client may send (rate limits). The old scripts lost data because of these limits.
The problem was the public servers, not websockets. This program still uses websockets, but it talks to your own node as an **admin** client.
xrpld does not rate-limit admin clients ([xrpl.org: Rate Limiting](https://xrpl.org/docs/references/http-websocket-apis/api-conventions/rate-limiting)).

## How it works

```
 XRPL peer-to-peer network
          |
          |  peer port 2459/tcp (older configs: 51235)
          v
 +----------------------+
 | xrpld  (your node)   |   full history in NuDB (~39 TB, XFS disk)
 | same machine         |
 +----------------------+
          |
          |  local ADMIN websocket  ws://127.0.0.1:6006
          |  JSON, API version 2, one "ledger" request per ledger
          |  (transactions + metadata included)
          v
 +----------------------+
 | xrpl_ingest          |   backfill: many processes, chunks of 1000 ledgers
 | (this program)       |   tail:     follows the newest validated ledger
 +----------------------+
          |
          |  pymongo, idempotent upserts
          |  order: transactions -> metadata -> ledger document LAST
          v
 +----------------------+
 | MongoDB 7.0 / 8.0    |   ledgers, transactions, metadata+memos,
 |                      |   failed ledgers, checkpoint + chunk progress
 +----------------------+
```

Important ideas:

- **One request per ledger.** The node returns the ledger header, all transactions and their metadata in one answer.
- **Ledger document last.** The ledger document is written only after all its transactions. If it exists, the ledger is complete. A crash in the middle is repaired by running again.
- **Upserts.** Writing the same ledger twice gives the same result. You can always re-run a command safely.
- **Network guard.** The program refuses a server that reports another network (for example testnet). This stops testnet data from mixing with mainnet data, a bug of the old scripts.

## Quick start

For people who already know Linux, xrpld and MongoDB. Everyone else: follow Steps 1 to 7.

```bash
# 0. Get the code (or download a ZIP: see Step 5.2)
git clone https://github.com/nhiteshbohra/ripplesync.git
cd ripplesync
# 1-2. Node: install xrpld, configure full history, start it (Ubuntu)
DB_PATH=/data/xrpld/db/nudb HISTORY_MODE=full bash setup/setup_ubuntu.sh node
# 3. Wait until server_state is "full"
curl -s http://127.0.0.1:5005 -d '{"method":"server_info"}' | python3 -m json.tool | grep -E '"(server_state|complete_ledgers)"'
# 4. MongoDB 8.0 (see Step 4), then this program (venv, .env, tests, systemd units):
bash setup/setup_ubuntu.sh ingest
source .venv/bin/activate
python -m xrpl_ingest doctor                               # everything OK?
python -m xrpl_ingest backfill --start 32570 --end 33569   # small trial (1000 ledgers)
python -m xrpl_ingest verify   --start 32570 --end 33569
cp backfill_range.txt.example backfill_range.txt           # full range: 32570 .. latest
sudo systemctl enable --now xrpl-tail                      # live ledgers, from day 1
sudo systemctl start xrpl-backfill                         # full history (resumable)
python -m xrpl_ingest status                               # progress and ETA
```

---

## Requirements

### Hardware for a full-history node

These are the official xrpld recommendations ([System Requirements](https://xrpl.org/docs/infrastructure/installation/system-requirements), [Configure Full History](https://xrpl.org/docs/infrastructure/configuration/data-retention/configure-full-history)):

| Item | Recommendation | Why |
|---|---|---|
| CPU | 3+ GHz x86_64, **8+ cores** | The node checks every transaction of the network. |
| RAM | **64 GB** | Needed for `node_size = huge`. Less RAM means the node may not stay in sync. |
| Disk type | SSD / **NVMe, 10,000 IOPS sustained** (not burst) | Slow disks are the most common reason a node does not sync. |
| Disk size | **~39 TB** for full history (as of 2026-01-25), growing **~12 GB per day** | Buy extra space for growth. |
| Filesystem | **XFS** (recommended) or ZFS. **Not ext4.** | NuDB creates single files larger than 16 TB. ext4 cannot hold a file larger than 16 TB. |
| Cloud disks | **Do not use AWS EBS** | Its latency is too high to sync reliably. |
| Network | Gigabit interface, data-center quality | Busy periods can fill a 100 Mbit/s link. |
| Time | NTP (`chrony` or `ntpd`) | The node needs the correct time. |

MongoDB needs **its own fast disk** too. Do not put MongoDB on the same disk as NuDB.
How big the MongoDB database gets for full history is **UNVERIFIED** (not measured yet). Measure it after a trial run (see [Step 6](#step-6-start-ingesting)) and plan many TB.

The number of ledgers is large: xrpl.org listed "81,000,000+" ledgers for full history already in July 2023 ([Capacity Planning](https://xrpl.org/docs/infrastructure/installation/capacity-planning)).

### Software versions

| Software | Version | Notes |
|---|---|---|
| xrpld | **3.4.x** (latest release 3.4.1, 2026-10-10) | `rippled` was renamed `xrpld` in 3.2.0. Old guides say `rippled`. |
| MongoDB | **7.0 or 8.0** | **Not 9.0.** Our pinned driver (`pymongo>=4.9,<4.11`) fully supports 7.0 and 8.0. MongoDB 9.0 needs pymongo 4.17 or newer (4.18 for full support) ([driver compatibility](https://www.mongodb.com/docs/drivers/compatibility/?driver-language=python&python-driver-framework=pymongo)). The cap `<4.11` exists because the test library `mongomock` breaks on newer pymongo. 7.0 ends support 2027-08-31, 8.0 ends 2029-10-31. **Use 8.0 for new installs.** `doctor` warns if the server is not 7.0 or 8.0. |
| Python | **3.10 or newer** | 3.12 is the tested and recommended version. |
| Python packages | from `requirements.txt` only | `pymongo`, `websockets`, `python-dotenv`, plus `pytest`, `mongomock` for tests. |

## Which operating system for what

There are three parts: the **node** (xrpld), **MongoDB**, and **this program**. They can run on different machines.

| | Linux server (x86_64) | macOS | Windows | WSL2 (Linux inside Windows) |
|---|---|---|---|---|
| **xrpld node** | **Yes.** Ubuntu 20.04+ / Debian 11+ via `apt`; RHEL / Rocky / Alma 9+ via `dnf`/`yum` | Development only (build from source) | Development only (build from source; no Windows package) | Testing only. **Not for full history** (see below) |
| **MongoDB 7.0/8.0** | Yes | Yes (Homebrew) | Yes (MSI installer, Windows service) | **Not supported by MongoDB** |
| **This program** | Yes (systemd units in `deploy/systemd/`) | Yes (run by hand; no service template) | Yes (Task Scheduler scripts in `deploy/windows/`) | Yes |

**Recommended production setup:** one Linux server (Ubuntu 24.04 LTS) for xrpld, and MongoDB on the same server on a separate disk, or on a second Linux server.
On macOS the program runs fine from a terminal, but the node there is only for development.

**WSL2 caveats:**

- The Linux disk of WSL2 is an ext4 filesystem inside a virtual disk file (VHDX). ext4 has the 16 TB single-file limit, so a full-history NuDB cannot work there.
- A database on `/mnt/c/...` (a Windows drive) is far too slow for NuDB.
- MongoDB says it is not supported on WSL ([MongoDB Windows install page](https://www.mongodb.com/docs/manual/administration/install-community-windows/)). If you use WSL, install MongoDB on Windows itself and connect from WSL.
- WSL networking: to reach a Windows service at `127.0.0.1` from inside WSL, use mirrored networking (Windows 11 22H2 or later; see [Troubleshooting](#troubleshooting)). **UNVERIFIED** with this project.
- systemd inside WSL must be switched on (`/etc/wsl.conf`: `[boot]` then `systemd=true`, then `wsl --shutdown`).

---

## Step 1: Install the node (xrpld)

Do this on the Linux server. You need a user with `sudo`.
The commands below are copied from the official pages ([Ubuntu/Debian](https://xrpl.org/docs/infrastructure/installation/install-xrpld-on-ubuntu), [RHEL](https://xrpl.org/docs/infrastructure/installation/install-xrpld-on-rhel)).
The packages need glibc 2.31 or newer.

### Option A: Ubuntu 20.04+ / Debian 11+ (apt)

1. Update the package list and install helper tools.

   ```bash
   sudo apt -y update
   sudo apt -y install apt-transport-https ca-certificates wget gnupg
   ```

2. Download the signing key of the XRP Ledger Foundation (XRPLF).

   ```bash
   sudo install -m 0755 -d /etc/apt/keyrings && \
       sudo wget -qO /etc/apt/keyrings/xrplf.asc https://packages.xrplf.org/xrplf.asc
   ```

3. **Check the key fingerprint.** Why: this proves the key is the real XRPLF key and not a fake one.

   ```bash
   gpg --show-keys /etc/apt/keyrings/xrplf.asc
   ```

   The output must show exactly this fingerprint on the second line:

   ```
   pub   rsa4096 2026-08-18 [SC]
         B655416741221F780FBCFBC9AA84D41A11D29FA9
   uid                      XRPLF Packages <distribution@xrplf.org>
   ```

   If the fingerprint is different, **stop**. Do not continue.

4. Add the package repository. The line is the same for every Ubuntu and Debian version (the suite is `any`).

   ```bash
   echo "deb [signed-by=/etc/apt/keyrings/xrplf.asc] https://packages.xrplf.org/repository/deb-stable any main" | \
       sudo tee /etc/apt/sources.list.d/xrplf.list
   ```

5. Install xrpld.

   ```bash
   sudo apt -y update && sudo apt -y install xrpld
   ```

6. Check the service. It starts automatically with the default config.

   ```bash
   systemctl status xrpld.service
   ```

Only use the `deb-stable` channel. Other channels (`deb-rc`, `deb-beta`, `deb-develop`) can break at any time.

### Option B: RHEL / Rocky Linux / AlmaLinux 9+ (dnf / yum)

On RHEL 9, `yum` is the same tool as `dnf`. The official page uses `yum`.

```bash
sudo rpm --import https://packages.xrplf.org/xrplf.asc
cat << REPOFILE | sudo tee /etc/yum.repos.d/xrplf.repo
[xrplf]
baseurl=https://packages.xrplf.org/repository/rpm-stable/x86_64/
gpgkey=https://packages.xrplf.org/xrplf.asc
gpgcheck=1
REPOFILE
sudo yum -y update
sudo yum install xrpld
sudo systemctl daemon-reload
sudo systemctl enable xrpld.service
sudo systemctl start xrpld.service
```

### Option C: the setup script (Ubuntu)

The script `setup/setup_ubuntu.sh` does Option A plus Step 2 for you (Ubuntu 20.04+ / Debian 11+, x86_64, glibc 2.31+). It makes a backup of the config first.

```bash
bash setup/setup_ubuntu.sh check        # read-only preflight: OS, glibc, RAM, disk, filesystem. Changes nothing.
DB_PATH=/data/xrpld/db/nudb HISTORY_MODE=full bash setup/setup_ubuntu.sh node
# later (Step 5):  bash setup/setup_ubuntu.sh ingest      # or: all = node + ingest
```

What `node` does: installs xrpld from the XRPLF apt repo, **checks the key fingerprint itself** (it refuses a key that is not `B655416741221F780FBCFBC9AA84D41A11D29FA9`), edits `/etc/xrpld/xrpld.cfg` (backup first), starts xrpld and prints the firewall note.

Useful variables: `DB_PATH` (NuDB folder on the big XFS disk; default `/var/lib/xrpld/db/nudb`), `HISTORY_MODE=full|pruned` (default `full`), `NODE_SIZE` (`tiny small medium large huge`; default `huge`), `FIXED_PEERS="host port;host port"`, `FORCE=1` (continue past failed checks).
`DB_PATH` must be a dedicated folder. The script runs `chown -R` on it, so `setup/db_path_guard.sh` refuses system folders and anything under `/home`, `/root` or `/run/user` (the xrpld service cannot see those, because of `ProtectHome=true`).
Shared helper code is in `setup/common.sh`.
**UNVERIFIED:** the scripts are not tested on a real host yet (only `bash -n` and shellcheck).

`setup_all.sh` (repo root) installs MongoDB 8.0 + xrpld + this program in one go. It defaults to a small **pruned** node (`HISTORY_MODE=pruned`, `NODE_SIZE=medium`) for testing; set `HISTORY_MODE=full` for this project. On WSL it skips MongoDB (not supported there) and prints how to use MongoDB on Windows. It overlaps with `setup/setup_ubuntu.sh`.

### Files the package installs

| What | Where |
|---|---|
| Program | `/usr/bin/xrpld` |
| Config | `/etc/xrpld/xrpld.cfg`, `/etc/xrpld/validators.txt` |
| Data (default) | `/var/lib/xrpld` |
| Log | `/var/log/xrpld/debug.log` |
| Service | `xrpld.service`, runs as user `xrpld` |

The service has `ProtectHome=true` and `ProtectSystem=full`. So do **not** put the database under `/home`.

---

## Step 2: Configure the node for full history

By default xrpld keeps only recent ledgers and deletes old ones. For this project it must keep **everything**.
Source: [Configure Full History](https://xrpl.org/docs/infrastructure/configuration/data-retention/configure-full-history).

### 2.1 Prepare the big disk (XFS)

Full history needs ~39 TB on XFS. Usually this is several NVMe drives combined (RAID or LVM). That part depends on your hardware.

**Warning: `mkfs` erases the disk.** Check the device name twice with `lsblk`.

```bash
lsblk                                   # find the big empty device, e.g. /dev/md0
sudo mkfs.xfs /dev/md0                  # ERASES /dev/md0
sudo mkdir -p /data
echo "UUID=$(sudo blkid -s UUID -o value /dev/md0) /data xfs defaults,noatime 0 0" | sudo tee -a /etc/fstab
sudo mount -a
df -hT /data                            # must show "xfs" and the full size
sudo mkdir -p /data/xrpld/db/nudb
sudo chown -R xrpld:xrpld /data/xrpld
```

### 2.2 Edit the config file

1. Stop the node and back up the config. Why stop: the node must not run while its database settings change.

   ```bash
   sudo systemctl stop xrpld
   sudo cp /etc/xrpld/xrpld.cfg /etc/xrpld/xrpld.cfg.bak.$(date +%F)
   sudo nano /etc/xrpld/xrpld.cfg
   ```

2. Change these stanzas. A stanza is a `[name]` line plus the lines below it.
   **Each stanza name must appear only once.** Replace the existing one. Do not add a second copy.

   ```ini
   [node_db]
   type=NuDB
   path=/data/xrpld/db/nudb
   nudb_block_size=4096
   # online_delete and advisory_delete are REMOVED for full history.
   # The packaged default has online_delete=512 -> it would delete history.

   [database_path]
   /data/xrpld/db

   [ledger_history]
   full

   [node_size]
   huge

   [ips_fixed]
   169.55.164.20 51235
   50.22.123.215 51235
   ```

   What each part does:

   - `[node_db]`: use **NuDB** (RocksDB needs too much RAM at this size). `path` points to the big XFS disk. **Delete the `online_delete` and `advisory_delete` lines**, otherwise the node deletes old ledgers.
   - `[database_path]`: the node's SQLite files. Putting them on the same big disk is our advice. The official page says: if you change `path`, change `[database_path]` too, or the node may fail to start.
   - `[ledger_history] full`: keep and download all history.
   - `[node_size]`: match your RAM (table below).
   - `[ips_fixed]`: peers that already have full history. Your node can only download old ledgers from a direct peer that has them. The two IPs are the official examples from xrpl.org. They may be out of date.
     **Tip from xrpl.org:** resolve `s2.ripple.com` a few times (`dig +short s2.ripple.com`) to get the IP addresses of Ripple's public full-history servers. Use them in `[ips_fixed]` with port `51235`. Ripple limits access to these servers; do not abuse them.

   `node_size` values ([Capacity Planning](https://xrpl.org/docs/infrastructure/installation/capacity-planning)):

   | RAM available | `node_size` | Notes |
   |---|---|---|
   | < 8 GB | `tiny` | Not recommended. May not sync. |
   | 8 GB | `small` | Test servers only. |
   | 16 GB | `medium` | Value in the example config. |
   | 32 GB | `large` | Not recommended. Often worse than `huge`. |
   | 64 GB | `huge` | **Recommended for production.** |

3. Check the admin websocket. This program connects here. The 3.4.1 default config already contains it. Make sure it is there and listed under `[server]`:

   ```ini
   [server]
   port_rpc_admin_local
   port_peer
   port_ws_admin_local

   [port_ws_admin_local]
   port = 6006
   ip = 127.0.0.1
   admin = 127.0.0.1
   protocol = ws
   send_queue_limit = 500
   ```

   `ip = 127.0.0.1` means only programs on the same machine can connect. `admin = 127.0.0.1` makes them admin clients (no rate limits).
   Keep it like this. Do not open port 6006 to the internet.
   Port `5005` (`port_rpc_admin_local`, HTTP) is used by `xrpld server_info` and `curl` checks.

4. Optional: name the network. Add:

   ```ini
   [network_id]
   main
   ```

   `main` means network id `0` (mainnet). Why: xrpld only reports `network_id` in `server_info` when this stanza is set.
   This program checks that the node's `network_id` equals `EXPECTED_NETWORK_ID` (default `0`). If the node reports no `network_id`, the program accepts it and logs a warning once.
   With `[network_id] main` the check really protects you.

5. If you ran the node before with online deletion, delete its old database files. They are ignored after the change anyway. **Only do this if the folder holds nothing else:**

   ```bash
   sudo rm -r /var/lib/xrpld/db/*        # only the OLD default location, and only if unused
   ```

### 2.3 Firewall

Open the **peer port** so other servers can connect to you. Why: with a closed port you get only about 10 peers, and history download is slower.

- xrpld 3.4.x default config: peer port **2459/tcp**. Older configs use **51235/tcp**. Check yours:

  ```bash
  grep -A3 '^\[port_peer\]' /etc/xrpld/xrpld.cfg
  ```

- Ubuntu (ufw): `sudo ufw allow 2459/tcp`
- RHEL (firewalld): `sudo firewall-cmd --zone=public --permanent --add-port=2459/tcp && sudo firewall-cmd --reload`
- Behind a router (NAT): forward the port to the server.
- **Never open 5005, 6006 or 50051.** These are local admin ports.

Source: [Forward Ports for Peering](https://xrpl.org/docs/infrastructure/configuration/peering/forward-ports-for-peering).

### 2.4 Start the node

```bash
sudo systemctl enable --now xrpld
sudo journalctl -u xrpld -f            # Ctrl+C to leave the log view
```

If it does not start: `sudo journalctl -u xrpld -n 100`. Usual causes: wrong path, wrong owner of the data folder, a stanza that appears twice.

---

## Step 3: Wait for the node and check it

### 3.1 What the node does now, and why you wait

After start, the node finds peers, loads the validator list, and downloads the newest ledger.
This normally takes **5 to 15 minutes** ([Server Doesn't Sync](https://xrpl.org/docs/infrastructure/troubleshooting/server-doesnt-sync)).
Until it is synced, it cannot answer reliably, so the program cannot ingest yet.

`server_state` values ([server states](https://xrpl.org/docs/references/http-websocket-apis/api-conventions/xrpld-server-states)):

| `server_state` | Meaning | What to do |
|---|---|---|
| `disconnected` | Not connected to the network. | Check internet, firewall, DNS. |
| `connected` | Connected, but not enough data yet. | Wait. |
| `syncing` | Behind, catching up. Normal for a few minutes. | Wait. |
| `tracking` | Agrees with the network. | Almost there. Wait. |
| **`full`** | **Fully caught up. This is the target.** | Ready. |
| `validating` / `proposing` | Validator states. Not expected for this node. | OK too. |

**Restart rule (from xrpl.org):** if the state is not `full` (or `proposing`) and `server_state_duration_us` is more than `900000000` (15 minutes), restart the node:

```bash
sudo systemctl restart xrpld
```

If it stays in `connected` for hours, or falls back to `connected` later, the machine is too slow. The usual cause is disk speed, then RAM, then network.

### 3.2 How to check

Use any of these. All are read-only.

```bash
# a) The node's own command line (talks to port 5005)
xrpld server_info

# b) curl, short output
curl -s http://127.0.0.1:5005 -d '{"method":"server_info"}' | python3 -m json.tool \
  | grep -E '"(server_state|server_state_duration_us|complete_ledgers|peers|network_id|build_version)"'

# c) This program (after Step 5): checks the node like the ingest will see it, over ws 6006
python -m xrpl_ingest doctor --node
```

Option c) is the best test, because it uses the same websocket and the same network check as the ingest.
It shows `server_state`, `network_id`, `complete_ledgers`, the validated ledger, peers and `build_version`.
A state other than `full`, `proposing` or `validating` is a warning (exit `1`).
`bash setup/check_node.sh` runs exactly `python -m xrpl_ingest doctor --node` (needs the venv from Step 5).

### 3.3 Reading `complete_ledgers`

`complete_ledgers` lists the ledgers the node has **completely** on disk. The program can only ingest these ledgers.

| Example value | Meaning |
|---|---|
| `empty` | Nothing complete yet. Normal in the first minutes. |
| `105000000-105000300` | Only recent ledgers. Old history not downloaded yet. |
| `32570-50000000,90000000-105000300` | History partly downloaded, with a gap. Still filling. |
| `32570-105000300` | **Full history complete:** starts at **32570** and has **no gaps** (one range, no comma). |

Why 32570: it is the earliest ledger that exists on mainnet. The first ledgers were lost in 2013 by a server bug.

### 3.4 Logs

```bash
sudo journalctl -u xrpld -f                  # service messages
sudo tail -f /var/log/xrpld/debug.log        # node log (level "warning" by default)
```

### 3.5 Why full history takes months

Downloading the whole history takes **several months** (xrpl.org).
Why: the node must fetch every old ledger from peers that have it, check it, and write it, while it also keeps up with new ledgers every few seconds.
Only a few peers have full history, and they share their bandwidth with others.
To speed it up, xrpl.org suggests getting a database dump from another full-history operator, or peering with them for a long time.

**You do not need to wait for the full history before you start ingesting.**
`backfill` takes the part of your range that the node already has. For the missing part it exits with code `3` ("node lacks part of range").
Run it again later. Finished chunks are skipped, so only new ledgers are fetched.

---

## Step 4: Install MongoDB

Install **MongoDB 8.0** (or 7.0). **Do not install 9.0** (our driver does not support it yet).
All commands come from the official MongoDB pages, see [Official sources](#official-sources).

### 4.1 Ubuntu (MongoDB 8.0, apt)

MongoDB 8.0 supports Ubuntu 20.04, 22.04 and 24.04 (x86_64). The Ubuntu package called `mongodb` is not the official one. Remove it if you have it.

```bash
sudo apt-get install -y gnupg curl
curl -fsSL https://pgp.mongodb.com/server-8.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
# Ubuntu 24.04 = noble, 22.04 = jammy, 20.04 = focal. Use your codename:
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" | \
   sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
sudo apt-get update
sudo apt-get install -y mongodb-org
sudo systemctl enable --now mongod
sudo systemctl status mongod
```

Optional, to stop apt from upgrading MongoDB by surprise:

```bash
echo "mongodb-org hold" | sudo dpkg --set-selections
echo "mongodb-org-server hold" | sudo dpkg --set-selections
```

### 4.2 RHEL / Rocky / Alma 9 (MongoDB 8.0, yum/dnf)

**UNVERIFIED:** the repo file below follows the pattern of the official 9.0 page with `9.0` changed to `8.0`. Compare with the [8.0 Red Hat page](https://www.mongodb.com/docs/v8.0/tutorial/install-mongodb-on-red-hat/) before use.

```bash
cat << 'REPOFILE' | sudo tee /etc/yum.repos.d/mongodb-org-8.0.repo
[mongodb-org-8.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/9/mongodb-org/8.0/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://pgp.mongodb.com/server-8.0.asc
REPOFILE
sudo yum install -y mongodb-org
sudo systemctl enable --now mongod
```

### 4.3 macOS (Homebrew)

Needs the Xcode command line tools and [Homebrew](https://brew.sh).
**UNVERIFIED:** the official page now shows `mongodb-community@9.0`; we use the same formula name with `@8.0`.

```bash
xcode-select --install
brew tap mongodb/brew
brew update
brew install mongodb-community@8.0
brew services start mongodb-community@8.0
```

### 4.4 Windows (MSI installer)

1. Download the **MongoDB Community Server** `.msi` from [mongodb.com/try/download/community](https://www.mongodb.com/try/download/community). Choose version **8.0**, platform Windows, package `msi`.
2. Double-click the `.msi`. Choose **Complete**.
3. Keep **"Install MongoD as a Service"** checked. The service is named `MongoDB` and starts after install.
4. Install the shell `mongosh` separately ([mongosh install](https://www.mongodb.com/docs/mongodb-shell/install/)). The MSI does not include it.
5. The config file is `<install directory>\bin\mongod.cfg`. After editing it, restart the service:

   ```powershell
   Restart-Service MongoDB
   ```

On Ubuntu, `setup_all.sh` does 4.1 for you (MongoDB 8.0; it prints the MongoDB key fingerprint but cannot pin it, because MongoDB publishes none to compare with).

### 4.5 Enable authentication (password)

A fresh MongoDB has **no password**. Anyone who can reach port 27017 can read and delete data. Turn on authentication.
Source: [SCRAM authentication](https://www.mongodb.com/docs/manual/tutorial/configure-scram-client-authentication/).

1. Create an admin user and an ingest user. `passwordPrompt()` asks for the password, so it does not land in your shell history.

   ```bash
   mongosh
   ```

   ```javascript
   use admin
   db.createUser({ user: "admin", pwd: passwordPrompt(),
                   roles: [ { role: "userAdminAnyDatabase", db: "admin" },
                            { role: "readWriteAnyDatabase", db: "admin" } ] })
   db.createUser({ user: "xrpl_ingest", pwd: passwordPrompt(),
                   roles: [ { role: "readWrite", db: "xrpl_data_collection" } ] })
   exit
   ```

2. Switch on access control in the config file (`/etc/mongod.conf` on Linux, `mongod.cfg` on Windows):

   ```yaml
   security:
     authorization: enabled
   ```

3. Restart: `sudo systemctl restart mongod` (Windows: `Restart-Service MongoDB`).

4. Your connection string is now:

   ```
   mongodb://xrpl_ingest:<password>@127.0.0.1:27017/?authSource=admin
   ```

**Special characters in the password must be percent-encoded** in the URI. For example `@` becomes `%40`, `:` becomes `%3A`, `/` becomes `%2F`, `%` becomes `%25`.
This command encodes a password without showing it or saving it in history:

```bash
python3 -c "import getpass, urllib.parse; print(urllib.parse.quote(getpass.getpass('password: '), safe=''))"
```

Alternative: put the whole URI into a file and set `MONGO_URI_FILE` (see [Step 5](#step-5-install-this-program)).

### 4.6 Storage and memory advice

- **Filesystem:** MongoDB strongly recommends **XFS** for WiredTiger on Linux ([Production Notes](https://www.mongodb.com/docs/manual/administration/production-notes/)).
- **Separate disk:** do not share the disk with NuDB. Both need many IOPS.
- **Memory:** WiredTiger's cache uses by default the larger of 50% of (RAM - 1 GB) or 256 MB.
  If xrpld (`node_size huge`, 64 GB) and MongoDB share one machine, both want most of the RAM.
  Best: give MongoDB its own machine. Otherwise limit the cache in `mongod.conf`:

  ```yaml
  storage:
    wiredTiger:
      engineConfig:
        cacheSizeGB: 16     # example value, UNVERIFIED: tune after measuring
  ```

- **Listen address:** keep `net.bindIp: 127.0.0.1` unless the program runs on another machine. If you open it, use a firewall and authentication.

---

## Step 5: Install this program

The program runs on Linux, macOS and Windows. It needs Python 3.10 or newer (3.12 tested).

### 5.1 Get Python

| OS | Command |
|---|---|
| Ubuntu / Debian | `sudo apt-get install -y python3 python3-venv python3-pip`. Ubuntu 24.04 ships 3.12 (`python3.12 python3.12-venv`). `setup/setup_ubuntu.sh ingest` uses `python3.12` when it is on PATH, else `python3` (must be 3.10+). |
| RHEL / Rocky / Alma 9 | `sudo dnf install -y python3.12 python3.12-pip` (**UNVERIFIED** package names; `python3` 3.9 of RHEL 9 is too old) |
| macOS | `brew install python@3.12` |
| Windows | The installer from [python.org](https://www.python.org/downloads/windows/) (keep "py launcher" ticked), or `winget install Python.Python.3.12`. `deploy\windows\setup-windows.ps1` needs `py -3.12`. |

Note: for Python 3.12 the last Windows/macOS installer from python.org is 3.12.10. Newer 3.12 releases are source only. Python 3.13 should also work (not tested).

### 5.2 Get the code

Put the code on the machine's own disk. In WSL use the Linux home folder, not `/mnt/c/...`.

The repository is public: no GitHub account is needed to download it.

**Option A: `git clone` (recommended; later updates are one `git pull`)**

You need git: Ubuntu/Debian `sudo apt-get install -y git`, RHEL/Rocky `sudo dnf install -y git`, macOS `xcode-select --install`, Windows [git-scm.com](https://git-scm.com/download/win) or `winget install Git.Git`.

Linux / macOS / WSL:

```bash
git clone https://github.com/nhiteshbohra/ripplesync.git
cd ripplesync
```

Windows (PowerShell):

```powershell
git clone https://github.com/nhiteshbohra/ripplesync.git
cd ripplesync
```

With SSH instead of HTTPS (only if you have a GitHub account with an SSH key; HTTPS above needs nothing):

```bash
git clone git@github.com:nhiteshbohra/ripplesync.git
cd ripplesync
```

Get the newest version later (from inside the folder): `git pull`

**Option B: download a ZIP (no git needed)**

In a browser: open https://github.com/nhiteshbohra/ripplesync, click the green **Code** button, then **Download ZIP**. You get `ripplesync-main.zip`.

Or from the command line, download, unpack and go into the folder:

Linux / macOS / WSL (`sudo apt-get install -y unzip` if `unzip` is missing):

```bash
curl -L -o ripplesync-main.zip https://github.com/nhiteshbohra/ripplesync/archive/refs/heads/main.zip
unzip ripplesync-main.zip
cd ripplesync-main
```

Windows (PowerShell):

```powershell
Invoke-WebRequest https://github.com/nhiteshbohra/ripplesync/archive/refs/heads/main.zip -OutFile ripplesync-main.zip
Expand-Archive .\ripplesync-main.zip -DestinationPath .
cd .\ripplesync-main
```

A ZIP has no git history and no `git pull`: to update, download a new ZIP and copy your `.env` (and `backfill_range.txt`, if you made one) into the new folder.

All commands in this guide are run from inside this folder (`ripplesync`, or `ripplesync-main` for the ZIP).

### 5.3 Create a virtual environment and install

A virtual environment (venv) keeps this program's packages apart from the system Python.

Linux / macOS (bash or zsh):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Windows: the script `deploy\windows\setup-windows.ps1` does all of 5.3 and 5.4 (creates `.venv` with `py -3.12`, installs `requirements.txt`, copies `.env.example` to `.env` if missing, runs `doctor`). It does not install MongoDB or xrpld. **Not tested on a real host.**

```powershell
powershell -ExecutionPolicy Bypass -File deploy\windows\setup-windows.ps1
```

Or by hand, in Windows PowerShell:

```powershell
py -3.12 -m venv .venv
# If activation is blocked ("running scripts is disabled"), allow it for this window only:
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Windows cmd.exe:

```bat
py -3.12 -m venv .venv
.venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
```

`requirements.txt` is the only install file. It contains the runtime and the test packages.
There is no installable package and no short command: always run the program as `python -m xrpl_ingest ...` with the venv active (or with `.venv/bin/python`, `.venv\Scripts\python.exe`).

On Ubuntu, `bash setup/setup_ubuntu.sh ingest` does 5.3 and 5.4 for you: venv, requirements, `.env` (chmod 600; it asks for `MONGO_URI` with hidden input), the tests, `doctor --mongo`, and the two systemd units (see [Step 7](#step-7-run-as-a-service)).

Optional: run the tests. Why: they prove the install works on this machine.

```bash
python -m pytest -q
# without a local MongoDB the integration tests skip; to skip them on purpose:
XRPL_INGEST_NO_INTEGRATION=1 python -m pytest -q      # PowerShell: $env:XRPL_INGEST_NO_INTEGRATION=1; python -m pytest -q
```

### 5.4 Create `.env`

The program reads its settings from environment variables and from a `.env` file.

**Where `.env` is read from:**

1. If `ENV_FILE` is set in the process environment, that file is used (a missing `ENV_FILE` file is a config error).
2. Otherwise `<XRPL_INGEST_HOME>/.env`, or `<current folder>/.env` when `XRPL_INGEST_HOME` is not set.
3. The program does **not** search parent folders. No `.env` at all is fine: then only the process environment is used.
4. **Process environment variables win** over values in `.env`. (`config` shows `env (overrides .env)` for such a value.)

**The base folder** is `XRPL_INGEST_HOME` (from the process environment or from `.env`), else the folder of the loaded `.env`, else the current folder.
Relative paths (stop file, range file, log file, lock files, `MONGO_URI_FILE`) are resolved against the base folder.
So: either `cd` into the project folder before every command, or set `XRPL_INGEST_HOME`. The systemd units and `deploy\windows\run.cmd` set `XRPL_INGEST_HOME` to the repo folder.

Create the file:

```bash
cp .env.example .env
chmod 600 .env          # only you can read it: it contains the database password
nano .env
```

```powershell
Copy-Item .env.example .env
notepad .env
```

Files written by Windows PowerShell may be UTF-16 or UTF-8 with BOM. The program reads UTF-8, UTF-8 with BOM and UTF-16 with BOM, so all of them work.

**Never commit `.env` to git.** It is in `.gitignore`.

### 5.5 Settings explained

All settings are optional except `MONGO_URI`. Defaults are shown.

**Connection**

| Variable | Default | Meaning |
|---|---|---|
| `MONGO_URI` | (required, or `MONGO_URI_FILE`) | MongoDB connection string, e.g. `mongodb://xrpl_ingest:<pw>@127.0.0.1:27017/?authSource=admin`. Must start with `mongodb://` or `mongodb+srv://`. |
| `MONGO_URI_FILE` | empty | Path of a file that holds the URI on exactly one line (max 4096 bytes). Keeps the password out of `.env`; `chmod 600` the file. **Setting both `MONGO_URI` and `MONGO_URI_FILE` is a config error.** |
| `DB_NAME` | `xrpl_data_collection` | Database name. **Use a fresh name if an old (legacy) database causes index errors.** |
| `SERVERS` | `ws://127.0.0.1:6006` | Your node's admin websocket. Comma-separated; first is preferred, others are failover. **Never list testnet servers.** |
| `EXPECTED_NETWORK_ID` | `0` | `0` = mainnet. A server reporting another id is refused. `none` switches the check off (not recommended). |
| `MONGO_SERVER_SELECTION_TIMEOUT_MS` | `10000` | How long to wait for MongoDB before "unreachable". |
| `MONGO_SOCKET_TIMEOUT_MS` | `120000` | Maximum time for one MongoDB network operation; `0` = no limit. |

If the same option (`serverSelectionTimeoutMS`, `socketTimeoutMS`) is written inside `MONGO_URI`, the URI value wins.

**Collections** (defaults are the legacy names; do not change them for an existing database)

| Variable | Default | Holds |
|---|---|---|
| `LEDGER_COLLECTION` | `block_number` | one document per ledger |
| `TX_COLLECTION` | `transactions` | one document per transaction |
| `META_MEMOS_COLLECTION` | `internal_data` | full transaction metadata + memos |
| `FAILED_TX_COLLECTION` | `failed_transactions` | ledgers that failed after all retries |
| `METADATA_COLLECTION` | `last_processed_block_no` | tail checkpoint + backfill chunk progress |

**Speed and retries**

| Variable | Default | Meaning |
|---|---|---|
| `NUM_PROCESSES` | `4` | Backfill worker processes. **Windows: at most 61** (process pool limit; more is a config error). |
| `CONCURRENCY` | `16` | Ledger requests in flight per process. Total load = `NUM_PROCESSES x CONCURRENCY`. |
| `CHUNK_SIZE` | `1000` | Ledgers per backfill chunk. **Do not change it after the backfill has started:** progress is stored per chunk, and a new size means a new chunk grid (work is redone, `doctor` warns). |
| `MAX_RETRIES` | `5` | Attempts per ledger before it goes to the failed collection. |
| `REQUEST_TIMEOUT` | `30` | Seconds to wait for one answer from the node. |
| `RECONNECT_DELAY_BASE` | `1` | First wait (seconds) before a reconnect / retry; it grows exponentially. |
| `BACKOFF_MAX` | `30` | Longest wait between retries (seconds). |
| `POLLING_INTERVAL_SECONDS` | `2` | How often `tail` asks the node for a new ledger. |
| `MAX_MESSAGE_MB` | `64` | Largest websocket message accepted. Must be larger than the biggest ledger. Raise it if you see "message too big" / close code 1009. |

**Files, logs, safety**

| Variable | Default | Meaning |
|---|---|---|
| `XRPL_INGEST_HOME` | folder of the loaded `.env`, else current folder | Base folder for relative paths. To also find `.env` there, set it in the process environment (service file, shell), not only inside `.env`. |
| `ENV_FILE` | empty | Explicit path of the `.env` file. Set it in the process environment. |
| `PROCESS_STATUS_FILE` | `process_status.txt` | The stop file, see [Stopping](#66-stopping-gracefully). |
| `BACKFILL_RANGE_FILE` | `backfill_range.txt` | The range file, see [Range file](#62-the-range-file-backfill_rangetxt). |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `LOG_FILE` | empty | Empty = log to the screen (stderr) only. A path (e.g. `logs/xrpl_ingest_tail.log`) = also write a rotating log file. **Give each command that runs at the same time its own file** (see [Logs](#67-logs)). |
| `LOG_MAX_MB` | `100` | Size of one log file before it rotates. |
| `LOG_BACKUPS` | `10` | How many old log files to keep (`0` allowed). |
| `MIN_FREE_GB` | `0` | Disk guard; `0` = off. If the filesystem that holds the MongoDB database has less free space than this, `tail` / `backfill` / `retry-failed` stop gracefully (exit `1`) before the disk is full. The number comes from MongoDB itself (`dbStats`), checked about once a minute, so it also works when MongoDB is on another host. `doctor` warns too. |

See your effective settings, with passwords hidden and the source of each value:

```bash
python -m xrpl_ingest config
```

Each line looks like `NAME = value    [source]`. The source is `env`, `.env`, `env (overrides .env)` or `default`. Paths are shown fully resolved. A query string in `MONGO_URI` is shown as `?<n option(s) hidden>`.

### 5.6 Run `doctor`

```bash
python -m xrpl_ingest doctor
```

Why: it checks everything before you start a run that may last weeks.
It checks: Python version; installed package versions against `requirements.txt`; that the config is valid; resolved paths (with a warning for hidden control characters such as a stray `\r`); `.env` file permissions (Linux/macOS; not checked on Windows); lock files; free disk in the base folder; RAM; `NUM_PROCESSES` (and the Windows limit 61); MongoDB (reachable, version 7.0/8.0, authenticated user, free space vs `MIN_FREE_GB`); index conflicts and duplicate hashes in an old (legacy) database; chunk documents made with a different `CHUNK_SIZE`; and the node (`server_state`, `network_id`, `complete_ledgers`, validated ledger, peers, `build_version`). At the end it prints the effective configuration like `config`.

Each line starts with `OK`, `WARN`, `FAIL` or `INFO`, for example:

```
OK    python           3.12.10 (requires >=3.10)
WARN  pymongo          4.8.0 installed; requires >=4.9,<4.11 (pip install -r requirements.txt)
FAIL  mongo            mongodb://127.0.0.1:27017 unreachable: ServerSelectionTimeoutError: ...
result: 1 failure(s), 1 warning(s) -> exit 4
```

- `doctor --node`: only the node checks. `doctor --mongo`: only the MongoDB checks. Both flags: both. `doctor --json`: machine-readable output.
- Every network check uses a short 5-second timeout.
- Exit code: `0` all OK, `1` warnings, `2` config problem (also wrong network or bad credentials), `4` node or MongoDB unreachable. The worst one wins (2 > 4 > 1 > 0).

---

## Step 6: Start ingesting

### 6.1 The right order, and why

1. **Node is `full`** (Step 3). Why: before that, it cannot serve ledgers reliably.
2. **`doctor` says OK.** Why: config and connection errors show up in seconds instead of hours later.
3. **Small trial backfill** (1000 ledgers). Why: proves the whole chain works and shows the speed and size per ledger.
4. **`verify` the trial range.** Why: proves the data really arrived complete.
5. **Start `tail`** (as a service, Step 7). Why: live ledgers are captured from now on. Starting it before the big backfill means there is no hole between "end of backfill" and "start of tail".
6. **Full backfill.** It runs for a long time. You can stop and restart it at any time.

```bash
python -m xrpl_ingest doctor
python -m xrpl_ingest backfill --start 32570 --end 33569
python -m xrpl_ingest verify   --start 32570 --end 33569
sudo systemctl enable --now xrpl-tail          # or: python -m xrpl_ingest tail
python -m xrpl_ingest backfill                 # range from backfill_range.txt or defaults
                                               # (or as a service: sudo systemctl start xrpl-backfill)
```

Size estimate: after the trial, look at the database size in `mongosh` (`use xrpl_data_collection` then `db.stats()`).
Early ledgers are small and recent ledgers are much bigger. So also try a recent range (for example 1000 ledgers near the current one) before you estimate the total.

About `tail` after a trial backfill: when `tail` has no checkpoint yet, it continues after the highest stored ledger only if that is at most 1000 ledgers behind.
After a trial of early history it is much further behind. Then `tail` starts at the current ledger and logs the exact `backfill --start ... --end ...` command for the hole. The full backfill fills that hole anyway.

### 6.2 The range file (`backfill_range.txt`)

When you run `backfill` **without** `--start` and `--end`, the range comes from the file `backfill_range.txt` in the base folder (setting `BACKFILL_RANGE_FILE` changes the name).
The systemd unit and the Windows task for backfill use this file too.
Copy the example file to start:

```bash
cp backfill_range.txt.example backfill_range.txt
```

Format: one `key = value` per line. Empty lines and lines starting with `#` are ignored. A ` #` after a value starts a comment.

```
# used by `backfill` when neither --start nor --end is given
start = 32570
end   = latest      # or a ledger number
```

| Key | Allowed values | Default |
|---|---|---|
| `start` | a number (underscores allowed: `50_000_000`) or `first` (= 32570). Must be 32570 or more. | `32570` |
| `end` | a number, or `latest` (the node's newest validated ledger when backfill starts). Must be >= `start`. | `latest` |

Examples:

```
# everything
start = first
end   = latest
```

```
# only a middle part of history (example numbers)
start = 50_000_000
end   = 60_000_000
```

Rules:

- **Precedence:** command line > file > defaults. If you give `--start` **or** `--end`, the file is ignored completely (the log says so).
- `--no-range-file` skips the file and uses the defaults. `--range-file PATH` reads another file; if that file does not exist the command stops with exit `2` (a missing *default* file just means: use the defaults).
- No file = defaults (`32570` to `latest`), with an INFO log line.
- Keys are not case-sensitive. Only `start` and `end` are allowed.
- An unknown key, a key given twice, or a line without `=` is a config error (exit code `2`). The message names the file and the line.
- At most 4096 bytes. UTF-8 (with or without BOM) or UTF-16 with BOM, like `.env`.
- The program reads the file once at start and never writes it. Changes apply at the next start.
- The file is in `.gitignore` (it is your local choice). The repo ships `backfill_range.txt.example`.

**Progress:** `status` shows progress for the range in the file: chunks done / total, percent, failed ledgers, speed, when the last chunk finished, and estimated time left. The speed is computed from the finish times of the stored chunks.

```bash
python -m xrpl_ingest status
```

Example output (the numbers are invented):

```
node complete_ledgers: 32570-105000300  latest validated: 105000300
database: {'ledgers': 1000, 'transactions': 1234, 'failed': 0, 'checkpoint': 0, 'chunks_done': 1}
backfill range 32570-105000300 (file /opt/xrpl-ingest/backfill_range.txt): chunks 1/104968 (0.0%), failed ledgers 0, 850 ledgers/s, last chunk 2s ago, ETA 1d10h (at that rate)
```

### 6.3 Command reference

Run every command as `python -m xrpl_ingest <command>`. `--help` works on every command and lists the exit codes.

| Command | What it does | Example |
|---|---|---|
| `status` | Node `complete_ledgers` and latest ledger; database counts; checkpoint; range-file progress. DB part is shown even if the node is down. | `status` |
| `backfill [--start N] [--end M] [--range-file PATH] [--no-range-file]` | Copies old ledgers in chunks, many processes. Resumable. Only ledgers the node has. | `backfill --start 32570 --end 33569` |
| `tail` | Follows new validated ledgers. Saves a checkpoint after each ledger. | `tail` |
| `retry-failed [--max-rounds N]` | Fetches the ledgers in the failed collection again (default 5 rounds). Connection-loss failures are retried last, one at a time. Prints `remaining failed: <n>`. | `retry-failed --max-rounds 10` |
| `verify --start N --end M [--repair] [--deep]` | Finds missing ledgers and ledgers whose stored transaction count is wrong. `--repair` fetches exactly those again (with `--deep` also the ledgers whose transaction hashes differ). `--deep` also checks the parent-hash chain (offline) and compares the stored transaction hashes with the node's list. | `verify --start 32570 --end 1000000 --repair` |
| `fetch --ledger N\|validated` | Fetches one ledger (or the newest validated one) now and stores it. Removes it from the failed list on success. Does not touch chunk progress. | `fetch --ledger 32570` |
| `tx --hash H` | Compares the stored transaction (64 hex digits hash) with the node's answer (spot check). | `tx --hash <64-hex-digit hash>` |
| `doctor [--node] [--mongo] [--json]` | Checks the whole setup (see 5.6). | `doctor --node` |
| `health [--max-lag N] [--max-age S]` | For monitoring: is `tail` keeping up? Healthy when the checkpoint trails the node's validated ledger by at most N ledgers (default 50) and was written in the last S seconds (default 120). Prints one line: `healthy: ...`, `lagging: ...`, `stale: ...` or `unhealthy: ...`. Exit `0` healthy, `1` lagging / stale / no checkpoint yet, `2` node on the wrong network, `4` unreachable. | `health --max-lag 50 --max-age 120` |
| `config` | Prints the effective settings (passwords hidden) and where each value came from. | `config` |

Details:

- `backfill --no-range-file`: ignore `backfill_range.txt` and use the defaults (32570 to latest).
- `backfill --range-file PATH`: use another range file.
- `verify --deep`: the hash comparison asks the node for every ledger, so it is slow. If the node is unreachable, the offline checks still run and the output says so.
- `verify --repair`: exit `0` if everything is OK afterwards, `1` if problems remain, `4` if the node is unreachable.
- `fetch`: exit `0` stored, `1` failed (recorded in the failed collection), `2` usage, `4` node or MongoDB unreachable.
- `tx --hash`: exit `0` same, `1` different, not validated, or missing (stored or on the node), `2` usage, `4` unreachable.
- Only `tail`, `backfill` and `retry-failed` take a lock and react to the stop file and to the first Ctrl+C.

### 6.4 Exit codes

Every command ends with one of these codes. Scripts and services can react to them.

| Code | Meaning | What to do |
|---|---|---|
| `0` | OK | Nothing. |
| `1` | Some ledgers failed, the run stopped early (stop file, signal, or `MIN_FREE_GB`), `verify` found problems, `health` lagging/stale, or `doctor` warnings | Read the log. Run `retry-failed` or `verify --repair`. |
| `2` | Config or usage error (also: node on the wrong network, bad Mongo credentials, bad range file) | Fix `.env`, the range file or the command. Retrying will not help. |
| `3` | `backfill`: no failures, but the node does not have part of the range yet (see its `complete_ledgers`) | Normal while the node downloads history. Run again later. |
| `4` | Node or MongoDB unreachable or failing | Check that xrpld / mongod run and the URIs are right. |
| `5` | Another instance of the same command is already running (lock held) | The message shows PID, host and start time of the other one. |
| `6` | Unexpected internal error (a bug; the log has the traceback) | Keep the log and report it. |
| `130` | Interrupted: second Ctrl+C, or Ctrl+C in a command without graceful stop | Run again; all work is resumable. |

`fetch` and `tx` use their own short list, see the notes under the command table.

### 6.5 Running tail and backfill at the same time

This is allowed and recommended (run `tail` from day 1 while the backfill fills the history). Each of `tail`, `backfill` and `retry-failed` takes its own lock file `<base>/xrpl_ingest.<command>.lock`. So one `tail` and one `backfill` can run together, but **not two backfills** and **not two tails** (the second one exits with code `5`).
Both write the same way (upserts). If both happen to write the same ledger, the result is the same.
Give each one its own `LOG_FILE` (see [Logs](#67-logs)).

The lock is held by the operating system (`flock` on Linux/macOS, `msvcrt.locking` on Windows), not by the file's existence. If a process dies, the lock is free again. A leftover `.lock` file is harmless and is never deleted; it keeps the last holder's PID, host and start time. `doctor` shows each lock as running or stale. Note: `doctor` takes the lock for a moment to test it, so a command started in that exact instant may exit `5`; just start it again.

### 6.6 Stopping gracefully

A graceful stop finishes the ledgers in progress, starts nothing new, and exits. Nothing is lost. The next run continues where it stopped.

**Way 1: the stop file** (works everywhere, also Windows). Put a line into `process_status.txt` in the base folder:

| Line | Effect |
|---|---|
| `current_process=0` | stops **all** commands (`tail`, `backfill`, `retry-failed`) |
| `tail=0` | stops only `tail` |
| `backfill=0` | stops only `backfill` |
| `retry-failed=0` | stops only `retry-failed` |

Rules:

- **Any line that applies and says `0` stops the command.** "Applies" means the key is `current_process` or the running command's own name. So a leftover `current_process=1` line can **not** hide a `tail=0` line below it.
- Any other value (for example `=1`) means "keep running". Lines with other keys, empty lines and `#` comments are skipped.
- Keys are not case-sensitive (`CURRENT_PROCESS=0` from the old scripts works).
- The file is read at most once per second while the command runs.
- A missing or unreadable file means "keep running".
- UTF-8, UTF-8 with BOM and UTF-16 with BOM all work (so PowerShell `>` is fine).

```bash
echo current_process=0 > process_status.txt               # Linux / macOS / cmd.exe
echo backfill=0 > process_status.txt                      # stop only the backfill
```

```powershell
Set-Content -Encoding ascii process_status.txt 'tail=0'    # PowerShell: stop only tail
```

**Important:** remove every `=0` line that applies (or change it to `=1`) **before you start again**. Otherwise the next run stops at once. Adding a `current_process=1` line does not help while a `tail=0` line is still there.

```bash
rm process_status.txt                                      # PowerShell: Remove-Item process_status.txt
```

**Way 2: signals.**

| Signal | Effect |
|---|---|
| SIGTERM (`kill <pid>`, `systemctl stop`) | graceful stop |
| Ctrl+C (SIGINT), first time | graceful stop (`tail`, `backfill`, `retry-failed`) |
| Ctrl+C, second time | abort at once, exit code `130` |
| Ctrl+Break (SIGBREAK, Windows console) | graceful stop |

Ctrl+C in any other command (for example `verify`) aborts at once with `130`.
On Windows, "End task" in Task Manager, `Stop-ScheduledTask` or "End" in Task Scheduler kills the process at once (not graceful; data stays consistent, unfinished chunks are redone). Use the stop file there.
Backfill worker processes ignore Ctrl+C themselves; the main process tells them to stop through a shared stop event.

### 6.7 Logs

- Default: logs go to the screen (stderr). Under systemd they go to the journal: `journalctl -u xrpl-tail -f`, `journalctl -u xrpl-backfill -f`.
- Set `LOG_FILE` for a rotating log file (`LOG_MAX_MB`, `LOG_BACKUPS`). Backfill worker processes send their log lines to the main process, so one command writes one file.
- **Each command that runs at the same time needs its own `LOG_FILE`**, for example `logs/tail.log` and `logs/backfill.log`. Two processes must not rotate the same file (on Windows the file is locked).

  ```bash
  mkdir -p logs
  nohup env LOG_FILE=logs/tail.log     python -m xrpl_ingest tail     >/dev/null 2>&1 &
  nohup env LOG_FILE=logs/backfill.log python -m xrpl_ingest backfill >/dev/null 2>&1 &
  ```

- `deploy\windows\run.cmd` sets `LOG_FILE=logs\xrpl_ingest_<command>.log` by default (for example `logs\xrpl_ingest_tail.log`), unless `LOG_FILE` is already set.
- Times are UTC, ISO 8601, ending with `Z` (e.g. `2026-10-18T09:15:02.123Z`).
- At start the program logs its version and all resolved paths (base folder, `.env`, stop file, range file, log file, lock). Check them if a file "is not found".
- Backfill progress lines look like:

  ```
  chunks 1234/98000 (1.3%), 512 ledgers/s, ETA 2d04h
  ```

---

## Step 7: Run as a service

A service starts the program at boot and restarts it after a crash. Use it at least for `tail`.
The templates are in `deploy/` (Linux: systemd; Windows: Task Scheduler). There is no service template for macOS: run the program there from a terminal.
**UNVERIFIED: the templates are not tested on a real host** (only reviewed against the systemd / ScheduledTasks documentation).

### 7.1 Linux: systemd (`deploy/systemd/`)

Two units:

| Unit | Runs | Installed by setup | Start |
|---|---|---|---|
| `xrpl-tail.service` | `python -m xrpl_ingest tail` | yes, not enabled | `sudo systemctl enable --now xrpl-tail` (also starts at every boot) |
| `xrpl-backfill.service` | `python -m xrpl_ingest backfill` (range from `backfill_range.txt`) | yes, not enabled | `sudo systemctl start xrpl-backfill` (by hand, one run) |

`bash setup/setup_ubuntu.sh ingest` (and `setup_all.sh`) render both templates into `/etc/systemd/system/`: they replace `@REPO_DIR@` (the repo path) and `@USER@` (the normal user who ran the script with sudo) and run `daemon-reload`. They refuse to install the units as root, and refuse a repo path that contains `%`, `$`, `"`, `\` or a newline.

```bash
bash setup/setup_ubuntu.sh ingest
sudo systemctl enable --now xrpl-tail
sudo systemctl start xrpl-backfill
systemctl status xrpl-tail xrpl-backfill
journalctl -u xrpl-tail -f          # or: journalctl -u xrpl-backfill -f
```

Without the setup script, render a unit by hand (replace the path and user):

```bash
sed -e 's|@REPO_DIR@|/opt/xrpl-ingest|g; s|@USER@|xrplops|g' deploy/systemd/xrpl-tail.service \
  | sudo tee /etc/systemd/system/xrpl-tail.service >/dev/null
sudo systemctl daemon-reload
```

What the units do:

- `User=@USER@`, `WorkingDirectory=` and `XRPL_INGEST_HOME=` = the repo folder, so `.env`, `process_status.txt` and `backfill_range.txt` are read from the repo.
- `PYTHONUTF8=1`, `PYTHONUNBUFFERED=1`; logs go to the journal.
- `Restart=on-failure`, `RestartSec=10`. A clean exit (`0`, for example after `tail=0` in the stop file) is not restarted.
- `RestartPreventExitStatus`: **tail `2 3 5`**, **backfill `1 2 3 5`**. A restart cannot fix a config error (2), a missing range on the node (3) or a second instance (5). For backfill, `1` also means "finished with recorded failed ledgers" or "stopped on request", so it is not restarted either: run `retry-failed` or start it again by hand. Crashes (`4`, `6`, signals) are restarted.
- `systemctl stop` sends SIGTERM = graceful stop. `KillMode=control-group` sends it to the backfill workers too. `TimeoutStopSec=300`: after 5 minutes systemd kills the rest.
- Graceful stop also works with the stop file: `backfill=0`, `tail=0` or `current_process=0` in the repo's `process_status.txt`.

### 7.2 Windows: Task Scheduler (`deploy/windows/`)

Files:

| File | Purpose |
|---|---|
| `setup-windows.ps1` | `.venv` with Python 3.12, `pip install -r requirements.txt`, `.env` from `.env.example` (if missing), `doctor` (see Step 5). |
| `install-task.ps1 -Command tail\|backfill [-Uninstall]` | Registers (or removes) the scheduled task `xrpl-ingest-tail` or `xrpl-ingest-backfill`. |
| `run.cmd <command> [args]` | What the task runs: `.venv\Scripts\python.exe -m xrpl_ingest <command>`, with `PYTHONUTF8=1`, `PYTHONUNBUFFERED=1`, `XRPL_INGEST_HOME=<repo>` and `LOG_FILE=logs\xrpl_ingest_<command>.log` (each only if not already set). You can also run it by hand: `deploy\windows\run.cmd status`. |

Register the tasks in an **elevated** PowerShell ("Run as administrator"), in the repo folder:

```powershell
powershell -ExecutionPolicy Bypass -File deploy\windows\install-task.ps1 -Command tail
powershell -ExecutionPolicy Bypass -File deploy\windows\install-task.ps1 -Command backfill
Start-ScheduledTask -TaskName xrpl-ingest-tail           # start now instead of at next boot
Get-ScheduledTaskInfo -TaskName xrpl-ingest-tail         # LastTaskResult = the exit code
```

Task settings: starts at system startup, runs as the user who registered it whether logged on or not (S4U logon: no password stored, no network shares), **runs on battery** (does not stop when going on battery), **no execution time limit** (`ExecutionTimeLimit 0`), restarts on failure every minute (tail up to 999 times, backfill 3 times), a second start while running is ignored.

- **Graceful stop = the stop file** in the repo folder:

  ```powershell
  Set-Content -Encoding ascii process_status.txt 'tail=0'     # or backfill=0 / current_process=0
  ```

  `Stop-ScheduledTask` or "End" in the Task Scheduler window kills the process at once. Remove the stop line again before the next start.
- The backfill task runs again at every boot (it resumes; finished chunks are skipped). When the backfill is done: `Disable-ScheduledTask -TaskName xrpl-ingest-backfill`.
- Remove a task: `powershell -ExecutionPolicy Bypass -File deploy\windows\install-task.ps1 -Command backfill -Uninstall` (stop it gracefully first; removing the task does not stop a running process).
- Logs: `logs\xrpl_ingest_tail.log`, `logs\xrpl_ingest_backfill.log` in the repo.

---

## Data model: nothing is dropped

### The rule

**Every piece of data the node sends is stored in MongoDB.** (Rule from `xrpl_ingest/transform.py`.)

- **Mapped fields** keep their names and conversions (tables below). Existing queries keep working.
- **Every other field** is stored as received, under its own name.
- **Name collisions:** when that name is already used in the document by a **different** value, the received value is stored under **`raw_<key>`** (`raw_raw_<key>`, ... if that is taken too). An **identical value is not stored twice**.
- **Fields the program computes** (for example `affected_accounts`, `delivered_amount`, `_inserted_at`): if the node sent a field with the same name, the node's value moves to `raw_<key>`.
- `_id` is reserved: a node field can never become the MongoDB `_id` (it goes to `raw__id`).
- **Key escaping:** MongoDB cannot store some field names, and the whole write would fail. Such keys are replaced by look-alike characters, in a reversible way (`unescape_key` in `transform.py` restores them):

  | Original | Stored as |
  |---|---|
  | `.` anywhere in a key | `．` (U+FF0E, fullwidth full stop) |
  | `$` at the start of a key | `＄` (U+FF04, fullwidth dollar sign) |
  | NUL character | `␀` (U+2400) |

- **Big integers:** a JSON integer larger than 64 bits (BSON cannot hold it) is stored as its exact decimal **string**.
- **Number strings:** `Fee`, `Flags`, `DestinationTag`, `SourceTag`, `Sequence`, `LastLedgerSequence`, `OfferSequence`, `PreviousTxnLgrSeq` and XRP amounts in drops (`Amount`, `DeliverMax`, `TakerGets`, `TakerPays`) are converted to integers. A value that is not a number, or does not fit in 64 bits, is stored as received (the log has one WARNING per field per ledger).
- **Ledger document:** every key of the ledger header **except `transactions`** (each transaction is its own document), plus every result-level key of the node's answer, including `validated` and node warnings (`warnings`; `node_warnings` if the answer already had different `warnings`).
- **Metadata:** the **full** transaction metadata (`meta`, including `AffectedNodes`) is stored in the meta collection, with only the key escaping above applied.
- Both answer shapes are handled: API v2 (`tx_json` + `meta`) and API v1 (flat transaction + `metaData`).

**Existing documents do not get new fields by themselves.** Documents written by an older version of this program (or by the legacy scripts) keep their old shape until that ledger is ingested again. To refresh a range, re-ingest it: `verify --start N --end M --repair` repairs only problem ledgers; to rewrite everything in a range, remove its chunk documents and run `backfill` for it again (see [Checking that data is complete](#checking-that-data-is-complete)).

### Collections (default names)

| Collection | Content | Key |
|---|---|---|
| `block_number` | one document per ledger | `block_number` (unique) |
| `transactions` | one document per transaction | `hash` (unique) |
| `internal_data` | metadata + memos per transaction | `transaction_hash` (unique) |
| `failed_transactions` | ledgers that failed after all retries | `block_number` (unique) |
| `last_processed_block_no` | tail checkpoint + backfill chunk progress | `_id` |

Database: `xrpl_data_collection`. All names can be changed in `.env` (they match the legacy database).

### Ledger document (`block_number`)

| Field | Type | Meaning |
|---|---|---|
| `block_number` | int | ledger index (node: `ledger_index`) |
| `block_hash` | string | ledger hash (node: `ledger_hash`) |
| `parent_hash` | string | hash of the previous ledger |
| `account_hash`, `transaction_hash` | string | state tree and transaction tree hashes |
| `close_time` | int | close time, seconds since 2000-01-01 (Ripple epoch) |
| `close_time_human` | string | as sent by the node |
| `close_time_iso` | string | close time as UTC ISO text |
| `total_coins` | int | total XRP in drops |
| `num_transactions` | int | number of transactions in this ledger |
| `_inserted_at` | date | when this program wrote it |
| `validated` | bool | from the node's answer (always `true`: unvalidated ledgers are refused) |
| `warnings` / `node_warnings` | as sent | only when the node sent warnings |
| all other header and answer keys | as sent | for example `close_flags`, `close_time_resolution`, `parent_close_time` (whatever the node sends) |

### Transaction document (`transactions`)

| Field | Type | Meaning |
|---|---|---|
| `hash` | string | transaction hash |
| `block_number`, `block_hash` | int, string | ledger that contains it |
| `close_time`, `close_time_iso` | int, string | ledger close time |
| `TransactionType` | string | `Payment`, `OfferCreate`, ... |
| `from_address` | string | sender (node: `Account`) |
| `to_address` | string | receiver, if any (node: `Destination`) |
| `status` | string | `success` (`tesSUCCESS`), `failed` (any other result), `unknown` (no metadata) |
| `result` | string | the exact result code, e.g. `tesSUCCESS`, `tecPATH_DRY` |
| `transaction_index` | int | position inside the ledger |
| `Fee`, `Flags`, `Sequence`, `DestinationTag`, `SourceTag`, `LastLedgerSequence`, `OfferSequence`, `PreviousTxnLgrSeq` | int | converted from strings |
| `Amount`, `TakerGets`, `TakerPays` | int or object | XRP as int **drops** (1 XRP = 1,000,000 drops); tokens as object `{currency, issuer, value}`. This mix is the legacy format and is kept on purpose. |
| `DeliverMax` | int or object | API v2 name of a Payment's `Amount`, converted like `Amount` |
| `delivered_amount` | int or object or string | from `meta.delivered_amount` (else `meta.DeliveredAmount`): what **really** arrived. Same int/object format. The string `"unavailable"` (partial payments before 2014) is kept as is. Missing when the metadata has neither. |
| `affected_accounts` | list of strings | sorted, unique list of every account address in the transaction's metadata: all `AffectedNodes` (`CreatedNode`, `ModifiedNode`, `DeletedNode`; in `FinalFields`, `NewFields`, `PreviousFields`: `Account`, `Destination`, `Owner`, `Issuer`, `HighLimit.issuer`, `LowLimit.issuer`), plus the tx `Account` and `Destination`. Only values that look like classic addresses (`r...`, 25-35 characters; no checksum test). |
| `amount_xrp` / `delivered_xrp` | Decimal128 | exact XRP value (drops / 1,000,000), XRP amounts only |
| `amount_currency` / `delivered_currency` | string | `"XRP"`, or the token's currency code |
| `amount_issuer` / `delivered_issuer` | string | token issuer (tokens only) |
| `amount_value` / `delivered_value` | Decimal128 | exact token value, parsed from the value string (tokens only; left out if the value is not a decimal number that Decimal128 can hold) |
| `_inserted_at` | date | when this program wrote it |
| all other transaction keys | as sent | every key of `tx_json` and of the outer transaction object |

**About Payments in API v2:** the node no longer sends `Amount` for Payments, only `DeliverMax`. The program copies `DeliverMax` into `Amount` when `Amount` is missing, and keeps `DeliverMax` too. The readable `amount_*` fields are computed from `Amount`.
Amounts that are neither drops nor a `{currency, issuer, value}` object get no readable fields (only the parts that exist are copied).
`Amount` / `DeliverMax` is the **maximum** to deliver. With partial payments less can arrive. **For "how much was paid", use `delivered_amount`** ([xrpl.org: Partial Payments](https://xrpl.org/docs/concepts/payment-types/partial-payments)).

The readable fields (`amount_*`, `delivered_*`) use Decimal128, never floating point, so values are exact.

### Metadata document (`internal_data`)

| Field | Meaning |
|---|---|
| `transaction_hash` | the transaction it belongs to |
| `block_number`, `block_hash`, `close_time_iso` | its ledger |
| `meta` | the **full** metadata object from the node (only key escaping applied) |
| `memos` | the transaction's `Memos` list (empty list if none; key escaping applied) |
| `_inserted_at` | when written |

### Progress and failure documents

In `last_processed_block_no`:

- **Checkpoint:** `{_id: "last_processed_ledger", index: <ledger>, updated_at: <date>}`. Only moves forward (`$max`).
- **Finished chunk:** `{_id: "backfill_chunk:<start>-<end>", kind: "backfill_chunk", start, end, ok, failed, completed_at}`. A chunk is marked only when all its ledgers were stored or recorded as failed.

In `failed_transactions`: `{block_number, reason, last_failed_at, retry_count}`. Removed when the ledger is fetched successfully.

### Indexes

Created automatically.

| Collection | Indexes |
|---|---|
| `block_number` (ledgers) | `block_number` (unique), `close_time` |
| `transactions` | `hash` (unique), `block_number`, `from_address`, `to_address`, `TransactionType`, `close_time`, `affected_accounts` (multikey: one entry per account) |
| `internal_data` (metadata) | `transaction_hash` (unique), `block_number` |
| `failed_transactions` | `block_number` (unique) |
| `last_processed_block_no` (progress) | `kind` + `start` |

Example queries in `mongosh`:

```javascript
use xrpl_data_collection
db.transactions.find({ affected_accounts: "rEXAMPLEaccount..." }).sort({ block_number: 1 })
db.transactions.find({ TransactionType: "Payment", delivered_currency: "XRP" }).limit(5)
db.internal_data.findOne({ transaction_hash: "<hash>" }).meta
```

---

## Checking that data is complete

"Complete" means: every ledger in the range has a ledger document, and each ledger has exactly `num_transactions` transaction documents. Because the ledger document is written last, a ledger document means its transactions were written.

Runbook:

1. **Find gaps and count errors.**

   ```bash
   python -m xrpl_ingest verify --start 32570 --end 1000000
   ```

   It prints `missing ledgers A-B` and `ledger N: expected X transactions, stored Y`, then `OK` or `<n> problem(s)`.

2. **Repair them.** Fetches exactly the missing and mismatched ledgers again. Safe to repeat. Exit `0` if all is OK afterwards, `1` if problems remain.

   ```bash
   python -m xrpl_ingest verify --start 32570 --end 1000000 --repair
   ```

3. **Retry recorded failures.**

   ```bash
   python -m xrpl_ingest retry-failed
   ```

   Exit `0` means the failed list is empty. Exit `1` prints how many remain. Look at `reason` in `failed_transactions`.
   If the reason says the node does not have the ledger, wait until `complete_ledgers` includes it.

4. **Deep check** (slower, needs the node for part of it).

   ```bash
   python -m xrpl_ingest verify --start 32570 --end 1000000 --deep
   ```

   It checks that ledger N's `parent_hash` equals ledger N-1's `block_hash` (offline), and compares the stored transaction hashes of each ledger with the node's list. Add `--repair` to re-ingest the ledgers whose hashes differ too.

5. **Spot-check single transactions.**

   ```bash
   python -m xrpl_ingest tx --hash <hash>
   ```

6. **Fix a single ledger by hand.**

   ```bash
   python -m xrpl_ingest fetch --ledger 12345678
   ```

7. **Watch live ingest.** Run `health` every minute from your monitoring (cron, Nagios, ...). Exit `0` healthy, `1` lagging, stale or no checkpoint yet, `2` wrong network, `4` unreachable.

   ```bash
   python -m xrpl_ingest health --max-lag 50 --max-age 120
   ```

8. **Rewrite a whole range** (for example to add the new fields to documents written by an older version). Backfill skips chunks that are marked done, so remove the chunk marks of that range first, then backfill it again. Upserts replace the old documents.

   Safe: this deletes only the *progress marks* (documents with `kind: "backfill_chunk"`), never ledger or transaction data. Use your real names from `.env`: `DB_NAME` (default `xrpl_data_collection`) and `METADATA_COLLECTION` (default `last_processed_block_no`). Do not change the `kind` filter.

   ```javascript
   // mongosh; example range 32570-1000000; chunk documents live in last_processed_block_no
   use xrpl_data_collection
   db.last_processed_block_no.deleteMany({ kind: "backfill_chunk", start: { $gte: 32570 }, end: { $lte: 1000000 } })
   ```

   ```bash
   python -m xrpl_ingest backfill --start 32570 --end 1000000
   ```

Suggestion: run `verify` over the whole stored range once a week, and always after a crash, disk problem or MongoDB restore.

---

## Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| Index error at start (`E11000 duplicate key`, index build fails) on an old database | The legacy database has duplicate hashes or different index options. `doctor` reports this. | Use a **fresh database**: set a new `DB_NAME` in `.env`. Keep the old one for reference. |
| `ConnectFailed` / connection refused on `ws://127.0.0.1:6006` | Node not running, `[port_ws_admin_local]` missing, or not listed under `[server]` | `systemctl status xrpld`; check Step 2.2; `doctor --node`. |
| `NetworkMismatch` (exit `2`) | The server reports another network (testnet?) | Fix `SERVERS`. Never use testnet servers. |
| Warning "node reports no network_id" | `[network_id]` not set in `xrpld.cfg` | Optional: add `[network_id]` `main` (Step 2.2). |
| Backfill exits `3` | Node does not have part of the range yet | Normal while history downloads. Run again later. |
| Node stays `connected` / `syncing` | Slow disk, too little RAM, slow network, or few peers | Restart rule in Step 3.1. Check hardware. Open the peer port. |
| Only 10 or 11 peers | Firewall blocks incoming peer connections | Open 2459/tcp (or 51235) and forward it on the router (Step 2.3). |
| `complete_ledgers` does not go back in time | No peer with old history | Add full-history peers to `[ips_fixed]` (Step 2.2). |
| Many `slowDown` / `tooBusy` errors | The node is overloaded by too many parallel requests | They are retried automatically. Lower `NUM_PROCESSES` or `CONCURRENCY`. |
| `message too big` / close code 1009 | A ledger is larger than `MAX_MESSAGE_MB` | Raise `MAX_MESSAGE_MB` (e.g. `128`), then `retry-failed`. |
| Exit `5` "lock held" | The same command already runs | Stop the other one, or wait. A dead process frees the lock by itself. |
| Program stops immediately after start | `process_status.txt` still has a `current_process=0` or `<command>=0` line (any such line stops it) | Delete the file, or delete that line. |
| Stop file seems ignored | Wrong folder (the base folder, not the current folder), or the key is for another command | Check the `stop file` path in the start log, `config` or `doctor`. |
| `.env` not found / settings ignored | Started from another folder, or a process environment variable overrides `.env` | `cd` to the project, or set `XRPL_INGEST_HOME` / `ENV_FILE`. `config` shows the source of each value. |
| `config error: set only one of MONGO_URI and MONGO_URI_FILE` | Both are set | Remove one of them. |
| `.env` written by PowerShell looks wrong | Encoding (UTF-16) | Supported. If in doubt, save it in Notepad as "UTF-8". |
| Windows: config error for `NUM_PROCESSES` above 61 | The Windows process pool allows at most 61 workers | Set `NUM_PROCESSES` to 61 or less. |
| `tail` / `backfill` stop with exit `1` and a low-disk message | MongoDB's disk has less free space than `MIN_FREE_GB` | Add disk space (or lower `MIN_FREE_GB`), then start again. |
| Two commands write a broken / missing log file (Windows) | Both use the same `LOG_FILE` | Give each command its own `LOG_FILE` (6.7). |
| Windows task does not run / `LastTaskResult` not 0 | See `Get-ScheduledTaskInfo -TaskName xrpl-ingest-tail`; the value is the program's exit code | Run `deploy\windows\run.cmd doctor` by hand. |
| PowerShell: "running scripts is disabled" | Execution policy | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` |
| WSL cannot reach MongoDB on Windows | WSL uses its own network (NAT) | In `C:\Users\<you>\.wslconfig` add `[wsl2]` and `networkingMode=mirrored`, then `wsl --shutdown`. Or use the Windows host IP (`ip route \| awk '/default/ {print $3}'`) and set MongoDB `bindIp` plus a firewall rule. |
| WSL: node very slow or disk full | Database on `/mnt/c` or ext4 VHDX limit | Use a native Linux server for full history. |
| MongoDB `Authentication failed` (exit `2`) | Wrong user/password, missing `authSource=admin`, or special characters not percent-encoded | See Step 4.5. |
| `doctor`: "MongoDB 9.x: supported are 7.0 and 8.0" | 9.0 is not supported by the pinned driver | Use MongoDB 8.0. |
| `doctor`: pymongo / python-dotenv version warning | Old packages in the venv | `pip install -r requirements.txt` |

Node problems in general: [Diagnosing Problems](https://xrpl.org/docs/infrastructure/troubleshooting/diagnosing-problems), [Server Won't Start](https://xrpl.org/docs/infrastructure/troubleshooting/server-wont-start), [Server Doesn't Sync](https://xrpl.org/docs/infrastructure/troubleshooting/server-doesnt-sync).

---

## Upgrading and backups

### Upgrade xrpld

Watch for new releases ([XRPLF/rippled releases](https://github.com/XRPLF/rippled/releases)). Old nodes can become "amendment blocked" and stop following the network.

```bash
sudo apt -y update && sudo apt -y install --only-upgrade xrpld    # RHEL: sudo yum update xrpld
sudo systemctl daemon-reload
sudo systemctl restart xrpld
```

There is **no automatic update** in the current packages: the packages from `packages.xrplf.org` do not include the old `update-rippled-cron` file. If `/etc/cron.d/update-rippled-cron` exists, it comes from an old package: remove the symlink and update manually ([Update Automatically on Linux](https://xrpl.org/docs/infrastructure/installation/update-xrpld-automatically-on-linux), [Update Manually on Ubuntu](https://xrpl.org/docs/infrastructure/installation/update-xrpld-manually-on-ubuntu)).

Coming from `rippled` (before 3.2.0)? Follow [Migrate to xrpld](https://xrpl.org/docs/infrastructure/installation/migrate-to-xrpld). Back up `xrpld.cfg`, `validators.txt` and `wallet.db` first.

### Upgrade this program

```bash
# stop gracefully first (stop file or systemctl stop)
git pull
source .venv/bin/activate
pip install -r requirements.txt
python -m xrpl_ingest doctor
sudo systemctl start xrpl-tail
```

If the unit templates in `deploy/systemd/` changed, re-run `bash setup/setup_ubuntu.sh ingest` to render them again (it keeps an existing `.env`).
Windows: `deploy\windows\setup-windows.ps1` (keeps `.venv` and `.env`), then start the tasks again.

New tx fields (for example `affected_accounts`, `amount_*`) appear only in documents written by the new version. Older documents get them when their ledgers are ingested again (see step 8 of [Checking that data is complete](#checking-that-data-is-complete)).

### Upgrade MongoDB

Stay on 7.0 or 8.0. Do not move to 9.0 until `requirements.txt` allows a driver that supports it.

### Backups

- **xrpld:** back up `/etc/xrpld/xrpld.cfg`, `/etc/xrpld/validators.txt`, and `wallet.db` (node identity) from `[database_path]`. The ledger data itself can be downloaded again from the network (but that takes months for full history).
- **MongoDB:** `mongodump` works, but is slow for many TB.

  ```bash
  mongodump --uri "mongodb://xrpl_ingest:<pw>@127.0.0.1:27017/?authSource=admin" \
            --db xrpl_data_collection --gzip --archive=/backup/xrpl-$(date +%F).gz
  ```

  For very large databases a filesystem snapshot (LVM / ZFS / cloud snapshot) of the MongoDB disk is faster. Take it while `mongod` is stopped or locked (`db.fsyncLock()`).
- **This program:** back up `.env` (it holds the password) and `backfill_range.txt` somewhere safe and private.
- After a restore, run `verify` over the whole range (see [Checking that data is complete](#checking-that-data-is-complete)).

---

## Not verified yet

Be honest about what is tested. These points come from documentation or reasoning, **not** from a run on real hardware:

- The program has **never run against a real xrpld node**. Tests use a fake node (`tests/fake_xrpld.py`) and a local MongoDB.
- The exact API v2 `ledger` answer shape (`tx_json` + `meta`, `DeliverMax`) is taken from xrpld 3.4.1 source and docs, not from a live node.
- Whether a mainnet node reports `network_id` without `[network_id]` in its config (source code says: only when configured).
- How the node answers while syncing (`notSynced`, `notReady` errors) and whether those should be retried.
- Port numbers, stanza names and default values of `xrpld.cfg` (taken from the 3.4.1 example config).
- `setup/setup_ubuntu.sh`, `setup/common.sh` and `setup_all.sh` are not tested on a real host (only `bash -n` and shellcheck).
- The service templates `deploy/systemd/xrpl-tail.service`, `deploy/systemd/xrpl-backfill.service` and the Windows scripts `deploy/windows/install-task.ps1`, `run.cmd`, `setup-windows.ps1` are **not tested on a real host** (Task Scheduler behaviour is UNVERIFIED).
- SIGTERM graceful stop under real systemd.
- Disk size of MongoDB for full history, and ingest speed (ledgers per second).
- The full-history disk numbers (39 TB, 12 GB/day) are from xrpl.org (January 2026), not measured by us.
- The MongoDB 8.0 commands for RHEL and Homebrew (marked UNVERIFIED above).
- Python package names on RHEL 9.
- The WiredTiger cache example value.
- WSL networking advice.

---

## Official sources

xrpld (xrpl.org, XRPLF):

- System Requirements: <https://xrpl.org/docs/infrastructure/installation/system-requirements>
- Capacity Planning (node_size, disk): <https://xrpl.org/docs/infrastructure/installation/capacity-planning>
- Install on Ubuntu or Debian: <https://xrpl.org/docs/infrastructure/installation/install-xrpld-on-ubuntu>
- Install on Red Hat Enterprise Linux: <https://xrpl.org/docs/infrastructure/installation/install-xrpld-on-rhel>
- Configure Full History: <https://xrpl.org/docs/infrastructure/configuration/data-retention/configure-full-history>
- Forward Ports for Peering: <https://xrpl.org/docs/infrastructure/configuration/peering/forward-ports-for-peering>
- Server states: <https://xrpl.org/docs/references/http-websocket-apis/api-conventions/xrpld-server-states>
- server_info method: <https://xrpl.org/docs/references/http-websocket-apis/public-api-methods/server-info-methods/server_info>
- Rate Limiting (admin clients are not limited): <https://xrpl.org/docs/references/http-websocket-apis/api-conventions/rate-limiting>
- Server Doesn't Sync: <https://xrpl.org/docs/infrastructure/troubleshooting/server-doesnt-sync>
- Diagnosing Problems: <https://xrpl.org/docs/infrastructure/troubleshooting/diagnosing-problems>
- Migrate from rippled to xrpld: <https://xrpl.org/docs/infrastructure/installation/migrate-to-xrpld>
- Update Automatically on Linux: <https://xrpl.org/docs/infrastructure/installation/update-xrpld-automatically-on-linux>
- Build from source (macOS/Windows development): <https://xrpl.org/docs/infrastructure/installation/build-on-linux-mac-windows>
- xrpld releases: <https://github.com/XRPLF/rippled/releases>

MongoDB:

- Install on Ubuntu (8.0): <https://www.mongodb.com/docs/v8.0/tutorial/install-mongodb-on-ubuntu/>
- Install on Red Hat (8.0): <https://www.mongodb.com/docs/v8.0/tutorial/install-mongodb-on-red-hat/>
- Install on macOS: <https://www.mongodb.com/docs/manual/administration/install-community-macos/>
- Install on Windows (also: not supported on WSL): <https://www.mongodb.com/docs/manual/administration/install-community-windows/>
- SCRAM authentication: <https://www.mongodb.com/docs/manual/tutorial/configure-scram-client-authentication/>
- Connection strings: <https://www.mongodb.com/docs/manual/reference/connection-string/>
- Production Notes (XFS, WiredTiger cache): <https://www.mongodb.com/docs/manual/administration/production-notes/>
- Driver compatibility (PyMongo vs server): <https://www.mongodb.com/docs/drivers/compatibility/?driver-language=python&python-driver-framework=pymongo>
- mongodump: <https://www.mongodb.com/docs/database-tools/mongodump/>

Python: <https://www.python.org/downloads/>
