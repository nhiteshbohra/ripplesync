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

Why a new program? The old scripts (folder `ripple blockchain pastcode/`, kept only for reference) used **public** XRPL servers.
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
# 1-2. Node: install xrpld, configure full history, start it (Ubuntu)
DB_PATH=/data/xrpld/db/nudb HISTORY_MODE=full bash setup/setup_ubuntu.sh node
# 3. Wait until server_state is "full"
curl -s http://127.0.0.1:5005 -d '{"method":"server_info"}' | python3 -m json.tool | grep -E '"(server_state|complete_ledgers)"'
# 4. MongoDB 8.0 (see Step 4), then this program:
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env && chmod 600 .env && nano .env        # set MONGO_URI
python -m xrpl_ingest doctor                               # everything OK?
python -m xrpl_ingest backfill --start 32570 --end 33570   # small trial
python -m xrpl_ingest verify   --start 32570 --end 33570
python -m xrpl_ingest tail                                 # live (better: as a service, Step 7)
python -m xrpl_ingest backfill                             # full history (resumable)
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
| MongoDB | **7.0 or 8.0** | **Not 9.0.** Our pinned driver (`pymongo>=4.9,<4.11`) fully supports 7.0 and 8.0. MongoDB 9.0 needs pymongo 4.18 ([driver compatibility](https://www.mongodb.com/docs/drivers/compatibility/?driver-language=python&python-driver-framework=pymongo)). 7.0 ends support 2027-08-31, 8.0 ends 2029-10-31. **Use 8.0 for new installs.** |
| Python | **3.10 or newer** | 3.12 is the tested version. |
| Python packages | from `requirements.txt` only | `pymongo`, `websockets`, `python-dotenv`, plus `pytest`, `mongomock` for tests. |

## Which operating system for what

There are three parts: the **node** (xrpld), **MongoDB**, and **this program**. They can run on different machines.

| | Linux server (x86_64) | macOS | Windows | WSL2 (Linux inside Windows) | Docker |
|---|---|---|---|---|---|
| **xrpld node** | **Yes.** Ubuntu 20.04+ / Debian 11+ via `apt`; RHEL / Rocky / Alma 9+ via `dnf`/`yum` | Development only (build from source) | Development only (build from source) | Testing only. **Not for full history** (see below) | No official XRPLF runtime image. Run xrpld on the host. |
| **MongoDB 7.0/8.0** | Yes | Yes (Homebrew) | Yes (MSI installer, Windows service) | **Not supported by MongoDB** | Yes (official image) |
| **This program** | Yes | Yes | Yes | Yes | Yes (`docker-compose.yml`) |

**Recommended production setup:** one Linux server (Ubuntu 24.04 LTS) for xrpld, and MongoDB on the same server on a separate disk, or on a second Linux server.

**WSL2 caveats:**

- The Linux disk of WSL2 is an ext4 filesystem inside a virtual disk file (VHDX). ext4 has the 16 TB single-file limit, so a full-history NuDB cannot work there.
- A database on `/mnt/c/...` (a Windows drive) is far too slow for NuDB.
- MongoDB says it is not supported on WSL ([MongoDB Windows install page](https://www.mongodb.com/docs/manual/administration/install-community-windows/)). If you use WSL, install MongoDB on Windows itself and connect from WSL.
- WSL networking: to reach a Windows service at `127.0.0.1` from inside WSL, use mirrored networking (see [Troubleshooting](#troubleshooting)).
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

The script `setup/setup_ubuntu.sh` does Option A plus Step 2 for you. It makes a backup of the config first.

```bash
bash setup/setup_ubuntu.sh check        # read-only preflight: RAM, disk, filesystem. Changes nothing.
DB_PATH=/data/xrpld/db/nudb HISTORY_MODE=full bash setup/setup_ubuntu.sh node
```

Useful variables: `DB_PATH` (NuDB folder on the big XFS disk), `HISTORY_MODE=full|pruned`, `NODE_SIZE` (`tiny small medium large huge`), `FIXED_PEERS="host port;host port"`, `FORCE=1` (continue past failed checks).
`DB_PATH` must be a dedicated folder. The script refuses system folders because it runs `chown -R` on it.
**UNVERIFIED:** the script has never run on a real Ubuntu machine yet.

`setup_all.sh` (repo root) installs MongoDB + xrpld + this program in one go. It defaults to a small **pruned** node for testing. It overlaps with `setup/setup_ubuntu.sh`.

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
`bash setup/check_node.sh` shows the same basic fields (needs the venv from Step 5).

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

### 4.5 Docker

The repo's `docker-compose.yml` starts MongoDB together with this program, see [Step 7](#step-7-run-as-a-service).
To run MongoDB alone with the official image ([MongoDB on Docker](https://www.mongodb.com/docs/manual/administration/install-community-docker/)):

```bash
docker run --name mongodb -p 127.0.0.1:27017:27017 -v mongodata:/data/db -d mongodb/mongodb-community-server:8.0-ubuntu2204
```

**UNVERIFIED:** the tag `8.0-ubuntu2204`. Pick an existing 8.0 tag from the [tag list](https://hub.docker.com/r/mongodb/mongodb-community-server/tags). Do not use `latest`: it is 9.0 now.
The image needs a CPU with AVX.

### 4.6 Enable authentication (password)

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

### 4.7 Storage and memory advice

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
| Ubuntu / Debian | `sudo apt-get install -y python3 python3-venv python3-pip` |
| RHEL / Rocky / Alma 9 | `sudo dnf install -y python3.12 python3.12-pip` (**UNVERIFIED** package names; `python3` 3.9 of RHEL 9 is too old) |
| macOS | `brew install python@3.12` |
| Windows | `winget install Python.Python.3.12` or the installer from [python.org](https://www.python.org/downloads/windows/). Tick "Add python.exe to PATH". |

Note: for Python 3.12 the last Windows/macOS installer from python.org is 3.12.10. Newer 3.12 releases are source only. Python 3.13 should also work (not tested).

### 5.2 Get the code

Put the code on the machine's own disk. In WSL use the Linux home folder, not `/mnt/c/...`.

```bash
git clone <your repository URL> xrpl-ingest
cd xrpl-ingest
```

### 5.3 Create a virtual environment and install

A virtual environment (venv) keeps this program's packages apart from the system Python.

Linux / macOS (bash or zsh):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Windows PowerShell:

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

Optional: install the short command `xrpl-ingest`:

```bash
pip install -e .
xrpl-ingest status        # same as: python -m xrpl_ingest status
```

`pip install -e .` uses `pyproject.toml`. pip may download `setuptools` for the build step, so it needs internet once.

Optional: run the tests. Why: they prove the install works on this machine.

```bash
python -m pytest -q
# without a local MongoDB the integration tests skip; to skip them on purpose:
XRPL_INGEST_NO_INTEGRATION=1 python -m pytest -q      # PowerShell: $env:XRPL_INGEST_NO_INTEGRATION=1; python -m pytest -q
```

### 5.4 Create `.env`

The program reads its settings from environment variables and from a `.env` file.

**Where `.env` is read from:**

1. If `ENV_FILE` is set (as a real environment variable), that file is used.
2. Otherwise `<base>/.env` is used. The base folder is `XRPL_INGEST_HOME` if set, else the **current folder** when you start the command.
3. The program does **not** search parent folders.

Relative paths (stop file, range file, log file, lock files) are resolved against the base folder. The base folder is the folder of the loaded `.env` when `XRPL_INGEST_HOME` is not set.
So: either `cd` into the project folder before every command, or set `XRPL_INGEST_HOME`.

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
| `MONGO_URI` | (required) | MongoDB connection string, e.g. `mongodb://xrpl_ingest:<pw>@127.0.0.1:27017/?authSource=admin`. Must start with `mongodb://` or `mongodb+srv://`. |
| `MONGO_URI_FILE` | empty | Path of a file that contains the URI. Keeps the password out of `.env` (useful for Docker secrets). |
| `DB_NAME` | `xrpl_data_collection` | Database name. **Use a fresh name if an old (legacy) database causes index errors.** |
| `SERVERS` | `ws://127.0.0.1:6006` | Your node's admin websocket. Comma-separated; first is preferred, others are failover. **Never list testnet servers.** |
| `EXPECTED_NETWORK_ID` | `0` | `0` = mainnet. A server reporting another id is refused. `none` switches the check off (not recommended). |
| `MONGO_SERVER_SELECTION_TIMEOUT_MS` | `10000` | How long to wait for MongoDB before "unreachable". |
| `MONGO_SOCKET_TIMEOUT_MS` | `120000` | Maximum time for one MongoDB operation. |

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
| `NUM_PROCESSES` | `4` | Backfill worker processes. **Windows: at most 61** (Python limit). |
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
| `XRPL_INGEST_HOME` | folder of the loaded `.env`, else current folder | Base folder for `.env` and all relative paths. Set it as a real environment variable (for example in the service file). |
| `ENV_FILE` | empty | Explicit path of the `.env` file. Set it as a real environment variable. |
| `PROCESS_STATUS_FILE` | `process_status.txt` | The stop file, see [Stopping](#66-stopping-gracefully). |
| `BACKFILL_RANGE_FILE` | `backfill_range.txt` | The range file, see [Range file](#62-the-range-file-backfill_rangetxt). |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `LOG_FILE` | empty | Empty = log to the screen (stderr) only. A path = also write a rotating log file. |
| `LOG_MAX_MB` | `100` | Size of one log file before it rotates. |
| `LOG_BACKUPS` | `10` | How many old log files to keep. |
| `MIN_FREE_GB` | `0` | Free-disk warning threshold for `doctor`; `0` = off. |

See your effective settings, with passwords hidden and the source of each value (environment, `.env` or default):

```bash
python -m xrpl_ingest config
```

### 5.6 Run `doctor`

```bash
python -m xrpl_ingest doctor
```

Why: it checks everything before you start a run that may last weeks.
It checks: Python version, installed package versions, the effective config (passwords hidden), resolved paths, `.env` file permissions (Linux/macOS), MongoDB (ping, version, authentication, free space), the node (`server_state`, `network_id`, `complete_ledgers`, validated ledger, peers, version), free disk, RAM, the `NUM_PROCESSES` limit on Windows, chunk documents with a different `CHUNK_SIZE`, index conflicts in an old (legacy) database, and stale lock files.

- `doctor --node`: only the node. `doctor --mongo`: only MongoDB. `doctor --json`: machine-readable output.
- Exit code: `0` all OK, `1` warnings, `2` config error, `4` node or MongoDB unreachable.

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
```

Size estimate: after the trial, look at the database size in `mongosh` (`use xrpl_data_collection` then `db.stats()`).
Early ledgers are small and recent ledgers are much bigger. So also try a recent range (for example 1000 ledgers near the current one) before you estimate the total.

About `tail` after a trial backfill: when `tail` has no checkpoint yet, it continues after the highest stored ledger only if that is at most 1000 ledgers behind.
After a trial of early history it is much further behind. Then `tail` starts at the current ledger and logs the exact `backfill --start ... --end ...` command for the hole. The full backfill fills that hole anyway.

### 6.2 The range file (`backfill_range.txt`)

When you run `backfill` **without** `--start` and `--end`, the range comes from the file `backfill_range.txt` in the base folder.
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
- No file = defaults (`32570` to `latest`), with an INFO log line.
- Keys are not case-sensitive. Only `start` and `end` are allowed.
- An unknown key, a key given twice, or a line without `=` is a config error (exit code `2`). The message names the file and the line.
- The program reads the file once at start and never writes it. Changes apply at the next start.
- The file is in `.gitignore` (it is your local choice).

**Progress:** `status` shows progress for the range in the file: chunks done / total, percent, failed ledgers, speed and estimated time left.

```bash
python -m xrpl_ingest status
```

### 6.3 Command reference

Run every command as `python -m xrpl_ingest <command>` (or `xrpl-ingest <command>` after `pip install -e .`). `--help` works on every command.

| Command | What it does | Example |
|---|---|---|
| `status` | Node `complete_ledgers` and latest ledger; database counts; checkpoint; range-file progress. DB part is shown even if the node is down. | `status` |
| `backfill [--start N] [--end M] [--range-file PATH] [--no-range-file]` | Copies old ledgers in chunks, many processes. Resumable. Only ledgers the node has. | `backfill --start 32570 --end 33569` |
| `tail` | Follows new validated ledgers. Saves a checkpoint after each ledger. | `tail` |
| `retry-failed [--max-rounds N]` | Fetches the ledgers in the failed collection again (default 5 rounds). Connection-loss failures are retried last, one at a time. | `retry-failed --max-rounds 10` |
| `verify --start N --end M [--repair] [--deep]` | Finds missing ledgers and ledgers whose stored transaction count is wrong. `--repair` fetches exactly those again. `--deep` also compares stored transaction hashes with the node and checks the parent-hash chain. | `verify --start 32570 --end 1000000 --repair` |
| `fetch --ledger N\|validated` | Fetches one ledger (or the newest validated one) and stores it. Removes it from the failed list on success. | `fetch --ledger 32570` |
| `tx --hash H` | Compares the stored transaction with the node's answer (spot check). | `tx --hash <64-hex-character hash>` |
| `doctor [--node] [--mongo] [--json]` | Checks the whole setup (see 5.6). | `doctor --node` |
| `health [--max-lag N] [--max-age S]` | For monitoring: is `tail` up to date? Healthy when the checkpoint is at most N ledgers behind the node (default 50) and was updated in the last S seconds (default 120). | `health --max-lag 50 --max-age 120` |
| `config` | Prints the effective settings (passwords hidden) and where each value came from. | `config` |

Details:

- `backfill --no-range-file`: ignore `backfill_range.txt` and use the defaults.
- `backfill --range-file PATH`: use another range file.
- `verify --deep`: the hash comparison asks the node for every ledger, so it is slow and needs the node. The parent-hash check works without the node.
- `fetch` does not mark a backfill chunk as done. It only stores that ledger.
- `tx --hash`: exit code `1` if the stored document differs from the node or is missing.

### 6.4 Exit codes

Every command ends with one of these codes. Scripts and services can react to them.

| Code | Meaning | What to do |
|---|---|---|
| `0` | OK | Nothing. |
| `1` | Some ledgers failed, the run stopped early (stop file / signal), `verify` found problems, `tx` differs, `health` lagging/stale, or `doctor` warnings | Read the log. Run `retry-failed` or `verify --repair`. |
| `2` | Config or argument error (also: node on the wrong network, bad range file) | Fix `.env`, the range file or the command. Retrying will not help. |
| `3` | `backfill`: no failures, but the node does not have part of the range yet | Normal while the node downloads history. Run again later. |
| `4` | Node or MongoDB unreachable or failing | Check that xrpld / mongod run and the URIs are right. |
| `5` | Another instance of the same command is already running (lock held) | The message shows PID, host and start time of the other one. |
| `6` | Unexpected internal error (a bug) | Keep the log and report it. |
| `130` | Hard stop by Ctrl+C (second Ctrl+C) | Run again; all work is resumable. |

### 6.5 Running tail and backfill at the same time

This is allowed and recommended. Each command takes its own lock file `<base>/xrpl_ingest.<command>.lock`. So one `tail` and one `backfill` can run together, but **not two backfills** and **not two tails** (the second one exits with code `5`).
Both write the same way (upserts). If both happen to write the same ledger, the result is the same.

The lock is held by the operating system, not by the file's existence. If a process dies, the lock is free again. A leftover `.lock` file is harmless. `doctor` reports stale locks. Do not delete a lock file while its process runs.

### 6.6 Stopping gracefully

A graceful stop finishes the ledgers in progress, starts nothing new, and exits. Nothing is lost. The next run continues where it stopped.

**Way 1: the stop file** (works everywhere, also Windows). Put a line into `process_status.txt` in the base folder:

| Line | Effect |
|---|---|
| `current_process=0` | stops **all** commands (`tail`, `backfill`, `retry-failed`) |
| `tail=0` | stops only `tail` |
| `backfill=0` | stops only `backfill` |
| `retry-failed=0` | stops only `retry-failed` |

Keys are not case-sensitive (`CURRENT_PROCESS=0` from the old scripts works).
The program reads the file from the top. The **first line that is about the running command** (its own key or `current_process`) decides.
A missing or unreadable file means "keep running".

```bash
echo current_process=0 > process_status.txt               # Linux / macOS / cmd.exe
```

```powershell
Set-Content -Path process_status.txt -Value 'current_process=0'   # PowerShell
```

**Important:** remove the line (or write `current_process=1`) **before you start again**. Otherwise the next run stops at once.

```bash
rm process_status.txt                                      # PowerShell: Remove-Item process_status.txt
```

**Way 2: signals.**

| Signal | Effect |
|---|---|
| SIGTERM (`kill <pid>`, `systemctl stop`, `docker stop`, launchd stop) | graceful stop |
| Ctrl+C (SIGINT), first time | graceful stop |
| Ctrl+C, second time | hard stop, exit code `130` |
| Ctrl+Break (SIGBREAK, Windows console) | graceful stop |

On Windows, "End task" in Task Manager or Task Scheduler kills the process at once (not graceful). Use the stop file there.
Backfill worker processes ignore Ctrl+C themselves; the main process tells them to stop.

### 6.7 Logs

- Default: logs go to the screen (stderr). Under systemd they go to the journal: `journalctl -u xrpl-tail -f`.
- Set `LOG_FILE=logs/xrpl_ingest.log` for a rotating log file (`LOG_MAX_MB`, `LOG_BACKUPS`). Worker processes log through the main process, so there is one file.
- Times are UTC, ISO format, ending with `Z`.
- At start the program logs its version and all resolved paths. Check them if a file "is not found".
- Backfill progress lines look like:

  ```
  chunks 1234/98000 (1.3%), 512 ledgers/s, ETA 2d04h
  ```

---

## Step 7: Run as a service

A service starts the program at boot and restarts it after a crash. Use it at least for `tail`.
The templates are in the `deploy/` folder. Each file has comments at the top: read them and change the paths and user name before installing.
**UNVERIFIED:** none of the templates has been tested on a real machine yet.

### 7.1 Linux: systemd (`deploy/systemd/`)

```bash
ls deploy/systemd/                                         # see the unit files
sudo cp deploy/systemd/xrpl-tail.service /etc/systemd/system/
sudo systemctl edit --full xrpl-tail                       # set User=, paths, XRPL_INGEST_HOME
sudo systemctl daemon-reload
sudo systemctl enable --now xrpl-tail
systemctl status xrpl-tail
journalctl -u xrpl-tail -f
```

- `systemctl stop xrpl-tail` sends SIGTERM = graceful stop.
- The unit should not run as root. Use your normal user.
- After a stop by stop file, the program exits normally, and systemd (with `Restart=on-failure`) does not restart it.
- A config error (exit `2`) should not be retried in a loop (`RestartPreventExitStatus=2`).
- `setup/setup_ubuntu.sh ingest` can also install the `xrpl-tail.service` for you.

### 7.2 macOS: launchd (`deploy/launchd/`)

```bash
ls deploy/launchd/
cp deploy/launchd/<file>.plist ~/Library/LaunchAgents/      # edit paths inside first
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/<file>.plist
launchctl print gui/$(id -u)/<label>                       # status
launchctl bootout gui/$(id -u)/<label>                     # stop (SIGTERM = graceful)
```

### 7.3 Windows: Task Scheduler (`deploy/windows/install-task.ps1`)

Open PowerShell **as Administrator** in the project folder:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
Get-Help .\deploy\windows\install-task.ps1 -Detailed      # read the options
.\deploy\windows\install-task.ps1
```

To stop it gracefully, use the **stop file** (`tail=0` or `current_process=0`). Ending the task in Task Scheduler kills the process at once.

### 7.4 Docker Compose (MongoDB + this program)

`docker-compose.yml` runs **MongoDB and the ingest program** in containers. **xrpld is not in Docker:** there is no official XRPLF runtime image, and a full-history node needs direct access to its 39 TB disk. Run xrpld on the host (Steps 1 to 3).

```bash
cp .env.example .env              # set MONGO_URI / SERVERS as explained in docker-compose.yml
docker compose up -d              # MongoDB + tail
docker compose logs -f
docker compose --profile backfill up -d     # also start the backfill service
docker compose stop               # SIGTERM = graceful stop
```

Reaching the node from a container: xrpld's admin websocket listens on `127.0.0.1` only, and a container's `127.0.0.1` is the container itself. Read the comments in `docker-compose.yml` for how it reaches the host's port 6006. Do not open port 6006 to the network to make this work.

---

## Data model: nothing is dropped

### The rule

**Every piece of data the node sends is stored in MongoDB.**

- **Mapped fields** keep their current names and formats (below). These never change.
- **Every other key** of the ledger header and of each transaction is **also stored**, with its original name.
- **Name collisions:** if a key from the node has the same name as a field the program sets itself, the program's field keeps its meaning, and the node's original value is stored under **`raw_<key>`**.
- **Unsafe key names:** MongoDB field names should not contain `.` or start with `$`. Such keys are **escaped in a reversible way**, so the original name can be restored. (The exact escape format is defined in `xrpl_ingest/transform.py`.)
- **Metadata:** the **complete** transaction metadata (`meta`, including `AffectedNodes`) is stored unchanged in the meta collection.
- **Numbers** that the node sends as strings (for example `Fee`, `Sequence`, XRP amounts in drops) are stored as integers. A value too large for a 64-bit integer stays a string. No information is lost.
- The ledger's `transactions` list is not repeated inside the ledger document. Each transaction is its own document, and its metadata is its own document.

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
| all other header keys | as sent | for example `close_flags`, `close_time_resolution`, `parent_close_time` (whatever the node sends) |

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
| `DeliverMax` | as sent | API v2 name of a Payment's `Amount` |
| `delivered_amount` | int or object or string | from `meta.delivered_amount`: what **really** arrived. Same int/object format. The string `"unavailable"` is kept as is (old ledgers). |
| `affected_accounts` | list of strings | sorted, unique list of every account touched by the transaction (from all `AffectedNodes` fields `Account`, `Destination`, `Owner`, `Issuer`, `HighLimit.issuer`, `LowLimit.issuer`, plus the tx `Account` and `Destination`) |
| `amount_xrp` / `delivered_xrp` | Decimal128 | exact XRP value (XRP amounts only) |
| `amount_currency` / `delivered_currency` | string | `"XRP"` or the token currency code |
| `amount_issuer` / `delivered_issuer` | string | token issuer (tokens only) |
| `amount_value` / `delivered_value` | Decimal128 | exact token value (tokens only) |
| `_inserted_at` | date | when this program wrote it |
| all other transaction keys | as sent | every key of `tx_json` and of the outer transaction object |

**About Payments in API v2:** the node no longer sends `Amount` for Payments, only `DeliverMax`. The program copies `DeliverMax` into `Amount` when `Amount` is missing, and keeps `DeliverMax` too.
`Amount` / `DeliverMax` is the **maximum** to deliver. With partial payments less can arrive. **For "how much was paid", use `delivered_amount`** ([xrpl.org: Partial Payments](https://xrpl.org/docs/concepts/payment-types/partial-payments)).

The readable fields (`amount_*`, `delivered_*`) use Decimal128, never floating point, so values are exact.

### Metadata document (`internal_data`)

| Field | Meaning |
|---|---|
| `transaction_hash` | the transaction it belongs to |
| `block_number`, `block_hash`, `close_time_iso` | its ledger |
| `meta` | the **full** metadata object from the node, unchanged |
| `memos` | the transaction's `Memos` list (empty list if none) |
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

2. **Repair them.** Fetches exactly the missing and mismatched ledgers again. Safe to repeat.

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

   It compares the stored transaction hashes of each ledger with the node's list, and checks that ledger N's `parent_hash` equals ledger N-1's `block_hash`.

5. **Spot-check single transactions.**

   ```bash
   python -m xrpl_ingest tx --hash <hash>
   ```

6. **Fix a single ledger by hand.**

   ```bash
   python -m xrpl_ingest fetch --ledger 12345678
   ```

7. **Watch live ingest.** Run `health` every minute from your monitoring (cron, Nagios, ...). Exit `0` healthy, `1` lagging or stale, `4` unreachable.

   ```bash
   python -m xrpl_ingest health --max-lag 50 --max-age 120
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
| Program stops immediately after start | `process_status.txt` still says `current_process=0` (or `<command>=0`) | Delete the file or write `current_process=1`. |
| Stop file seems ignored | Wrong folder (the base folder, not the current folder), or a line for another command comes first | Check the paths in the start log or `config`. |
| `.env` not found / settings ignored | Started from another folder | `cd` to the project, or set `XRPL_INGEST_HOME` / `ENV_FILE`. |
| `.env` written by PowerShell looks wrong | Encoding (UTF-16) | Supported. If in doubt, save it in Notepad as "UTF-8". |
| Windows: error with `NUM_PROCESSES` above 61 | Python on Windows allows at most 61 worker processes | Set `NUM_PROCESSES` to 61 or less. |
| PowerShell: "running scripts is disabled" | Execution policy | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` |
| WSL cannot reach MongoDB on Windows | WSL uses its own network (NAT) | In `C:\Users\<you>\.wslconfig` add `[wsl2]` and `networkingMode=mirrored`, then `wsl --shutdown`. Or use the Windows host IP (`ip route \| awk '/default/ {print $3}'`) and set MongoDB `bindIp` plus a firewall rule. |
| WSL: node very slow or disk full | Database on `/mnt/c` or ext4 VHDX limit | Use a native Linux server for full history. |
| MongoDB `Authentication failed` | Wrong user/password, missing `authSource=admin`, or special characters not percent-encoded | See Step 4.6. |
| `pymongo` errors with MongoDB 9.0 | 9.0 is not supported by the pinned driver | Use MongoDB 8.0. |

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
- `setup/setup_ubuntu.sh` and `setup_all.sh` have never run on a real Ubuntu machine.
- The service templates in `deploy/` (systemd, launchd, Windows Task Scheduler) and the Docker files are untested.
- SIGTERM graceful stop under real systemd.
- Disk size of MongoDB for full history, and ingest speed (ledgers per second).
- The full-history disk numbers (39 TB, 12 GB/day) are from xrpl.org (January 2026), not measured by us.
- The MongoDB 8.0 commands for RHEL and Homebrew, and the Docker image tag (marked UNVERIFIED above).
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
- Install with Docker: <https://www.mongodb.com/docs/manual/administration/install-community-docker/>
- SCRAM authentication: <https://www.mongodb.com/docs/manual/tutorial/configure-scram-client-authentication/>
- Connection strings: <https://www.mongodb.com/docs/manual/reference/connection-string/>
- Production Notes (XFS, WiredTiger cache): <https://www.mongodb.com/docs/manual/administration/production-notes/>
- Driver compatibility (PyMongo vs server): <https://www.mongodb.com/docs/drivers/compatibility/?driver-language=python&python-driver-framework=pymongo>
- mongodump: <https://www.mongodb.com/docs/database-tools/mongodump/>

Python: <https://www.python.org/downloads/>
