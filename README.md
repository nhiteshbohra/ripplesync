# XRPL ingest: full XRP Ledger history into MongoDB

This program copies every ledger and every transaction of the XRP Ledger (XRPL) into MongoDB.
It reads from **your own xrpld node** (for a small test it can also read from public servers).
It copies the old history ("backfill") and follows new ledgers live ("tail").

**Start here:** [Choose how to set up](#choose-how-to-set-up). There is an **automatic** installer for Linux and for Windows, and every step is also written out for a **manual** setup.

> **Status:** the code is tested with unit tests, a fake node and a local MongoDB, and a small trial ran on real mainnet data from public servers (Windows, 2026-10-19; see [Manual test on a small computer](#manual-test-on-a-small-computer)).
> It has **not yet run against your own xrpld node** or on a large range, and the installers are not yet tested on a real Ubuntu host. See [Not verified yet](#not-verified-yet).
> Text marked **UNVERIFIED** comes from documentation or reasoning, not from a test on real hardware.

## Table of contents

**Setup**

1. [Choose how to set up](#choose-how-to-set-up)
2. [Automatic setup](#automatic-setup) ([Linux](#automatic-setup-linux-ubuntu--debian--wsl2), [Windows](#automatic-setup-windows-10--11))
3. [Manual setup](#manual-setup)
4. [Manual test on a small computer](#manual-test-on-a-small-computer)
5. [Requirements](#requirements)
6. [Which operating system for what](#which-operating-system-for-what)
7. [Step 1: Install the node (xrpld)](#step-1-install-the-node-xrpld)
8. [Step 2: Configure the node for full history](#step-2-configure-the-node-for-full-history)
9. [Step 3: Wait for the node and check it](#step-3-wait-for-the-node-and-check-it)
10. [Step 4: Install MongoDB](#step-4-install-mongodb)
11. [Step 5: Install this program](#step-5-install-this-program)
12. [Step 6: Start ingesting](#step-6-start-ingesting)
13. [Step 7: Run as a service](#step-7-run-as-a-service)

**Reference**

14. [What it does](#what-it-does)
15. [How it works](#how-it-works)
16. [Data model: nothing is dropped](#data-model-nothing-is-dropped)
17. [Checking that data is complete](#checking-that-data-is-complete)
18. [Troubleshooting](#troubleshooting)
19. [Upgrading and backups](#upgrading-and-backups)
20. [Not verified yet](#not-verified-yet)
21. [Official sources](#official-sources)

---

## Choose how to set up

First pick **what** you want to run, then **how** you want to install it.

**What:**

| Setup | What you get | What it needs |
|---|---|---|
| **test** (Linux) | **The server setup in small.** Your own node, MongoDB, this program, the same settings and the same services as the server. Only the node is small: it keeps the recent ledgers (about 2 hours, ~1 GB). The installer then checks that real data arrives: live `tail`, `backfill`, `verify`. | 16 GB RAM, 4 CPU cores, 50 GB SSD (xrpl.org minimum for testing). 8 GB RAM works, slower. |
| **server** (Linux) | The real thing: your own full-history node and the whole history in MongoDB. | Linux server, 64 GB RAM, 8+ cores, ~39 TB NVMe with XFS. See [Requirements](#requirements) |
| **test** (Windows) | The node cannot run on Windows, so the test reads a little real data from public XRPL servers. | Any Windows 10 / 11 computer |
| **connect** (Windows) | This program on Windows, the node on a Linux machine. | A node you can reach from Windows |

**test vs server on Linux:** the same program, settings, commands and services. The differences: the node keeps only recent ledgers (so `backfill` works only on the last ~2 hours, not on the old history), the node size is `small` or `medium` instead of `huge`, and the data goes into its own database `xrpl_test` instead of `xrpl_data_collection`.

**How:**

| Your system | Automatic (recommended) | Manual |
|---|---|---|
| Ubuntu 20.04+ / Debian 11+ (also WSL2) | `bash install.sh`: see [Automatic setup: Linux](#automatic-setup-linux-ubuntu--debian--wsl2) | [Manual setup](#manual-setup) |
| Windows 10 / 11 | double-click `install.cmd`: see [Automatic setup: Windows](#automatic-setup-windows-10--11) | [Manual setup](#manual-setup) |
| macOS | `bash install.sh`: Python environment only (Homebrew, else uv); MongoDB and node setup are manual | [Manual setup](#manual-setup) |
| RHEL / Rocky / Alma | no installer | [Manual setup](#manual-setup) |

Automatic and manual do the **same steps**. The installers first show what they found and what they will change, and ask before they change anything. You can run them again at any time; that is safe.

---

## Automatic setup

### Python version

**Python 3.12 is the tested version.** Python 3.10+ may run, but Python 3.14 is known
to hang `test_pool_worker_death_stops_backfill_and_counts_lost_chunks`.

The installer looks for 3.12 on PATH, in uv's Python directory and pyenv, then asks
`Install Python 3.12 now? [Y/n]` if it is missing. Enter means yes. On Linux it uses
apt when a 3.12 candidate is available, otherwise uv (including Ubuntu releases
that do not package 3.12). On macOS it tries Homebrew first, otherwise uv. The uv
installation is per user and does not replace system Python. The installer never
adds a third-party PPA automatically.

`--yes` accepts installation and backs up an incompatible `.venv` before rebuilding
it. `--check` only reports what would happen and changes nothing. If installing
3.12 is declined or fails, using another Python >= 3.10 requires a separate explicit
confirmation; unattended runs stop instead of accepting that fallback. An existing
3.12 `.venv` is reused. Windows also uses `py -3.12` and offers to back up and rebuild
an incompatible `.venv`.

For manual commands, see [Create and use the virtual environment](#53-create-and-use-the-virtual-environment).

Installer self-check: `bash setup/check_python.sh` exercises the Python choices and
venv replacement using stubs; it does not install Python or modify your services.

### Get the code

Linux / WSL2:

```bash
git clone https://github.com/nhiteshbohra/ripplesync.git
cd ripplesync
```

Windows (PowerShell):

```powershell
git clone https://github.com/nhiteshbohra/ripplesync.git
cd ripplesync
```

No git? Download the ZIP instead: [Step 5.2](#52-get-the-code).

### Automatic setup: Linux (Ubuntu / Debian / WSL2)

Run it as your **normal user**, not with `sudo`. It asks for your password when it needs it.

```bash
bash install.sh --check     # optional first: only look and report, changes nothing
bash install.sh             # look, recommend, ask, install
```

What happens:

1. **It looks at this computer** and prints a report: system, RAM, CPU cores, free disk, Python, MongoDB, the xrpld node (installed? running? synced?), and whether the node's validator settings are correct.
2. **It asks: `test` or `server`?** It recommends one for this computer (`server` only with 64 GB RAM and a ~39 TB disk).
3. **It shows the plan** (what it will install or change) and asks `Continue? [Y/n]`.
4. **It does the work**, in this order:
   - system packages (Python, venv, pip, curl);
   - MongoDB 8.0 from the official MongoDB repository, if it is missing (not inside WSL2: there MongoDB runs on Windows);
   - xrpld from the official XRPLF repository (the signing key is checked) and its config (a backup is kept): `test` = recent ledgers only, node size `small` or `medium` (from the RAM); `server` = full history, node size `huge`. Then the validator check (and repair, if you agree), the start, and a 20-second check that xrpld keeps running;
   - the Python environment `.venv` and its packages;
   - the settings file `.env`, the same settings for both setups; only the database differs (`xrpl_test` / `xrpl_data_collection`). Existing MongoDB settings, including `MONGO_URI_FILE`, are kept. UTF-8/BOM and UTF-16/BOM files are supported; changed files have uniquely named backups. Linux reads the existing node WebSocket port; set `NODE_WS` explicitly to override that endpoint;
   - the test suite and the `doctor` check;
   - the services `xrpl-tail` and `xrpl-backfill` (installed, not started).
5. **`test` only: the real-data checks.** It waits until the node is synced (up to 30 minutes; a new node usually needs 5-20), then runs the same commands you use on the server and reports the result:
   1. `fetch --ledger validated`: the newest ledger is stored;
   2. `backfill` of 200 recent ledgers from your node;
   3. `verify` of the same range;
   4. `tail` for 60 seconds with a graceful stop, then `health` and `status`.

   If the node needs longer, it says so; run the checks later with `bash install.sh --trial`.
6. **It prints the next commands.**

Options:

| Command | What it does |
|---|---|
| `bash install.sh --check` | Only look and report. Changes nothing. |
| `bash install.sh --mode test` | Test setup (server in small), without the question |
| `bash install.sh --mode server` | Full-history server, without the question |
| `bash install.sh --trial` | Test setup only: run the real-data checks again (waits for the node first) |
| `bash install.sh --skip-tests` | Everything except the test suite (for example after a stuck test) |
| add `--yes` | Accept every default, no questions |

You can also give settings in front of the command, for example `DB_PATH=/data/xrpld/db/nudb bash install.sh --mode server`:
`MONGO_URI=...` (MongoDB to use), `DB_PATH=...` (folder for the node database; default `/var/lib/xrpld/db/nudb`), `NODE_SIZE=small|medium|huge` (default: `test` from the RAM, `server` huge), `FIXED_PEERS="host port;host port"` (peers with full history), `SYNC_WAIT_MIN=30` (`test`: minutes to wait for the node before the checks), `TEST_TIMEOUT_MIN=10` (the test suite normally needs about 1 minute; after this many minutes it is stopped, and the installer asks whether to continue), `FORCE=1` (continue when a hardware check fails).

If xrpld does not keep running, the installer prints the reason from xrpld's own log (`/var/log/xrpld/debug.log`) and checks the validator settings. See [Troubleshooting](#troubleshooting), fix it, and run the installer again.

Before `--mode server`, prepare the big XFS disk ([Step 2.1](#21-prepare-the-big-disk-xfs)) and pass its folder as `DB_PATH`.

### Automatic setup: Windows 10 / 11

Windows cannot run the XRPL node itself (xrpl.org: Windows is for development only). On Windows the program runs as a **test** (public servers) or **connected** to your node on a Linux machine.

1. Get the code (see above).
2. Open the folder in Explorer and **double-click `install.cmd`**. (Or in PowerShell, in the folder: `.\install.cmd`.)

What happens:

1. **It looks at this computer**: Windows version, RAM, CPU, free disk, Python, MongoDB, and winget (the package manager built into Windows 10 / 11).
2. **It asks**: `test` or `connect`. For `connect` it asks for the node's address.
3. **It shows the plan** and asks `Continue? [Y/n]`.
4. **It does the work**: Python 3.12 (with winget, if missing), MongoDB 8.0 (with winget, if missing; Windows asks for administrator rights; it runs as a Windows service), `.venv`, `.env`, the tests and `doctor`. `test` also stores one real ledger as a final check.
5. **It prints the next commands.**

Options (PowerShell, in the folder):

| Command | What it does |
|---|---|
| `.\install.cmd -Check` | Only look and report |
| `.\install.cmd -Mode test` | Test setup, without the question |
| `.\install.cmd -Mode connect -NodeUrl ws://127.0.0.1:6006` | Use your own node |
| add `-Yes` | Accept every default, no questions |

**Connecting to a node on another machine.** The node's admin port (6006) only listens on that machine (`127.0.0.1`), on purpose. Reach it through an SSH tunnel; Windows 10 / 11 includes `ssh`. Keep this window open while the program runs:

```powershell
ssh -N -L 6006:127.0.0.1:6006 you@your-linux-server
```

Then use `-NodeUrl ws://127.0.0.1:6006`. A node in WSL2 with mirrored networking is also reachable as `ws://127.0.0.1:6006`. **UNVERIFIED:** both ways are not tested yet.

### After the automatic setup

Open a terminal in the project folder and activate the environment once per session:

```bash
source .venv/bin/activate          # Linux
```

```powershell
.\.venv\Scripts\Activate.ps1          # Windows
```

Then use the commands in [Step 6](#step-6-start-ingesting) and, for a server, the services in [Step 7](#step-7-run-as-a-service).

**From test to real use:** on the server machine run `bash install.sh --mode server` (Windows: `-Mode connect`). Running it on the test computer switches `.env` to the server database and keeps a backup. The test database `xrpl_test` stays until you delete it as shown in [Manual test on a small computer](#manual-test-on-a-small-computer) (step 4).

**Not done by the installers** (do these by hand when you need them): preparing the big XFS disk ([Step 2.1](#21-prepare-the-big-disk-xfs)), the firewall and router ([Step 2.3](#23-firewall)), a MongoDB password ([Step 4.5](#45-enable-authentication-password)), starting `tail` / `backfill` once the node is synced ([Step 6](#step-6-start-ingesting)).

---

## Manual setup

The same steps as the installers, by hand, with the reason for each step. Pick your path:

| Goal | Follow |
|---|---|
| Test on a laptop or desktop (any OS) | [Manual test on a small computer](#manual-test-on-a-small-computer) |
| Node or full-history server (Linux) | [Requirements](#requirements), [Which operating system](#which-operating-system-for-what), then [Step 1](#step-1-install-the-node-xrpld) to [Step 7](#step-7-run-as-a-service) in order |
| Windows, connected to your node | [Step 4.4](#44-windows-msi-installer) (MongoDB), [Step 5](#step-5-install-this-program) (this program; set `SERVERS` to your node), [Step 6](#step-6-start-ingesting), [Step 7.2](#72-windows-task-scheduler-deploywindows) |

---

## Manual test on a small computer

**Automatic instead:** on Linux `bash install.sh --mode test` does Levels 1 and 2 and Level 3 with choice B (your own small node). On Windows `install.cmd -Mode test` does Levels 1 to 3 with choice A (public servers).

You do not need a server, a node or 39 TB to see the program work. On a normal laptop or desktop (Windows, macOS or Linux) you can test it in three levels. Each level adds one thing. Stop at the level you need.

| Level | What you need | What it proves | Time |
|---|---|---|---|
| **1. Tests only** | Python 3.12 (tested) and the code | The code works on this computer: all commands, retries, stop, lock, lossless storage, against a fake node and an in-memory database | ~5 minutes |
| **2. Tests + local MongoDB** | Level 1 + MongoDB installed on this computer | The whole backfill path end to end: real worker processes, real websockets (fake node), real MongoDB | +15 minutes |
| **3. Real XRPL data, small amount** | Level 2 + internet | Real mainnet ledgers go into your MongoDB: `fetch`, `backfill`, `verify`, `tx`, `tail`, `status`, `health` | +15 minutes |

What a small computer is enough for: Python 3.12 (3.10+ may run; 3.14 has a known test hang), about 2 GB free disk, any normal RAM. MongoDB 8.0 on an x86_64 CPU needs AVX support (most CPUs from 2011 on).
Do **not** run a full-history backfill this way. That needs the server in [Requirements](#requirements).

### Level 1: tests only (no node, no MongoDB)

Get the code and install it (details in [Step 5.2](#52-get-the-code) and [Step 5.3](#53-create-and-use-the-virtual-environment)):

Linux / macOS / WSL:

```bash
git clone https://github.com/nhiteshbohra/ripplesync.git
cd ripplesync
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
XRPL_INGEST_NO_INTEGRATION=1 python -m pytest -q
```

Windows (PowerShell):

```powershell
git clone https://github.com/nhiteshbohra/ripplesync.git
cd ripplesync
py -3.12 -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass   # only if activation is blocked
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:XRPL_INGEST_NO_INTEGRATION=1; python -m pytest -q
```

Good result: a last line like `634 passed, 9 skipped`. The exact numbers can change between versions; what matters is **0 failed**. Some tests are skipped on purpose on one operating system (for example Linux-only signal tests on Windows).

### Level 2: add a local MongoDB

1. Install MongoDB 8.0 on this computer: [Windows](#44-windows-msi-installer), [macOS](#43-macos-homebrew), [Ubuntu](#41-ubuntu-mongodb-80-apt). Keep the default: it listens on `127.0.0.1:27017` without a password. Do **not** expose it to the network.
2. Run all tests, now including the end-to-end ones:

```bash
python -m pytest -q                  # all tests
python -m pytest -q -m integration   # only the end-to-end tests
```

The end-to-end tests start a fake node on your computer, run a real backfill with worker processes into a **throwaway database** named `xrpl_ingest_test_<random>`, check it, and **delete that database** at the end. No other database is touched and your `.env` is not read.
If they show as skipped, MongoDB is not reachable at `mongodb://127.0.0.1:27017` (is the service running?). They also expect MongoDB **without** a password; on a MongoDB with authentication switched on, use Level 1 instead.

### Level 3: real XRPL data, a small amount

A normal computer cannot hold a full-history node. For a small test you have two choices for the "node":

- **A. A public mainnet server (easiest, any OS; what the Windows installer uses).** Public servers such as `wss://s2.ripple.com` and `wss://xrplcluster.com` have the full history and accept the same requests. They **rate-limit** clients, so use them only for a few hundred ledgers with low speed settings. This is a test only; for real ingestion use your own node (that is why this program exists).
- **B. A small node of your own (Linux or WSL2 only; what the Linux installer uses, the same as the server in small).** xrpld that keeps only recent ledgers (*pruned*) fits on a normal disk: about 1 GB for its database. xrpl.org asks for 16 GB RAM, 4 CPU cores and 50 GB SSD even for testing. Automatic: `bash install.sh --mode test`. By hand: install xrpld ([Step 1](#step-1-install-the-node-xrpld)), then set it to pruned with the same tool the installer uses (a backup is kept):
  ```bash
  sudo python3 setup/edit_cfg.py /etc/xrpld/xrpld.cfg --db-path /var/lib/xrpld/db/nudb --history pruned --node-size medium
  sudo python3 setup/xrpld_check.py check        # validator settings correct?
  sudo systemctl restart xrpld
  ```
  Then wait until it is synced ([Step 3](#step-3-wait-for-the-node-and-check-it)). It only has the newest ledgers: use `status` to see its `complete_ledgers` and test `tail`, `fetch --ledger validated` and a `backfill` inside that range.

**1. Make a separate settings file for the test.** Use `.env.test`, not `.env`, so your test settings never mix with server settings (`.env.test` is ignored by git). Put this in it (a separate database name `xrpl_test`, one worker process, two requests at a time):

```ini
MONGO_URI=mongodb://127.0.0.1:27017
DB_NAME=xrpl_test
# Choice A (public mainnet servers). Choice B: SERVERS=ws://127.0.0.1:6006
SERVERS=wss://s2.ripple.com,wss://xrplcluster.com
EXPECTED_NETWORK_ID=0
NUM_PROCESSES=1
CONCURRENCY=2
CHUNK_SIZE=100
```

Never put testnet servers in `SERVERS` (they report another `network_id` and are refused anyway).

**2. Tell the program to use that file** (in every new terminal window, inside the project folder, venv active):

```bash
export ENV_FILE=.env.test            # Linux / macOS / WSL
```

```powershell
$env:ENV_FILE=".env.test"            # Windows PowerShell
```

**3. Run the commands:**

```bash
python -m xrpl_ingest config                                 # shows DB_NAME = xrpl_test, SERVERS = ... [.env]
python -m xrpl_ingest doctor                                 # no FAIL lines wanted; see the note below
python -m xrpl_ingest fetch --ledger 32570                   # the first mainnet ledger
python -m xrpl_ingest backfill --start 32570 --end 32769     # 200 old ledgers
python -m xrpl_ingest verify --start 32570 --end 32769       # prints OK if nothing is missing
python -m xrpl_ingest fetch --ledger validated               # the newest ledger (has transactions)
python -m xrpl_ingest status                                 # counts and progress
python -m xrpl_ingest tail                                   # live ledgers; stop with Ctrl+C (or a line tail=0 in process_status.txt)
python -m xrpl_ingest health                                 # after tail: is it keeping up?
```

`doctor` on a test computer: `WARN node state ? (not synced yet)` is normal with public servers (they do not show their state); `node network` must say `network_id 0`. `INFO mongo auth not authenticated` is fine for a MongoDB that only listens on `127.0.0.1`. Any `WARN <package> installed; requires ...` means: run `pip install -r requirements.txt` again with the venv active.

Look up one stored transaction against the node: take any `hash` from the `transactions` collection (MongoDB Compass, or `mongosh`) and run `python -m xrpl_ingest tx --hash <hash>`. It prints the stored copy, the node's copy, and `same` if they match.

What we saw on a Windows laptop with choice A (2026-10-19): `fetch` stored ledger 32570; the 200-ledger `backfill` finished in about 70 seconds (about 3 ledgers per second at these low settings) with 0 failed; `verify` printed `OK`; the newest ledger had 43 transactions; `tx` printed `same`; `tail` followed new ledgers and stopped cleanly on `tail=0`; `health` reported `healthy` with a lag of 1 ledger.

Note: `tail` starts after the highest ledger already stored. Backfill fills the history behind it; `tail` never goes back. `status` shows an ETA of hundreds of days at test speed: that is expected, the real speed comes from your own node and more workers.

**4. Clean up.** Delete only the test database `xrpl_test`.

With `mongosh` (Linux / macOS / WSL):

```bash
mongosh --quiet --eval 'db.getSiblingDB("xrpl_test").dropDatabase()'
```

On every OS, including Windows, with the venv active (no `mongosh` needed):

```bash
python -c "from pymongo import MongoClient; MongoClient('mongodb://127.0.0.1:27017').drop_database('xrpl_test')"
```

Then `unset ENV_FILE` (PowerShell: `Remove-Item Env:ENV_FILE`), or close the terminal. You can also delete `process_status.txt` and `xrpl_ingest.*.lock` if they were created in the project folder.

**5. Moving to the server.** Nothing from the test is copied. On the server run `bash install.sh --mode server`, or follow Steps 1 to 7: your own node, a real `.env` (from `.env.example`), MongoDB with a password, and the normal speed settings.

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
| Python | **3.12 (tested)** | 3.10+ may run; 3.14 is known to hang one test. The installer offers to install 3.12. |
| Python packages | from `requirements.txt` only | `pymongo`, `websockets`, `python-dotenv`, plus `pytest`, `mongomock` for tests. |

---

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

### Option C: automatic (Ubuntu / Debian)

`bash install.sh --mode server` (full history) or `--mode test` (the same in small: recent ledgers only) does Option A, Step 2 and the rest for you. See [Automatic setup: Linux](#automatic-setup-linux-ubuntu--debian--wsl2).
It installs xrpld from the XRPLF apt repository and **checks the key fingerprint itself** (it refuses a key that is not `B655416741221F780FBCFBC9AA84D41A11D29FA9`).

To change only the node config, use the same tools by hand (a backup is kept; safe to repeat):

```bash
sudo python3 setup/edit_cfg.py /etc/xrpld/xrpld.cfg --db-path /data/xrpld/db/nudb --history full --node-size huge
sudo python3 setup/xrpld_check.py check      # validator settings correct?
```

`DB_PATH` must be a dedicated folder. The installer runs `chown -R` on it, so `setup/db_path_guard.sh` refuses system folders and anything under `/home`, `/root` or `/run/user` (the xrpld service cannot see those, because of `ProtectHome=true`).

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

**Validator settings: leave them as the package installed them.** They are in `/etc/xrpld/validators.txt` (`[validators_file]` in `xrpld.cfg`). Do not copy validator keys into `xrpld.cfg` and do not type keys by hand: one wrong character makes xrpld stop a second after it starts. Check, and restore the official settings if needed (backups are kept):

```bash
sudo python3 setup/xrpld_check.py check
sudo python3 setup/xrpld_check.py fix        # only if check reports a problem
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

The installers do this step for you: `install.sh` does 4.1 (it prints the MongoDB key fingerprint but cannot pin it, because MongoDB publishes none to compare with); `install.cmd` does 4.4 with winget.

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

The program runs on Linux, macOS and Windows. Use Python 3.12 (tested). Python 3.10+ may run, but 3.14 is known to hang one test.

### 5.1 Get Python

| OS | Command |
|---|---|
| Ubuntu / Debian | `bash install.sh` detects 3.12 or offers to install it: apt (`python3.12 python3.12-venv`) when available, otherwise uv. Other versions require explicit confirmation. |
| RHEL / Rocky / Alma 9 | `sudo dnf install -y python3.12 python3.12-pip` (**UNVERIFIED** package names; `python3` 3.9 of RHEL 9 is too old) |
| macOS | `bash install.sh` sets up Python and `.venv` only: `brew install python@3.12` if Homebrew exists, otherwise uv. |
| Windows | The installer from [python.org](https://www.python.org/downloads/windows/) (keep "py launcher" ticked), or `winget install Python.Python.3.12`. `install.cmd` installs Python 3.12 with winget if it is missing. |

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

### 5.3 Create and use the virtual environment

A virtual environment (venv) keeps this program's packages apart from the system Python.

Linux / macOS (bash or zsh):

```bash
python3.12 --version
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python --version                 # must say 3.12.x
python -m xrpl_ingest doctor
python -m xrpl_ingest status
```

`doctor` and `status` use your `.env`; configure MongoDB and the node as described
in [Step 5.5](#55-settings-explained) before expecting those checks to pass.

With uv instead (install uv first using the [official instructions](https://docs.astral.sh/uv/getting-started/installation/)):

```bash
uv python install 3.12
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
python --version                 # must say 3.12.x
```

Windows: `install.cmd` does 5.3 and 5.4 for you (see [Automatic setup: Windows](#automatic-setup-windows-10--11)). By hand, in Windows PowerShell:

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

Activate the venv again in every new terminal (`source .venv/bin/activate` on
Linux/macOS, `.\.venv\Scripts\Activate.ps1` in PowerShell). With `.env` configured:

```bash
python -m xrpl_ingest tail
python -m xrpl_ingest backfill --start N --end M
python -m xrpl_ingest verify --start N --end M
python -m xrpl_ingest status
```

Replace `N` and `M` with ledger numbers available on your node. Stop `tail` with
Ctrl+C. Leave the venv with `deactivate`.

On Ubuntu / Debian, `bash install.sh` does 5.3 and 5.4 for you: venv, requirements, `.env` (chmod 600; it asks for `MONGO_URI` with hidden input if no local MongoDB is found), the tests, `doctor`, and for `node` / `server` the two systemd units (see [Step 7](#step-7-run-as-a-service)).

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
| `MONGO_URI` | (required, or `MONGO_URI_FILE`) | MongoDB connection string, e.g. `mongodb://xrpl_ingest:<pw>@127.0.0.1:27017/?authSource=admin`. Must start with `mongodb://` or `mongodb+srv://`. Store initialization rejects unacknowledged writes (`w=0`) with a configuration error; successful writes must report failures before completion markers advance. Other write-concern settings are preserved. |
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
| `CONCURRENCY` | `16` | Fetch/transform producers per backfill process; at most this many ledger requests in flight. A separate bounded queue feeds up to `min(CONCURRENCY, 16)` writers. Total fetch concurrency = `NUM_PROCESSES x CONCURRENCY`. |
| `CHUNK_SIZE` | `1000` | Ledgers per backfill chunk. **Do not change it after the backfill has started:** progress is stored per chunk, and a new size means a new chunk grid (work is redone, `doctor` warns). |
| `MAX_RETRIES` | `5` | Attempts per ledger before it goes to the failed collection. |
| `REQUEST_TIMEOUT` | `30` | Deadline in seconds for sending one request and receiving its answer. |
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

**Progress:** `status` shows progress for the range in the file: chunks done / total, percent, failed ledgers, speed, when the last chunk finished, and estimated time left. Overlapping chunk marks from old grids count each ledger once; a current-grid chunk counts as done only when its whole range is covered. Partial marks and marks crossing the requested range are included. Speed uses stored chunk finish times; speed and ETA are omitted when marks overlap because those timestamps cannot distinguish replayed work from new progress. A marked range may still contain recorded failures; check the failed-ledger count and run `retry-failed`.

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
| `1` | Some ledgers failed, chunk progress could not be saved, the run stopped early (stop file, signal, or `MIN_FREE_GB`), `verify` found problems, `health` lagging/stale, or `doctor` warnings | Read the log. Rerun backfill for unsaved chunk progress; run `retry-failed` or `verify --repair` for failed ledgers. |
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

The lock is held by the operating system (`flock` on Linux/macOS, `msvcrt.locking` on Windows), not by the file's existence. If a process dies, the lock is free again. A leftover `.lock` file is harmless and is never deleted; it keeps the last holder's PID, host and start time. `doctor` reads this metadata without acquiring the lock and reports current ownership as unknown. It cannot block ingestion startup; ingestion itself still enforces the exclusive lock.

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
- Each log event occupies one physical line. Newlines, terminal control characters and traceback line breaks are escaped (for example `\n` and `\x1b`), preserving diagnostic text while preventing node errors from forging separate log lines. This applies to stderr, rotating files and worker records.
- At start the program logs its version and all resolved paths (base folder, `.env`, stop file, range file, log file, lock). Check them if a file "is not found".
- Backfill progress lines look like:

  ```
  chunks 1234/98000 (1.3%), 512 ledgers/s, ETA 2d04h
  ```

- Each completed chunk also logs cumulative fetch, transform and write seconds. Stages
  overlap and several requests run together, so these totals are **not additive elapsed
  time**. Transform time includes executor waiting; it is not a CPU utilization measure.

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

`bash install.sh` (`--mode test` or `--mode server`) renders both templates into `/etc/systemd/system/`: it replaces `@REPO_DIR@` (the repo path) and `@USER@` (your user) and runs `daemon-reload`. It refuses to install the units as root, and refuses a repo path that contains `%`, `$`, `"`, `\` or a newline.

```bash
sudo systemctl enable --now xrpl-tail
sudo systemctl start xrpl-backfill
systemctl status xrpl-tail xrpl-backfill
journalctl -u xrpl-tail -f          # or: journalctl -u xrpl-backfill -f
```

Without the installer, render a unit by hand (replace the path and user):

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

Files (the setup itself is `install.cmd` in the repo folder, see [Automatic setup: Windows](#automatic-setup-windows-10--11)):

| File | Purpose |
|---|---|
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

---

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
- **Explicit transaction lists.** Expanded ledger responses and deep-verification hash responses must contain `transactions`, including `[]` for an empty ledger. An omitted list is treated as malformed rather than saved or verified as an empty ledger.
- **Transaction identity.** Transaction hashes must be nonempty strings and unique within a ledger, including deep-verification hash lists. Invalid identities are rejected before saving a bundle.
- **Unique history coverage.** Backfill merges overlapping or adjacent node history ranges before splitting them into chunks, so repeated availability entries do not download the same ledger twice in a run.
- **Ledger document last.** The ledger document is written only after all its transactions. If it exists, the ledger is complete. A crash in the middle is repaired by running again.
- **Upserts.** Writing the same ledger twice gives the same result. You can always re-run a command safely.
- **Checkpoint conversion.** Legacy string checkpoints are converted only if the value has not changed since it was read; a competing writer's newer numeric checkpoint is preserved.
- **Damaged checkpoints.** A malformed, negative or fractional checkpoint stops tail/status/health with a configuration error; it is never silently reset. Back up the metadata document, inspect stored ledgers and recorded failures, then restore a verified non-negative integer checkpoint. Deleting it can invoke the documented start-at-latest behavior and leave a gap.
- **Network guard.** The program refuses a server that reports another network (for example testnet). This stops testnet data from mixing with mainnet data, a bug of the old scripts.
- **Verification after repair.** `verify --repair --deep` compares node hashes for every repair target, including previously missing ledgers and count mismatches. A failed comparison remains a verification problem; a stop request prevents further comparison requests.
- **Lossless collisions.** Nested booleans, integers and floats remain distinct when retaining conflicting node fields under `raw_` names. Operational error messages remove URL usernames/passwords before logging or saving failure reasons.

### Backfill pipeline and performance checks

Backfill producers fetch and validate/transform ledgers while independent writers save
earlier results. Ledgers with 64 or more transactions use one transform thread per process
to keep that work off the WebSocket reader's event loop; smaller ledgers avoid the thread
handoff. This transaction-count heuristic can miss unusually large metadata in a small
ledger. The existing process pool supplies CPU parallelism. The waiting
queue holds at most `min(CONCURRENCY, 8)` prepared ledgers. This is a ledger-count bound,
not a byte budget: large responses still need enough RAM for in-flight work and batches.

Writers drain available results into batches, capped at 16 ledgers with a soft limit of
2,000 documents (one ledger can exceed that limit). They do not wait for a batch to fill.
Transactions and metadata for the batch are upserted before its ledger completion
documents. Failed batches are retried, then isolated into individual ledger saves so
one invalid ledger does not prevent its neighbours from being saved. Database retries
reuse prepared documents instead of downloading and transforming again; this also
applies to `tail`, `fetch`, retries and repair. Graceful stops drain prepared work before
returning, and interrupted chunks remain unmarked for rerunning.

If the chunk completion write fails after its data was saved, the summary counts it in
`chunks_unmarked` and backfill exits `1`. Successful data stays stored; the next backfill
safely replays the unmarked chunk. The per-chunk log includes `marker_failed=True`.

The stored schema and indexes are unchanged. This pipeline does not store a second raw
copy or mark a ledger complete before transformation. Address/type queries still need
to be evaluated separately from ingestion throughput; their retired indexes are not recreated.

To compare the previous steady-state flow with the pipeline on your machine:

```bash
python setup/benchmark_ingest.py --ledgers 300 --transactions 20 --repeats 3 --compare
python setup/benchmark_ingest.py --ledgers 100 --transactions 200 --repeats 3 --compare
```

The benchmark uses a synthetic node in a separate process and a real MongoDB at
`127.0.0.1:27017`. It uses and removes only randomly named `xrpl_ingest_bench_*`
databases, never loads `.env`, and checks document counts and ledger completeness
after every run. It reports median throughput, write-command counts and pipeline stage
timings. This is an isolated comparison, **not a full-history ETA**: benchmark real
historical ranges on the intended server before forecasting completion. Batching may
reduce database calls without increasing throughput when another resource is saturated.

---

## Data model: nothing is dropped

### The rule

**Every piece of data the node sends is stored in MongoDB.** (Rule from `xrpl_ingest/transform.py`.)

This applies to accepted ledger responses. If two keys in the same nested object would
encode to the same MongoDB field name (for example `a.b` and `a` followed by U+FF0E and
`b`), the response is rejected and the ledger is recorded as failed. Its transformed
bundle is not saved; one field must never silently overwrite the other during conversion.

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
| `block_number` (ledgers) | `block_number` (unique) |
| `transactions` | `hash` (unique), `block_number` |
| `internal_data` (metadata) | `transaction_hash` (unique), `block_number` |
| `failed_transactions` | `block_number` (unique) |
| `last_processed_block_no` (progress) | `kind` + `start` |

Startup does not create the retired address, transaction type, close time, or
`kind_1_start_1_close_time_1` indexes. The separate `kind_1_start_1` and
`transaction_hash_1` indexes are preserved.

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
| xrpld stops a second after it starts (`status=255/EXCEPTION`, last log line "Manifest counts", then "Start request repeated too quickly") | A config error, most often a wrong validator list key (typed by hand) or validator settings copied into `xrpld.cfg` | The real reason is only in xrpld's own log: `sudo tail -n 40 /var/log/xrpld/debug.log`. Check the validators: `sudo python3 setup/xrpld_check.py check`. Restore the official settings (backups kept): `sudo python3 setup/xrpld_check.py fix`, then `sudo systemctl reset-failed xrpld && sudo systemctl restart xrpld`. |
| Installer: "run this script WITHOUT sudo" | Started as `sudo bash install.sh` | Run `bash install.sh`. It asks for sudo itself; as root, `.venv` and `.env` would belong to root. |
| Installer stops at a hardware check (RAM, cores, disk) | This computer is below what that setup needs (`server`: 64 GB RAM, ~39 TB; `test`: at least 8 GB RAM and 10 GB free disk) | Use `--mode test` for `server` problems, or `FORCE=1 bash install.sh ...` if you know what you are doing. |
| Installer seems stuck at "Running the test suite" | Python 3.14 is known to hang one test | Use Python 3.12: rerun the installer and accept installation and rebuilding `.venv`. The Linux test timeout remains `TEST_TIMEOUT_MIN` (10) minutes; output is in `logs/install-tests.log`. |
| `test`: "the node is not synced yet" after 30 minutes | A new node needs time; with 8 GB RAM or a slow disk it can take longer | Normal. Run the checks later: `bash install.sh --trial`. Watch it: `bash setup/check_node.sh`. |
| `test`: backfill exits `3` "node does not have ledgers" | The small node keeps only the last ~2 hours | Use a range inside `node complete_ledgers` from `python -m xrpl_ingest status`. |
| Windows installer: `winget` not found | Old Windows without "App Installer" | Install "App Installer" from the Microsoft Store, or install Python 3.12 and MongoDB 8.0 by hand (Steps 4.4 and 5.1). |
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

If the unit templates in `deploy/systemd/` changed, run the installer again with your setup (`bash install.sh --mode test` or `--mode server`); it renders them again and keeps your `.env` (backup kept).
Windows: run `install.cmd` again (it keeps `.venv` and the MongoDB address in `.env`), then start the tasks again.

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

- The program has **never run against its own xrpld node** (admin websocket on `127.0.0.1:6006`). Tests use a fake node (`tests/fake_xrpld.py`) and a local MongoDB.
- Real data was tested only in a small trial (2026-10-19, Windows, public servers `wss://s2.ripple.com` / `wss://xrplcluster.com`): 200 early ledgers (32570-32769), about 15 recent ledgers with transactions (around ledger 107,758,500), `verify`, `tx`, `tail`, `health`. Large ranges, busy historical periods and long runs are not tested.
- The API v2 `ledger` answer shape (`tx_json` + `meta`, `DeliverMax`) is taken from xrpld 3.4.1 source and docs; the trial above stored real answers without errors, but not every transaction type was checked by hand.
- Whether a mainnet node reports `network_id` without `[network_id]` in its config (source code says: only when configured).
- How the node answers while syncing (`notSynced`, `notReady` errors) and whether those should be retried.
- Port numbers, stanza names and default values of `xrpld.cfg` (taken from the 3.4.1 example config).
- `install.sh` (with `setup/common.sh`) is not tested on a real Ubuntu host yet: checked with `bash -n`, shellcheck and a `--check` run. `install.ps1` ran end to end on Windows 11 (2026-10-20) in `test` mode (tests, `doctor`, one real ledger stored), again in `test` mode (nothing changed), and in `connect` mode (`.env` switched, backup kept); Python and MongoDB were already installed there, so the winget install path is not tested yet. The `test` setup's real-data checks (wait for sync, backfill of recent ledgers, 60-second tail) have not run against a real node yet. The tools they use (`setup/edit_cfg.py`, `setup/envfile.py`, `setup/xrpld_check.py`) have unit tests.
- The service templates `deploy/systemd/xrpl-tail.service`, `deploy/systemd/xrpl-backfill.service` and the Windows scripts `deploy/windows/install-task.ps1` and `run.cmd` are **not tested on a real host** (Task Scheduler behaviour is UNVERIFIED).
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
