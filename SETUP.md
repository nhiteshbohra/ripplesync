# SETUP (Ubuntu / WSL Ubuntu)

## 0. Read first — full history is huge
Per xrpl.org "Configure Full History" (Jan 2026): **~39 TB SSD**, +~12 GB/day, NuDB files exceed ext4's 16 TB file limit → **use XFS or ZFS**, sync from peers takes **months**. WSL's default virtual disk is ext4 → full history will not fit/work there. Use a native Linux box with a big XFS disk, or `HISTORY_MODE=pruned` for testing. MongoDB needs its own large disk too.

## 1. Copy the project to Ubuntu
```
cp -r /mnt/f/downloads/ripple*/ ~/xrpl && cd ~/xrpl      # keep it on the Linux filesystem
```

## 2. Preflight (changes nothing)
```
bash setup/setup_ubuntu.sh check
```
Fix anything it reports (RAM ≥ 64 GB for `huge`, filesystem, free space, systemd in WSL).

## 3. Install + configure the node
```
DB_PATH=/data/xrpld/nudb HISTORY_MODE=full bash setup/setup_ubuntu.sh node
# testing on a small disk:  HISTORY_MODE=pruned NODE_SIZE=medium bash setup/setup_ubuntu.sh node
```
Uses the official XRPLF apt repo, backs up `/etc/xrpld/xrpld.cfg`, sets `[node_db]` (NuDB), `[ledger_history] full`, `[node_size]`, `[rpc_startup]` log level, and a local admin websocket on `127.0.0.1:6006`. For full history add peers holding full history (docs page above): `FIXED_PEERS="host 51235;host2 51235"`.

## 4. Ingest side
```
MONGO_URI=mongodb://127.0.0.1:27017 bash setup/setup_ubuntu.sh ingest
```
Creates `.venv`, installs deps, writes `.env` (chmod 600), runs the tests, pings Mongo, installs `xrpl-tail.service`.
WSL → Windows Mongo: enable `networkingMode=mirrored` in `C:\Users\<you>\.wslconfig` (then `wsl --shutdown`), or use the host IP the script prints and set Mongo `bindIp` + firewall.

## 5. Watch the node sync, then ingest
```
bash setup/check_node.sh          # server_state, complete_ledgers
source .venv/bin/activate
python -m xrpl_ingest backfill --start 32570 --end 33570    # trial
python -m xrpl_ingest verify   --start 32570 --end 33570
nohup python -m xrpl_ingest backfill > backfill.log 2>&1 &  # full; resumable, safe to rerun
sudo systemctl enable --now xrpl-tail                        # live ledgers
```
Stop gracefully: `echo current_process=0 > process_status.txt`. Retry gaps: `python -m xrpl_ingest retry-failed`.

---

# Manual install of the node (what the script automates)

`rippled` was renamed **`xrpld`** (package, service and config names all use `xrpld`; older guides say `rippled`). Commands below are copied from xrpl.org "Install on Ubuntu". Needs Ubuntu 20.04+ (22.04/24.04 x86_64 best tested), glibc 2.31+.

## A. Install
```
sudo apt -y update
sudo apt -y install apt-transport-https ca-certificates wget gnupg
sudo install -m 0755 -d /etc/apt/keyrings && \
    sudo wget -qO /etc/apt/keyrings/xrplf.asc https://packages.xrplf.org/xrplf.asc
gpg --show-keys /etc/apt/keyrings/xrplf.asc        # compare fingerprint with the docs page
echo "deb [signed-by=/etc/apt/keyrings/xrplf.asc] https://packages.xrplf.org/repository/deb-stable any main" | \
    sudo tee /etc/apt/sources.list.d/xrplf.list
sudo apt -y update && sudo apt -y install xrpld
systemctl status xrpld.service
```

## B. Configure (`/etc/xrpld/xrpld.cfg`)
Stop first: `sudo systemctl stop xrpld`. Back up: `sudo cp /etc/xrpld/xrpld.cfg /etc/xrpld/xrpld.cfg.bak`. Edit with `sudo nano /etc/xrpld/xrpld.cfg`. Each stanza name appears **once**; replace the packaged one, don't add a second.

```
[node_db]
type=NuDB
path=/data/xrpld/nudb        # big SSD, XFS/ZFS. NO online_delete / advisory_delete lines for full history

[ledger_history]
full

[node_size]
huge                         # 64 GB RAM; use large/medium on smaller machines

[rpc_startup]
{ "command": "log_level", "severity": "warning" }

[server]                     # add this port name to the existing list
port_ws_admin_local

[port_ws_admin_local]        # what the ingest code connects to
port = 6006
ip = 127.0.0.1
admin = 127.0.0.1
protocol = ws

[ips_fixed]                  # full history only: peers that hold full history (see docs; s2.ripple.com is mentioned there)
<host> 51235
```
Pruned test node instead: keep `online_delete=2000` and `advisory_delete=0` in `[node_db]` and delete the `[ledger_history]` stanza (keeps only ~2000 ledgers).

## C. Data directory and start
```
sudo mkdir -p /data/xrpld/nudb
id xrpld && sudo chown -R xrpld:xrpld /data/xrpld      # service user; check the name with: ps -o user= -C xrpld
sudo systemctl enable --now xrpld
sudo journalctl -u xrpld -f                            # Ctrl+C to leave
```
Without systemd (plain WSL): `xrpld --conf /etc/xrpld/xrpld.cfg`.

## D. Check it works
```
bash setup/check_node.sh          # after ingest step; shows server_state, complete_ledgers
# or the node's own CLI (path may differ: dpkg -L xrpld | grep bin/)
xrpld server_info
```
Good signs: `server_state` goes `connected → syncing → tracking → full/proposing`; `complete_ledgers` (e.g. `32570-1000000`) grows. Full history: months. Ingest only sees ledgers listed there.

## E. Common problems
- **Service won't start:** `journalctl -u xrpld -n 100` — usually bad path/ownership, or a duplicated stanza.
- **Connection refused on 6006:** stanza missing, not listed in `[server]`, or node not running.
- **Disk full / very slow:** DB on `/mnt/c`, or non-SSD; move `path=` and re-sync.
- **`complete_ledgers` = `empty`:** still syncing; wait.

## Not covered
Automatic xrpld updates (docs: "Update Automatically on Linux") — not scripted yet; see TODO.md.
