#!/usr/bin/env bash
# ==============================================================================
# Complete All-In-One Setup: MongoDB + Ripple Node (xrpld) + XRPL Ingest
# ==============================================================================
# This script sets up everything from scratch on Ubuntu (20.04 / 22.04 / 24.04 / WSL2):
#   1. System preflight checks
#   2. Installs official MongoDB Community Edition 7.0/8.0 & starts service
#   3. Installs official XRPLF Ripple Node (xrpld), configures /etc/xrpld/xrpld.cfg & starts service
#   4. Sets up Python 3 virtual environment (.venv) and installs all dependencies
#   5. Configures .env with local Mongo & WebSocket admin connections
#   6. Runs test suite to verify full setup health
#   7. Installs systemd xrpl-tail.service for automatic live ledger ingestion
#
# Usage:
#   bash setup_all.sh                               # Interactive or standard pruned defaults
#   HISTORY_MODE=pruned bash setup_all.sh           # Pruned mode (~50 GB disk, syncs in ~20 mins)
#   HISTORY_MODE=full bash setup_all.sh             # Full history mode (~39 TB disk, months sync)
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$SCRIPT_DIR"

# Defaults
HISTORY_MODE="${HISTORY_MODE:-pruned}"
NODE_SIZE="${NODE_SIZE:-medium}"
DB_PATH="${DB_PATH:-/var/lib/xrpld/db/nudb}"
FIXED_PEERS="${FIXED_PEERS:-}"
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"
FORCE="${FORCE:-0}"

# Colors
CYAN='\033[1;36m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
RED='\033[1;31m'
NC='\033[0m'

say()  { printf "${CYAN}==>${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}✔${NC} %s\n" "$*"; }
warn() { printf "${YELLOW}WARN:${NC} %s\n" "$*" >&2; }
die()  { printf "${RED}ERROR:${NC} %s\n" "$*" >&2; exit 1; }

need_ok() {
  if [ "$FORCE" = 1 ]; then
    warn "$1 (continuing because FORCE=1)"
  else
    die "$1 (set FORCE=1 to bypass)"
  fi
}

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  SUDO="sudo"
fi

is_wsl() { grep -qi microsoft /proc/version 2>/dev/null; }
has_systemd() { [ -d /run/systemd/system ]; }

clear 2>/dev/null || true
cat << "BANNER"
  __   __ _____  _____   _        _____                         _   
  \ \ / /|  __ \|  __ \ | |      |_   _|                       | |  
   \ V / | |__) | |__) || |        | |  _ __   __ _   ___  ___ | |_ 
    > <  |  _  /|  ___/ | |        | | | '_ \ / _` | / _ \/ __|| __|
   / . \ | | \ \| |     | |____   _| |_| | | | (_| ||  __/\__ \| |_ 
  /_/ \_\|_|  \_\_|     |______| |_____|_| |_|\__, | \___||___/ \__|
                                               __/ |                
                                              |___/                 
====================================================================
 Complete Automated Setup: MongoDB + Ripple Node (xrpld) + Ingest
====================================================================
BANNER

echo "Configuration:"
echo "  Mode:       $HISTORY_MODE"
echo "  Node Size:  $NODE_SIZE"
echo "  DB Path:    $DB_PATH"
echo "  MongoDB:    $(printf '%s' "$MONGO_URI" | sed -E 's#//[^@/]*@#//***@#')"  # hide credentials
echo ""

# ------------------------------------------------------------------------------
# Phase 1: Preflight & System Check
# ------------------------------------------------------------------------------
say "[Phase 1/6] System Preflight Checks"

. /etc/os-release
echo "OS: $PRETTY_NAME | Arch: $(uname -m) | WSL: $(is_wsl && echo yes || echo no) | systemd: $(has_systemd && echo yes || echo no)"

[ "${ID:-}" = ubuntu ] || warn "Distribution is not Ubuntu. (Ubuntu 22.04/24.04 x86_64 are recommended)"
[ "$(uname -m)" = x86_64 ] || need_ok "Architecture $(uname -m) is not x86_64"

RAM_GB=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
echo "System RAM: ${RAM_GB} GB"
if [ "$HISTORY_MODE" = "full" ] && [ "$RAM_GB" -lt 60 ]; then
  need_ok "Full history mode with node_size=huge strongly recommends >= 64 GB RAM (detected ${RAM_GB} GB)"
fi

PROBE_DIR="$DB_PATH"
while [ ! -e "$PROBE_DIR" ]; do PROBE_DIR="$(dirname "$PROBE_DIR")"; done
FS_TYPE=$(stat -f -c %T "$PROBE_DIR" 2>/dev/null || echo "unknown")
FREE_GB=$(df -BG --output=avail "$PROBE_DIR" 2>/dev/null | tail -1 | tr -dc 0-9 || echo 0)
echo "Storage for DB: $PROBE_DIR ($FS_TYPE, ${FREE_GB} GB free)"

case "$PROBE_DIR" in
  /mnt/*)
    need_ok "DB_PATH is on a Windows mount (/mnt/..). Virtual 9P mounts are too slow and can cause NuDB corruption! Please use native Linux path."
    ;;
esac

if [ "$HISTORY_MODE" = "full" ]; then
  FREE_TB=$((FREE_GB / 1024))
  if [ "$FREE_TB" -lt 40 ]; then
    need_ok "Full history requires ~39 TB SSD/NVMe (detected ~${FREE_TB} TB free). For personal/dev setups, run HISTORY_MODE=pruned instead."
  fi
  case "$FS_TYPE" in
    ext*) need_ok "NuDB full history single files exceed ext4's 16 TB file limit! You must use an XFS or ZFS volume for DB_PATH." ;;
  esac
fi

if is_wsl && ! has_systemd; then
  warn "WSL detected without systemd enabled."
  echo "To enable systemd in WSL, add this to /etc/wsl.conf:"
  echo -e "[boot]\nsystemd=true"
  echo "Then run 'wsl --shutdown' from Windows PowerShell and restart WSL."
fi

ok "Preflight complete"
echo ""

# ------------------------------------------------------------------------------
# Phase 2: Install MongoDB Community Edition
# ------------------------------------------------------------------------------
say "[Phase 2/6] Installing MongoDB Community Edition"

$SUDO apt-get update -y
$SUDO apt-get install -y curl gnupg lsb-release ca-certificates

CODENAME=$(grep -oP '(?<=VERSION_CODENAME=)[a-z]+' /etc/os-release 2>/dev/null || lsb_release -cs 2>/dev/null || echo "jammy")
MONGO_VER="7.0"
if [ "$CODENAME" = "noble" ]; then
  MONGO_VER="8.0"
fi

say "Adding MongoDB $MONGO_VER official APT repository for Ubuntu $CODENAME..."
$SUDO install -m 0755 -d /etc/apt/keyrings
curl -fsSL "https://www.mongodb.org/static/pgp/server-${MONGO_VER}.asc" | \
  $SUDO gpg --dearmor --yes -o "/etc/apt/keyrings/mongodb-server-${MONGO_VER}.gpg" 2>/dev/null || \
  curl -fsSL "https://pgp.mongodb.com/server-${MONGO_VER}.asc" | \
  $SUDO gpg --dearmor --yes -o "/etc/apt/keyrings/mongodb-server-${MONGO_VER}.gpg"

echo "deb [ arch=amd64,arm64 signed-by=/etc/apt/keyrings/mongodb-server-${MONGO_VER}.gpg ] https://repo.mongodb.org/apt/ubuntu ${CODENAME}/mongodb-org/${MONGO_VER} multiverse" \
  | $SUDO tee "/etc/apt/sources.list.d/mongodb-org-${MONGO_VER}.list" >/dev/null

$SUDO apt-get update -y
$SUDO apt-get install -y mongodb-org

if has_systemd; then
  say "Starting mongod service..."
  $SUDO systemctl daemon-reload
  $SUDO systemctl enable --now mongod
  sleep 2
  $SUDO systemctl is-active --quiet mongod && ok "MongoDB service is active and running" || warn "MongoDB service started with warnings"
else
  warn "No systemd: start mongod manually with 'sudo mongod --config /etc/mongod.conf &'"
fi

ok "MongoDB setup complete"
echo ""

# ------------------------------------------------------------------------------
# Phase 3: Install & Configure Ripple Node (xrpld)
# ------------------------------------------------------------------------------
say "[Phase 3/6] Installing Ripple Node (xrpld)"

say "Adding official XRPLF repository..."
$SUDO install -m 0755 -d /etc/apt/keyrings
$SUDO wget -qO /etc/apt/keyrings/xrplf.asc https://packages.xrplf.org/xrplf.asc
echo "deb [signed-by=/etc/apt/keyrings/xrplf.asc] https://packages.xrplf.org/repository/deb-stable any main" \
  | $SUDO tee /etc/apt/sources.list.d/xrplf.list >/dev/null

$SUDO apt-get update -y
$SUDO apt-get install -y xrpld python3

# Stop service before modifying config
has_systemd && $SUDO systemctl stop xrpld 2>/dev/null || true

say "Configuring storage directory at $DB_PATH..."
$SUDO mkdir -p "$DB_PATH"
if id xrpld >/dev/null 2>&1; then
  $SUDO chown -R xrpld:xrpld "$(dirname "$DB_PATH")" "$DB_PATH"
else
  warn "User 'xrpld' not found yet; make sure permissions match the service user."
fi

say "Configuring /etc/xrpld/xrpld.cfg (Mode: $HISTORY_MODE, Size: $NODE_SIZE, WS Port: 6006)..."
$SUDO mkdir -p /etc/xrpld /etc/opt/ripple
PEER_ARGS=()
if [ -n "$FIXED_PEERS" ]; then
  IFS=';' read -ra PARTS <<< "$FIXED_PEERS"
  for P in "${PARTS[@]}"; do
    [ -n "$P" ] && PEER_ARGS+=(--peer "$P")
  done
fi

$SUDO python3 "$REPO_DIR/setup/edit_cfg.py" /etc/xrpld/xrpld.cfg \
  --db-path "$DB_PATH" \
  --history "$HISTORY_MODE" \
  --node-size "$NODE_SIZE" \
  "${PEER_ARGS[@]}"

$SUDO ln -sf /etc/xrpld/xrpld.cfg /etc/opt/ripple/xrpld.cfg 2>/dev/null || true
$SUDO ln -sf /etc/xrpld/xrpld.cfg /etc/opt/ripple/rippled.cfg 2>/dev/null || true

if has_systemd; then
  say "Starting xrpld service..."
  $SUDO systemctl daemon-reload
  $SUDO systemctl enable --now xrpld
  sleep 3
  $SUDO systemctl is-active --quiet xrpld && ok "Ripple node (xrpld) service is active and running" || warn "xrpld started with warnings"
else
  warn "No systemd: start xrpld manually with 'xrpld --conf /etc/xrpld/xrpld.cfg &'"
fi

ok "Ripple node installed and configured"
echo ""

# ------------------------------------------------------------------------------
# Phase 4: Setup Python Environment & Dependencies
# ------------------------------------------------------------------------------
say "[Phase 4/6] Setting Up Python Ingestion Environment"

$SUDO apt-get install -y python3 python3-venv python3-pip

if [ ! -d "$REPO_DIR/.venv" ]; then
  say "Creating Python virtual environment at $REPO_DIR/.venv..."
  python3 -m venv "$REPO_DIR/.venv"
fi

say "Installing dependencies from requirements.txt..."
"$REPO_DIR/.venv/bin/pip" install -q --upgrade pip
"$REPO_DIR/.venv/bin/pip" install -q -r "$REPO_DIR/requirements.txt"

# Configure .env
if [ ! -f "$REPO_DIR/.env" ]; then
  say "Creating .env configuration..."
  cp "$REPO_DIR/.env.example" "$REPO_DIR/.env"
  chmod 600 "$REPO_DIR/.env"
  ESC_URI=$(printf '%s' "$MONGO_URI" | sed -e 's/[\\&|]/\\&/g')
  sed -i "s|^MONGO_URI=.*|MONGO_URI=${ESC_URI}|" "$REPO_DIR/.env"
  ok ".env created (chmod 600)"
else
  ok ".env already exists (preserved)"
fi

echo ""

# ------------------------------------------------------------------------------
# Phase 5: Verification & Connectivity Tests
# ------------------------------------------------------------------------------
say "[Phase 5/6] Verifying Tests & Connections"

say "Running pytest suite..."
(cd "$REPO_DIR" && .venv/bin/python -m pytest -q) && ok "All tests passed successfully" || die "Test suite failed; fix it before continuing"

say "Verifying MongoDB connectivity..."
(cd "$REPO_DIR" && .venv/bin/python - <<'PY'
import os, sys
from dotenv import load_dotenv
from pymongo import MongoClient
load_dotenv()
uri = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017")
try:
    MongoClient(uri, serverSelectionTimeoutMS=4000).admin.command("ping")
    print("MongoDB connected successfully")
except Exception as e:
    print(f"MongoDB connection failed: {e}", file=sys.stderr)
    sys.exit(1)
PY
) && ok "MongoDB ping verified" || warn "MongoDB ping check failed; verify mongod is running"

echo ""

# ------------------------------------------------------------------------------
# Phase 6: Service Registration (xrpl-tail.service)
# ------------------------------------------------------------------------------
say "[Phase 6/6] Registering Background Ingest Service"

if has_systemd; then
  $SUDO tee /etc/systemd/system/xrpl-tail.service >/dev/null <<UNIT
[Unit]
Description=XRPL ledger live tail into MongoDB
After=network-online.target xrpld.service mongod.service

[Service]
User=$(id -un)
WorkingDirectory=$REPO_DIR
ExecStart=$REPO_DIR/.venv/bin/python -m xrpl_ingest tail
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT
  $SUDO systemctl daemon-reload
  ok "Registered /etc/systemd/system/xrpl-tail.service"
fi

# ------------------------------------------------------------------------------
# Summary & Next Steps
# ------------------------------------------------------------------------------
cat << EOF

====================================================================
✔ COMPLETE SETUP FINISHED SUCCESSFULLY!
====================================================================

Components Installed & Running:
  1. MongoDB Community Edition : listening on 127.0.0.1:27017
  2. Ripple Node (xrpld)       : listening on ws://127.0.0.1:6006
  3. Python Environment        : $REPO_DIR/.venv

Next Steps:

  1. Watch your Ripple Node sync:
       bash setup/check_node.sh
       # Or view live logs:
       sudo journalctl -u xrpld -f

     (State will go: disconnected -> connected -> syncing -> tracking -> full)
     (Pruned sync usually takes 15-25 minutes).

  2. Once the node reports complete_ledgers, test backfill:
       source .venv/bin/activate
       python -m xrpl_ingest status
       python -m xrpl_ingest backfill --start <START_LEDGER> --end <END_LEDGER>
       python -m xrpl_ingest verify   --start <START_LEDGER> --end <END_LEDGER>

  3. Enable continuous live ledger tailing:
       sudo systemctl enable --now xrpl-tail

  4. Stop any backfill or tail gracefully:
       echo current_process=0 > process_status.txt

====================================================================
EOF
