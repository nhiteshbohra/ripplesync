#!/usr/bin/env bash
# ==============================================================================
# Complete All-In-One Setup: MongoDB + Ripple Node (xrpld) + XRPL Ingest
# ==============================================================================
# NOT TESTED ON A REAL HOST: only `bash -n` and shellcheck so far.
# Sets up everything from scratch on x86_64 Ubuntu/Debian with glibc >= 2.31 (xrpl.org:
# Ubuntu 20.04+ / Debian 11+; MongoDB 8.0 apt repo: Ubuntu 20.04/22.04/24.04 only), or WSL2:
#   1. System preflight checks
#   2. Installs official MongoDB Community Edition 8.0 & starts service
#      (skipped on WSL -- MongoDB does not support WSL; run it on Windows, instructions printed --
#       and on releases without a MongoDB 8.0 apt repo)
#   3. Installs official XRPLF xrpld (pinned key fingerprint), configures /etc/xrpld/xrpld.cfg & starts it
#   4. Sets up the Python virtual environment (.venv; Python >= 3.10, 3.12 recommended)
#   5. Configures .env with local Mongo & WebSocket admin connections
#   6. Runs the test suite and `python -m xrpl_ingest doctor --mongo`
#   7. Installs systemd units xrpl-tail.service + xrpl-backfill.service from deploy/systemd/
#      (neither enabled; see the next steps printed at the end)
#
# Usage:
#   bash setup_all.sh                               # Interactive or standard pruned defaults
#   HISTORY_MODE=pruned bash setup_all.sh           # Pruned mode (expected: tens of GB disk; sync time not measured)
#   HISTORY_MODE=full bash setup_all.sh             # Full history mode (~39 TB disk per xrpl.org; months to sync)
# Env: DB_PATH, NODE_SIZE, FIXED_PEERS, MONGO_URI, PYTHON=python3.12, SKIP_MONGO=1, FORCE=1
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$SCRIPT_DIR"

# Defaults
HISTORY_MODE="${HISTORY_MODE:-pruned}"
NODE_SIZE="${NODE_SIZE:-medium}"
DB_PATH="${DB_PATH:-/var/lib/xrpld/db/nudb}"
FIXED_PEERS="${FIXED_PEERS:-}"
MONGO_URI_GIVEN="${MONGO_URI:+1}"
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"
FORCE="${FORCE:-0}"
WARNED=0  # set by warn(); the final banner reports it

# Colors
CYAN='\033[1;36m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
RED='\033[1;31m'
NC='\033[0m'

say()  { printf "${CYAN}==>${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}✔${NC} %s\n" "$*"; }
warn() { WARNED=1; printf "${YELLOW}WARN:${NC} %s\n" "$*" >&2; }
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

# Shared helpers: platform/python checks, MongoDB + xrpld install (pinned XRPLF key),
# systemd units from deploy/systemd/, doctor checks, next steps.
# shellcheck source=setup/common.sh
. "$REPO_DIR/setup/common.sh"
# check_db_path / check_db_owner: DB_PATH gets `chown -R`, refuse system dirs and foreign data.
# shellcheck source=setup/db_path_guard.sh
. "$REPO_DIR/setup/db_path_guard.sh"

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
echo "  MongoDB:    $(printf '%s' "$MONGO_URI" | sed -E 's#://.*@#://***@#')"  # hide credentials (greedy: passwords may contain @ or /)
echo ""

# ------------------------------------------------------------------------------
# Phase 1: Preflight & System Check
# ------------------------------------------------------------------------------
say "[Phase 1/6] System Preflight Checks"

check_platform   # glibc >= 2.31 on x86_64; sources /etc/os-release

RAM_GB=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
echo "System RAM: ${RAM_GB} GB (node_size=$NODE_SIZE)"
if [ "$NODE_SIZE" = huge ] && [ "$RAM_GB" -lt 60 ]; then
  need_ok "node_size=huge wants 64 GB RAM (detected ${RAM_GB} GB); try NODE_SIZE=large"
fi

# DB_PATH gets `chown -R`: refuse system dirs (and top-level ones) so a typo cannot re-own the system.
check_db_path

PROBE_DIR="$DB_PATH"
while [ ! -e "$PROBE_DIR" ]; do PROBE_DIR="$(dirname "$PROBE_DIR")"; done
FS_TYPE=$(stat -f -c %T "$PROBE_DIR" 2>/dev/null || echo "unknown")
FREE_GB=$(df -BG --output=avail "$PROBE_DIR" 2>/dev/null | tail -1 | tr -dc 0-9 || echo 0)
echo "Storage for DB: $PROBE_DIR ($FS_TYPE, ${FREE_GB} GB free)"

if is_wsl; then
  case "$PROBE_DIR" in
    /mnt/*)
      need_ok "DB_PATH is on a Windows mount (/mnt/..). Virtual 9P mounts are too slow and can cause NuDB corruption! Please use native Linux path."
      ;;
  esac
fi

if [ "$HISTORY_MODE" = "full" ]; then
  FREE_TB=$((FREE_GB / 1024))
  if [ "$FREE_TB" -lt 45 ]; then  # 45 TB: ~39 TB (xrpl.org) + growth headroom, per user notes, not verified
    need_ok "Full history needs ~39 TB SSD/NVMe and grows; this script wants 45 TB free (per user notes, not verified), detected ~${FREE_TB} TB. For personal/dev setups, run HISTORY_MODE=pruned instead."
  fi
  case "$FS_TYPE" in
    ext*) need_ok "NuDB full history single files exceed ext4's 16 TB file limit! You must use an XFS or ZFS volume for DB_PATH." ;;
  esac
fi

if is_wsl && ! has_systemd; then
  warn "WSL detected without systemd enabled."
  echo "To enable systemd in WSL, add this to /etc/wsl.conf:"
  printf '[boot]\nsystemd=true\n'
  echo "Then run 'wsl --shutdown' from Windows PowerShell and restart WSL."
fi

ok "Preflight complete"
echo ""

# ------------------------------------------------------------------------------
# Phase 2: Install MongoDB Community Edition
# ------------------------------------------------------------------------------
say "[Phase 2/6] MongoDB Community Edition $MONGO_SERIES"

install_mongodb   # skips WSL (unsupported there), SKIP_MONGO=1, releases without a repo, other installed versions

ok "MongoDB step complete"
echo ""

# ------------------------------------------------------------------------------
# Phase 3: Install & Configure Ripple Node (xrpld)
# ------------------------------------------------------------------------------
say "[Phase 3/6] Installing Ripple Node (xrpld)"

install_xrpld_pkg   # XRPLF apt repo; refuses a key whose fingerprint is not the pinned one

# Stop service before modifying config
if has_systemd; then $SUDO systemctl stop xrpld 2>/dev/null || true; fi

say "Configuring storage directory at $DB_PATH..."
$SUDO mkdir -p "$DB_PATH"
if id xrpld >/dev/null 2>&1; then
  check_db_owner
  $SUDO chown -R xrpld:xrpld "$DB_PATH"  # parent left as is: xrpld only needs to traverse it
else
  warn "User 'xrpld' not found yet; make sure permissions match the service user."
fi

say "Configuring $XRPLD_CFG (Mode: $HISTORY_MODE, Size: $NODE_SIZE, WS Port: 6006)..."
$SUDO mkdir -p "$(dirname "$XRPLD_CFG")"
PEER_ARGS=()
if [ -n "$FIXED_PEERS" ]; then
  IFS=';' read -ra PARTS <<< "$FIXED_PEERS"
  for P in "${PARTS[@]}"; do
    [ -n "$P" ] && PEER_ARGS+=(--peer "$P")
  done
fi

$SUDO python3 "$REPO_DIR/setup/edit_cfg.py" "$XRPLD_CFG" \
  --db-path "$DB_PATH" \
  --history "$HISTORY_MODE" \
  --node-size "$NODE_SIZE" \
  "${PEER_ARGS[@]}"
remove_old_cfg_links

start_xrpld
firewall_note

ok "Ripple node installed and configured"
echo ""

# ------------------------------------------------------------------------------
# Phase 4: Setup Python Environment & Dependencies
# ------------------------------------------------------------------------------
say "[Phase 4/6] Setting Up Python Ingestion Environment"

$SUDO apt-get install -y python3 python3-venv python3-pip
pick_python   # >= 3.10 required, 3.12 recommended (PYTHON=... to choose)
make_venv     # .venv + pip install -r requirements.txt

# Configure .env
if [ ! -f "$REPO_DIR/.env" ]; then
  say "Creating .env configuration..."
  cp "$REPO_DIR/.env.example" "$REPO_DIR/.env"
  chmod 600 "$REPO_DIR/.env"
  ESC_URI=$(sed_escape_repl "$MONGO_URI")
  sed -i "s|^MONGO_URI=.*|MONGO_URI=${ESC_URI}|" "$REPO_DIR/.env"
  ok ".env created (chmod 600)"
else
  chmod 600 "$REPO_DIR/.env"
  ok ".env already exists (preserved, chmod 600)"
  [ -z "$MONGO_URI_GIVEN" ] || warn "MONGO_URI from the environment was ignored because .env already exists; edit .env to change it"
fi

echo ""

# ------------------------------------------------------------------------------
# Phase 5: Verification & Connectivity Tests
# ------------------------------------------------------------------------------
say "[Phase 5/6] Verifying Tests & Connections"

say "Running pytest suite..."
if (cd "$REPO_DIR" && .venv/bin/python -m pytest -q); then
  ok "All tests passed successfully"
else
  die "Test suite failed; fix it before continuing"
fi

say "Verifying MongoDB connectivity (python -m xrpl_ingest doctor --mongo)..."
check_with_doctor "MongoDB" --mongo

echo ""

# ------------------------------------------------------------------------------
# Phase 6: Service Registration (xrpl-tail.service, xrpl-backfill.service)
# ------------------------------------------------------------------------------
say "[Phase 6/6] Registering Background Ingest Services"

UNITS=0
if install_ingest_units "${SUDO_USER:-$(id -un)}"; then UNITS=1; fi

# ------------------------------------------------------------------------------
# Summary & Next Steps
# ------------------------------------------------------------------------------
if [ "$WARNED" = 1 ]; then
  RESULT="SETUP FINISHED WITH WARNINGS -- read the WARN lines above before relying on it"
else
  RESULT="SETUP FINISHED (no warnings; services still need checking, see below)"
fi

cat << EOF

====================================================================
$RESULT
====================================================================

Components (expected endpoints; not verified by this script):
  1. MongoDB Community Edition : expected on 127.0.0.1:27017 (check: python -m xrpl_ingest doctor --mongo)
  2. Ripple Node (xrpld)       : expected on ws://127.0.0.1:6006 (check: bash setup/check_node.sh)
  3. Python Environment        : $REPO_DIR/.venv
  xrpld logs: sudo journalctl -u xrpld -f  (and /var/log/xrpld/debug.log)
  Expected node states: disconnected -> connected -> syncing -> tracking -> full
EOF
print_next_steps "$UNITS"
echo "===================================================================="
