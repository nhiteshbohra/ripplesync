#!/usr/bin/env bash
# One-shot setup for Ubuntu (22.04/24.04 x86_64): xrpld node + XRPL ingest.
#
#   ./setup/setup_ubuntu.sh check     # read-only preflight, changes nothing
#   ./setup/setup_ubuntu.sh node      # install + configure xrpld
#   ./setup/setup_ubuntu.sh ingest    # python venv, deps, .env, tests, tail service
#   ./setup/setup_ubuntu.sh all       # node + ingest
#
# Env overrides:
#   DB_PATH=/data/xrpld/nudb   NuDB location (put it on a big XFS/ZFS disk)  [/var/lib/xrpld/db/nudb]
#   HISTORY_MODE=full|pruned   full = keep everything (~39 TB, Jan 2026)    [full]
#   NODE_SIZE=huge             xrpld [node_size]                            [huge]
#   FIXED_PEERS="host port;host port"   extra [ips_fixed] entries (see docs)
#   MONGO_URI=mongodb://...    written to .env (else you are asked)
#   FORCE=1                    continue past failed hardware checks
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${DB_PATH:-/var/lib/xrpld/db/nudb}"
HISTORY_MODE="${HISTORY_MODE:-full}"
NODE_SIZE="${NODE_SIZE:-huge}"
CFG=/etc/xrpld/xrpld.cfg
FORCE="${FORCE:-0}"

say()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mWARN:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }
need_ok() { [ "$FORCE" = 1 ] || die "$1  (set FORCE=1 to continue anyway)"; warn "$1 -- continuing (FORCE=1)"; }

is_wsl()     { grep -qi microsoft /proc/version 2>/dev/null; }
has_systemd(){ [ -d /run/systemd/system ]; }
SUDO=""; [ "$(id -u)" -eq 0 ] || SUDO="sudo"

preflight() {
  say "Preflight"
  . /etc/os-release
  echo "OS: $PRETTY_NAME   arch: $(uname -m)   WSL: $(is_wsl && echo yes || echo no)   systemd: $(has_systemd && echo yes || echo no)"
  [ "${ID:-}" = ubuntu ] || warn "not Ubuntu; docs say 22.04/24.04 x86_64 are the best tested"
  case "${VERSION_ID:-}" in 22.04|24.04) ;; *) warn "Ubuntu ${VERSION_ID:-?} is not 22.04/24.04" ;; esac
  [ "$(uname -m)" = x86_64 ] || need_ok "architecture is not x86_64"

  local ram_gb; ram_gb=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
  echo "RAM: ${ram_gb} GB (node_size=huge wants 64 GB)"
  [ "$ram_gb" -ge 60 ] || { [ "$NODE_SIZE" != huge ] && true || need_ok "only ${ram_gb} GB RAM for node_size=huge (try NODE_SIZE=large)"; }

  local probe="$DB_PATH"; while [ ! -e "$probe" ]; do probe="$(dirname "$probe")"; done
  local fs free_tb
  fs=$(stat -f -c %T "$probe"); free_tb=$(df -BG --output=avail "$probe" | tail -1 | tr -dc 0-9); free_tb=$((free_tb/1024))
  echo "DB path: $DB_PATH   filesystem: $fs   free: ~${free_tb} TB"
  if [ "$HISTORY_MODE" = full ]; then
    [ "$free_tb" -ge 45 ] || need_ok "full history needs ~39 TB SSD and grows ~12 GB/day; only ~${free_tb} TB free"
    case "$fs" in ext*) need_ok "NuDB files exceed ext4's 16 TB file limit: use XFS or ZFS for DB_PATH" ;; esac
  fi
  case "$probe" in /mnt/*) need_ok "DB_PATH is on a Windows drive (/mnt/..): far too slow for NuDB" ;; esac
  is_wsl && has_systemd || { is_wsl && warn "WSL without systemd: add '[boot]\nsystemd=true' to /etc/wsl.conf, then 'wsl --shutdown'"; true; }
}

install_node() {
  preflight
  say "Installing xrpld (official XRPLF apt repo)"
  $SUDO apt-get -y update
  $SUDO apt-get -y install apt-transport-https ca-certificates wget gnupg python3
  $SUDO install -m 0755 -d /etc/apt/keyrings
  $SUDO wget -qO /etc/apt/keyrings/xrplf.asc https://packages.xrplf.org/xrplf.asc
  echo "Key fingerprint (compare with https://xrpl.org/docs/infrastructure/installation/install-rippled-on-ubuntu):"
  gpg --show-keys /etc/apt/keyrings/xrplf.asc
  echo "deb [signed-by=/etc/apt/keyrings/xrplf.asc] https://packages.xrplf.org/repository/deb-stable any main" \
    | $SUDO tee /etc/apt/sources.list.d/xrplf.list >/dev/null
  $SUDO apt-get -y update
  $SUDO apt-get -y install xrpld

  say "Stopping service before config change"
  has_systemd && $SUDO systemctl stop xrpld.service || true

  say "Preparing DB dir $DB_PATH"
  $SUDO mkdir -p "$DB_PATH"
  if id xrpld >/dev/null 2>&1; then $SUDO chown -R xrpld:xrpld "$(dirname "$DB_PATH")" "$DB_PATH"
  else warn "no 'xrpld' user found; fix ownership of $DB_PATH to match the service user"; fi

  say "Editing $CFG (backup made; history=$HISTORY_MODE)"
  $SUDO mkdir -p "$(dirname "$CFG")" /etc/opt/ripple
  local peers=() p
  IFS=';' read -ra parts <<<"${FIXED_PEERS:-}"
  for p in "${parts[@]:-}"; do [ -n "$p" ] && peers+=(--peer "$p"); done
  $SUDO python3 "$REPO_DIR/setup/edit_cfg.py" "$CFG" --db-path "$DB_PATH" \
       --history "$HISTORY_MODE" --node-size "$NODE_SIZE" "${peers[@]}"
  $SUDO ln -sf "$CFG" /etc/opt/ripple/xrpld.cfg 2>/dev/null || true
  $SUDO ln -sf "$CFG" /etc/opt/ripple/rippled.cfg 2>/dev/null || true
  [ "$HISTORY_MODE" = full ] && [ -z "${FIXED_PEERS:-}" ] && \
    warn "full history: add [ips_fixed] peers that hold full history (see xrpl.org 'Configure Full History'); re-run with FIXED_PEERS='host port'"

  if has_systemd; then
    $SUDO systemctl enable --now xrpld.service
    sleep 5; systemctl --no-pager --lines=5 status xrpld.service || true
  else
    warn "no systemd: start manually with: xrpld --conf $CFG"
  fi
  say "Node installed. Syncing takes hours; full history takes months (per XRPL docs)."
  echo "Watch:  ./setup/check_node.sh"
}

setup_ingest() {
  say "Python environment in $REPO_DIR/.venv"
  $SUDO apt-get -y install python3 python3-venv python3-pip
  [ -d "$REPO_DIR/.venv" ] || python3 -m venv "$REPO_DIR/.venv"
  "$REPO_DIR/.venv/bin/pip" install -q --upgrade pip
  "$REPO_DIR/.venv/bin/pip" install -q -r "$REPO_DIR/requirements.txt"

  if [ ! -f "$REPO_DIR/.env" ]; then
    local uri="${MONGO_URI:-}"
    if [ -z "$uri" ]; then
      if is_wsl; then echo "WSL hint: Windows host IP is $(ip route | awk '/default/ {print $3}') (or use mirrored networking, then 127.0.0.1)"; fi
      read -r -p "MONGO_URI (e.g. mongodb://127.0.0.1:27017): " uri
    fi
    cp "$REPO_DIR/.env.example" "$REPO_DIR/.env"
    chmod 600 "$REPO_DIR/.env"
    # '|' delimiter; escape sed specials in the URI
    local esc; esc=$(printf '%s' "$uri" | sed -e 's/[\\&|]/\\&/g')
    sed -i "s|^MONGO_URI=.*|MONGO_URI=${esc}|" "$REPO_DIR/.env"
    say ".env created (chmod 600)"
  else
    say ".env already exists; leaving it alone"
  fi

  say "Running tests"
  (cd "$REPO_DIR" && .venv/bin/python -m pytest -q)

  say "Checking MongoDB connection"
  (cd "$REPO_DIR" && .venv/bin/python - <<'PY'
import os, sys
from dotenv import load_dotenv
from pymongo import MongoClient
load_dotenv()
try:
    MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=5000).admin.command("ping")
    print("MongoDB: OK")
except Exception as e:
    print(f"MongoDB: FAILED ({e})\nFix MONGO_URI in .env (WSL: mirrored networking or host IP; Mongo bindIp + firewall).")
    sys.exit(1)
PY
  ) || warn "MongoDB not reachable yet"

  if has_systemd; then
    say "Installing xrpl-tail.service (live follow; starts once node has synced)"
    $SUDO tee /etc/systemd/system/xrpl-tail.service >/dev/null <<UNIT
[Unit]
Description=XRPL ledger tail into MongoDB
After=network-online.target xrpld.service

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
    echo "Enable when ready:  sudo systemctl enable --now xrpl-tail"
  fi

  cat <<EOF

Next (after the node reports ledgers in complete_ledgers):
  cd $REPO_DIR && source .venv/bin/activate
  python -m xrpl_ingest status
  python -m xrpl_ingest backfill --start 32570 --end 33570     # small trial first
  nohup python -m xrpl_ingest backfill > backfill.log 2>&1 &   # then the full run
  python -m xrpl_ingest verify --start 32570 --end 33570
  echo current_process=0 > process_status.txt                  # graceful stop
EOF
}

case "${1:-}" in
  check)  preflight ;;
  node)   install_node ;;
  ingest) setup_ingest ;;
  all)    install_node; setup_ingest ;;
  *) sed -n '2,15p' "$0"; exit 2 ;;
esac
