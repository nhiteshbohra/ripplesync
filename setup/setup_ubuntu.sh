#!/usr/bin/env bash
# One-shot setup for Ubuntu/Debian x86_64 with glibc >= 2.31 (xrpl.org: Ubuntu 20.04+,
# Debian 11+): xrpld node + XRPL ingest. MongoDB itself: see setup_all.sh or your own server.
# NOT TESTED ON A REAL HOST: only `bash -n` and shellcheck so far.
#
#   ./setup/setup_ubuntu.sh check     # read-only preflight, changes nothing
#   ./setup/setup_ubuntu.sh node      # install + configure xrpld
#   ./setup/setup_ubuntu.sh ingest    # python venv, deps, .env, tests, systemd units
#   ./setup/setup_ubuntu.sh all       # node + ingest
#
# Env overrides:
#   DB_PATH=/data/xrpld/nudb   NuDB location (big XFS/ZFS disk; not under /home) [/var/lib/xrpld/db/nudb]
#   HISTORY_MODE=full|pruned   full = keep everything (~39 TB, Jan 2026)       [full]
#   NODE_SIZE=huge             xrpld [node_size]                               [huge]
#   FIXED_PEERS="host port;host port"   extra [ips_fixed] entries (see docs)
#   MONGO_URI=mongodb://...    written to .env (else you are asked)
#   PYTHON=python3.12          interpreter for .venv (>= 3.10; 3.12 recommended)
#   FORCE=1                    continue past failed hardware/OS checks
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${DB_PATH:-/var/lib/xrpld/db/nudb}"
HISTORY_MODE="${HISTORY_MODE:-full}"
NODE_SIZE="${NODE_SIZE:-huge}"
FORCE="${FORCE:-0}"

say()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mWARN:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }
need_ok() { [ "$FORCE" = 1 ] || die "$1  (set FORCE=1 to continue anyway)"; warn "$1 -- continuing (FORCE=1)"; }

SUDO=""; [ "$(id -u)" -eq 0 ] || SUDO="sudo"

# Shared helpers: platform/python checks, pinned XRPLF key, systemd units, doctor, next steps.
# shellcheck source=setup/common.sh
. "$REPO_DIR/setup/common.sh"
# check_db_path / check_db_owner: DB_PATH gets `chown -R`, refuse system dirs and foreign data.
# shellcheck source=setup/db_path_guard.sh
. "$REPO_DIR/setup/db_path_guard.sh"

CFG="$XRPLD_CFG"

preflight() {
  say "Preflight"
  check_platform

  local ram_gb; ram_gb=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
  echo "RAM: ${ram_gb} GB (node_size=huge wants 64 GB)"
  if [ "$NODE_SIZE" = huge ] && [ "$ram_gb" -lt 60 ]; then
    need_ok "only ${ram_gb} GB RAM for node_size=huge (try NODE_SIZE=large)"
  fi

  check_db_path
  local probe="$DB_PATH"; while [ ! -e "$probe" ]; do probe="$(dirname "$probe")"; done
  local fs free_tb
  fs=$(stat -f -c %T "$probe"); free_tb=$(df -BG --output=avail "$probe" | tail -1 | tr -dc 0-9); free_tb=$((free_tb/1024))
  echo "DB path: $DB_PATH   filesystem: $fs   free: ~${free_tb} TB"
  if [ "$HISTORY_MODE" = full ]; then
    # 45 TB: ~39 TB (xrpl.org) + growth headroom, per user notes, not verified
    [ "$free_tb" -ge 45 ] || need_ok "full history needs ~39 TB SSD and grows ~12 GB/day; this script wants 45 TB free (per user notes, not verified), only ~${free_tb} TB free"
    case "$fs" in ext*) need_ok "NuDB files exceed ext4's 16 TB file limit: use XFS or ZFS for DB_PATH" ;; esac
  fi
  if is_wsl; then
    case "$probe" in /mnt/*) need_ok "DB_PATH is on a Windows drive (/mnt/..): far too slow for NuDB" ;; esac
    has_systemd || warn "WSL without systemd: add '[boot]' + 'systemd=true' to /etc/wsl.conf, then 'wsl --shutdown'"
    echo "WSL: MongoDB is not supported inside WSL (MongoDB docs); run it on Windows, see 'ingest' output."
  fi
}

install_node() {
  preflight
  install_xrpld_pkg

  say "Stopping service before config change"
  if has_systemd; then $SUDO systemctl stop xrpld.service || true; fi

  say "Preparing DB dir $DB_PATH"
  $SUDO mkdir -p "$DB_PATH"
  if id xrpld >/dev/null 2>&1; then
    check_db_owner
    $SUDO chown -R xrpld:xrpld "$DB_PATH"  # parent left as is: xrpld only needs to traverse it
  else warn "no 'xrpld' user found; fix ownership of $DB_PATH to match the service user"; fi

  say "Editing $CFG (backup made; history=$HISTORY_MODE)"
  $SUDO mkdir -p "$(dirname "$CFG")"
  local peers=() p parts=()
  IFS=';' read -ra parts <<<"${FIXED_PEERS:-}"
  for p in "${parts[@]:-}"; do [ -n "$p" ] && peers+=(--peer "$p"); done
  $SUDO python3 "$REPO_DIR/setup/edit_cfg.py" "$CFG" --db-path "$DB_PATH" \
       --history "$HISTORY_MODE" --node-size "$NODE_SIZE" "${peers[@]}"
  remove_old_cfg_links
  if [ "$HISTORY_MODE" = full ] && [ -z "${FIXED_PEERS:-}" ]; then
    warn "full history: add [ips_fixed] peers that hold full history (see xrpl.org 'Configure Full History'); re-run with FIXED_PEERS='host port'"
  fi

  start_xrpld
  firewall_note
  say "Node installed. Syncing takes hours; full history takes months (per XRPL docs)."
  echo "Watch:  bash setup/check_node.sh   (after the 'ingest' step)"
}

setup_ingest() {
  say "Python environment in $REPO_DIR/.venv"
  $SUDO apt-get -y update
  $SUDO apt-get -y install python3 python3-venv python3-pip
  pick_python
  make_venv

  if [ ! -f "$REPO_DIR/.env" ]; then
    local uri="${MONGO_URI:-}"
    if [ -z "$uri" ]; then
      if is_wsl; then
        echo "WSL: MongoDB runs on Windows (not supported inside WSL). With mirrored networking use"
        echo "     mongodb://127.0.0.1:27017; otherwise the Windows host IP: $(ip route | awk '/default/ {print $3; exit}')"
      fi
      while [ -z "$uri" ]; do
        read -rs -p "MONGO_URI (input hidden; e.g. mongodb://127.0.0.1:27017): " uri; echo
      done
    fi
    cp "$REPO_DIR/.env.example" "$REPO_DIR/.env"
    chmod 600 "$REPO_DIR/.env"
    local esc; esc=$(sed_escape_repl "$uri")
    sed -i "s|^MONGO_URI=.*|MONGO_URI=${esc}|" "$REPO_DIR/.env"
    say ".env created (chmod 600)"
  else
    chmod 600 "$REPO_DIR/.env"
    say ".env already exists; leaving its contents alone (chmod 600)"
    [ -z "${MONGO_URI:-}" ] || warn "MONGO_URI from the environment was ignored because .env already exists; edit .env to change it"
  fi

  say "Running tests"
  (cd "$REPO_DIR" && .venv/bin/python -m pytest -q)

  say "Checking MongoDB connection (python -m xrpl_ingest doctor --mongo)"
  check_with_doctor "MongoDB" --mongo

  local units=0 svc_user="${SUDO_USER:-$(id -un)}"
  if install_ingest_units "$svc_user"; then units=1; fi
  print_next_steps "$units"
}

case "${1:-}" in
  check)  preflight ;;
  node)   install_node ;;
  ingest) setup_ingest ;;
  all)    install_node; setup_ingest ;;
  *) sed -n '2,18p' "$0"; exit 2 ;;
esac
