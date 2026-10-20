#!/usr/bin/env bash
# Automatic setup for Linux: Ubuntu 20.04+ / Debian 11+ on x86_64 (also inside WSL2).
#
# It looks at this computer (RAM, CPU, disk, Python, MongoDB, xrpld), recommends one of three
# setups, installs what is missing, configures .env and checks that everything works.
# Every change is shown first and needs a yes (or --yes). Run it as your normal user, not
# with sudo: it asks for sudo itself when needed.
#
#   bash install.sh                  # look, recommend, ask, install
#   bash install.sh --check          # only look and report; changes nothing
#   bash install.sh --mode test      # 1. test on this computer: public servers, no node (laptops)
#   bash install.sh --mode node      # 2. small node on this computer (recent ledgers only; 16 GB+ RAM)
#   bash install.sh --mode server    # 3. full-history server (~39 TB XFS disk, 64 GB RAM)
#   bash install.sh --mode test --yes   # no questions, accept every default
#
# Settings you can pass in the environment:
#   MONGO_URI=mongodb://...          MongoDB to use (default: the local one; asked if none is found)
#   DB_PATH=/data/xrpld/nudb         where the node keeps its database        [/var/lib/xrpld/db/nudb]
#   NODE_SIZE=small|medium|huge      xrpld [node_size]                        [chosen from the RAM]
#   FIXED_PEERS="host port;host port"  extra [ips_fixed] peers (full history)
#   FORCE=1                          continue past failed hardware checks
#
# NOT TESTED ON A REAL HOST yet: checked with bash -n and shellcheck. The manual steps are in
# README.md ("Manual setup"); this script runs the same steps.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE=""
YES=0
CHECK_ONLY=0
FORCE="${FORCE:-0}"
DB_PATH="${DB_PATH:-/var/lib/xrpld/db/nudb}"
PUBLIC_SERVERS="wss://s2.ripple.com,wss://xrplcluster.com"
LOCAL_MONGO="mongodb://127.0.0.1:27017"

usage() { sed -n '2,26p' "$0"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE="${2:-}"; shift 2 ;;
    --mode=*) MODE="${1#--mode=}"; shift ;;
    --yes|-y) YES=1; shift ;;
    --check) CHECK_ONLY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 2 ;;
  esac
done
case "$MODE" in ""|test|node|server) ;; *) echo "--mode must be test, node or server" >&2; exit 2 ;; esac

C_B='\033[1;34m'; C_G='\033[1;32m'; C_Y='\033[1;33m'; C_R='\033[1;31m'; C_0='\033[0m'
[ -t 1 ] || { C_B=''; C_G=''; C_Y=''; C_R=''; C_0=''; }
say()  { printf "${C_B}==>${C_0} %s\n" "$*"; }
ok()   { printf "${C_G}OK${C_0}   %s\n" "$*"; }
warn() { printf "${C_Y}WARN${C_0} %s\n" "$*" >&2; }
die()  { printf "${C_R}ERROR${C_0} %s\n" "$*" >&2; exit 1; }
need_ok() { [ "$FORCE" = 1 ] || die "$1  (set FORCE=1 to continue anyway)"; warn "$1 -- continuing (FORCE=1)"; }

# ask "question" Y|N  -> 0 = yes. --yes accepts the default.
ask() {
  local q="$1" def="$2" a hint="[Y/n]"
  [ "$def" = N ] && hint="[y/N]"
  if [ "$YES" = 1 ]; then [ "$def" = Y ]; return; fi
  read -r -p "$q $hint " a || a=""
  a="${a:-$def}"
  case "$a" in [Yy]*) return 0 ;; *) return 1 ;; esac
}

SUDO=""; [ "$(id -u)" -eq 0 ] || SUDO="sudo"
if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ] && [ "$CHECK_ONLY" = 0 ]; then
  need_ok "run this script WITHOUT sudo (as $SUDO_USER): it asks for sudo itself; as root, .venv and .env would belong to root"
fi

[ -f "$REPO_DIR/setup/common.sh" ] || die "run install.sh from the downloaded repo folder (setup/common.sh is missing)"
# shellcheck source=setup/common.sh
. "$REPO_DIR/setup/common.sh"
# shellcheck source=setup/db_path_guard.sh
. "$REPO_DIR/setup/db_path_guard.sh"
CFG="$XRPLD_CFG"

# ------------------------------------------------------------------------------ 1. look
port_open() { (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; }

xrpld_state_json() {  # server_state and complete_ledgers from the local admin RPC port (5005 in the 3.4.1 cfg)
  python3 - <<'EOF' 2>/dev/null || true
import json, urllib.request
req = urllib.request.Request("http://127.0.0.1:5005", data=b'{"method":"server_info"}',
                             headers={"Content-Type": "application/json"})
info = json.load(urllib.request.urlopen(req, timeout=3))["result"]["info"]
print(info.get("server_state", "?"), info.get("complete_ledgers", "?"))
EOF
}

detect() {
  # shellcheck source=/dev/null
  . /etc/os-release 2>/dev/null || true
  OS_NAME="${PRETTY_NAME:-unknown Linux}"
  ARCH="$(uname -m)"
  WSL=no; is_wsl && WSL=yes
  SYSTEMD=no; has_systemd && SYSTEMD=yes
  APT=no; command -v apt-get >/dev/null 2>&1 && APT=yes
  RAM_GB=$(awk '/MemTotal/ {printf "%d", ($2/1024/1024)+0.5}' /proc/meminfo)
  CORES=$(nproc 2>/dev/null || echo 1)
  FREE_GB=$(df -BG --output=avail "$REPO_DIR" | tail -1 | tr -dc 0-9)
  PY_VER="none"
  if command -v python3 >/dev/null 2>&1; then
    PY_VER="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || echo none)"
  fi
  MONGO="not installed"
  if port_open 127.0.0.1 27017; then MONGO="running on 127.0.0.1:27017"
  elif command -v mongod >/dev/null 2>&1; then MONGO="installed, not running"; fi
  XRPLD="not installed"; XRPLD_INFO=""
  if command -v xrpld >/dev/null 2>&1 || [ -x /usr/bin/xrpld ] || [ -x /opt/xrpld/bin/xrpld ]; then
    XRPLD="installed, not running"
    if [ "$SYSTEMD" = yes ] && systemctl is-active --quiet xrpld.service; then
      XRPLD="running"
      [ "$(systemctl show -p NRestarts --value xrpld.service 2>/dev/null || echo 0)" = 0 ] || XRPLD="crashing (keeps restarting)"
      XRPLD_INFO="$(xrpld_state_json)"
    elif [ "$SYSTEMD" = yes ] && systemctl is-failed --quiet xrpld.service; then
      XRPLD="failed (stopped after errors)"
    fi
  fi
  VALIDATORS=""
  if [ -f "$CFG" ]; then
    VALIDATORS="$(python3 "$REPO_DIR/setup/xrpld_check.py" check --cfg "$CFG" 2>&1 || true)"
  fi
  ENV_STATE="none"; if [ -f "$REPO_DIR/.env" ]; then ENV_STATE="exists"; fi
  VENV_STATE="none"; if [ -x "$REPO_DIR/.venv/bin/python" ]; then VENV_STATE="exists"; fi
}

recommend() {
  if [ "$RAM_GB" -ge 60 ] && [ "$FREE_GB" -ge 40000 ] && [ "$WSL" = no ]; then echo server
  elif [ "$RAM_GB" -ge 15 ] && [ "$CORES" -ge 4 ] && [ "$FREE_GB" -ge 50 ]; then echo node
  else echo test; fi
}

report() {
  echo
  echo "This computer"
  printf '  %-12s %s\n' "System" "$OS_NAME ($ARCH)$( [ "$WSL" = yes ] && echo ', inside WSL2')" \
    "RAM" "${RAM_GB} GB" "CPU cores" "$CORES" "Free disk" "${FREE_GB} GB (in $REPO_DIR)" \
    "Python" "$PY_VER" "MongoDB" "$MONGO" "xrpld node" "$XRPLD${XRPLD_INFO:+ (state, ledgers: $XRPLD_INFO)}" \
    ".env" "$ENV_STATE" ".venv" "$VENV_STATE"
  if [ -n "$VALIDATORS" ]; then
    echo "  Node validator settings:"
    printf '%s\n' "$VALIDATORS" | sed 's/^/    /'
  fi
  echo
  echo "Setups"
  echo "  1. test    Try it on this computer. No node: reads a little real data from public XRPL"
  echo "             servers. Needs: Python, MongoDB (installed for you). Any laptop."
  echo "  2. node    Small node on this computer (keeps only recent ledgers, about 1 GB)."
  echo "             Needs: 16 GB RAM, 4 CPU cores, 50 GB SSD (xrpl.org minimum for testing)."
  echo "  3. server  Full history. Needs: 64 GB RAM, 8+ cores, ~39 TB NVMe with XFS. Months to sync."
  echo "  Recommended for this computer: $(recommend)"
  echo
}

# ------------------------------------------------------------------------------ 2. steps
step_base_packages() {
  say "System packages (python3, venv, pip, curl)"
  $SUDO apt-get -y update
  $SUDO apt-get -y install python3 python3-venv python3-pip curl ca-certificates
}

step_mongodb() {
  if port_open 127.0.0.1 27017; then ok "MongoDB is running on 127.0.0.1:27017"; return 0; fi
  if command -v mongod >/dev/null 2>&1; then
    say "MongoDB is installed but not running: starting it"
    if [ "$SYSTEMD" = yes ]; then $SUDO systemctl enable --now mongod; sleep 3; fi
    port_open 127.0.0.1 27017 && { ok "MongoDB started"; return 0; }
    warn "MongoDB did not start; see: sudo systemctl status mongod"
    return 0
  fi
  if [ "$WSL" = yes ]; then wsl_mongo_notes; return 0; fi
  if [ -n "${MONGO_URI:-}" ] && [ "${MONGO_URI#"$LOCAL_MONGO"}" = "$MONGO_URI" ] && [ "${MONGO_URI#mongodb://localhost}" = "$MONGO_URI" ]; then
    ok "using the MongoDB from MONGO_URI (not installing a local one)"; return 0
  fi
  if ask "MongoDB is not installed. Install MongoDB $MONGO_SERIES now (official repository)?" Y; then
    install_mongodb
  else
    warn "no MongoDB: set MONGO_URI to an existing MongoDB and run this script again"
  fi
}

node_size_for_ram() {
  if [ -n "${NODE_SIZE:-}" ]; then echo "$NODE_SIZE"; return; fi
  if   [ "$RAM_GB" -ge 60 ]; then echo huge
  elif [ "$RAM_GB" -ge 15 ]; then echo medium
  else echo small; fi
}

step_node() {
  local history="$1" size peers=() p parts=()
  size="$(node_size_for_ram)"
  say "xrpld node: history=$history, node_size=$size, database in $DB_PATH"
  check_platform
  if [ "$history" = full ]; then
    [ "$RAM_GB" -ge 60 ] || need_ok "full history wants 64 GB RAM; this computer has ${RAM_GB} GB"
  else
    [ "$RAM_GB" -ge 15 ] || need_ok "a node needs 16 GB RAM (xrpl.org minimum for testing); this computer has ${RAM_GB} GB. Use --mode test instead"
    [ "$CORES" -ge 4 ] || need_ok "a node needs 4+ CPU cores; this computer has $CORES"
  fi
  check_db_path
  local probe="$DB_PATH" fs free_gb
  while [ ! -e "$probe" ]; do probe="$(dirname "$probe")"; done
  fs=$(stat -f -c %T "$probe"); free_gb=$(df -BG --output=avail "$probe" | tail -1 | tr -dc 0-9)
  echo "  database disk: $probe ($fs, ${free_gb} GB free)"
  if [ "$history" = full ]; then
    [ "$free_gb" -ge 45000 ] || need_ok "full history needs ~39 TB (+ growth); only ${free_gb} GB free"
    case "$fs" in ext*) need_ok "full history needs XFS or ZFS (NuDB files grow past ext4's 16 TB limit)" ;; esac
  else
    [ "$free_gb" -ge 50 ] || need_ok "a node needs 50 GB free for its database; only ${free_gb} GB free"
  fi
  if [ "$WSL" = yes ]; then
    case "$probe" in /mnt/*) need_ok "DB_PATH is on a Windows drive (/mnt/...): far too slow for the node" ;; esac
  fi

  if command -v xrpld >/dev/null 2>&1 || [ -x /usr/bin/xrpld ]; then
    ok "xrpld is installed"
  elif ask "Install xrpld (official XRPLF package; signing key checked)?" Y; then
    install_xrpld_pkg
  else
    die "a node setup needs xrpld"
  fi

  [ "$SYSTEMD" = no ] || $SUDO systemctl stop xrpld.service 2>/dev/null || true
  $SUDO mkdir -p "$DB_PATH"
  if id xrpld >/dev/null 2>&1; then check_db_owner; $SUDO chown -R xrpld:xrpld "$DB_PATH"
  else warn "no 'xrpld' user found; make $DB_PATH writable for the user that runs xrpld"; fi

  say "Configuring $CFG (a backup is kept next to it)"
  IFS=';' read -ra parts <<<"${FIXED_PEERS:-}"
  for p in "${parts[@]:-}"; do [ -n "$p" ] && peers+=(--peer "$p"); done
  $SUDO python3 "$REPO_DIR/setup/edit_cfg.py" "$CFG" --db-path "$DB_PATH" --history "$history" \
    --node-size "$size" "${peers[@]}"
  remove_old_cfg_links

  if ! $SUDO python3 "$REPO_DIR/setup/xrpld_check.py" check --cfg "$CFG"; then
    if ask "The node's validator settings are wrong (see above); xrpld stops at start with them. Restore the official ones (backups are kept)?" Y; then
      $SUDO python3 "$REPO_DIR/setup/xrpld_check.py" fix --cfg "$CFG"
    else
      warn "validator settings left as they are"
    fi
  fi

  if ! start_xrpld; then
    die "xrpld does not start; the lines above show why. Fix it, then run this script again (it is safe to repeat)."
  fi
  [ "$history" = full ] && [ -z "${FIXED_PEERS:-}" ] && \
    warn "full history: add peers that hold full history (FIXED_PEERS='host port'; see xrpl.org 'Configure Full History')"
  firewall_note
}

step_python() {
  pick_python
  make_venv
  ok "Python environment ready: $REPO_DIR/.venv"
}

# env_set KEY=VALUE ... : write into .env (created from .env.example; backup kept; values never printed)
env_set() { python3 "$REPO_DIR/setup/envfile.py" "$REPO_DIR/.env" --example "$REPO_DIR/.env.example" "$@"; }
env_get() { "$REPO_DIR/.venv/bin/python" -c "from dotenv import dotenv_values as d; print(d('$REPO_DIR/.env').get('$1') or '')" 2>/dev/null || true; }

step_env() {
  local mode="$1" uri="${MONGO_URI:-}" have_uri=0 db
  python3 "$REPO_DIR/setup/envfile.py" "$REPO_DIR/.env" --has MONGO_URI && have_uri=1 || true
  if [ -z "$uri" ] && [ "$have_uri" = 0 ]; then
    if port_open 127.0.0.1 27017; then uri="$LOCAL_MONGO"
    else
      [ "$WSL" = yes ] && echo "WSL: MongoDB runs on Windows. With mirrored networking use $LOCAL_MONGO."
      while [ -z "$uri" ]; do read -rs -p "MongoDB address (MONGO_URI, input hidden; e.g. $LOCAL_MONGO): " uri; echo; done
    fi
  fi
  local args=()
  [ -z "$uri" ] || args+=("MONGO_URI=$uri")
  db="$(env_get DB_NAME)"
  case "$mode" in
    test)
      args+=("SERVERS=$PUBLIC_SERVERS" "DB_NAME=xrpl_trial" "NUM_PROCESSES=1" "CONCURRENCY=2" "CHUNK_SIZE=100") ;;
    node|server)
      args+=("SERVERS=ws://127.0.0.1:6006" "CHUNK_SIZE=1000" "NUM_PROCESSES=$(( CORES < 4 ? CORES : 4 ))" "CONCURRENCY=16")
      [ "$db" != xrpl_trial ] || args+=("DB_NAME=xrpl_data_collection") ;;
  esac
  say "Writing settings into .env ($mode setup)"
  env_set "${args[@]}"
}

step_tests() {
  local uri
  uri="$(env_get MONGO_URI)"
  say "Running the test suite"
  case "$uri" in
    "$LOCAL_MONGO"*|mongodb://localhost:27017*)
      (cd "$REPO_DIR" && .venv/bin/python -m pytest -q) ;;
    *)  # end-to-end tests expect a local MongoDB without password
      (cd "$REPO_DIR" && XRPL_INGEST_NO_INTEGRATION=1 .venv/bin/python -m pytest -q) ;;
  esac || die "tests failed (output above). Nothing is broken on your system; please report the output."
  ok "tests passed"
}

step_doctor() {
  say "Checking the setup (python -m xrpl_ingest doctor)"
  if [ "$1" = test ]; then check_with_doctor "Setup"
  else check_with_doctor "MongoDB" --mongo; check_with_doctor "Node" --node; fi
}

step_trial() {
  say "Storing one real ledger to prove it works (python -m xrpl_ingest fetch --ledger validated)"
  if (cd "$REPO_DIR" && XRPL_INGEST_HOME="$REPO_DIR" PYTHONUTF8=1 .venv/bin/python -m xrpl_ingest fetch --ledger validated); then
    ok "a real XRPL ledger is now in MongoDB (database xrpl_trial)"
  else
    warn "fetch did not work (see above); check internet access and MongoDB, then: python -m xrpl_ingest doctor"
  fi
}

finish() {
  local mode="$1" units="$2"
  echo
  say "Done: $mode setup"
  echo "Start every session with:   cd \"$REPO_DIR\" && source .venv/bin/activate"
  case "$mode" in
    test) cat <<'EOF'
Try:
  python -m xrpl_ingest backfill --start 32570 --end 32769   # 200 old ledgers (about a minute)
  python -m xrpl_ingest verify   --start 32570 --end 32769   # prints OK
  python -m xrpl_ingest tail                                 # live ledgers; Ctrl+C stops
  python -m xrpl_ingest status                               # what is stored
This is a test setup (public servers, database xrpl_trial). For real use run this script on a
server with --mode node or --mode server; it switches .env over (a backup is kept).
EOF
      ;;
    *) print_next_steps "$units" ;;
  esac
}

# ------------------------------------------------------------------------------ main
[ "$(uname -s)" = Linux ] || die "this installer is for Linux. Windows: run install.cmd. Others: see README.md 'Manual setup'"
detect
report

if [ "$CHECK_ONLY" = 1 ]; then
  echo "(--check: nothing was changed)"
  exit 0
fi
[ "$APT" = yes ] || die "this installer needs apt (Ubuntu / Debian). On other Linux systems follow README.md 'Manual setup'"

if [ -z "$MODE" ]; then
  if [ "$YES" = 1 ]; then MODE="$(recommend)"
  else
    def="$(recommend)"
    read -r -p "Which setup? [test/node/server, Enter = $def] " MODE || MODE=""
    MODE="${MODE:-$def}"
    case "$MODE" in 1) MODE="test" ;; 2) MODE="node" ;; 3) MODE="server" ;; esac
    case "$MODE" in test|node|server) ;; *) die "please answer test, node or server" ;; esac
  fi
fi

echo
echo "Plan for the '$MODE' setup:"
echo "  - system packages: python3, python3-venv, pip, curl"
case "$MONGO" in running*) ;; *) echo "  - MongoDB: $MONGO -> install / start it" ;; esac
if [ "$MODE" != test ]; then
  echo "  - xrpld node: $XRPLD -> install if needed, configure ($( [ "$MODE" = server ] && echo 'full history' || echo 'recent ledgers only')), check, start"
fi
echo "  - .venv with the Python packages from requirements.txt"
echo "  - .env settings for this setup (existing .env: backup kept, MONGO_URI kept)"
echo "  - run the tests and the doctor check"
[ "$MODE" = test ] && echo "  - store one real ledger as a final check"
[ "$MODE" = test ] || echo "  - install the services xrpl-tail / xrpl-backfill (not started)"
ask "Continue?" Y || { echo "Nothing changed."; exit 0; }

step_base_packages
step_mongodb
case "$MODE" in
  node)   step_node pruned ;;
  server) step_node full ;;
esac
step_python
step_env "$MODE"
step_tests
step_doctor "$MODE"
UNITS=0
if [ "$MODE" = test ]; then
  step_trial
else
  install_ingest_units "${SUDO_USER:-$(id -un)}" && UNITS=1 || true
fi
finish "$MODE" "$UNITS"
