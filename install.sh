#!/usr/bin/env bash
# Automatic setup for Linux: Ubuntu 20.04+ / Debian 11+ on x86_64 (also inside WSL2).
# macOS: Python 3.12 and .venv only; node and MongoDB setup remain manual.
#
# Two setups, both with your own xrpld node and the same program, settings and services:
#   test    the server setup in small: a small node that keeps only recent ledgers (about 2 hours,
#           ~1 GB), node_size small/medium. Shows that real data arrives: live tail, backfill,
#           verify, retries, services and speed, before you build the big server.
#   server  full history: ~39 TB XFS disk, 64 GB RAM, node_size huge. Months to sync.
#
# It looks at this computer (RAM, CPU, disk, Python, MongoDB, xrpld), recommends a setup,
# shows what it will change and asks first (or --yes). Run it as your normal user, not with
# sudo: it asks for sudo itself when needed. Running it again is safe.
#
#   bash install.sh                  # look, recommend, ask, install
#   bash install.sh --check          # only look and report; changes nothing
#   bash install.sh --mode test      # test setup without the question
#   bash install.sh --mode server    # server setup without the question
#   bash install.sh --trial          # test setup only: run the real-data checks again
#   bash install.sh --skip-tests     # do everything except the test suite
#   add --yes to accept every default without questions
#
# Settings you can pass in the environment:
#   MONGO_URI=mongodb://...          MongoDB to use (default: the local one; asked if none is found)
#   DB_PATH=/data/xrpld/nudb         where the node keeps its database        [/var/lib/xrpld/db/nudb]
#   NODE_SIZE=small|medium|huge      xrpld [node_size]       [test: from the RAM; server: huge]
#   FIXED_PEERS="host port;host port"  extra [ips_fixed] peers (full history)
#   SYNC_WAIT_MIN=30                 test: minutes to wait for the node to sync before the checks
#   TEST_TIMEOUT_MIN=10              stop the test suite if it takes longer (it normally needs ~1 minute)
#   FORCE=1                          continue past failed hardware checks
#
# NOT TESTED ON A REAL HOST yet: checked with bash -n and shellcheck. The manual steps are in
# README.md ("Manual setup"); this script runs the same steps.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE=""
YES=0
CHECK_ONLY=0
TRIAL_ONLY=0
SKIP_TESTS=0
TEST_TIMEOUT_MIN="${TEST_TIMEOUT_MIN:-10}"
FORCE="${FORCE:-0}"
DB_PATH="${DB_PATH:-/var/lib/xrpld/db/nudb}"
SYNC_WAIT_MIN="${SYNC_WAIT_MIN:-30}"
LOCAL_MONGO="mongodb://127.0.0.1:27017"
NODE_WS="${NODE_WS:-}"
TEST_DB="xrpl_test"
SERVER_DB="xrpl_data_collection"

usage() { sed -n '2,31p' "$0"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE="${2:-}"; shift 2 ;;
    --mode=*) MODE="${1#--mode=}"; shift ;;
    --yes|-y) YES=1; shift ;;
    --check) CHECK_ONLY=1; shift ;;
    --trial) TRIAL_ONLY=1; shift ;;
    --skip-tests) SKIP_TESTS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 2 ;;
  esac
done
case "$MODE" in ""|test|server) ;; *) echo "--mode must be test or server" >&2; exit 2 ;; esac

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

# "server_state complete_ledgers" from the node's local admin RPC port (5005 in the 3.4.1 cfg); empty if no answer.
xrpld_state() {
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
      XRPLD_INFO="$(xrpld_state)"
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
  if [ "$RAM_GB" -ge 60 ] && [ "$FREE_GB" -ge 40000 ] && [ "$WSL" = no ]; then echo server; else echo test; fi
}

node_size_for() {  # node_size_for test|server
  if [ -n "${NODE_SIZE:-}" ]; then echo "$NODE_SIZE"; return; fi
  if [ "$1" = server ]; then echo huge
  elif [ "$RAM_GB" -ge 15 ]; then echo medium
  else echo small; fi
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
  echo "Setups (both: your own node, MongoDB, this program, the same settings and services)"
  echo "  test    The server setup in small. The node keeps only recent ledgers (~2 hours, ~1 GB),"
  echo "          node_size $(node_size_for test). After setup it waits for the node and checks that real data"
  echo "          arrives (tail, backfill, verify). Database: $TEST_DB."
  echo "          Good: 16 GB RAM, 4 cores, 50 GB SSD (xrpl.org minimum for testing). 8 GB works, slower."
  echo "  server  Full history: 64 GB RAM, 8+ cores, ~39 TB NVMe with XFS, node_size huge. Months to"
  echo "          sync. Database: $SERVER_DB."
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

step_node() {
  local mode="$1" history size peers=() p parts=()
  if [ "$mode" = server ]; then history=full; else history=pruned; fi
  size="$(node_size_for "$mode")"
  say "xrpld node: $( [ "$mode" = server ] && echo 'full history' || echo 'recent ledgers only') (history=$history), node_size=$size, database in $DB_PATH"
  check_platform
  if [ "$mode" = server ]; then
    [ "$RAM_GB" -ge 60 ] || need_ok "full history wants 64 GB RAM; this computer has ${RAM_GB} GB. Use --mode test on this computer"
  else
    [ "$RAM_GB" -ge 7 ] || need_ok "only ${RAM_GB} GB RAM: below 8 GB xrpld may not keep up with the network (xrpl.org: node_size tiny not recommended)"
    [ "$RAM_GB" -ge 15 ] || warn "${RAM_GB} GB RAM: below the xrpl.org test minimum of 16 GB; node_size $size, the node may sync slowly or fall behind"
    [ "$CORES" -ge 4 ] || warn "$CORES CPU cores: xrpl.org asks for 4+; the node may fall behind"
  fi
  check_db_path
  local probe="$DB_PATH" fs free_gb
  while [ ! -e "$probe" ]; do probe="$(dirname "$probe")"; done
  fs=$(stat -f -c %T "$probe"); free_gb=$(df -BG --output=avail "$probe" | tail -1 | tr -dc 0-9)
  echo "  database disk: $probe ($fs, ${free_gb} GB free)"
  if [ "$mode" = server ]; then
    [ "$free_gb" -ge 45000 ] || need_ok "full history needs ~39 TB (+ growth); only ${free_gb} GB free"
    case "$fs" in ext*) need_ok "full history needs XFS or ZFS (NuDB files grow past ext4's 16 TB limit)" ;; esac
  else
    [ "$free_gb" -ge 10 ] || need_ok "only ${free_gb} GB free for the node database; keep at least 10 GB free"
    [ "$free_gb" -ge 50 ] || warn "${free_gb} GB free: xrpl.org asks for 50 GB for the node database"
  fi
  if [ "$WSL" = yes ]; then
    case "$probe" in /mnt/*) need_ok "DB_PATH is on a Windows drive (/mnt/...): far too slow for the node" ;; esac
  fi

  if command -v xrpld >/dev/null 2>&1 || [ -x /usr/bin/xrpld ]; then
    ok "xrpld is installed"
  elif ask "Install xrpld (official XRPLF package; signing key checked)?" Y; then
    install_xrpld_pkg
  else
    die "the $mode setup needs xrpld"
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
  if [ "$mode" = server ] && [ -z "${FIXED_PEERS:-}" ]; then
    warn "full history: add peers that hold full history (FIXED_PEERS='host port'; see xrpl.org 'Configure Full History')"
  fi
  firewall_note
}

step_python() {
  make_venv
  ok "Python environment ready: $REPO_DIR/.venv"
}

# env_set KEY=VALUE ... : write into .env (created from .env.example; backup kept; values never printed)
env_set() { python3 "$REPO_DIR/setup/envfile.py" "$REPO_DIR/.env" --example "$REPO_DIR/.env.example" "$@"; }
env_get() { "$REPO_DIR/.venv/bin/python" -c 'import sys; from xrpl_ingest.support.files import load_env; print(load_env({}, cwd=sys.argv[1]).values.get(sys.argv[2]) or "")' "$REPO_DIR" "$1"; }

step_env() {
  local mode="$1" uri="${MONGO_URI:-}" have_uri=0 db
  if [ -n "${MONGO_URI_FILE:-}" ] || python3 "$REPO_DIR/setup/envfile.py" "$REPO_DIR/.env" --has MONGO_URI || \
     python3 "$REPO_DIR/setup/envfile.py" "$REPO_DIR/.env" --has MONGO_URI_FILE; then have_uri=1; fi
  if [ -z "$uri" ] && [ "$have_uri" = 0 ]; then
    if port_open 127.0.0.1 27017; then uri="$LOCAL_MONGO"
    else
      [ "$WSL" = yes ] && echo "WSL: MongoDB runs on Windows. With mirrored networking use $LOCAL_MONGO."
      while [ -z "$uri" ]; do read -rs -p "MongoDB address (MONGO_URI, input hidden; e.g. $LOCAL_MONGO): " uri; echo; done
    fi
  fi
  if [ -z "$NODE_WS" ]; then
    NODE_WS="$($SUDO python3 "$REPO_DIR/setup/edit_cfg.py" "$CFG" --ws-url)" || die "cannot read the node WebSocket endpoint; set NODE_WS explicitly"
  fi
  # Same settings for test and server (test is the server in small); only the database differs.
  local args=("SERVERS=$NODE_WS" "CHUNK_SIZE=1000" "NUM_PROCESSES=$(( CORES < 4 ? CORES : 4 ))" "CONCURRENCY=16")
  [ -z "$uri" ] || args+=("MONGO_URI=$uri")
  if [ -n "${MONGO_URI_FILE:-}" ] && [ -z "$uri" ] && [ ! -f "$REPO_DIR/.env" ]; then args+=("MONGO_URI="); fi
  db="$(env_get DB_NAME)"
  if [ "$mode" = test ]; then
    args+=("DB_NAME=$TEST_DB")
  else
    case "$db" in "$TEST_DB"|xrpl_trial) args+=("DB_NAME=$SERVER_DB") ;; esac
  fi
  say "Writing settings into .env ($mode setup)"
  env_set "${args[@]}"
}

step_tests() {
  local uri no_int=1 rc=0 logf="$REPO_DIR/logs/install-tests.log"
  if [ "$SKIP_TESTS" = 1 ]; then warn "test suite skipped (--skip-tests)"; return 0; fi
  uri="$(env_get MONGO_URI)"
  case "$uri" in "$LOCAL_MONGO"*|mongodb://localhost:27017*) no_int=0 ;; esac  # end-to-end tests need a local MongoDB without password
  mkdir -p "$REPO_DIR/logs"
  say "Running the test suite (normally about 1 minute; stopped after $TEST_TIMEOUT_MIN minutes; output also in logs/install-tests.log)"
  # -v: every test name is printed, so a stuck test is visible; faulthandler_timeout: after
  # 120 s in one test, Python prints where it is stuck (into the log).
  local envs=(PYTHONUTF8=1)
  if [ "$no_int" = 1 ]; then envs+=(XRPL_INGEST_NO_INTEGRATION=1); fi
  (cd "$REPO_DIR" && env "${envs[@]}" timeout -s INT -k 30 "$((TEST_TIMEOUT_MIN * 60))" \
     .venv/bin/python -m pytest -v -o faulthandler_timeout=120 2>&1 \
     | tee "$logf" \
     | grep -E --line-buffered " (PASSED|FAILED|ERROR|SKIPPED)|passed|failed|Timeout|Traceback" \
     | sed -u 's/^/   /'; exit "${PIPESTATUS[0]}") || rc=$?
  if [ "$rc" = 0 ]; then ok "tests passed"; return 0; fi
  if [ "$rc" = 124 ] || [ "$rc" = 130 ] || [ "$rc" = 137 ]; then
    warn "the test suite did not finish within $TEST_TIMEOUT_MIN minutes (a test is stuck). Details: $logf"
  else
    warn "some tests failed (exit $rc). Details: $logf"
  fi
  echo "     Your system is not changed by this. Please send logs/install-tests.log to the developers."
  ask "Continue the setup anyway (node, services, real-data checks)?" N || \
    die "stopped after the tests. Continue later with:  bash install.sh --skip-tests"
}

step_doctor() {
  say "Checking the setup (python -m xrpl_ingest doctor)"
  check_with_doctor "MongoDB" --mongo
  check_with_doctor "Node" --node
}

# xi ARGS...: python -m xrpl_ingest ARGS from the repo (base dir = repo)
xi() { (cd "$REPO_DIR" && XRPL_INGEST_HOME="$REPO_DIR" PYTHONUTF8=1 .venv/bin/python -m xrpl_ingest "$@"); }

# wait_for_sync: until server_state is full/proposing/validating and complete_ledgers has a range.
# Sets NODE_LEDGERS. Returns 1 after SYNC_WAIT_MIN minutes.
wait_for_sync() {
  local waited=0 info state
  say "Waiting for the node to sync (up to $SYNC_WAIT_MIN minutes; a new node usually needs 5-20)"
  while :; do
    info="$(xrpld_state)"; state="${info%% *}"; NODE_LEDGERS="${info#* }"
    case "$state" in
      full|proposing|validating)
        if printf '%s' "$NODE_LEDGERS" | grep -Eq '[0-9]+-[0-9]+'; then ok "node synced: state $state, ledgers $NODE_LEDGERS"; return 0; fi ;;
    esac
    [ "$waited" -lt $((SYNC_WAIT_MIN * 60)) ] || return 1
    printf '   %3d min: node state %s, ledgers %s\n' $((waited / 60)) "${state:-no answer}" "${NODE_LEDGERS:-?}"
    sleep 30; waited=$((waited + 30))
  done
}

# step_trial: the checks that show real data arrives, the same commands as on the server.
step_trial() {
  local last a b start end fails=0
  say "Real-data checks (test setup)"
  if ! wait_for_sync; then
    warn "the node is not synced yet (state: ${NODE_LEDGERS:-?}). This is normal for a new node."
    echo "     Run the checks later:  bash install.sh --trial"
    return 0
  fi
  last="$(printf '%s' "$NODE_LEDGERS" | grep -Eo '[0-9]+-[0-9]+' | tail -1)"
  a="${last%-*}"; b="${last#*-}"
  end=$((b - 2)); start=$((end - 199)); [ "$start" -ge "$a" ] || start="$a"
  say "1/4 newest ledger:   fetch --ledger validated"
  xi fetch --ledger validated || fails=$((fails + 1))
  say "2/4 backfill:        200 recent ledgers ($start-$end) from your node"
  xi backfill --start "$start" --end "$end" || fails=$((fails + 1))
  say "3/4 verify:          the same range"
  xi verify --start "$start" --end "$end" || fails=$((fails + 1))
  say "4/4 live tail:       60 seconds, then a graceful stop"
  (cd "$REPO_DIR" && XRPL_INGEST_HOME="$REPO_DIR" PYTHONUTF8=1 timeout -s INT -k 30 60 .venv/bin/python -m xrpl_ingest tail) || true
  xi health || fails=$((fails + 1))
  xi status || true
  if [ "$fails" = 0 ]; then ok "real data arrives: newest ledger, backfill, verify and live tail all worked (database $TEST_DB)"
  else warn "$fails check(s) failed (see above). python -m xrpl_ingest doctor shows more."; fi
}

finish() {
  local mode="$1" units="$2"
  echo
  say "Done: $mode setup"
  echo "Start every session with:   cd \"$REPO_DIR\" && source .venv/bin/activate"
  if [ "$mode" = test ]; then
    cat <<EOF
The test setup works like the server. Differences: the node keeps only recent ledgers (about
2 hours), so backfill only ranges inside 'node complete_ledgers' (see: python -m xrpl_ingest status);
the database is $TEST_DB.
  python -m xrpl_ingest status                               # node ledgers, what is stored
  python -m xrpl_ingest tail                                 # live ledgers; Ctrl+C stops gracefully
  python -m xrpl_ingest backfill --start N --end M           # N..M inside the node's ledgers
  python -m xrpl_ingest verify   --start N --end M
EOF
    if [ "$units" = 1 ]; then
      echo "  sudo systemctl enable --now xrpl-tail                      # the live service, as on the server"
    fi
    echo "For the real server: on the server machine run  bash install.sh --mode server"
  else
    print_next_steps "$units"
  fi
}

# ------------------------------------------------------------------------------ main
case "$(uname -s)" in
  Darwin)
    [ "$TRIAL_ONLY" = 0 ] && [ -z "$MODE" ] || die "--mode and --trial need the Linux node setup; on macOS omit them"
    say "macOS: Python environment only; see README.md for MongoDB and node setup"
    pick_python
    [ "$CHECK_ONLY" = 0 ] || { echo "(--check: nothing was changed)"; exit 0; }
    step_python
    echo "Activate with: source .venv/bin/activate; then run: python -m pytest -v"
    exit 0 ;;
  Linux) ;;
  *) die "Windows: run install.cmd. Other systems: see README.md 'Manual setup'" ;;
esac
detect

if [ "$CHECK_ONLY" = 1 ]; then
  report
  pick_python
  echo "(--check: nothing was changed)"
  exit 0
fi

if [ "$TRIAL_ONLY" = 1 ]; then
  [ "$VENV_STATE" = exists ] && [ "$ENV_STATE" = exists ] || die "--trial needs a finished test setup (run: bash install.sh --mode test)"
  [ "$(env_get DB_NAME)" = "$TEST_DB" ] || die "--trial runs only on the test setup (DB_NAME=$TEST_DB in .env), so it never writes into the server database"
  step_trial
  exit 0
fi

report
[ "$APT" = yes ] || die "this installer needs apt (Ubuntu / Debian). On other Linux systems follow README.md 'Manual setup'"

if [ -z "$MODE" ]; then
  if [ "$YES" = 1 ]; then MODE="$(recommend)"
  else
    def="$(recommend)"
    read -r -p "Which setup? [test/server, Enter = $def] " MODE || MODE=""
    MODE="${MODE:-$def}"
    case "$MODE" in 1) MODE="test" ;; 2) MODE="server" ;; esac
    case "$MODE" in test|server) ;; *) die "please answer test or server" ;; esac
  fi
fi

echo
echo "Plan for the '$MODE' setup:"
echo "  - system packages: python3, python3-venv, pip, curl"
case "$MONGO" in running*) ;; *) echo "  - MongoDB: $MONGO -> install / start it" ;; esac
echo "  - xrpld node: $XRPLD -> install if needed, configure ($( [ "$MODE" = server ] && echo 'full history' || echo 'recent ledgers only'), node_size $(node_size_for "$MODE")), check, start"
echo "  - .venv with the Python packages from requirements.txt"
echo "  - .env settings (database $( [ "$MODE" = server ] && echo "$SERVER_DB" || echo "$TEST_DB"); existing .env: backup kept, MONGO_URI kept)"
echo "  - run the tests and the doctor check"
echo "  - install the services xrpl-tail / xrpl-backfill (not started)"
[ "$MODE" = server ] || echo "  - wait for the node, then check that real data arrives (tail, backfill, verify)"
ask "Continue?" Y || { echo "Nothing changed."; exit 0; }

pick_python
step_base_packages
step_mongodb
step_node "$MODE"
step_python
step_env "$MODE"
step_tests
step_doctor
UNITS=0
install_ingest_units "${SUDO_USER:-$(id -un)}" && UNITS=1 || true
[ "$MODE" = server ] || step_trial
finish "$MODE" "$UNITS"
