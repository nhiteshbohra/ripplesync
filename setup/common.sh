# shellcheck shell=bash
# Helpers for install.sh (sourced, not run).
# NOT TESTED ON A REAL HOST: only `bash -n` and shellcheck so far. Every fact about
# packages, paths and ports below comes from the vendor docs cited next to it and is
# still unverified against a real install.
# The caller defines say, warn, die, need_ok, SUDO and REPO_DIR.

# xrpl.org "Install on Ubuntu or Debian Linux" (xrpl-dev-portal master, 2026-09-29).
XRPLF_KEY_URL=https://packages.xrplf.org/xrplf.asc
XRPLF_KEY_FPR=B655416741221F780FBCFBC9AA84D41A11D29FA9   # pub rsa4096 2026-08-18, XRPLF Packages
XRPLF_KEYRING=/etc/apt/keyrings/xrplf.asc
XRPLF_APT_LINE="deb [signed-by=$XRPLF_KEYRING] https://packages.xrplf.org/repository/deb-stable any main"
XRPLD_CFG=/etc/xrpld/xrpld.cfg
# Same command line as the packaged xrpld.service (ExecStart, User=xrpld), for hosts without systemd.
XRPLD_START_CMD="sudo -u xrpld /usr/bin/xrpld --net --silent --conf $XRPLD_CFG"

# MongoDB 8.0 docs "Install MongoDB Community Edition on Ubuntu" (Ubuntu 20.04/22.04/24.04).
# 8.0, not 9.0: the pinned pymongo (<4.11, mongomock cap) does not support MongoDB 9.0.
MONGO_SERIES=8.0
MONGO_KEY_URL=https://pgp.mongodb.com/server-8.0.asc
MONGO_KEYRING=/usr/share/keyrings/mongodb-server-8.0.gpg
MONGO_WINDOWS_URL=https://www.mongodb.com/try/download/community

is_wsl()      { grep -qi microsoft /proc/version 2>/dev/null; }
has_systemd() { [ -d /run/systemd/system ]; }

# version_ge A B: true if dotted version A >= B.
version_ge() { [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -n1)" = "$2" ]; }

glibc_version() {
  local v
  v="$(getconf GNU_LIBC_VERSION 2>/dev/null | awk '{print $2}')" || true
  [ -n "$v" ] || v="$(ldd --version 2>/dev/null | head -n1 | awk '{print $NF}')" || true
  printf '%s' "$v"
}

# check_platform: the XRPLF .deb needs glibc >= 2.31 on x86_64 (xrpl.org: Ubuntu 20.04+ or
# Debian 11+). Newer releases (e.g. Ubuntu 26.04, Debian 12/13) pass without a warning.
# Sources /etc/os-release, so ID, VERSION_CODENAME, UBUNTU_CODENAME, PRETTY_NAME are set after.
check_platform() {
  local arch glibc
  # shellcheck source=/dev/null
  . /etc/os-release
  arch="$(uname -m)"
  glibc="$(glibc_version)"
  echo "OS: ${PRETTY_NAME:-unknown}   arch: $arch   glibc: ${glibc:-?}   WSL: $(is_wsl && echo yes || echo no)   systemd: $(has_systemd && echo yes || echo no)"
  [ "$arch" = x86_64 ] || need_ok "architecture is $arch; the XRPLF xrpld packages are for x86_64"
  if [ -z "$glibc" ]; then
    need_ok "cannot determine the glibc version (the xrpld package needs glibc >= 2.31)"
  elif ! version_ge "$glibc" 2.31; then
    need_ok "glibc $glibc is older than 2.31; the xrpld package needs Ubuntu 20.04+ or Debian 11+"
  fi
}

python312_hint() {
  echo "To get Python 3.12: Ubuntu 24.04 ships it (sudo apt-get install python3.12 python3.12-venv);" \
       "on other releases use pyenv or uv, or the deadsnakes PPA (third-party, not from Canonical:" \
       "sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt-get install python3.12 python3.12-venv)," \
       "then re-run this script (python3.12 is used automatically when it is on PATH)."
}

# pick_python: set PYTHON to python3.12 if on PATH, else python3; require >= 3.10.
pick_python() {
  local cand v
  if command -v python3.12 >/dev/null 2>&1; then cand=python3.12; else cand=python3; fi
  command -v "$cand" >/dev/null 2>&1 || die "$cand not found. $(python312_hint)"
  v="$("$cand" -c 'import sys; print("%d.%d" % sys.version_info[:2])')" || die "cannot run $cand"
  version_ge "$v" 3.10 || die "$cand is Python $v; xrpl_ingest needs Python >= 3.10 (3.12 recommended). $(python312_hint)"
  [ "$v" = 3.12 ] || warn "$cand is Python $v: supported (>= 3.10), but 3.12 is the version this project targets. $(python312_hint)"
  PYTHON="$cand"
  echo "Python for .venv: $PYTHON ($v)"
}

# make_venv: create $REPO_DIR/.venv with $PYTHON (pick_python first) and install requirements.txt.
make_venv() {
  local venv="$REPO_DIR/.venv" v
  if [ -x "$venv/bin/python" ]; then
    v="$("$venv/bin/python" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || echo 0)"
    version_ge "$v" 3.10 || die "existing $venv uses Python $v (< 3.10); remove it (rm -rf '$venv') and re-run"
    say "Using existing $venv (Python $v)"
  else
    say "Creating $venv with $PYTHON"
    "$PYTHON" -m venv "$venv" || die "venv creation failed; install the venv package: sudo apt-get install $(basename "$PYTHON")-venv"
  fi
  # Skip pip when requirements.txt has not changed since the last successful install.
  local stamp="$venv/.requirements.sha256" want
  want="$(sha256sum "$REPO_DIR/requirements.txt" | cut -d' ' -f1)"
  if [ -f "$stamp" ] && [ "$(cat "$stamp")" = "$want" ] && "$venv/bin/python" -m pip check >/dev/null 2>&1; then
    say "Python packages already installed (requirements.txt unchanged)"
    return 0
  fi
  "$venv/bin/python" -m pip install -q --upgrade pip
  "$venv/bin/python" -m pip install -q -r "$REPO_DIR/requirements.txt"
  printf '%s\n' "$want" >"$stamp"
}

# print_key_fprs FILE: primary-key fingerprint of every key in FILE, one per line.
print_key_fprs() {
  local gh rc=0
  gh="$(mktemp -d)"   # throwaway GNUPGHOME: never touch the user's or root's keyring
  GNUPGHOME="$gh" gpg --batch --show-keys --with-colons "$1" 2>/dev/null \
    | awk -F: '$1 == "pub" { want = 1; next } $1 == "fpr" && want { print $10; want = 0 }' || rc=$?
  rm -rf "$gh"
  return "$rc"
}

# install_xrplf_key: download the XRPLF apt key and refuse it unless it is exactly the one
# key whose fingerprint xrpl.org publishes.
install_xrplf_key() {
  local tmp fprs
  tmp="$(mktemp)"
  if ! wget -qO "$tmp" "$XRPLF_KEY_URL"; then rm -f "$tmp"; die "download failed: $XRPLF_KEY_URL"; fi
  fprs="$(print_key_fprs "$tmp" || true)"
  if [ "$fprs" != "$XRPLF_KEY_FPR" ]; then
    rm -f "$tmp"
    die "XRPLF key fingerprint mismatch: expected $XRPLF_KEY_FPR (xrpl.org install docs), got '${fprs:-none}'. Not installing it; check https://xrpl.org/docs/infrastructure/installation/install-xrpld-on-ubuntu"
  fi
  $SUDO install -m 0755 -d "$(dirname "$XRPLF_KEYRING")"
  $SUDO install -m 0644 "$tmp" "$XRPLF_KEYRING"
  rm -f "$tmp"
  say "XRPLF key OK (fingerprint $XRPLF_KEY_FPR matches the pinned value)"
}

install_xrpld_pkg() {
  say "Installing xrpld (official XRPLF apt repo)"
  $SUDO apt-get -y update
  $SUDO apt-get -y install apt-transport-https ca-certificates wget gnupg python3
  install_xrplf_key
  echo "$XRPLF_APT_LINE" | $SUDO tee /etc/apt/sources.list.d/xrplf.list >/dev/null
  $SUDO apt-get -y update
  $SUDO apt-get -y install xrpld
}

# remove_old_cfg_links: earlier versions of these scripts symlinked /etc/opt/ripple/{xrpld,rippled}.cfg
# to /etc/xrpld/xrpld.cfg. xrpld 3.4.1 does not search /etc/opt/ripple, so drop links that point
# at our cfg; anything else there is left alone.
remove_old_cfg_links() {
  local l
  for l in /etc/opt/ripple/xrpld.cfg /etc/opt/ripple/rippled.cfg; do
    if [ -L "$l" ] && [ "$(readlink -- "$l")" = "$XRPLD_CFG" ]; then
      $SUDO rm -f -- "$l"
      say "removed old symlink $l (not read by xrpld)"
    fi
  done
}

# xrpld_failure_report: show why xrpld does not stay up. The journal only shows the exit code;
# the reason is in xrpld's own log file (debug_logfile, /var/log/xrpld/debug.log by default).
xrpld_failure_report() {
  warn "xrpld does not stay running. Its last log lines:"
  $SUDO journalctl -u xrpld.service -n 8 --no-pager 2>/dev/null || true
  echo "---- /var/log/xrpld/debug.log (last 25 lines) ----"
  $SUDO tail -n 25 /var/log/xrpld/debug.log 2>/dev/null || echo "(no debug.log)"
  echo "---- validator settings check ----"
  $SUDO python3 "$REPO_DIR/setup/xrpld_check.py" check --cfg "$XRPLD_CFG" || true
}

# start_xrpld: enable + start the packaged unit and make sure it keeps running (a bad config
# makes it exit within seconds and systemd restarts it in a loop). Returns 1 if it does not stay up.
# Without systemd: print the manual command.
start_xrpld() {
  local restarts
  if ! has_systemd; then
    warn "no systemd: xrpld was not started. Start it as the xrpld user (same command line as the packaged unit; not verified here):"
    echo "    $XRPLD_START_CMD"
    return 0
  fi
  $SUDO systemctl daemon-reload
  $SUDO systemctl reset-failed xrpld.service 2>/dev/null || true
  $SUDO systemctl enable xrpld.service >/dev/null 2>&1 || true
  $SUDO systemctl restart xrpld.service || true
  say "Waiting 20 seconds to see that xrpld keeps running"
  sleep 20
  restarts="$(systemctl show -p NRestarts --value xrpld.service 2>/dev/null || echo 0)"
  if systemctl is-active --quiet xrpld.service && [ "${restarts:-0}" = 0 ]; then
    say "xrpld is running"
    return 0
  fi
  xrpld_failure_report
  return 1
}

firewall_note() {
  echo "Firewall: allow incoming TCP 2459, the xrpld peer port in the 3.4.1 default cfg (51235 in"
  echo "  older configs; check [port_peer] in $XRPLD_CFG), and forward it on a NAT router."
}

wsl_mongo_notes() {
  warn "WSL detected: MongoDB is not supported on WSL (MongoDB Windows install docs), so it is not installed here."
  cat <<EOF
  Run MongoDB on Windows instead (NOT tested by this project):
    1. Install MongoDB Community $MONGO_SERIES (.msi, Windows service) from
       $MONGO_WINDOWS_URL
    2. Let WSL reach it as 127.0.0.1 with mirrored networking (Windows 11 22H2 or later):
       in %UserProfile%\\.wslconfig on Windows add
         [wsl2]
         networkingMode=mirrored
       then run 'wsl --shutdown' in PowerShell and reopen WSL.
       Without mirrored networking: use the Windows host IP ($(ip route 2>/dev/null | awk '/default/ {print $3; exit}')),
       set net.bindIp in mongod.cfg and allow TCP 27017 in Windows Firewall (add auth first).
    3. Put MONGO_URI=mongodb://127.0.0.1:27017 (mirrored) or the host IP into .env.
EOF
}

# install_mongodb: MongoDB 8.0 from the official repo on Ubuntu 20.04/22.04/24.04.
# Skipped on WSL (MongoDB does not support WSL). Other releases: stop with a message.
install_mongodb() {
  local codename tmp fprs list=/etc/apt/sources.list.d/mongodb-org-$MONGO_SERIES.list
  if is_wsl; then wsl_mongo_notes; return 0; fi
  codename="${UBUNTU_CODENAME:-${VERSION_CODENAME:-}}"  # derivatives (Mint, Pop!_OS) set UBUNTU_CODENAME
  case "$codename" in
    focal|jammy|noble) ;;
    *) die "No MongoDB $MONGO_SERIES apt repo for codename '${codename:-unknown}' (MongoDB docs: Ubuntu focal/jammy/noble only). Install MongoDB 8.0 (or 7.0) manually; 9.0 is not supported by the pinned pymongo." ;;
  esac
  say "Installing MongoDB $MONGO_SERIES (official repo, Ubuntu $codename)"
  $SUDO apt-get -y update
  $SUDO apt-get -y install curl gnupg ca-certificates
  tmp="$(mktemp)"
  if ! curl -fsSL "$MONGO_KEY_URL" -o "$tmp"; then rm -f "$tmp"; die "download failed: $MONGO_KEY_URL"; fi
  fprs="$(print_key_fprs "$tmp" || true)"
  # Trust on first use: we have no officially published fingerprint to pin for this key.
  echo "MongoDB $MONGO_SERIES signing key fingerprint(s), NOT pinned (trust on first use):"
  printf '    %s\n' "${fprs:-<none parsed>}"
  $SUDO gpg --batch --yes --dearmor -o "$MONGO_KEYRING" <"$tmp"
  $SUDO chmod 0644 "$MONGO_KEYRING"
  rm -f "$tmp"
  echo "deb [ arch=amd64,arm64 signed-by=$MONGO_KEYRING ] https://repo.mongodb.org/apt/ubuntu $codename/mongodb-org/$MONGO_SERIES multiverse" \
    | $SUDO tee "$list" >/dev/null
  $SUDO apt-get -y update
  $SUDO apt-get -y install mongodb-org
  if has_systemd; then
    $SUDO systemctl daemon-reload
    $SUDO systemctl enable --now mongod
    sleep 2
    if $SUDO systemctl is-active --quiet mongod; then say "mongod is active"; else warn "mongod started with warnings: systemctl status mongod"; fi
  else
    warn "no systemd: start mongod manually with 'sudo mongod --config /etc/mongod.conf &'"
  fi
}

# run_doctor ARGS...: python -m xrpl_ingest doctor ARGS from the repo (base dir = repo).
run_doctor() {
  (cd "$REPO_DIR" && XRPL_INGEST_HOME="$REPO_DIR" PYTHONUTF8=1 .venv/bin/python -m xrpl_ingest doctor "$@")
}

# check_with_doctor LABEL ARGS...: run doctor, translate its exit code, never fail the script.
check_with_doctor() {
  local label="$1" rc=0
  shift
  run_doctor "$@" || rc=$?
  case "$rc" in
    0) say "$label: OK" ;;
    1) warn "$label: doctor reported warnings (see above)" ;;
    2) warn "$label: configuration problem (exit 2); fix .env, then: python -m xrpl_ingest doctor $*" ;;
    4) warn "$label: not reachable yet (exit 4); check again later: python -m xrpl_ingest doctor $*" ;;
    *) warn "$label: doctor exited with $rc" ;;
  esac
  return 0
}

# sed_escape_repl STRING: escape | & \ for the replacement part of s|...|...|.
sed_escape_repl() { printf '%s' "$1" | sed -e 's/[|&\\]/\\&/g'; }

# unit_value_problem VALUE: why VALUE cannot be put into a unit file as is (empty = fine).
unit_value_problem() {
  case "$1" in
    *%*)          echo "contains % (a systemd specifier)" ;;
    *'$'*)        echo "contains \$ (systemd variable expansion)" ;;
    *'"'*|*\\*)   echo "contains a double quote or backslash" ;;
    *$'\n'*)      echo "contains a newline" ;;
  esac
}

# install_ingest_units USER: render deploy/systemd/xrpl-{tail,backfill}.service with REPO_DIR
# and USER into /etc/systemd/system and daemon-reload. Enables neither. Returns 1 (after a
# warning) if the units cannot be installed safely.
install_ingest_units() {
  local user="$1" why name src dst tmp esc_repo esc_user
  has_systemd || { warn "no systemd: service units not installed (run tail/backfill by hand, see next steps)"; return 1; }
  case "$user" in
    ""|root) warn "not installing the xrpl-* units: they would run as root; re-run as a normal user (with sudo)"; return 1 ;;
  esac
  why="$(unit_value_problem "$REPO_DIR")"
  [ -z "$why" ] || { warn "not installing the xrpl-* units: repo path '$REPO_DIR' $why; move the repo to a plain path"; return 1; }
  esc_repo="$(sed_escape_repl "$REPO_DIR")"
  esc_user="$(sed_escape_repl "$user")"
  for name in xrpl-tail xrpl-backfill; do
    src="$REPO_DIR/deploy/systemd/$name.service"
    dst="/etc/systemd/system/$name.service"
    [ -f "$src" ] || die "missing unit template $src"
    tmp="$(mktemp)"
    # Substitute on directive lines only; the template's comments keep naming the placeholders.
    sed -e "/^[[:space:]]*[#;]/!{s|@REPO_DIR@|$esc_repo|g;s|@USER@|$esc_user|g;}" "$src" >"$tmp"
    if grep -v '^[[:space:]]*[#;]' "$tmp" | grep -q '@[A-Z_]*@'; then
      rm -f "$tmp"; die "unreplaced placeholder left in rendered $name.service"
    fi
    $SUDO install -m 0644 "$tmp" "$dst"
    rm -f "$tmp"
    say "installed $dst (User=$user, not enabled)"
  done
  $SUDO systemctl daemon-reload
}

# print_next_steps UNITS_INSTALLED(0|1)
print_next_steps() {
  local units="$1"
  cat <<EOF

Next steps (from $REPO_DIR, after: source .venv/bin/activate):
  1. Wait for the node to sync (hours; full history takes much longer):
       bash setup/check_node.sh          # = python -m xrpl_ingest doctor --node
     server_state should reach "full" and complete_ledgers should cover your range.
  2. Set the backfill range once:
       cp backfill_range.txt.example backfill_range.txt    # then edit start / end
     Small trial first (CLI flags ignore the range file):
       python -m xrpl_ingest backfill --start 32570 --end 33570
       python -m xrpl_ingest verify   --start 32570 --end 33570
  3. Then run the live tail from day 1, alongside the full backfill:
EOF
  if [ "$units" = 1 ]; then
    cat <<'EOF'
       sudo systemctl enable --now xrpl-tail     # keeps following new ledgers, restarts on failure
       sudo systemctl start xrpl-backfill        # one run over backfill_range.txt (not enabled at boot)
       journalctl -u xrpl-tail -f   /   journalctl -u xrpl-backfill -f
EOF
  else
    cat <<'EOF'
       mkdir -p logs
       nohup env LOG_FILE=logs/tail.log     python -m xrpl_ingest tail     >/dev/null 2>&1 &
       nohup env LOG_FILE=logs/backfill.log python -m xrpl_ingest backfill >/dev/null 2>&1 &
EOF
  fi
  cat <<'EOF'
  4. Watch and check:
       python -m xrpl_ingest status          # chunk progress for the range file's range, failed ledgers, ETA
       python -m xrpl_ingest health          # 0 healthy, 1 lagging/stale, 4 unreachable
       python -m xrpl_ingest retry-failed    # re-fetch ledgers recorded as failed
       python -m xrpl_ingest verify --start N --end M --repair    # find and re-ingest gaps
  5. Graceful stop: sudo systemctl stop xrpl-backfill (or xrpl-tail), or one line in process_status.txt:
       echo backfill=0 > process_status.txt        # tail=0 / retry-failed=0 / current_process=0 (all)
     Remove that line (or the file) before starting the command again.
EOF
}
