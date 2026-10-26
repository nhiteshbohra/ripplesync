#!/usr/bin/env bash
# Runnable installer self-check. All installs and interpreters below are stubs.
set -euo pipefail
project="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
for script in "$project/install.sh" "$project/setup/"*.sh; do bash -n "$script"; done
echo 'PASS bash -n install.sh setup/*.sh (each file checked)'
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
export MOCK_ROOT="$work" MOCK_APT_CANDIDATE='(none)' MOCK_FAIL=0 MOCK_APT_FAIL=0 MOCK_OS=Linux
real_awk="$(command -v awk)"
real_df="$(command -v df)"
export MOCK_AWK="$real_awk" MOCK_DF="$real_df"
mkdir -p "$work/bin" "$work/repo"
cp "$project/requirements.txt" "$work/repo/"
cat >"$work/bin/python-stub" <<'EOF'
#!/usr/bin/env bash
v=3.14
case "$0" in
  *python3.12) [ -f "$MOCK_ROOT/installed" ] || exit 1; v=3.12 ;;
  */.venv/bin/python) v="$(cat "$0.version")" ;;
esac
case "$1" in
  --version) echo "Python $v.0" ;;
  -c)
    if [[ "$2" == *hashlib* ]]; then sha256sum "$3" | cut -d' ' -f1
    else echo "$v"; fi ;;
  -m)
    if [ "$2" = venv ]; then
      mkdir -p "$3/bin"
      cp "$MOCK_ROOT/bin/python-stub" "$3/bin/python"
      printf '%s\n' "$v" >"$3/bin/python.version"
    else echo "pip $*" >>"$MOCK_ROOT/actions"; fi ;;
esac
EOF
cat >"$work/bin/tool-stub" <<'EOF'
#!/usr/bin/env bash
case "${0##*/}" in
  apt-cache) echo "  Candidate: $MOCK_APT_CANDIDATE" ;;
  apt-get|brew)
    echo "${0##*/} $*" >>"$MOCK_ROOT/actions"
    if [ "${0##*/}" = apt-get ] && [ "$MOCK_APT_FAIL" = 1 ]; then exit 1; fi
    [ "$MOCK_FAIL" = 0 ] || exit 1
    touch "$MOCK_ROOT/installed" ;;
  uv)
    if [[ "$*" == *'python find'* ]]; then
      [ -f "$MOCK_ROOT/installed" ] || exit 1
      echo "${MOCK_UV_PYTHON:-$MOCK_ROOT/bin/python3.12}"
    else
      echo "uv $*" >>"$MOCK_ROOT/actions"
      [ "$MOCK_FAIL" = 0 ] || exit 1
      touch "$MOCK_ROOT/installed"
    fi ;;
  uname) [ "${1:-}" != -s ] || { echo "$MOCK_OS"; exit; }; echo x86_64 ;;
  awk)
    if [[ "$*" == *'/proc/meminfo'* ]]; then echo 16
    else exec "$MOCK_AWK" "$@"; fi ;;
  df)
    if [[ "$*" == *'--output=avail'* ]]; then printf 'Avail\n100G\n'
    else exec "$MOCK_DF" "$@"; fi ;;
esac
EOF
for name in python3 python3.12; do cp "$work/bin/python-stub" "$work/bin/$name"; done
for name in apt-cache apt-get brew uv uname awk df; do cp "$work/bin/tool-stub" "$work/bin/$name"; done
chmod +x "$work/bin/"*
export PATH="$work/bin:$PATH"
say() { echo "$*"; }
warn() { echo "WARN $*" >&2; }
die() { echo "ERROR $*" >&2; exit 1; }
. <(sed -n '/^ask() {/,/^}/p' "$project/install.sh")
# Redirect discovery paths into the sandbox without changing the user's HOME.
mkdir -p "$work/repo/setup"
cp "$project/install.sh" "$work/repo/"
cp "$project/setup/db_path_guard.sh" "$work/repo/setup/"
sed -e 's/\$HOME/\$MOCK_ROOT/g' \
    -e 's|/opt/homebrew/opt|$MOCK_ROOT/homebrew/opt|g' \
    -e 's|/usr/local/opt|$MOCK_ROOT/local/opt|g' "$project/setup/common.sh" >"$work/repo/setup/common.sh"
. "$work/repo/setup/common.sh"
REPO_DIR="$work/repo" SUDO= YES=0 CHECK_ONLY=0

# --check must not install, build a venv, or run --trial's data checks.
bash "$REPO_DIR/install.sh" --check --trial >"$work/check.log" 2>&1 || { cat "$work/check.log"; exit 1; }
grep 'Python 3.12: missing, would be installed with uv' "$work/check.log" || { cat "$work/check.log"; exit 1; }
[ ! -e "$work/actions" ] && [ ! -e "$REPO_DIR/.venv" ]
echo 'PASS --check --trial: read-only, reports uv'

# Declining both prompts leaves everything untouched; EOF is not fallback consent.
if (pick_python) <<< $'n\nn' >"$work/no.log" 2>&1; then exit 1; fi
grep 'WARN Python 3.14.*tests may hang' "$work/no.log"
[ ! -e "$work/actions" ]
if (pick_python) <<< n >"$work/eof.log" 2>&1; then exit 1; fi
(pick_python; [ "$PYTHON" = "$work/bin/python3" ]) <<< $'n\ny'
echo 'PASS decline, explicit fallback, and EOF refusal'

# Start with a 3.14 venv. Accepting installation and replacement must preserve its backup.
"$work/bin/python3" -m venv "$REPO_DIR/.venv"
pick_python <<< ''
make_venv <<< ''
[ "$("$REPO_DIR/.venv/bin/python" -c version)" = 3.12 ]
[ "$(cat "$REPO_DIR"/.venv-backup.*/.venv/bin/python.version)" = 3.14 ]
"$REPO_DIR/.venv/bin/python" --version
grep 'uv python install 3.12' "$work/actions"
echo 'PASS uv install, 3.14 backup, 3.12 rebuild and pip install'
printf keep >"$REPO_DIR/.venv/keep"
make_venv </dev/null
[ "$(cat "$REPO_DIR/.venv/keep")" = keep ]
echo 'PASS existing 3.12 venv preserved'

# Candidate detection, apt failure -> uv, all failures -> stop even under --yes.
rm "$work/installed"
find_python312
[ "$PYTHON" = "$REPO_DIR/.venv/bin/python" ]
echo 'PASS existing 3.12 venv discovered without Python 3.12 on PATH'
printf '3.14\n' >"$REPO_DIR/.venv/bin/python.version"
export MOCK_APT_CANDIDATE=3.12.10
YES=1 pick_python
grep 'apt-get install -y python3.12 python3.12-venv' "$work/actions"
rm "$work/installed"
export MOCK_APT_FAIL=1
YES=1 pick_python
echo 'PASS apt failure falls back to uv'
rm "$work/installed"
export MOCK_FAIL=1
if (YES=1 pick_python); then exit 1; fi
echo 'PASS all installation methods fail: unattended fallback refused'
export MOCK_FAIL=0 MOCK_OS=Darwin
pick_python <<< ''
grep 'brew install python@3.12' "$work/actions"
echo 'PASS macOS Homebrew method'

# Discover a pyenv shim and a uv interpreter that is not on PATH.
mkdir -p "$work/pyenv/shims" "$work/managed"
cp "$work/bin/python-stub" "$work/pyenv/shims/python3.12"
mv "$work/bin/python3.12" "$work/managed/python3.12"
# Mask the host interpreter and the already-tested venv for isolated lookup checks.
printf '#!/usr/bin/env bash\nexit 1\n' >"$work/bin/python3.12"
chmod +x "$work/bin/python3.12"
printf '3.14\n' >"$REPO_DIR/.venv/bin/python.version"
PYENV_ROOT="$work/pyenv" find_python312
[ "$PYTHON" = "$work/pyenv/shims/python3.12" ]
export MOCK_UV_PYTHON="$work/managed/python3.12"
find_python312
[ "$PYTHON" = "$MOCK_UV_PYTHON" ]
echo 'PASS pyenv shim and uv interpreter lookup outside PATH'

# Rebuilding still requires confirmation, independently of interpreter installation.
printf '3.14\n' >"$REPO_DIR/.venv/bin/python.version"
if (make_venv) <<< n; then exit 1; fi
[ "$(cat "$REPO_DIR/.venv/bin/python.version")" = 3.14 ]
version_ge 3.12 3.10
! version_ge 3.9 3.10
echo 'PASS declined rebuild preserves old venv; numeric version comparison'

# A copied venv can run its interpreter while activation still points at the old folder.
printf '3.12\n' >"$REPO_DIR/.venv/bin/python.version"
printf 'VIRTUAL_ENV=/old/copied/repo/.venv\n' >"$REPO_DIR/.venv/bin/activate"
if (make_venv) <<< n; then exit 1; fi
[ -f "$REPO_DIR/.venv/bin/activate" ]
make_venv <<< ''
[ ! -f "$REPO_DIR/.venv/bin/activate" ]  # the stub venv builder has no activation script
[ "$("$REPO_DIR/.venv/bin/python" -c version)" = 3.12 ]
echo 'PASS relocated venv: refusal preserves files; acceptance backs up and rebuilds'
ln -s "$REPO_DIR" "$work/repo-alias"
printf 'VIRTUAL_ENV=%s/.venv\n' "$work/repo-alias" >"$REPO_DIR/.venv/bin/activate"
make_venv </dev/null
[ -f "$REPO_DIR/.venv/bin/activate" ]
echo 'PASS symlink alias of the same venv is reused'
