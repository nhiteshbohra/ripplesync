#!/usr/bin/env bash
# Show xrpld sync state through the ingest package: python -m xrpl_ingest doctor --node
# (server_state, network_id, complete_ledgers, validated seq, peers, build_version).
# Needs the venv and .env from `bash install.sh`. Extra args are passed on
# (e.g. --json). Exit code is doctor's: 0 ok, 1 warnings, 2 config error, 4 unreachable.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"
if [ ! -x .venv/bin/python ]; then
  echo "no $REPO_DIR/.venv: run: bash install.sh" >&2
  exit 2
fi
export XRPL_INGEST_HOME="${XRPL_INGEST_HOME:-$REPO_DIR}" PYTHONUTF8=1
exec .venv/bin/python -m xrpl_ingest doctor --node "$@"
