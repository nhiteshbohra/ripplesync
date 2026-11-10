#!/usr/bin/env bash
set -u

repo_dir="${1:?usage: monitor_services.sh REPO_DIR}"
python="$repo_dir/.venv/bin/python"
failed=0

for unit in xrpl-tail.service xrpl-backfill.service xrpl-recover.service xrpl-reconcile.service xrpl-reconcile.timer; do
  if systemctl is-failed --quiet "$unit"; then
    printf 'ALERT: %s is failed\n' "$unit" >&2
    failed=1
  fi
done

if "$python" -m xrpl_ingest health; then
  :
else
  rc=$?
  printf 'ALERT: XRPL tail health check failed (exit %s)\n' "$rc" >&2
  failed=1
fi

exit "$failed"
