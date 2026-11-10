#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUDO=""
say() { :; }
warn() { printf '%s\n' "$*" >&2; }
die() { printf '%s\n' "$*" >&2; exit 1; }
# shellcheck source=../setup/common.sh
. "$repo_dir/setup/common.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf -- "$tmp_dir"' EXIT
source_file="$tmp_dir/source"
target_file="$tmp_dir/target"

printf 'managed content\n' >"$source_file"
install_file_if_absent_or_same "$source_file" "$target_file" 0600
cmp -s "$source_file" "$target_file"

install_file_if_absent_or_same "$source_file" "$target_file" 0600
cmp -s "$source_file" "$target_file"

printf 'operator content\n' >"$source_file"
if (install_file_if_absent_or_same "$source_file" "$target_file" 0600) >/dev/null 2>&1; then
  echo "unexpectedly replaced an existing file" >&2
  exit 1
fi
grep -qx 'managed content' "$target_file"
