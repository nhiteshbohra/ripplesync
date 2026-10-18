# shellcheck shell=bash
# DB_PATH guards shared by setup_all.sh and setup/setup_ubuntu.sh (sourced, not run).
# DB_PATH gets `chown -R xrpld:xrpld`, so a typo must not be able to re-own system files.
# The caller defines die, need_ok and SUDO.

# db_path_problem PATH: print why PATH must not be used as DB_PATH; print nothing if it is fine.
db_path_problem() {
  local p
  p="$(realpath -m -- "$1")"
  case "$p" in
    /|//|/bin|/boot|/dev|/etc|/home|/lib|/lib64|/opt|/proc|/root|/run|/sbin|/srv|/sys|/tmp|/usr|/var|/var/lib|/mnt|/media)
      echo "$p is a system directory"; return ;;
    /etc/*|/usr/*|/proc/*|/sys/*|/dev/*|/boot/*)
      echo "$p is inside a system directory"; return ;;
    # The packaged xrpld.service sets ProtectHome=true: /home, /root and /run/user are
    # invisible to xrpld, so a DB there would fail at start (per the xrpld 3.4.1 unit file).
    /home/*|/root/*|/run/user|/run/user/*)
      echo "$p is under /home, /root or /run/user, which xrpld.service hides (ProtectHome=true)"; return ;;
  esac
  case "$(dirname "$p")" in
    /|//) echo "$p is directly under /" ;;
  esac
}

# check_db_path: die unless DB_PATH is a dedicated directory.
check_db_path() {
  local why
  why="$(db_path_problem "$DB_PATH")"
  [ -z "$why" ] || die "DB_PATH=$DB_PATH refused: $why (it gets chown -R); use a dedicated directory, e.g. /data/xrpld/nudb"
}

# check_db_owner: before chown -R, require FORCE=1 if DB_PATH already holds files not owned by xrpld.
check_db_owner() {
  local owner
  [ -d "$DB_PATH" ] || return 0
  owner="$($SUDO stat -c %U -- "$DB_PATH")"
  [ "$owner" != xrpld ] || return 0
  if [ -n "$($SUDO find "$DB_PATH" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    need_ok "DB_PATH=$DB_PATH is not empty and is owned by '$owner', not xrpld; chown -R xrpld:xrpld would re-own everything in it"
  fi
}
