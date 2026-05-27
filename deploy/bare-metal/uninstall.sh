#!/usr/bin/env bash
# Uninstall the bare-metal VectorDB install.
#
#   sudo ./uninstall.sh             # interactive
#   sudo FORCE=1 ./uninstall.sh     # no prompts
#   sudo KEEP_DATA=1 ./uninstall.sh # keep /var/lib/vectordb
#   sudo KEEP_USER=1 ./uninstall.sh # keep the vectordb system user

set -euo pipefail

if [ "$(id -u)" != "0" ]; then
  echo "ERROR: run with sudo." >&2
  exit 2
fi

PREFIX="${VECTORDB_PREFIX:-/usr/local}"
DAEMON_USER="${VECTORDB_USER:-vectordb}"
DATA_DIR="${VECTORDB_DATA_DIR:-/var/lib/vectordb}"
CFG_DIR="${VECTORDB_CONFIG_DIR:-/etc/vectordb}"

confirm() {
  if [ "${FORCE:-0}" = "1" ]; then return 0; fi
  read -rp "$1 [y/N] " ans
  case "$(printf '%s' "$ans" | tr '[:upper:]' '[:lower:]')" in
    y|yes) return 0 ;;
    *)     return 1 ;;
  esac
}

echo "→ stopping services"
systemctl disable --now vectordb-gateway.service 2>/dev/null || true
systemctl disable --now vectordb-server.service  2>/dev/null || true

echo "→ removing systemd units"
rm -f /etc/systemd/system/vectordb-gateway.service
rm -f /etc/systemd/system/vectordb-server.service
systemctl daemon-reload

if confirm "Remove binaries from $PREFIX/bin?"; then
  rm -f "$PREFIX/bin/vectordb-server" "$PREFIX/bin/vectordb-gateway" "$PREFIX/bin/vectordb"
fi

if confirm "Remove config dir $CFG_DIR?"; then
  rm -rf "$CFG_DIR"
fi

if [ "${KEEP_DATA:-0}" = "1" ]; then
  echo "→ KEEP_DATA=1; leaving $DATA_DIR intact"
elif confirm "Remove data dir $DATA_DIR (THIS WIPES YOUR DATA)?"; then
  rm -rf "$DATA_DIR"
fi

if [ "${KEEP_USER:-0}" = "1" ]; then
  echo "→ KEEP_USER=1; leaving system user '$DAEMON_USER' intact"
elif id "$DAEMON_USER" >/dev/null 2>&1; then
  if confirm "Remove system user '$DAEMON_USER'?"; then
    userdel "$DAEMON_USER" 2>/dev/null || true
  fi
fi

echo "done."
