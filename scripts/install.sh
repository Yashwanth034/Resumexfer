#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DEB=$("$ROOT/scripts/build-deb.sh")
if [ "${RESUMEXFER_DRY_RUN:-0}" = 1 ]; then
  printf 'apt-get install -y %q\n' "$DEB"
  exit 0
fi
if [ "$(id -u)" -eq 0 ]; then
  apt-get install -y "$DEB"
else
  sudo apt-get install -y "$DEB"
fi
