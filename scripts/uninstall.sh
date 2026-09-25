#!/usr/bin/env bash
set -euo pipefail
if [ "${RESUMEXFER_DRY_RUN:-0}" = 1 ]; then
  printf '%s\n' 'apt-get purge -y resumexfer'
  exit 0
fi
if [ "$(id -u)" -eq 0 ]; then
  apt-get purge -y resumexfer
else
  sudo apt-get purge -y resumexfer
fi
