#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
VERSION=$(tr -d '[:space:]' < "$ROOT/VERSION")
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

INSTALL_OUT=$(RESUMEXFER_DRY_RUN=1 DIST_DIR="$TMP/dist" "$ROOT/scripts/install.sh")
grep -Fq "resumexfer_${VERSION}_" <<<"$INSTALL_OUT"
grep -Fq 'apt-get install -y' <<<"$INSTALL_OUT"

UNINSTALL_OUT=$(RESUMEXFER_DRY_RUN=1 "$ROOT/scripts/uninstall.sh")
grep -Fq 'apt-get purge -y resumexfer' <<<"$UNINSTALL_OUT"
