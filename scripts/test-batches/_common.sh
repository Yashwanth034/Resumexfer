#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"

if [[ -x "$ROOT/.rt_hw/go1.23.2/bin/go" ]]; then
  export PATH="$ROOT/.rt_hw/go1.23.2/bin:$PATH"
fi
export GOTOOLCHAIN=${GOTOOLCHAIN:-go1.23.2}
export GOFLAGS=${GOFLAGS:--buildvcs=false}

run() {
  local label=$1
  shift
  printf '\n== %s ==\n' "$label"
  "$@"
  printf 'PASS: %s\n' "$label"
}

run_timeout() {
  local seconds=$1
  local label=$2
  shift 2
  printf '\n== %s (timeout %ss) ==\n' "$label" "$seconds"
  timeout --foreground "$seconds" "$@"
  printf 'PASS: %s\n' "$label"
}
