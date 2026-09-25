#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
test -x "$ROOT/scripts/verify.sh"
bash -n "$ROOT/scripts/verify.sh"
grep -Fq 'go test ./...' "$ROOT/scripts/verify.sh"
grep -Fq 'go test -race ./...' "$ROOT/scripts/verify.sh"
grep -Fq 'go vet ./...' "$ROOT/scripts/verify.sh"
grep -Fq 'seq 1 10' "$ROOT/scripts/verify.sh"
grep -Fq 'go test -count=1 ./...' "$ROOT/scripts/verify.sh"
grep -Fq 'acceptance_script_test.sh' "$ROOT/scripts/verify.sh"
grep -Fq 'transport_v2_verify_script_test.sh' "$ROOT/scripts/verify.sh"
grep -Fq 'transport_v2_acceptance_script_test.sh' "$ROOT/scripts/verify.sh"
