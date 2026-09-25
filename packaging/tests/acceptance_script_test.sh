#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SCRIPT="$ROOT/scripts/hardware-acceptance.sh"

test -x "$SCRIPT"
bash -n "$SCRIPT"

grep -Fq -- '--preflight' "$SCRIPT"
grep -Fq -- '--self-test' "$SCRIPT"
grep -Fq 'systemctl --user is-active resumexfer.service' "$SCRIPT"
grep -Fq 'resumexfer/control.sock' "$SCRIPT"
grep -Fq 'mtp:host=' "$SCRIPT"
grep -Fq 'state.json' "$SCRIPT"
grep -Fq 'phone-to-laptop' "$SCRIPT"
grep -Fq 'laptop-to-phone' "$SCRIPT"
grep -Fq 'TEST A - PHONE -> LAPTOP' "$SCRIPT"
grep -Fq 'TEST B - LAPTOP -> PHONE' "$SCRIPT"
grep -Fq 'TEST C - MULTI-FILE' "$SCRIPT"
grep -Fq 'TEST D - WRONG CONTENT' "$SCRIPT"
grep -Fq 'TEST E - 24 HOUR CLEANUP' "$SCRIPT"
grep -Fq 'checksum' "$SCRIPT"
grep -Fq 'generated multi-file test set' "$SCRIPT"
grep -Fq 'controlled test clock' "$SCRIPT"
grep -Fq 'RESUMEXFER_ACCEPTANCE_TIMEOUT:-600' "$SCRIPT"
grep -Fq 'append reopen is unsupported' "$SCRIPT"
grep -Fq 'old interrupted percentage' "$SCRIPT"

OUT=$("$SCRIPT" --self-test)
grep -Fq 'SELF-TEST PASS' <<<"$OUT"
