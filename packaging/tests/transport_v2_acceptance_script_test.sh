#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SCRIPT="$ROOT/scripts/transport-v2-acceptance.sh"

test -x "$SCRIPT"
bash -n "$SCRIPT"

grep -Fq -- '--preflight' "$SCRIPT"
grep -Fq -- '--prepare' "$SCRIPT"
grep -Fq -- '--usb-to-wifi' "$SCRIPT"
grep -Fq -- '--wifi-to-usb' "$SCRIPT"
grep -Fq -- '--self-test' "$SCRIPT"
grep -Fq 'portal_continue_manifest' "$SCRIPT"
grep -Fq 'portal_pause' "$SCRIPT"
grep -Fq 'portal_close' "$SCRIPT"
grep -Fq 'phone-to-laptop' "$SCRIPT"
grep -Fq 'wait_for_new_manifest' "$SCRIPT"
grep -Fq 'wait_portal_bytes' "$SCRIPT"
grep -Fq 'SHA-256 verified' "$SCRIPT"
grep -Fq 'USB → Wi-Fi same-manifest continuation passed' "$SCRIPT"
grep -Fq 'Wi-Fi → USB verified-checkpoint continuation passed' "$SCRIPT"
grep -Fq 'RESUMEXFER_TRANSPORT_TEST_MIB:-256' "$SCRIPT"
grep -Fq 'RESUMEXFER_ACCEPTANCE_TIMEOUT:-600' "$SCRIPT"

OUT=$($SCRIPT --self-test)
grep -Fq 'SELF-TEST PASS' <<<"$OUT"
