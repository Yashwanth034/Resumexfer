#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SCRIPT="$ROOT/scripts/verify-transport-v2.sh"

test -x "$SCRIPT"
bash -n "$SCRIPT"
grep -Fq 'GOTOOLCHAIN=${GOTOOLCHAIN:-auto}' "$SCRIPT"
grep -Fq 'TestManagedReceiveContinuesSameManifestFromVerifiedPhysicalPartial' "$SCRIPT"
grep -Fq 'TestOrdinaryWifiUploadAdoptsVerifiedCheckpointIntoManagedUSBRecovery' "$SCRIPT"
grep -Fq 'TestReceiveUploadSerializesSameOffsetAcrossPortalRoutes' "$SCRIPT"
grep -Fq 'TestNetworkLinksFromSysfsReportsConnectedUnconfiguredLinks' "$SCRIPT"
grep -Fq 'TestEnableIPv4LinkLocalUsesTemporaryDeviceModification' "$SCRIPT"
grep -Fq 'go test -race -count=1 ./internal/portal' "$SCRIPT"
grep -Fq "python3 -m unittest discover -s nemo -p 'test_*.py'" "$SCRIPT"
grep -Fq './packaging/tests/package_test.sh' "$SCRIPT"
grep -Fq './packaging/tests/transport_v2_acceptance_script_test.sh' "$SCRIPT"
grep -Fq 'git diff --check' "$SCRIPT"
