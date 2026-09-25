#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

run_timeout 20 "live GTK completion auto-close" \
  xvfb-run -a python3 nemo/live_portal_ui_check.py

run_timeout 120 "daemon → GTK completion/status contract" \
  go test -count=1 ./internal/daemon -run '^TestHandlePortalShareReceiveStatusAndClose$' -v

run_timeout 120 "send delivery completion contracts" \
  go test -count=1 ./internal/portal -run '^(TestSendSnapshotWaitsForDeliveryFinalizationAfterBytesReach100Percent|TestZeroByteSharedFileCompletesOnlyAfterGET|TestZipDeliveryFinalizesAllFilesIncludingZeroByteEntries)$' -v

run_timeout 120 "portal completion/UI contracts" \
  python3 -m unittest nemo.test_portal_contract nemo.test_extension_contract

printf '\nLIVE UI BATCH: PASS\n'
