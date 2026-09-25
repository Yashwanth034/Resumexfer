#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

PORTAL='^(TestManifestShareContinuesEveryRemainingLaptopToPhoneEntry|TestManifestShareHandles207FileUSBToWiFiJob|TestManagedReceiveContinuesEveryRemainingPhoneToLaptopEntry|TestManagedReceiveHandles207FileUSBToWiFiJob|TestSendSnapshotWaitsForDeliveryFinalizationAfterBytesReach100Percent|TestCompletedSendDeliveryStateSurvivesPortalRestart|TestZeroByteSharedFileCompletesOnlyAfterGET|TestZipDeliveryFinalizesAllFilesIncludingZeroByteEntries|TestReceiveMultiFileBatchWaitsForExplicitFinish|TestBrowserControlAPIPausesResumesAndCancelsSession|TestPauseControlDoesNotWaitForActiveUploadPersistenceLock|TestCancelControlDoesNotWaitForActiveUploadLock|TestManagedReceiveContinuesSameManifestFromVerifiedPhysicalPartial|TestManagedReceiveHandsOffFromWifiToUSBWithoutRestarting|TestFastWebRTCDownloadResumesFromStoredOffset)$'
DAEMON='^(TestHandlePortalShareReceiveStatusAndClose|TestPersistedReconnectStateRecoversAfterDaemonRestart|TestAutomaticUploadRecoveryRunsAfterDestinationReconnect|TestManagedUploadRetriesAndCompletesAfterStaleMountReconnect|TestLargeOrdinaryWifiUploadAdoptsCheckpointIntoManagedUSBRecovery|TestOrdinaryWifiUploadAdoptsVerifiedCheckpointIntoManagedUSBRecovery)$'

run_timeout 240 "whole-job handoff + completion contracts"   go test -count=1 ./internal/portal -run "$PORTAL" -v

run_timeout 240 "daemon reconnect + portal status contracts"   go test -count=1 ./internal/daemon -run "$DAEMON" -v

run_timeout 420 "whole-job handoff race detector"   go test -race -count=1 -timeout=390s ./internal/portal -run "$PORTAL"

run_timeout 240 "daemon handoff/status race detector"   go test -race -count=1 -timeout=210s ./internal/daemon -run "$DAEMON"

run_timeout 30 "real GTK Share/Receive/Continue close lifecycle"   xvfb-run -a python3 nemo/live_portal_ui_check.py

run_timeout 120 "Nemo whole-job UI/core contracts"   python3 -m unittest     nemo.test_resumexfer_core     nemo.test_extension_contract     nemo.test_portal_contract

printf '\nWHOLE-JOB HANDOFF BATCH: PASS\n'
