#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

USB_DAEMON='^(TestManagedLaptopToPhoneNestedFolderPreservesPathsAndBytes|TestManagedPhoneToLaptopNestedFolderPreservesPathsAndBytes|TestManagedLaptopToPhoneHandles100MixedFiles|TestManagedPhoneToLaptopHandles100MixedFiles|TestManagedPhoneToLaptopHandles403MixedFilesWithoutFalseReconnect)$'
WIFI_PORTAL='^(TestManifestShareContinuesEveryRemainingLaptopToPhoneEntry|TestManifestShareHandles207FileUSBToWiFiJob|TestManagedReceiveContinuesEveryRemainingPhoneToLaptopEntry|TestManagedReceiveHandles207FileUSBToWiFiJob|TestShareDirectoryProvidesDownloadAllZip|TestReceiveMultiFileBatchWaitsForExplicitFinish|TestFastWebRTCDownloadTransfersExactBytes|TestFastWebRTCDownloadResumesFromStoredOffset)$'
HANDOFF_PORTAL='^(TestManagedReceiveContinuesSameManifestFromVerifiedPhysicalPartial|TestManagedReceiveHandsOffFromWifiToUSBWithoutRestarting|TestManagedReceiveUntouchedWifiEntrySurvivesSecondDisconnect|TestManagedReceiveRejectsChangedSourceAndPartialRace|TestAdoptReceivePartialFreezesVerifiedWirelessCheckpointForUSB|TestManagedReceiveUsesBoundedUSBCheckpointProof|TestReceiveUploadContinuesAcrossPortalRouteAddresses|TestSharedFileRangeContinuesAcrossPortalRouteAddresses|TestReceiveUploadSurvivesAbruptPortalRestart)$'
HANDOFF_DAEMON='^(TestPersistedReconnectStateRecoversAfterDaemonRestart|TestAutomaticUploadRecoveryRunsAfterDestinationReconnect|TestManagedUploadRetriesAndCompletesAfterStaleMountReconnect|TestLargeOrdinaryWifiUploadAdoptsCheckpointIntoManagedUSBRecovery|TestOrdinaryWifiUploadAdoptsVerifiedCheckpointIntoManagedUSBRecovery)$'
CONTROL_PORTAL='^(TestSharePauseBlocksDownloadUntilResume|TestReceivePauseBlocksUploadChunkUntilResume|TestBrowserControlAPIPausesResumesAndCancelsSession|TestPauseControlDoesNotWaitForActiveUploadPersistenceLock|TestCancelControlDoesNotWaitForActiveUploadLock|TestStreamingUploadWithPortalStateDoesNotDeadlock|TestCloseReceiveWaitsForActiveWriterThenCleansPartial)$'
CONTROL_RECOVERY='^(TestManagedPhoneToLaptopPauseResumeAcrossMultipleFiles|TestManagedUploadPausePreservesAndResumeCompletes|TestManagedPhoneToLaptopPauseKeepsDisplayedProgressDurable|TestManagedUploadCancelModesAcross100Files|TestCancelManagedDownloadModesCleanSidecarAndRespectCompletedFiles|TestCancelManagedUploadKeepCompletedRemovesOwnedPartialOnly|TestCancelManagedUploadUndoAllRemovesOnlyJobOwnedFiles)$'
COMPLETION_PORTAL='^(TestSendSnapshotWaitsForDeliveryFinalizationAfterBytesReach100Percent|TestCompletedSendDeliveryStateSurvivesPortalRestart|TestZeroByteSharedFileCompletesOnlyAfterGET|TestZeroByteShareMarksDaemonVisibleSessionComplete|TestZipDeliveryFinalizesAllFilesIncludingZeroByteEntries)$'

run_timeout 240 "USB exact multi-file/folder matrix — both directions"   go test -count=1 ./internal/daemon -run "$USB_DAEMON" -v

run_timeout 240 "Wi-Fi exact multi-file/folder matrix — both directions"   go test -count=1 ./internal/portal -run "$WIFI_PORTAL" -v

run_timeout 240 "USB/Wi-Fi disconnect, reconnect and handoff matrix"   go test -count=1 ./internal/portal -run "$HANDOFF_PORTAL" -v

run_timeout 240 "daemon reconnect and Wi-Fi→USB adoption matrix"   go test -count=1 ./internal/daemon -run "$HANDOFF_DAEMON" -v

run_timeout 240 "pause/resume/cancel/no-hang portal controls"   go test -count=1 ./internal/portal -run "$CONTROL_PORTAL" -v

run_timeout 300 "multi-file pause/resume and 100-file cancel cleanup"   go test -count=1 ./internal/recovery -run "$CONTROL_RECOVERY" -v

run_timeout 180 "100%-stuck/zero-byte/restart completion contracts"   go test -count=1 ./internal/portal -run "$COMPLETION_PORTAL" -v

run_timeout 30 "real GTK Share/Receive/Continue completion lifecycle"   xvfb-run -a python3 nemo/live_portal_ui_check.py

# Repeat the high-value matrices shuffled so ordering/races do not hide state bugs.
run_timeout 360 "USB matrix shuffled x3"   go test -shuffle=on -count=3 -timeout=330s ./internal/daemon -run "$USB_DAEMON"

run_timeout 300 "Wi-Fi multi-file/folder matrix shuffled x5"   go test -shuffle=on -count=5 -timeout=270s ./internal/portal -run "$WIFI_PORTAL"

run_timeout 300 "handoff/reconnect matrix shuffled x5"   go test -shuffle=on -count=5 -timeout=270s ./internal/portal -run "$HANDOFF_PORTAL"

run_timeout 300 "pause/cancel/no-hang portal matrix shuffled x5"   go test -shuffle=on -count=5 -timeout=270s ./internal/portal -run "$CONTROL_PORTAL"

run_timeout 240 "completion/stuck-window matrix shuffled x5"   go test -shuffle=on -count=5 -timeout=210s ./internal/portal -run "$COMPLETION_PORTAL"

run_timeout 300 "recovery pause/cancel matrix shuffled x5"   go test -shuffle=on -count=5 -timeout=270s ./internal/recovery -run "$CONTROL_RECOVERY"

# Race detector on the same core user flows.
USB_RACE='^(TestManagedLaptopToPhoneNestedFolderPreservesPathsAndBytes|TestManagedPhoneToLaptopNestedFolderPreservesPathsAndBytes|TestManagedLaptopToPhoneHandles100MixedFiles|TestManagedPhoneToLaptopHandles100MixedFiles)$'
PORTAL_RACE='^(TestManifestShareHandles207FileUSBToWiFiJob|TestManagedReceiveHandles207FileUSBToWiFiJob|TestManagedReceiveContinuesSameManifestFromVerifiedPhysicalPartial|TestManagedReceiveHandsOffFromWifiToUSBWithoutRestarting|TestBrowserControlAPIPausesResumesAndCancelsSession|TestPauseControlDoesNotWaitForActiveUploadPersistenceLock|TestCancelControlDoesNotWaitForActiveUploadLock|TestStreamingUploadWithPortalStateDoesNotDeadlock|TestSendSnapshotWaitsForDeliveryFinalizationAfterBytesReach100Percent)$'
RECOVERY_RACE='^(TestManagedPhoneToLaptopPauseResumeAcrossMultipleFiles|TestManagedUploadPausePreservesAndResumeCompletes|TestManagedUploadCancelModesAcross100Files)$'

run_timeout 360 "USB exact matrix race detector"   go test -race -count=1 -timeout=330s ./internal/daemon -run "$USB_RACE"

run_timeout 420 "Wi-Fi/handoff/control race detector"   go test -race -count=1 -timeout=390s ./internal/portal -run "$PORTAL_RACE"

run_timeout 360 "pause/cancel recovery race detector"   go test -race -count=1 -timeout=330s ./internal/recovery -run "$RECOVERY_RACE"

printf '\nSTRONG TRANSFER MATRIX: PASS\n'
