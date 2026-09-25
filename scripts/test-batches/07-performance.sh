#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

PORTAL='^(TestTrackingResponseWriterKeepsFileBackedReaderFrom|TestTrackingReadSeekerBatchesProgressCallbacks|TestSendPageUsesPipelinedFastPathAndLargeFileDirectFallback|TestReceivePageUsesContinuousStreamingUpload|TestManagedReceiveUsesBoundedUSBCheckpointProof|TestFastWebRTCDownloadTransfersExactBytes|TestFastWebRTCDownloadResumesFromStoredOffset)$'
DAEMON='^(TestManagedLaptopToPhoneHandles100MixedFiles|TestManagedPhoneToLaptopHandles100MixedFiles|TestManagedPhoneToLaptopHandles403MixedFilesWithoutFalseReconnect)$'
RECOVERY='^(TestManagedUploadStreamUsesLargeWritesWithoutPeriodicDeviceSync|TestManagedUploadPersistsEarlyFreshProgressBeforeSecondWrite|TestManagedPhoneToLaptopReportsProgressBeforeLargeCheckpoint|TestManagedSmallPhoneToLaptopUsesFastSidecarPath)$'

run_timeout 180 "portal fast-path performance contracts x5" go test -count=5 -timeout=150s ./internal/portal -run "$PORTAL"
run_timeout 300 "mixed-file performance deadlines x3" go test -count=3 -timeout=270s ./internal/daemon -run "$DAEMON"
run_timeout 180 "recovery I/O performance contracts x5" go test -count=5 -timeout=150s ./internal/recovery -run "$RECOVERY"

printf '\nPERFORMANCE BATCH: PASS\n'
