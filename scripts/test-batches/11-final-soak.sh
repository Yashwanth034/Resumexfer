#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

printf '\nFINAL SOAK: repeated deterministic + race + fuzz coverage\n'

# Keep the complete Nemo suite in the soak. The exact test count can grow as
# regressions are added, so do not hard-code a stale execution total here.
for i in $(seq 1 25); do
  printf 'Nemo full-suite soak %02d/25\n' "$i"
  timeout --foreground 60 python3 -m unittest discover -s nemo -p 'test_*.py' >/dev/null
done
printf 'PASS: Nemo full-suite soak (25 complete suite runs)\n'

PORTAL='^(TestReceiveMultiFileBatchWaitsForExplicitFinish|TestStreamingUploadWithPortalStateDoesNotDeadlock|TestBrowserControlAPIPausesResumesAndCancelsSession|TestPauseControlDoesNotWaitForActiveUploadPersistenceLock|TestCancelControlDoesNotWaitForActiveUploadLock|TestReceiveUploadContinuesAcrossPortalRouteAddresses|TestReceiveUploadSerializesSameOffsetAcrossPortalRoutes|TestManagedReceiveContinuesSameManifestFromVerifiedPhysicalPartial|TestManagedReceiveHandsOffFromWifiToUSBWithoutRestarting|TestManagedReceiveUsesBoundedUSBCheckpointProof|TestFastWebRTCDownloadResumesFromStoredOffset)$'
DAEMON='^(TestPersistedReconnectStateRecoversAfterDaemonRestart|TestAutomaticUploadRecoveryRunsAfterDestinationReconnect|TestManagedUploadRetriesAndCompletesAfterStaleMountReconnect|TestManagedPauseResumeAndCancelControls|TestManagedLaptopToPhoneHandles100MixedFiles|TestManagedPhoneToLaptopHandles100MixedFiles|TestLargeOrdinaryWifiUploadAdoptsCheckpointIntoManagedUSBRecovery|TestOrdinaryWifiUploadAdoptsVerifiedCheckpointIntoManagedUSBRecovery)$'
RECOVERY='^(TestLaptopToPhoneRecoveryResumesExistingPartialByVerifiedAppend|TestLaptopToPhoneRecoveryContinuesAllFilesByVerifiedAppend|TestPhoneToLaptopRecoveryContinuesWholeManifest|TestManagedUploadRecoveryPersistsProgressAndCompletion|TestManagedDownloadRecoveryPersistsProgressAndCompletion|TestManagedPhoneToLaptopPauseKeepsDisplayedProgressDurable)$'

# 11 portal cases x 50 = 550.
run_timeout 300 "portal critical soak x50 (550 executions)"   go test -shuffle=on -count=50 -timeout=270s ./internal/portal -run "$PORTAL"

# 8 daemon cases x 25 = 200; includes 100-file directions.
run_timeout 480 "daemon critical soak x25 (200 executions)"   go test -shuffle=on -count=25 -timeout=450s ./internal/daemon -run "$DAEMON"

# 6 recovery cases x 50 = 300.
run_timeout 300 "recovery critical soak x50 (300 executions)"   go test -shuffle=on -count=50 -timeout=270s ./internal/recovery -run "$RECOVERY"

# Race detector repetitions: 11*8 + 8*6 + 6*8 = 184 race executions.
run_timeout 360 "portal critical race x8 (88 executions)"   go test -race -shuffle=on -count=8 -timeout=330s ./internal/portal -run "$PORTAL"
run_timeout 420 "daemon critical race x6 (48 executions)"   go test -race -shuffle=on -count=6 -timeout=390s ./internal/daemon -run "$DAEMON"
run_timeout 300 "recovery critical race x8 (48 executions)"   go test -race -shuffle=on -count=8 -timeout=270s ./internal/recovery -run "$RECOVERY"

# Fuzzing is input-count based rather than test-count based. These three targets
# should exercise hundreds of thousands to millions of randomized malformed inputs.
run_timeout 180 "fuzz session path parser 30s"   env GOMAXPROCS=2 go test ./internal/portal -run='^$' -parallel=2 -fuzz='^FuzzSplitSessionPath$' -fuzztime=30s
run_timeout 180 "fuzz upload relative paths 30s"   env GOMAXPROCS=2 go test ./internal/portal -run='^$' -parallel=2 -fuzz='^FuzzCleanRelativePath$' -fuzztime=30s
run_timeout 180 "fuzz remote-address parser 30s"   env GOMAXPROCS=2 go test ./internal/portal -run='^$' -parallel=2 -fuzz='^FuzzPortalRemoteAllowed$' -fuzztime=30s

printf '\nFINAL SOAK: PASS (repeated deterministic/race suites + fuzz inputs)\n'
