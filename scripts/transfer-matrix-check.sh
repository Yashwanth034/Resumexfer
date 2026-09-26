#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

if [[ -x "$ROOT/.rt_hw/go1.23.2/bin/go" ]]; then
  export PATH="$ROOT/.rt_hw/go1.23.2/bin:$PATH"
fi
export GOTOOLCHAIN=${GOTOOLCHAIN:-auto}
export GOFLAGS=${GOFLAGS:--buildvcs=false}

PORTAL_MATRIX='^(TestReceiveMultiFileBatchWaitsForExplicitFinish|TestStreamingUploadWithPortalStateDoesNotDeadlock|TestManagedReceiveUsesBoundedUSBCheckpointProof|TestBrowserControlAPIPausesResumesAndCancelsSession|TestPauseControlDoesNotWaitForActiveUploadPersistenceLock|TestCancelControlDoesNotWaitForActiveUploadLock|TestSharePauseBlocksDownloadUntilResume|TestReceivePauseBlocksUploadChunkUntilResume|TestReceiveUploadResumesFromExistingPartialAndPromotesSafely|TestReceiveUploadContinuesAcrossPortalRouteAddresses|TestReceiveUploadSerializesSameOffsetAcrossPortalRoutes|TestSharedFileRangeContinuesAcrossPortalRouteAddresses|TestManagedReceiveStartsAfterInterruptedLargeRecovery|TestManagedReceiveContinuesSameManifestFromVerifiedPhysicalPartial|TestManagedReceiveHandsOffFromWifiToUSBWithoutRestarting|TestAdoptReceivePartialFreezesVerifiedWirelessCheckpointForUSB|TestFastWebRTCDownloadResumesFromStoredOffset|TestReceiveUploadSurvivesAbruptPortalRestart|TestReceiveUploadWithStoreRestartsFromEngineCheckpoint|TestReceiveStreamingUploadCompletesExactBytesAndCleansCheckpoint)$'
DAEMON_MATRIX='^(TestPersistedReconnectStateRecoversAfterDaemonRestart|TestAutomaticUploadRecoveryRunsAfterDestinationReconnect|TestManagedUploadRetriesAndCompletesAfterStaleMountReconnect|TestLargeOrdinaryWifiUploadAdoptsCheckpointIntoManagedUSBRecovery|TestOrdinaryWifiUploadAdoptsVerifiedCheckpointIntoManagedUSBRecovery|TestManagedLaptopToPhoneHandles100MixedFiles|TestManagedPhoneToLaptopHandles100MixedFiles|TestManagedPhoneToLaptopHandles403MixedFilesWithoutFalseReconnect)$'
RECOVERY_MATRIX='^(TestLaptopToPhoneRecoveryContinuesUnstartedManifestEntries|TestManagedPhoneToLaptopReportsProgressBeforeLargeCheckpoint|TestManagedPhoneToLaptopPauseKeepsDisplayedProgressDurable|TestManagedUploadStreamUsesLargeWritesWithoutPeriodicDeviceSync|TestManagedUploadPersistsEarlyFreshProgressBeforeSecondWrite|TestRecoveryRespectsCompetingTransportLeaseAndReleasesAfterSuccess)$'

run_step() {
  local label=$1
  shift
  printf '\n== %s ==\n' "$label"
  "$@"
}

run_step "Portal transfer matrix" go test -count=1 ./internal/portal -run "$PORTAL_MATRIX" -v
run_step "Daemon reconnect/batch matrix" go test -count=1 ./internal/daemon -run "$DAEMON_MATRIX" -v
run_step "Recovery cross-transport matrix" go test -count=1 ./internal/recovery -run "$RECOVERY_MATRIX" -v

run_step "Portal transfer matrix (race)" go test -race -count=1 ./internal/portal -run "$PORTAL_MATRIX"
run_step "Daemon reconnect/batch matrix (race)" go test -race -count=1 ./internal/daemon -run "$DAEMON_MATRIX"
run_step "Recovery cross-transport matrix (race)" go test -race -count=1 ./internal/recovery -run "$RECOVERY_MATRIX"

run_step "Nemo/browser contracts" python3 -m unittest discover -s nemo -p 'test_*.py'

printf '\nTRANSFER MATRIX: PASS\n'
