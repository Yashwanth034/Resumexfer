#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

MIXED='^(TestManagedLaptopToPhoneHandles100MixedFiles|TestManagedPhoneToLaptopHandles100MixedFiles|TestManagedPhoneToLaptopHandles403MixedFilesWithoutFalseReconnect|TestManagedPauseResumeAndCancelControls|TestPersistedReconnectStateRecoversAfterDaemonRestart)$'
PORTAL='^(TestReceiveMultiFileBatchWaitsForExplicitFinish|TestStreamingUploadWithPortalStateDoesNotDeadlock|TestReceiveUploadContinuesAcrossPortalRouteAddresses|TestReceiveUploadSerializesSameOffsetAcrossPortalRoutes|TestBrowserControlAPIPausesResumesAndCancelsSession)$'

run_timeout 360 "daemon shuffled stress x12" go test -shuffle=on -count=12 -timeout=330s ./internal/daemon -run "$MIXED"
run_timeout 240 "portal shuffled stress x20" go test -shuffle=on -count=20 -timeout=210s ./internal/portal -run "$PORTAL"
run_timeout 300 "race shuffled stress x4" go test -race -shuffle=on -count=4 -timeout=270s ./internal/portal ./internal/daemon -run 'Test(ReceiveMultiFileBatchWaitsForExplicitFinish|StreamingUploadWithPortalStateDoesNotDeadlock|ManagedPauseResumeAndCancelControls|PersistedReconnectStateRecoversAfterDaemonRestart)$'

printf '\nSTRESS/CHAOS BATCH: PASS\n'
