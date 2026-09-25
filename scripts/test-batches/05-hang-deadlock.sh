#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

PORTAL='^(TestPauseControlDoesNotWaitForActiveUploadPersistenceLock|TestCancelControlDoesNotWaitForActiveUploadLock|TestStreamingUploadWithPortalStateDoesNotDeadlock|TestReceiveUploadSerializesSameOffsetAcrossPortalRoutes|TestManagedReceiveKeepsLeaseWhileSlowChunkIsActive|TestCloseReceiveWaitsForActiveWriterThenCleansPartial)$'
DAEMON='^(TestServerRejectsSecondLiveInstance|TestServerRemovesStaleUnixSocket|TestStaleConcurrentBindCannotOverwriteNewerDestination|TestUploadStartSurvivesNewerDestinationBindInFlight|TestManagedCancelQueuesCleanupWhileAnotherTransportOwnsManifest)$'

run_timeout 180 "portal no-hang regression" go test -race -count=3 -timeout=150s ./internal/portal -run "$PORTAL"
run_timeout 180 "daemon no-hang regression" go test -race -count=3 -timeout=150s ./internal/daemon -run "$DAEMON"
run_timeout 90 "Nemo worker hang/restart behavior" python3 -m unittest nemo.test_resumexfer_worker nemo.test_resumexfer_core.ExternalRequestWorkerTests nemo.test_resumexfer_core.ExternalRequestWorkerRestartTests

printf '\nHANG/DEADLOCK BATCH: PASS\n'
