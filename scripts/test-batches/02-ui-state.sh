#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

PORTAL_UI='^(TestBrowserControlAPIPausesResumesAndCancelsSession|TestPauseControlDoesNotWaitForActiveUploadPersistenceLock|TestCancelControlDoesNotWaitForActiveUploadLock|TestReceiveMultiFileBatchWaitsForExplicitFinish|TestStreamingUploadWithPortalStateDoesNotDeadlock|TestSharePauseBlocksDownloadUntilResume|TestReceivePauseBlocksUploadChunkUntilResume)$'
DAEMON_UI='^(TestManagedPauseResumeAndCancelControls|TestManagedCancelControlPersistsUndoAllModeAndRejectsUnknownMode)$'

run_timeout 120 "desktop/browser contracts" python3 -m unittest nemo.test_extension_contract nemo.test_portal_contract nemo.test_resumexfer_core nemo.test_resumexfer_worker
run_timeout 120 "portal UI state" go test -count=1 ./internal/portal -run "$PORTAL_UI" -v
run_timeout 120 "daemon UI controls" go test -count=1 ./internal/daemon -run "$DAEMON_UI" -v

printf '\nUI/STATE BATCH: PASS\n'
