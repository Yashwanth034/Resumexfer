#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

printf '\nTransport transition contract\n'
printf '%-34s %s\n' 'Laptop → phone  USB → USB' 'true resume'
printf '%-34s %s\n' 'Phone → laptop  USB → USB' 'true resume'
printf '%-34s %s\n' 'Phone → laptop  USB → Wi-Fi' 'true resume'
printf '%-34s %s\n' 'Phone → laptop  Wi-Fi → USB' 'true resume'
printf '%-34s %s\n' 'Laptop → phone  Wi-Fi → Wi-Fi' 'true resume (browser-stored offset)'
printf '%-34s %s\n' 'Laptop → phone  USB → Wi-Fi' 'restart-only on zero-install HTTP browser'
printf '%-34s %s\n' 'Laptop → phone  Wi-Fi → USB' 'no shared browser/MTP partial ownership'

run_timeout 120 "UI truthfully handles laptop→phone USB→Wi-Fi whole-job fallback"   python3 -m unittest     nemo.test_resumexfer_core.ManagedTransferTests.test_laptop_to_phone_usb_to_wifi_is_restart_only_not_true_resume     nemo.test_extension_contract.PortalMenuContractTests.test_managed_progress_window_hands_off_the_whole_remaining_job

run_timeout 180 "Laptop→phone USB reconnect resumes verified MTP partial"   go test -count=1 ./internal/recovery -run '^(TestLaptopToPhoneRecoveryResumesExistingPartialByVerifiedAppend|TestLaptopToPhoneRecoveryContinuesAllFilesByVerifiedAppend)$' -v

run_timeout 180 "Phone→laptop USB reconnect resumes verified partial"   go test -count=1 ./internal/recovery -run '^(TestPhoneToLaptopRecoveryContinuesWholeManifest|TestManagedDownloadRecoveryPersistsProgressAndCompletion)$' -v

run_timeout 180 "Phone→laptop USB→Wi-Fi continues same manifest/offset"   go test -count=1 ./internal/portal -run '^(TestManagedReceiveContinuesSameManifestFromVerifiedPhysicalPartial|TestManagedReceiveUsesBoundedUSBCheckpointProof)$' -v

run_timeout 180 "Phone→laptop Wi-Fi→USB adopts verified checkpoint"   go test -count=1 ./internal/daemon -run '^(TestLargeOrdinaryWifiUploadAdoptsCheckpointIntoManagedUSBRecovery|TestOrdinaryWifiUploadAdoptsVerifiedCheckpointIntoManagedUSBRecovery)$' -v

run_timeout 180 "Laptop→phone Wi-Fi reconnect requests stored offset"   go test -count=1 ./internal/portal -run '^(TestFastWebRTCDownloadResumesFromStoredOffset|TestSharedFileRangeContinuesAcrossPortalRouteAddresses)$' -v

printf '\nTRANSPORT TRANSITION BATCH: PASS\n'
