#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

SECURITY='^(TestDefaultPortalAddressAndTokenStayCompact|TestCapabilityTokensAreUniqueAndURLSafe|TestPortalRemotePolicyAllowsOnlyLocalNetworkClients|TestPortalSecurityHeadersAndRejectsRebindingHost|TestCompactPortalURLAndLegacyPathParsing|TestReceivePageLearnsUpdatedRoutesAndAllowsCrossOriginAPI|TestCloseAndExpiryInvalidateCapabilityLink|TestShareRejectsSameMetadataContentReplacement|TestShareZipRejectsSameMetadataContentReplacement|TestReceiveUploadRejectsChangedSourceWithSameNameAndSize|TestReceiveRejectsTraversalAndExistingDestination)$'

run_timeout 120 "portal security" go test -count=1 ./internal/portal -run "$SECURITY" -v
run_timeout 180 "portal security race" go test -race -count=1 ./internal/portal -run "$SECURITY"
run "portal vet" go vet ./internal/portal

printf '\nSECURITY BATCH: PASS\n'
