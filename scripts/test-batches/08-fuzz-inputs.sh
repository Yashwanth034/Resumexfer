#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

run_timeout 180 "fuzz session path parser" env GOMAXPROCS=2 go test ./internal/portal -run='^$' -parallel=2 -fuzz='^FuzzSplitSessionPath$' -fuzztime=20s
run_timeout 180 "fuzz upload relative paths" env GOMAXPROCS=2 go test ./internal/portal -run='^$' -parallel=2 -fuzz='^FuzzCleanRelativePath$' -fuzztime=20s
run_timeout 180 "fuzz local remote-address parser" env GOMAXPROCS=2 go test ./internal/portal -run='^$' -parallel=2 -fuzz='^FuzzPortalRemoteAllowed$' -fuzztime=20s

printf '\nFUZZ INPUT BATCH: PASS\n'
