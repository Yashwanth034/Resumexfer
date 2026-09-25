#!/usr/bin/env bash
source "$(dirname "$0")/_common.sh"

run_timeout 120 "shell syntax" bash -c 'find scripts packaging -type f -name "*.sh" -print0 | xargs -0 -n1 bash -n'
run_timeout 120 "Python compile" python3 -m compileall -q nemo
run_timeout 180 "Go vet" go vet ./...
run_timeout 180 "package tests" ./packaging/tests/package_test.sh
run_timeout 180 "lifecycle tests" ./packaging/tests/lifecycle_test.sh
run_timeout 180 "installer tests" ./packaging/tests/installer_test.sh
run_timeout 180 "acceptance script contracts" ./packaging/tests/acceptance_script_test.sh
run_timeout 180 "Transport V2 acceptance script contracts" ./packaging/tests/transport_v2_acceptance_script_test.sh
run_timeout 180 "Transport V2 verify script contracts" ./packaging/tests/transport_v2_verify_script_test.sh

printf '\nPACKAGING/LIFECYCLE BATCH: PASS\n'
