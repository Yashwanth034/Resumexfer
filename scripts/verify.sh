#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

go test ./...
go test -race ./...
go vet ./...
python3 -m unittest discover -s nemo -p 'test_*.py'
./packaging/tests/package_test.sh
./packaging/tests/lifecycle_test.sh
./packaging/tests/installer_test.sh
./packaging/tests/acceptance_script_test.sh
./packaging/tests/transport_v2_acceptance_script_test.sh
./packaging/tests/transport_v2_verify_script_test.sh

if [ "${RESUMEXFER_SKIP_STRESS:-0}" != 1 ]; then
  for _ in $(seq 1 10); do
    go test -count=1 ./...
  done
fi
