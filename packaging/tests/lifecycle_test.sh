#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
VERSION=$(tr -d '[:space:]' < "$ROOT/VERSION")
TMP=$(mktemp -d)
SOCKET_PID=""
cleanup() {
  if [ -n "$SOCKET_PID" ]; then kill "$SOCKET_PID" 2>/dev/null || true; fi
  rm -rf "$TMP"
}
trap cleanup EXIT

VERSION="$VERSION" DIST_DIR="$TMP/dist" "$ROOT/scripts/build-deb.sh" >/dev/null
DEB="$TMP/dist/resumexfer_${VERSION}_$(dpkg --print-architecture).deb"
mkdir -p "$TMP/control" "$TMP/fakebin" "$TMP/run/user/1000"
dpkg-deb -e "$DEB" "$TMP/control"
LOG="$TMP/actions.log"

cat > "$TMP/fakebin/systemctl" <<EOF2
#!/bin/sh
printf 'systemctl %s\n' "\$*" >> "$LOG"
exit 0
EOF2
cat > "$TMP/fakebin/runuser" <<EOF2
#!/bin/sh
printf 'runuser %s\n' "\$*" >> "$LOG"
exit 0
EOF2
cat > "$TMP/fakebin/pgrep" <<EOF2
#!/bin/sh
printf 'pgrep %s\n' "\$*" >> "$LOG"
exit 0
EOF2
cat > "$TMP/fakebin/pkill" <<EOF2
#!/bin/sh
printf 'pkill %s\n' "\$*" >> "$LOG"
exit 0
EOF2
cat > "$TMP/fakebin/getent" <<'EOF2'
#!/bin/sh
if [ "$1" = passwd ] && [ "$2" = 1000 ]; then
  echo 'testuser:x:1000:1000:Test User:/home/test:/bin/sh'
  exit 0
fi
exit 2
EOF2
chmod +x "$TMP/fakebin/systemctl" "$TMP/fakebin/runuser" "$TMP/fakebin/pgrep" "$TMP/fakebin/pkill" "$TMP/fakebin/getent"

python3 - "$TMP/run/user/1000/bus" <<'PY' &
import socket, sys, time
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.bind(sys.argv[1])
s.listen(1)
time.sleep(30)
PY
SOCKET_PID=$!
for _ in $(seq 1 100); do [ -S "$TMP/run/user/1000/bus" ] && break; sleep 0.01; done
test -S "$TMP/run/user/1000/bus"

export PATH="$TMP/fakebin:$PATH"
export RESUMEXFER_RUNTIME_BASE="$TMP/run/user"
"$TMP/control/postinst" configure
"$TMP/control/prerm" remove
"$TMP/control/postrm" remove

grep -Fq 'systemctl --global enable resumexfer.service' "$LOG"
grep -Fq 'systemctl --global disable resumexfer.service' "$LOG"
grep -Fq 'systemctl --user enable resumexfer.service' "$LOG"
grep -Fq 'systemctl --user restart resumexfer.service' "$LOG"
grep -Fq 'systemctl --user disable --now resumexfer.service' "$LOG"
grep -Fq 'nemo -q' "$LOG"
grep -Fq 'pgrep -u 1000 -x nemo' "$LOG"
grep -Fq 'pkill -TERM -u 1000 -x nemo' "$LOG"
