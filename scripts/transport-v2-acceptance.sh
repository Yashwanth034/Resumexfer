#!/usr/bin/env bash
set -euo pipefail

RUNTIME_DIR=${RESUMEXFER_XDG_RUNTIME_DIR:-${XDG_RUNTIME_DIR:-}}
STATE_HOME=${RESUMEXFER_XDG_STATE_HOME:-${XDG_STATE_HOME:-${HOME:-}/.local/state}}
SOCKET_PATH=${RESUMEXFER_SOCKET_PATH:-${RUNTIME_DIR:+$RUNTIME_DIR/resumexfer/control.sock}}
STATE_FILE=${RESUMEXFER_STATE_FILE:-$STATE_HOME/resumexfer/state.json}
TIMEOUT=${RESUMEXFER_ACCEPTANCE_TIMEOUT:-600}
ACCEPT_ROOT=${RESUMEXFER_TRANSPORT_ACCEPT_ROOT:-${TMPDIR:-/tmp}/resumexfer-transport-v2-${UID:-$(id -u)}}
TEST_MIB=${RESUMEXFER_TRANSPORT_TEST_MIB:-256}
POLL_SECONDS=${RESUMEXFER_TRANSPORT_POLL_SECONDS:-0.25}

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'PASS: %s\n' "$*"; }
info() { printf '%s\n' "$*"; }

control_request() {
  local payload=$1
  [ -n "$SOCKET_PATH" ] || fail 'XDG_RUNTIME_DIR is not set'
  python3 - "$SOCKET_PATH" "$payload" <<'PY'
import json, socket, sys
path, raw = sys.argv[1:]
request = json.loads(raw)
payload = (json.dumps(request, separators=(",", ":")) + "\n").encode()
with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
    conn.settimeout(5)
    conn.connect(path)
    conn.sendall(payload)
    response = conn.makefile("rb").readline()
if not response:
    raise SystemExit("daemon returned no response")
decoded = json.loads(response)
if not decoded.get("ok"):
    raise SystemExit(decoded.get("error") or "daemon rejected request")
print(json.dumps(decoded.get("data")))
PY
}

preflight() {
  [ -n "$RUNTIME_DIR" ] || fail 'XDG_RUNTIME_DIR is not set'
  [ -S "$SOCKET_PATH" ] || fail "control socket missing: $SOCKET_PATH"
  systemctl --user is-active resumexfer.service >/dev/null 2>&1 || fail 'resumexfer.service is not active'
  [ -f "$STATE_FILE" ] || fail "state file missing: $STATE_FILE"
  mkdir -p "$ACCEPT_ROOT"
  pass 'Resumexfer daemon, control socket, and state file are ready'
}

prepare_source() {
  mkdir -p "$ACCEPT_ROOT"
  local path="$ACCEPT_ROOT/resumexfer-transport-v2-${TEST_MIB}MiB.bin"
  if [ ! -f "$path" ] || [ "$(stat -c %s "$path")" -ne $((TEST_MIB * 1024 * 1024)) ]; then
    python3 - "$path" "$TEST_MIB" <<'PY'
import os, sys
path, mib = sys.argv[1], int(sys.argv[2])
size = mib * 1024 * 1024
block = bytes((i * 37 + 11) % 251 for i in range(1024 * 1024))
with open(path, "wb") as f:
    remaining = size
    while remaining:
        chunk = block[:min(len(block), remaining)]
        f.write(chunk)
        remaining -= len(chunk)
os.chmod(path, 0o600)
PY
  fi
  printf '%s\n' "$path"
}

sha256_file() { sha256sum "$1" | awk '{print $1}'; }

manifest_ids() {
  local direction=$1
  python3 - "$STATE_FILE" "$direction" <<'PY'
import json, os, sys
path, direction = sys.argv[1:]
if not os.path.exists(path):
    raise SystemExit(0)
with open(path, encoding="utf-8") as f:
    data = json.load(f)
for mid, manifest in data.get("manifests", {}).items():
    if manifest.get("direction") == direction:
        print(mid)
PY
}

wait_for_new_manifest() {
  local direction=$1 before=$2 start now current id
  start=$(date +%s)
  while :; do
    current=$(manifest_ids "$direction" || true)
    while IFS= read -r id; do
      [ -n "$id" ] || continue
      if ! grep -Fxq "$id" <<<"$before"; then printf '%s\n' "$id"; return 0; fi
    done <<<"$current"
    now=$(date +%s)
    [ $((now-start)) -lt "$TIMEOUT" ] || return 1
    sleep "$POLL_SECONDS"
  done
}

manifest_field() {
  local id=$1 field=$2
  python3 - "$STATE_FILE" "$id" "$field" <<'PY'
import json, sys
path, mid, field = sys.argv[1:]
with open(path, encoding="utf-8") as f:
    data = json.load(f)
m = data.get("manifests", {}).get(mid) or {}
entries = m.get("entries") or []
if field == "pending":
    print(sum(1 for e in entries if not e.get("complete", False)))
elif field == "awaiting":
    print("1" if m.get("awaiting_reconnect") else "0")
elif field == "entry_id":
    print(entries[0].get("id", "") if len(entries) == 1 else "")
elif field == "destination":
    print(entries[0].get("destination", "") if len(entries) == 1 else "")
else:
    raise SystemExit(2)
PY
}

wait_manifest() {
  local id=$1 field=$2 expected=$3 start now value
  start=$(date +%s)
  while :; do
    value=$(manifest_field "$id" "$field" 2>/dev/null || true)
    [ "$value" = "$expected" ] && return 0
    now=$(date +%s)
    [ $((now-start)) -lt "$TIMEOUT" ] || return 1
    sleep "$POLL_SECONDS"
  done
}

wait_partial() {
  local id=$1 start now result
  start=$(date +%s)
  while :; do
    if result=$(python3 - "$STATE_FILE" "$id" <<'PY'
import json, os, sys
with open(sys.argv[1], encoding="utf-8") as f:
    data = json.load(f)
m = data.get("manifests", {}).get(sys.argv[2]) or {}
for e in m.get("entries") or []:
    path = e.get("destination") or ""
    total = int(e.get("size") or 0)
    if path and total > 0 and os.path.isfile(path):
        size = os.path.getsize(path)
        if 0 < size < total:
            print(size, total)
            raise SystemExit(0)
raise SystemExit(1)
PY
); then
      printf '%s\n' "$result"
      return 0
    fi
    now=$(date +%s)
    [ $((now-start)) -lt "$TIMEOUT" ] || return 1
    sleep "$POLL_SECONDS"
  done
}

session_value() {
  local json=$1 field=$2
  python3 - "$json" "$field" <<'PY'
import ipaddress, json, sys, urllib.parse
info = json.loads(sys.argv[1]) or {}
field = sys.argv[2]
if field == "id":
    print(info.get("id", ""))
elif field == "bytes_done":
    print(int(info.get("bytes_done") or 0))
elif field == "url":
    for route in info.get("routes") or []:
        url = str(route.get("url") or "")
        if route.get("kind") == "loopback":
            continue
        host = urllib.parse.urlparse(url).hostname
        try:
            if host and ipaddress.ip_address(host).is_loopback:
                continue
        except ValueError:
            pass
        if url:
            print(url); raise SystemExit
    for url in info.get("urls") or []:
        host = urllib.parse.urlparse(url).hostname
        try:
            if host and ipaddress.ip_address(host).is_loopback:
                continue
        except ValueError:
            pass
        if url:
            print(url); raise SystemExit
PY
}

portal_status() {
  local id=$1
  control_request "{\"action\":\"portal_status\",\"session_id\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$id")}"
}

wait_portal_bytes() {
  local id=$1 minimum=$2 start now info bytes
  start=$(date +%s)
  while :; do
    info=$(portal_status "$id") || return 1
    bytes=$(session_value "$info" bytes_done)
    if [ "$bytes" -ge "$minimum" ]; then printf '%s\n' "$bytes"; return 0; fi
    now=$(date +%s)
    [ $((now-start)) -lt "$TIMEOUT" ] || return 1
    sleep "$POLL_SECONDS"
  done
}

verify_expected_file() {
  local expected=$1 destination=$2
  [ -f "$destination" ] || fail "completed destination missing: $destination"
  local a b
  a=$(sha256_file "$expected")
  b=$(sha256_file "$destination")
  [ "$a" = "$b" ] || fail "SHA-256 mismatch: $destination"
  pass "SHA-256 verified: $b"
}

require_source() {
  local source=${1:-}
  [ -n "$source" ] || source=$(prepare_source)
  [ -f "$source" ] || fail "source file missing: $source"
  printf '%s\n' "$source"
}

run_usb_to_wifi() {
  local source before manifest entry partial session sid url destination
  source=$(require_source "${1:-}")
  preflight
  mkdir -p "$ACCEPT_ROOT/usb-to-wifi"
  rm -f "$ACCEPT_ROOT/usb-to-wifi/$(basename "$source")"

  info "Known test source: $source"
  info "SHA-256: $(sha256_file "$source")"
  info 'First make sure this exact file is already on the phone.'
  read -r -p 'Press Enter when the phone copy is ready and USB File Transfer is connected: ' _

  before=$(manifest_ids phone-to-laptop || true)
  info "In Nemo, copy the phone file $(basename "$source") into: $ACCEPT_ROOT/usb-to-wifi"
  manifest=$(wait_for_new_manifest phone-to-laptop "$before") || fail 'managed USB manifest was not observed'
  read -r partial _ < <(wait_partial "$manifest") || fail 'transfer completed before an interruptible partial was observed; use a larger test file'
  pass "USB partial observed at $partial bytes"
  info 'UNPLUG the USB cable now.'
  wait_manifest "$manifest" awaiting 1 || fail 'manifest did not enter reconnect-pending state'

  entry=$(manifest_field "$manifest" entry_id)
  [ -n "$entry" ] || fail 'expected exactly one manifest entry'
  session=$(control_request "{\"action\":\"portal_continue_manifest\",\"manifest_id\":\"$manifest\",\"entry_id\":\"$entry\"}")
  sid=$(session_value "$session" id)
  url=$(session_value "$session" url)
  [ -n "$url" ] || fail 'no cross-device network URL is available'
  info "Open this on the phone: $url"
  info "Select the SAME file: $(basename "$source")"

  wait_manifest "$manifest" pending 0 || fail 'USB→Wi-Fi continuation did not complete'
  destination=$(manifest_field "$manifest" destination)
  verify_expected_file "$source" "$destination"
  control_request "{\"action\":\"portal_close\",\"session_id\":\"$sid\"}" >/dev/null 2>&1 || true
  pass 'USB → Wi-Fi same-manifest continuation passed'
}

run_wifi_to_usb() {
  local source before session sid url bytes manifest destination root filename
  source=$(require_source "${1:-}")
  preflight
  root="$ACCEPT_ROOT/wifi-to-usb"
  filename=$(basename "$source")
  mkdir -p "$root"
  rm -f "$root/$filename"

  before=$(manifest_ids phone-to-laptop || true)
  session=$(control_request "$(python3 - "$root" <<'PY'
import json, sys
print(json.dumps({"action":"portal_receive","destination":sys.argv[1]}, separators=(",", ":")))
PY
)")
  sid=$(session_value "$session" id)
  url=$(session_value "$session" url)
  [ -n "$url" ] || fail 'no cross-device network URL is available'
  info "Open this on the phone: $url"
  info "Upload the known test file: $filename"

  bytes=$(wait_portal_bytes "$sid" $((8 * 1024 * 1024))) || fail 'wireless upload did not reach an adoptable partial before timeout'
  control_request "{\"action\":\"portal_pause\",\"session_id\":\"$sid\"}" >/dev/null
  pass "Wi-Fi upload paused after $bytes bytes"
  info 'Now connect USB, unlock the phone, and select File Transfer.'
  info "In Nemo, copy the SAME phone file into: $root"

  manifest=$(wait_for_new_manifest phone-to-laptop "$before") || fail 'managed USB manifest was not observed'
  wait_manifest "$manifest" pending 0 || fail 'Wi-Fi→USB continuation did not complete'
  destination=$(manifest_field "$manifest" destination)
  verify_expected_file "$source" "$destination"
  control_request "{\"action\":\"portal_close\",\"session_id\":\"$sid\"}" >/dev/null 2>&1 || true
  pass 'Wi-Fi → USB verified-checkpoint continuation passed'
}

self_test() {
  local tmp old_state expected destination
  tmp=$(mktemp -d)
  trap "rm -rf -- '$tmp'" EXIT
  expected="$tmp/source.bin"
  destination="$tmp/destination.bin"
  printf 'transport-v2-self-test\n' > "$expected"
  cp "$expected" "$destination"
  old_state=$STATE_FILE
  STATE_FILE="$tmp/state.json"
  cat > "$STATE_FILE" <<JSON
{"manifests":{"m":{"direction":"phone-to-laptop","awaiting_reconnect":false,"entries":[{"id":"e","destination":"$destination","size":$(stat -c %s "$destination"),"complete":true}]}}}
JSON
  [ "$(manifest_field m pending)" = 0 ] || fail 'self-test pending parser failed'
  [ "$(manifest_field m entry_id)" = e ] || fail 'self-test entry parser failed'
  verify_expected_file "$expected" "$destination" >/dev/null
  STATE_FILE=$old_state
  printf 'SELF-TEST PASS\n'
}

case "${1:-}" in
  --preflight) preflight ;;
  --prepare) prepare_source ;;
  --usb-to-wifi) run_usb_to_wifi "${2:-}" ;;
  --wifi-to-usb) run_wifi_to_usb "${2:-}" ;;
  --self-test) self_test ;;
  *) fail 'usage: transport-v2-acceptance.sh --preflight|--prepare|--usb-to-wifi [known-source]|--wifi-to-usb [known-source]|--self-test' ;;
esac
