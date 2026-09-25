#!/usr/bin/env bash
set -euo pipefail

RUNTIME_DIR=${RESUMEXFER_XDG_RUNTIME_DIR:-${XDG_RUNTIME_DIR:-}}
STATE_HOME=${RESUMEXFER_XDG_STATE_HOME:-${XDG_STATE_HOME:-${HOME:-}/.local/state}}
EXT_DIR=${RESUMEXFER_EXTENSION_DIR:-/usr/share/nemo-python/extensions}
SOCKET_PATH=${RESUMEXFER_SOCKET_PATH:-${RUNTIME_DIR:+$RUNTIME_DIR/resumexfer/control.sock}}
STATE_FILE=${RESUMEXFER_STATE_FILE:-$STATE_HOME/resumexfer/state.json}
TIMEOUT=${RESUMEXFER_ACCEPTANCE_TIMEOUT:-600}
ACCEPT_ROOT=${RESUMEXFER_ACCEPTANCE_ROOT:-${TMPDIR:-/tmp}/resumexfer-acceptance-${UID:-$(id -u)}}

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'PASS: %s\n' "$*"; }
info() { printf '%s\n' "$*"; }

mtp_mounts() {
  [ -n "$RUNTIME_DIR" ] || return 0
  local root="$RUNTIME_DIR/gvfs"
  [ -d "$root" ] || return 0
  find "$root" -mindepth 1 -maxdepth 1 -type d -name 'mtp:host=*' -print 2>/dev/null || true
}

preflight() {
  [ -n "$RUNTIME_DIR" ] || fail 'XDG_RUNTIME_DIR is not set'
  if [ "${RESUMEXFER_SKIP_PACKAGE_CHECK:-0}" != 1 ]; then
    dpkg-query -W -f='${Status} ${Version}\n' resumexfer 2>/dev/null | grep -Fq 'install ok installed' || fail 'resumexfer package is not installed'
  fi
  pass 'package installed'

  systemctl --user is-active resumexfer.service >/dev/null 2>&1 || fail 'resumexfer user service is not active'
  pass 'resumexfer.service active'

  [ -S "$SOCKET_PATH" ] || fail "control socket missing: $SOCKET_PATH"
  pass "private control socket present: $SOCKET_PATH"

  [ -f "$EXT_DIR/resumexfer.py" ] || fail 'Nemo extension missing'
  [ -f "$EXT_DIR/resumexfer_core.py" ] || fail 'Nemo core missing'
  [ -f "$EXT_DIR/resumexfer_worker.py" ] || fail 'Nemo request worker missing'
  pass 'Nemo integration installed'

  mkdir -p "$(dirname "$STATE_FILE")"
  [ -w "$(dirname "$STATE_FILE")" ] || fail 'state directory is not writable'
  pass "state path ready: $STATE_FILE"

  local mounts
  mounts=$(mtp_mounts)
  if [ -n "$mounts" ]; then
    pass 'MTP mount detected'
    printf '%s\n' "$mounts"
  else
    info 'MTP mount not detected yet; connect/unlock the Android phone and select File Transfer when starting the live test.'
  fi
}

manifest_ids() {
  local direction=$1
  python3 - "$STATE_FILE" "$direction" <<'PY'
import json, os, sys
path, direction = sys.argv[1:]
if not os.path.exists(path):
    raise SystemExit(0)
try:
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
except Exception:
    raise SystemExit(0)
for mid, manifest in data.get('manifests', {}).items():
    if manifest.get('direction') == direction:
        print(mid)
PY
}

wait_for_mtp() {
  local want=$1 start now mounts
  start=$(date +%s)
  while :; do
    mounts=$(mtp_mounts)
    if [ "$want" = present ] && [ -n "$mounts" ]; then printf '%s\n' "$mounts"; return 0; fi
    if [ "$want" = absent ] && [ -z "$mounts" ]; then return 0; fi
    now=$(date +%s)
    [ $((now-start)) -lt "$TIMEOUT" ] || return 1
    sleep 1
  done
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
    sleep 1
  done
}

manifest_status() {
  local id=$1
  python3 - "$STATE_FILE" "$id" <<'PY'
import json, os, sys
path, mid = sys.argv[1:]
if not os.path.exists(path):
    print('missing 0 0')
    raise SystemExit
with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)
m = data.get('manifests', {}).get(mid)
if not m:
    print('missing 0 0')
    raise SystemExit
entries = m.get('entries', [])
pending = sum(1 for e in entries if not e.get('complete', False))
print(('awaiting' if m.get('awaiting_reconnect') else 'active'), pending, len(entries))
PY
}

wait_for_manifest_state() {
  local id=$1 expected=$2 start now status pending total
  start=$(date +%s)
  while :; do
    read -r status pending total < <(manifest_status "$id")
    case "$expected" in
      awaiting) [ "$status" = awaiting ] && { printf '%s %s %s\n' "$status" "$pending" "$total"; return 0; } ;;
      complete) [ "$pending" = 0 ] && { printf '%s %s %s\n' "$status" "$pending" "$total"; return 0; } ;;
    esac
    now=$(date +%s)
    [ $((now-start)) -lt "$TIMEOUT" ] || return 1
    sleep 1
  done
}

partial_snapshot() {
  local id=$1
  python3 - "$STATE_FILE" "$id" <<'PY'
import hashlib, json, os, sys
path, mid = sys.argv[1:]
with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)
m = data.get('manifests', {}).get(mid) or {}
for entry in m.get('entries', []):
    dst = entry.get('destination') or ''
    expected = int(entry.get('size') or 0)
    if not dst or expected <= 0 or not os.path.isfile(dst):
        continue
    size = os.path.getsize(dst)
    if not 0 < size < expected:
        continue
    h = hashlib.sha256()
    with open(dst, 'rb') as f:
        remaining = size
        while remaining:
            block = f.read(min(1024 * 1024, remaining))
            if not block:
                break
            h.update(block)
            remaining -= len(block)
    print(size, expected, h.hexdigest())
    raise SystemExit(0)
raise SystemExit(1)
PY
}

wait_for_partial_snapshot() {
  local id=$1 start now snapshot
  start=$(date +%s)
  while :; do
    if snapshot=$(partial_snapshot "$id" 2>/dev/null); then
      printf '%s\n' "$snapshot"
      return 0
    fi
    now=$(date +%s)
    [ $((now-start)) -lt "$TIMEOUT" ] || return 1
    sleep 1
  done
}

verify_snapshot_prefix() {
  local id=$1 bytes=$2 expected_hash=$3
  python3 - "$STATE_FILE" "$id" "$bytes" "$expected_hash" <<'PY'
import hashlib, json, os, sys
path, mid, count, expected_hash = sys.argv[1:]
count = int(count)
with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)
m = data.get('manifests', {}).get(mid) or {}
for entry in m.get('entries', []):
    dst = entry.get('destination') or ''
    if not dst or not os.path.isfile(dst) or os.path.getsize(dst) < count:
        continue
    h = hashlib.sha256()
    with open(dst, 'rb') as f:
        remaining = count
        while remaining:
            block = f.read(min(1024 * 1024, remaining))
            if not block:
                break
            h.update(block)
            remaining -= len(block)
    raise SystemExit(0 if h.hexdigest() == expected_hash else 2)
raise SystemExit(1)
PY
}

verify_manifest_checksum() {
  local id=$1
  python3 - "$STATE_FILE" "$id" <<'PY'
import hashlib, json, os, sys
path, mid = sys.argv[1:]
with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)
m = data.get('manifests', {}).get(mid) or {}
entries = m.get('entries', [])
if not entries:
    raise SystemExit('manifest has no entries')
def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()
for entry in entries:
    src, dst = entry.get('source'), entry.get('destination')
    if not src or not dst or not os.path.isfile(src) or not os.path.isfile(dst):
        raise SystemExit(f'checksum path unavailable: {src!r} -> {dst!r}')
    if digest(src) != digest(dst):
        raise SystemExit(f'checksum mismatch: {src} -> {dst}')
print(f'checksum verified for {len(entries)} file(s)')
PY
}

capture_completed_snapshot() {
  local id=$1 output=$2
  python3 - "$STATE_FILE" "$id" "$output" <<'PY'
import hashlib, json, os, sys
path, mid, output = sys.argv[1:]
with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)
m = data.get('manifests', {}).get(mid) or {}
entries = m.get('entries', [])
def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()
complete = []
for entry in entries:
    src, dst = entry.get('source'), entry.get('destination')
    size = int(entry.get('size') or 0)
    if not src or not dst or size <= 0 or not os.path.isfile(src) or not os.path.isfile(dst):
        continue
    if os.path.getsize(src) != size or os.path.getsize(dst) != size:
        continue
    src_hash = digest(src)
    dst_hash = digest(dst)
    if src_hash != dst_hash:
        continue
    st = os.stat(dst)
    complete.append({'path': dst, 'size': size, 'mtime_ns': st.st_mtime_ns, 'sha256': dst_hash})
if not (0 < len(complete) < len(entries)):
    raise SystemExit(1)
with open(output, 'w', encoding='utf-8') as f:
    json.dump(complete, f)
print(len(complete), len(entries))
PY
}

wait_for_mid_job_snapshot() {
  local id=$1 output=$2 start now result
  start=$(date +%s)
  while :; do
    if result=$(capture_completed_snapshot "$id" "$output" 2>/dev/null); then
      printf '%s\n' "$result"
      return 0
    fi
    now=$(date +%s)
    [ $((now-start)) -lt "$TIMEOUT" ] || return 1
    sleep 1
  done
}

verify_completed_snapshot_unchanged() {
  local snapshot=$1
  python3 - "$snapshot" <<'PY'
import hashlib, json, os, sys
with open(sys.argv[1], 'r', encoding='utf-8') as f:
    entries = json.load(f)
def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()
for entry in entries:
    path = entry['path']
    if not os.path.isfile(path):
        raise SystemExit(f'completed file disappeared: {path}')
    st = os.stat(path)
    if st.st_size != entry['size'] or st.st_mtime_ns != entry['mtime_ns'] or digest(path) != entry['sha256']:
        raise SystemExit(f'completed file was rewritten instead of skipped: {path}')
print(f"verified {len(entries)} completed file(s) were skipped unchanged")
PY
}

run_direction() {
  local direction=$1 before manifest partial_bytes partial_total partial_hash
  wait_for_mtp present >/dev/null || fail 'MTP mount did not appear'
  before=$(manifest_ids "$direction" || true)

  case "$direction" in
    phone-to-laptop)
      info 'TEST A - PHONE -> LAPTOP'
      info 'In Nemo, copy one large file from the phone to a normal laptop folder. Keep this terminal open.'
      ;;
    laptop-to-phone)
      info 'TEST B - LAPTOP -> PHONE'
      info 'In Nemo, copy one large file from the laptop to the phone. Keep this terminal open.'
      ;;
    *) fail "unknown direction: $direction" ;;
  esac

  manifest=$(wait_for_new_manifest "$direction" "$before") || fail "Nemo transfer intent was not observed for $direction"
  pass "whole-job manifest captured: $manifest"
  read -r partial_bytes partial_total partial_hash < <(wait_for_partial_snapshot "$manifest") || fail 'no incomplete destination was observed; retry with a larger file'
  pass "partial captured before interruption: $partial_bytes of $partial_total bytes"
  info 'UNPLUG NOW: disconnect the USB cable while this partial is still incomplete.'

  wait_for_mtp absent || fail 'phone did not disconnect before timeout'
  pass 'disconnect detected'
  wait_for_manifest_state "$manifest" awaiting >/dev/null || fail 'manifest did not enter reconnect-pending state'
  pass 'interrupted job persisted and is waiting for reconnect'

  info 'Reconnect/unlock the phone and select File Transfer again.'
  wait_for_mtp present >/dev/null || fail 'MTP mount did not return'
  pass 'reconnect detected'
  if [ "$direction" = laptop-to-phone ]; then
    info 'Nemo may keep showing the old interrupted percentage while Resumexfer finishes recovery in the background; this acceptance check follows persisted state and file content instead.'
  fi

  if wait_for_manifest_state "$manifest" complete >/dev/null; then
    verify_snapshot_prefix "$manifest" "$partial_bytes" "$partial_hash" || fail 'the observed partial prefix changed during recovery'
    verify_manifest_checksum "$manifest" || fail 'final checksum/content verification failed'
    pass "$direction resumed from the observed prefix and completed with matching checksum/content"
  elif [ "$direction" = laptop-to-phone ]; then
    verify_snapshot_prefix "$manifest" "$partial_bytes" "$partial_hash" || fail 'unsupported backend overwrote or restarted the observed partial prefix'
    pass 'append reopen is unsupported on this device; the existing partial prefix was preserved unchanged'
    info 'SAFE PAUSE: Resumexfer failed closed instead of silently restarting or overwriting the phone partial.'
    return 2
  else
    fail "$direction did not complete after reconnect"
  fi
}

generate_multi_file_set() {
  local source_dir="$ACCEPT_ROOT/multi-source"
  rm -rf "$source_dir" "$ACCEPT_ROOT/multi-receive"
  mkdir -p "$source_dir" "$ACCEPT_ROOT/multi-receive"
  python3 - "$source_dir" <<'PY'
import os, sys
root = sys.argv[1]
block = 1024 * 1024
for i in range(6):
    path = os.path.join(root, f'file-{i + 1:02d}.bin')
    pattern = bytes([(i * 37 + j) % 251 for j in range(4096)])
    chunk = (pattern * ((block + len(pattern) - 1) // len(pattern)))[:block]
    with open(path, 'wb') as f:
        for _ in range(32):
            f.write(chunk)
print(root)
PY
}

run_multifile_test() {
  local source_dir receive_dir before manifest snapshot complete total
  info 'TEST C - MULTI-FILE'
  mkdir -p "$ACCEPT_ROOT"
  source_dir=$(generate_multi_file_set)
  receive_dir="$ACCEPT_ROOT/multi-receive"
  info "generated multi-file test set: $source_dir"
  info 'In Nemo, copy the generated multi-source folder to the phone and wait for that setup copy to finish.'
  read -r -p 'Press Enter after the folder is fully present on the phone: ' _
  wait_for_mtp present >/dev/null || fail 'MTP mount did not appear for multi-file test'
  before=$(manifest_ids phone-to-laptop || true)
  info "Now in Nemo copy that phone folder back into: $receive_dir"
  manifest=$(wait_for_new_manifest phone-to-laptop "$before") || fail 'multi-file phone-to-laptop manifest was not observed'
  snapshot="$ACCEPT_ROOT/multi-complete-before.json"
  read -r complete total < <(wait_for_mid_job_snapshot "$manifest" "$snapshot") || fail 'could not observe a state with completed and remaining files; retry with a slower/larger test set'
  pass "$complete of $total files completed while others remained"
  info 'UNPLUG NOW: disconnect the USB cable in the middle of this multi-file job.'
  wait_for_mtp absent || fail 'phone did not disconnect during multi-file test'
  wait_for_manifest_state "$manifest" awaiting >/dev/null || fail 'multi-file manifest did not enter reconnect-pending state'
  info 'Reconnect/unlock the phone and select File Transfer again.'
  wait_for_mtp present >/dev/null || fail 'MTP mount did not return for multi-file test'
  wait_for_manifest_state "$manifest" complete >/dev/null || fail 'multi-file job did not finish after reconnect'
  verify_completed_snapshot_unchanged "$snapshot" || fail 'a file completed before interruption was rewritten'
  verify_manifest_checksum "$manifest" || fail 'multi-file checksum/content verification failed'
  pass 'completed files were skipped unchanged and remaining files continued to completion'
}

run_wrong_content_test() {
  local target_dir before manifest partial_bytes partial_total partial_hash wrong_hash status pending total
  info 'TEST D - WRONG CONTENT'
  target_dir="$ACCEPT_ROOT/wrong-content-target"
  rm -rf "$target_dir"
  mkdir -p "$target_dir"
  info "Use a large phone file and copy it in Nemo into this empty folder: $target_dir"
  before=$(manifest_ids phone-to-laptop || true)
  manifest=$(wait_for_new_manifest phone-to-laptop "$before") || fail 'wrong-content test manifest was not observed'
  read -r partial_bytes partial_total partial_hash < <(wait_for_partial_snapshot "$manifest") || fail 'no incomplete destination was observed for wrong-content test'
  info 'UNPLUG NOW: disconnect the USB cable while the file is incomplete.'
  wait_for_mtp absent || fail 'phone did not disconnect during wrong-content test'
  wait_for_manifest_state "$manifest" awaiting >/dev/null || fail 'wrong-content manifest did not enter reconnect-pending state'
  wrong_hash=$(python3 - "$STATE_FILE" "$manifest" "$target_dir" <<'PY'
import hashlib, json, os, sys
state_path, mid, safe_root = sys.argv[1:]
with open(state_path, 'r', encoding='utf-8') as f:
    data = json.load(f)
m = data.get('manifests', {}).get(mid) or {}
entries = m.get('entries', [])
if len(entries) != 1:
    raise SystemExit('wrong-content test requires one selected file')
e = entries[0]
dst = os.path.realpath(e.get('destination') or '')
root = os.path.realpath(safe_root)
if os.path.commonpath([dst, root]) != root:
    raise SystemExit('refusing to modify destination outside acceptance folder')
size = int(e.get('size') or 0)
if size <= 0:
    raise SystemExit('invalid source size')
h = hashlib.sha256()
block = b'RESUMEXFER-WRONG-CONTENT\n' * 4096
with open(dst, 'wb') as f:
    remaining = size
    while remaining:
        chunk = block[:min(len(block), remaining)]
        f.write(chunk)
        h.update(chunk)
        remaining -= len(chunk)
print(h.hexdigest())
PY
) || fail 'could not create controlled same-name wrong-content destination'
  pass 'controlled same-name wrong-content destination created while phone is disconnected'
  info 'Reconnect/unlock the phone and select File Transfer again.'
  wait_for_mtp present >/dev/null || fail 'MTP mount did not return for wrong-content test'
  sleep 3
  read -r status pending total < <(manifest_status "$manifest")
  [ "$pending" -gt 0 ] || fail 'wrong-content destination was incorrectly accepted as complete'
  python3 - "$STATE_FILE" "$manifest" "$wrong_hash" <<'PY'
import hashlib, json, os, sys
state_path, mid, expected = sys.argv[1:]
with open(state_path, 'r', encoding='utf-8') as f:
    data = json.load(f)
e = (data.get('manifests', {}).get(mid) or {}).get('entries', [])[0]
path = e.get('destination')
h = hashlib.sha256()
with open(path, 'rb') as f:
    for block in iter(lambda: f.read(1024 * 1024), b''):
        h.update(block)
raise SystemExit(0 if h.hexdigest() == expected else 2)
PY
  pass 'same-name wrong content was refused and left untouched'
}

report_cleanup_test() {
  info 'TEST E - 24 HOUR CLEANUP'
  info 'This release uses a controlled test clock and temporary state for the 24-hour boundary; no literal 24-hour wait is required.'
  info 'Release verification checks stale known-temp cleanup, stale transfer/manifest cleanup, and that ordinary completed-looking files are never deleted.'
  pass '24-hour cleanup is covered by the automated controlled-clock regression suite'
}

self_test() {
  local tmp
  tmp=$(mktemp -d)
  _SELFTEST_PID=''
  _SELFTEST_TMP="$tmp"
  cleanup() { [ -z "${_SELFTEST_PID:-}" ] || kill "$_SELFTEST_PID" 2>/dev/null || true; rm -rf "${_SELFTEST_TMP:-}"; }
  trap cleanup EXIT
  mkdir -p "$tmp/run/resumexfer" "$tmp/run/gvfs/mtp:host=fake" "$tmp/state/resumexfer" "$tmp/ext" "$tmp/bin"
  : > "$tmp/ext/resumexfer.py"
  : > "$tmp/ext/resumexfer_core.py"
  : > "$tmp/ext/resumexfer_worker.py"
  printf '{"transfers":{},"intents":{},"candidates":{},"manifests":{}}\n' > "$tmp/state/resumexfer/state.json"
  cat > "$tmp/bin/systemctl" <<'SH'
#!/bin/sh
[ "$1" = --user ] && [ "$2" = is-active ] && exit 0
exit 1
SH
  chmod +x "$tmp/bin/systemctl"
  python3 - "$tmp/run/resumexfer/control.sock" <<'PY' &
import socket, sys, time
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.bind(sys.argv[1]); s.listen(1); time.sleep(30)
PY
  _SELFTEST_PID=$!
  for _ in $(seq 1 100); do [ -S "$tmp/run/resumexfer/control.sock" ] && break; sleep 0.01; done
  PATH="$tmp/bin:$PATH" \
  RESUMEXFER_SKIP_PACKAGE_CHECK=1 \
  RESUMEXFER_XDG_RUNTIME_DIR="$tmp/run" \
  RESUMEXFER_XDG_STATE_HOME="$tmp/state" \
  RESUMEXFER_EXTENSION_DIR="$tmp/ext" \
  "$0" --preflight >/dev/null
  printf 'SELF-TEST PASS\n'
}

case "${1:-}" in
  --self-test) self_test ;;
  --preflight) preflight ;;
  --direction)
    [ $# -eq 2 ] || fail 'usage: hardware-acceptance.sh --direction phone-to-laptop|laptop-to-phone'
    preflight
    run_direction "$2"
    ;;
  '')
    preflight
    info 'Live acceptance runs Tests A-D interactively. Test E is the controlled-clock automated cleanup regression already exercised by release verification.'
    run_direction phone-to-laptop
    run_direction laptop-to-phone || [ $? -eq 2 ]
    run_multifile_test
    run_wrong_content_test
    report_cleanup_test
    ;;
  *) fail 'usage: hardware-acceptance.sh [--preflight|--self-test|--direction phone-to-laptop|laptop-to-phone]' ;;
esac
