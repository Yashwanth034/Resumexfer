#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
VERSION=$(tr -d '[:space:]' < "$ROOT/VERSION")
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

VERSION="$VERSION" DIST_DIR="$TMP/dist" "$ROOT/scripts/build-deb.sh"
DEB="$TMP/dist/resumexfer_${VERSION}_$(dpkg --print-architecture).deb"
test -f "$DEB"

CONTENTS=$(dpkg-deb -c "$DEB")
for path in \
  ./usr/bin/resumexfer \
  ./usr/bin/resumexferd \
  ./usr/bin/resumexfer-acceptance \
  ./usr/bin/resumexfer-transport-v2-acceptance \
  ./usr/lib/systemd/user/resumexfer.service \
  ./usr/share/nemo-python/extensions/resumexfer.py \
  ./usr/share/nemo-python/extensions/resumexfer_core.py \
  ./usr/share/nemo-python/extensions/resumexfer_worker.py \
  ./usr/share/nemo-python/extensions/resumexfer_portal.py; do
  grep -Fq "$path" <<<"$CONTENTS" || { echo "missing package path: $path" >&2; exit 1; }
done

DEPENDS=$(dpkg-deb -f "$DEB" Depends)
for dep in nemo nemo-python python3-gi gvfs-backends procps qrencode; do
  grep -Eq "(^|, )${dep}([ ,(]|$)" <<<"$DEPENDS" || { echo "missing dependency: $dep" >&2; exit 1; }
done

mkdir -p "$TMP/control" "$TMP/root"
dpkg-deb -e "$DEB" "$TMP/control"
for script in postinst prerm postrm; do
  test -x "$TMP/control/$script" || { echo "missing executable $script" >&2; exit 1; }
  sh -n "$TMP/control/$script"
done

dpkg-deb -x "$DEB" "$TMP/root"
test -x "$TMP/root/usr/bin/resumexfer"
test -x "$TMP/root/usr/bin/resumexferd"
test -x "$TMP/root/usr/bin/resumexfer-acceptance"
test "$("$TMP/root/usr/bin/resumexfer" version)" = "Resumexfer $VERSION"
test -x "$TMP/root/usr/bin/resumexfer-transport-v2-acceptance"
python3 -m py_compile \
  "$TMP/root/usr/share/nemo-python/extensions/resumexfer.py" \
  "$TMP/root/usr/share/nemo-python/extensions/resumexfer_core.py" \
  "$TMP/root/usr/share/nemo-python/extensions/resumexfer_worker.py" \
  "$TMP/root/usr/share/nemo-python/extensions/resumexfer_portal.py"

grep -Fq 'systemctl --global enable resumexfer.service' "$TMP/control/postinst"
grep -Fq 'systemctl --global disable resumexfer.service' "$TMP/control/prerm"
grep -Fq 'nemo -q' "$TMP/control/postinst"
grep -Fq 'pgrep -u "$uid" -x nemo' "$TMP/control/postinst"
grep -Fq 'pkill -TERM -u "$uid" -x nemo' "$TMP/control/postinst"
grep -Fq 'nemo -q' "$TMP/control/postrm"
grep -Fq 'pgrep -u "$uid" -x nemo' "$TMP/control/postrm"
grep -Fq 'pkill -TERM -u "$uid" -x nemo' "$TMP/control/postrm"

rm -rf "$TMP/root"
test ! -e "$TMP/root/usr/bin/resumexferd"
