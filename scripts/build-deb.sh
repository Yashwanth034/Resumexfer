#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VERSION=${VERSION:-$(tr -d '[:space:]' < "$ROOT/VERSION")}
ARCH=${ARCH:-$(dpkg --print-architecture)}
DIST_DIR=${DIST_DIR:-"$ROOT/dist"}
BUILD_DIR=$(mktemp -d)
trap 'rm -rf "$BUILD_DIR"' EXIT
PKG="$BUILD_DIR/pkg"

case "$ARCH" in
  amd64) GOARCH=amd64 ;;
  arm64) GOARCH=arm64 ;;
  armhf) GOARCH=arm ;;
  *) echo "unsupported Debian architecture: $ARCH" >&2; exit 1 ;;
esac

install -d \
  "$PKG/DEBIAN" \
  "$PKG/usr/bin" \
  "$PKG/usr/lib/systemd/user" \
  "$PKG/usr/share/nemo-python/extensions"

(
  cd "$ROOT"
  CGO_ENABLED=0 GOOS=linux GOARCH="$GOARCH" go build -trimpath -ldflags='-s -w' -o "$PKG/usr/bin/resumexferd" ./cmd/resumexferd
  CGO_ENABLED=0 GOOS=linux GOARCH="$GOARCH" go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o "$PKG/usr/bin/resumexfer" ./cmd/resumexfer
)
install -m 0755 "$ROOT/scripts/hardware-acceptance.sh" "$PKG/usr/bin/resumexfer-acceptance"
install -m 0755 "$ROOT/scripts/transport-v2-acceptance.sh" "$PKG/usr/bin/resumexfer-transport-v2-acceptance"
install -m 0644 "$ROOT/packaging/systemd/resumexfer.service" "$PKG/usr/lib/systemd/user/resumexfer.service"
install -m 0644 "$ROOT/nemo/resumexfer.py" "$PKG/usr/share/nemo-python/extensions/resumexfer.py"
install -m 0644 "$ROOT/nemo/resumexfer_core.py" "$PKG/usr/share/nemo-python/extensions/resumexfer_core.py"
install -m 0755 "$ROOT/nemo/resumexfer_worker.py" "$PKG/usr/share/nemo-python/extensions/resumexfer_worker.py"
install -m 0755 "$ROOT/nemo/resumexfer_portal.py" "$PKG/usr/share/nemo-python/extensions/resumexfer_portal.py"
install -m 0755 "$ROOT/packaging/debian/postinst" "$PKG/DEBIAN/postinst"
install -m 0755 "$ROOT/packaging/debian/prerm" "$PKG/DEBIAN/prerm"
install -m 0755 "$ROOT/packaging/debian/postrm" "$PKG/DEBIAN/postrm"

cat > "$PKG/DEBIAN/control" <<CONTROL
Package: resumexfer
Version: $VERSION
Section: utils
Priority: optional
Architecture: $ARCH
Maintainer: Resumexfer Project
Depends: nemo, nemo-python, python3-gi, gvfs-backends, procps, qrencode
Description: resumable USB and zero-install local-network transfers for Nemo
 Resumexfer provides a per-user daemon and Nemo integration for resumable Android
 transfers through GVfs/MTP plus local-network browser sharing where the other
 phone or laptop needs no Resumexfer installation.
CONTROL

mkdir -p "$DIST_DIR"
OUT="$DIST_DIR/resumexfer_${VERSION}_${ARCH}.deb"
dpkg-deb --build --root-owner-group "$PKG" "$OUT" >/dev/null
printf '%s\n' "$OUT"
