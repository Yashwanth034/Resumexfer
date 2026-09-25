#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VERSION=${VERSION:-$(tr -d '[:space:]' < "$ROOT/VERSION")}
DIST_DIR=${DIST_DIR:-"$ROOT/dist"}

mkdir -p "$DIST_DIR"

build() {
  local goos=$1
  local goarch=$2
  local suffix=${3:-}
  local out="$DIST_DIR/resumexfer_${VERSION}_${goos}_${goarch}${suffix}"

  printf 'Building %s/%s -> %s\n' "$goos" "$goarch" "$out"
  (
    cd "$ROOT"
    CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
      go build -trimpath \
      -ldflags="-s -w -X main.version=$VERSION" \
      -o "$out" ./cmd/resumexfer
  )
}

build linux amd64
build linux arm64
build windows amd64 .exe
build windows arm64 .exe
build darwin amd64
build darwin arm64

CHECKSUMS="$DIST_DIR/resumexfer_${VERSION}_SHA256SUMS.txt"
(
  cd "$DIST_DIR"
  files=(
    "resumexfer_${VERSION}_linux_amd64"
    "resumexfer_${VERSION}_linux_arm64"
    "resumexfer_${VERSION}_windows_amd64.exe"
    "resumexfer_${VERSION}_windows_arm64.exe"
    "resumexfer_${VERSION}_darwin_amd64"
    "resumexfer_${VERSION}_darwin_arm64"
  )
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${files[@]}" > "$(basename "$CHECKSUMS")"
  else
    shasum -a 256 "${files[@]}" > "$(basename "$CHECKSUMS")"
  fi
)

printf 'Checksums: %s\n' "$CHECKSUMS"
