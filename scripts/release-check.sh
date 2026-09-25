#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

if [[ -x "$ROOT/.rt_hw/go1.23.2/bin/go" ]]; then
  export PATH="$ROOT/.rt_hw/go1.23.2/bin:$PATH"
fi
export GOTOOLCHAIN=${GOTOOLCHAIN:-go1.23.2}
export GOFLAGS=${GOFLAGS:--buildvcs=false}

mkdir -p "$ROOT/.rt_hw"
REPORT=${RELEASE_CHECK_REPORT:-"$ROOT/.rt_hw/release-check-report.txt"}
DIST_DIR=${RELEASE_CHECK_DIST:-"$ROOT/.rt_hw/release-check-dist"}
CROSS_DIST=${RELEASE_CHECK_CROSS_DIST:-"$ROOT/.rt_hw/release-check-cross"}
ALLOW_DIRTY=${ALLOW_DIRTY:-0}
SKIP_STRESS=${RESUMEXFER_SKIP_STRESS:-0}
VERSION_VALUE=${RELEASE_VERSION:-$(tr -d '[:space:]' < VERSION)}

: > "$REPORT"

step=0
pass=0
fail=0

log() {
  printf '%s\n' "$*" | tee -a "$REPORT"
}

have_git_repo() {
  git rev-parse --is-inside-work-tree >/dev/null 2>&1
}

run_step() {
  local label=$1
  shift
  step=$((step + 1))
  log ""
  log "[$step] $label"
  log "------------------------------------------------------------"
  local start end elapsed rc
  start=$(date +%s)
  set +e
  "$@" 2>&1 | tee -a "$REPORT"
  rc=${PIPESTATUS[0]}
  set -e
  end=$(date +%s)
  elapsed=$((end - start))
  if [[ $rc -eq 0 ]]; then
    pass=$((pass + 1))
    log "PASS: $label (${elapsed}s)"
    return 0
  fi
  fail=$((fail + 1))
  log "FAIL: $label (exit=$rc, ${elapsed}s)"
  return 0
}

check_clean_tracked_tree() {
  if ! have_git_repo; then
    if [[ "$ALLOW_DIRTY" == "1" ]]; then
      echo "Git metadata is unavailable in this sandbox; tracked-tree cleanliness requires external Git validation."
      return 0
    fi
    echo "Git metadata is unavailable; refusing to mark the release tree clean."
    return 1
  fi

  git diff --check || return 1
  if [[ "$ALLOW_DIRTY" == "1" ]]; then
    return 0
  fi
  if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    echo "Tracked working tree is not clean."
    git status --short --untracked-files=no
    return 1
  fi
}

check_sensitive_tracked_files() {
  local hits grep_rc
  if have_git_repo; then
    hits=$(git ls-files | grep -Ei '(^|/)(\.env([.][^/]*)?|id_rsa|id_ed25519|web_session[.]json|cookies[.]json|credentials[.]json)$|[.](p12|pfx)$' || true)
    if [[ -n "$hits" ]]; then
      echo "Sensitive-looking files are tracked:"
      echo "$hits"
      return 1
    fi
    if git grep -n -I -E 'BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY' -- .; then
      echo "Private-key material appears in tracked source."
      return 1
    fi
    return 0
  fi

  hits=$(find . -type f \
    -not -path './.git/*' \
    -not -path './.rt_hw/*' \
    | grep -Ei '(^|/)(\.env([.][^/]*)?|id_rsa|id_ed25519|web_session[.]json|cookies[.]json|credentials[.]json)$|[.](p12|pfx)$' || true)
  if [[ -n "$hits" ]]; then
    echo "Sensitive-looking files are present in the release tree:"
    echo "$hits"
    return 1
  fi

  set +e
  grep -R -n -I -E 'BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY' \
    --exclude-dir=.git --exclude-dir=.rt_hw -- .
  grep_rc=$?
  set -e
  if [[ $grep_rc -eq 0 ]]; then
    echo "Private-key material appears in the release tree."
    return 1
  fi
  if [[ $grep_rc -ne 1 ]]; then
    echo "Sensitive-file scan failed (grep exit=$grep_rc)."
    return 1
  fi
  return 0
}

build_release_package() {
  rm -rf "$DIST_DIR"
  mkdir -p "$DIST_DIR"
  VERSION="$VERSION_VALUE" DIST_DIR="$DIST_DIR" ./scripts/build-deb.sh
  local count
  count=$(find "$DIST_DIR" -maxdepth 1 -type f -name '*.deb' | wc -l)
  if [[ "$count" -ne 1 ]]; then
    echo "Expected exactly one .deb, found $count"
    return 1
  fi
}

build_cross_platform_release() {
  rm -rf "$CROSS_DIST"
  mkdir -p "$CROSS_DIST"
  VERSION="$VERSION_VALUE" DIST_DIR="$CROSS_DIST" bash ./scripts/build-cross-platform.sh

  local expected=(
    "resumexfer_${VERSION_VALUE}_linux_amd64"
    "resumexfer_${VERSION_VALUE}_linux_arm64"
    "resumexfer_${VERSION_VALUE}_windows_amd64.exe"
    "resumexfer_${VERSION_VALUE}_windows_arm64.exe"
    "resumexfer_${VERSION_VALUE}_darwin_amd64"
    "resumexfer_${VERSION_VALUE}_darwin_arm64"
    "resumexfer_${VERSION_VALUE}_SHA256SUMS.txt"
  )
  local name
  for name in "${expected[@]}"; do
    [[ -s "$CROSS_DIST/$name" ]] || {
      echo "Missing cross-platform artifact: $name"
      return 1
    }
  done

  test "$("$CROSS_DIST/resumexfer_${VERSION_VALUE}_linux_amd64" version)" = "Resumexfer $VERSION_VALUE"
  (cd "$CROSS_DIST" && sha256sum -c "resumexfer_${VERSION_VALUE}_SHA256SUMS.txt")
}

validate_release_package() {
  local deb package version arch
  deb=$(find "$DIST_DIR" -maxdepth 1 -type f -name '*.deb' -print -quit)
  [[ -n "$deb" && -f "$deb" ]] || {
    echo "Release package not found"
    return 1
  }

  package=$(dpkg-deb -f "$deb" Package)
  version=$(dpkg-deb -f "$deb" Version)
  arch=$(dpkg-deb -f "$deb" Architecture)

  [[ "$package" == "resumexfer" ]] || {
    echo "Unexpected package: $package"
    return 1
  }
  [[ "$version" == "$VERSION_VALUE" ]] || {
    echo "Package version $version does not match requested $VERSION_VALUE"
    return 1
  }
  [[ "$arch" == "amd64" ]] || {
    echo "Unexpected package architecture: $arch"
    return 1
  }

  echo "Package: $package"
  echo "Version: $version"
  echo "Architecture: $arch"
  echo "Depends: $(dpkg-deb -f "$deb" Depends)"
  sha256sum "$deb"
  dpkg-deb --info "$deb" >/dev/null
  dpkg-deb --contents "$deb" >/dev/null
}

log "Resumexfer Release Check"
log "========================"
if have_git_repo; then
  log "HEAD: $(git rev-parse HEAD)"
  log "Branch: $(git branch --show-current)"
else
  log "HEAD: unavailable (Git metadata hidden)"
  log "Branch: unavailable (Git metadata hidden)"
fi
log "Package version: $VERSION_VALUE"
log "Go: $(go version)"
log "Stress suite: $([[ "$SKIP_STRESS" == "1" ]] && echo skipped || echo enabled)"
log "Report: $REPORT"

run_step "Git and tracked-tree cleanliness" check_clean_tracked_tree
run_step "Sensitive tracked-file guard" check_sensitive_tracked_files
run_step "Public repository safety" python3 ./scripts/public-safety-check.py
run_step "Complete project verification" env RESUMEXFER_SKIP_STRESS="$SKIP_STRESS" ./scripts/verify.sh
run_step "Transport V2 regression and race gate" ./scripts/verify-transport-v2.sh
run_step "Transfer reliability matrix" ./scripts/transfer-matrix-check.sh
run_step "Cross-platform standalone builds" build_cross_platform_release
run_step "Build release package" build_release_package
run_step "Validate release package" validate_release_package

log ""
log "============================================================"
if [[ $fail -eq 0 ]]; then
  log "RESULT: RELEASE READY"
  log "Passed gates: $pass/$step"
  log "Artifact directory: $DIST_DIR"
  log "Report: $REPORT"
  exit 0
fi

log "RESULT: RELEASE BLOCKED"
log "Passed gates: $pass/$step"
log "Failed gates: $fail"
log "Report: $REPORT"
exit 1
