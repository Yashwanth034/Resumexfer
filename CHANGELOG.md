# Changelog

## 0.2.1 — 2026-09-26

### Security

- Updated the portable WebRTC/Pion dependency stack, including WebRTC 4.2.20 and DTLS 3.1.8.
- Updated `golang.org/x/crypto` to 0.53.0, `golang.org/x/net` to 0.56.0, and `golang.org/x/sys` to 0.46.0.

### Maintenance

- Raised the source-build requirement to Go 1.25 and stopped verification scripts from forcing the obsolete Go 1.23.2 toolchain.
- Updated GitHub Actions checkout, setup-go, and upload-artifact actions to their current Dependabot-proposed major versions.
- Grouped future Go-module and GitHub Actions Dependabot updates to reduce pull-request noise.
- Added the MIT license for the public repository.

## 0.2.0 — 2026-09-25

### Added

- Cross-platform standalone `resumexfer share` and `resumexfer receive` commands.
- Build targets for Windows amd64/arm64, macOS Intel/Apple Silicon, and Linux amd64/arm64.
- Public architecture, platform-support, security, troubleshooting, and contribution documentation.
- Public-safety CI and dependency-update configuration.
- Linux Debian package now includes the standalone `resumexfer` command in addition to the managed daemon/Nemo integration.

### Unchanged

- Linux managed USB/MTP recovery remains the same verified manifest/recovery engine proven in 0.1.15.
- Windows/macOS support in this release is local-network/browser transfer; native Explorer/Finder Android USB resume is not claimed.

## 0.1.15 — 2026-09-25

- Whole-job USB→Wi‑Fi handoff instead of falling back to only the current file.
- 207-file bidirectional handoff validation.
- Stronger completion semantics so Share, Receive, and Continue windows close after verified completion.
- Compact capability links and hardened local portal Host/Origin checks.
- Multi-file pause/resume/cancel, reconnect, deadlock, fuzz, and exact byte/path validation.
- Nested folder coverage including duplicate basenames, Unicode names, and zero-byte files.

## 0.1.14 and earlier

The 0.1.x series built the Linux/Nemo managed USB/MTP engine incrementally:

- authoritative cross-device Ctrl+C/Ctrl+V ownership,
- automatic reconnect recovery,
- durable progress with Pause/Resume/Cancel,
- verified append for laptop→phone MTP recovery,
- local sidecar recovery for phone→laptop,
- duplicate suppression with full-byte confirmation,
- multi-file cancellation/Undo ownership rules,
- throughput and checkpoint optimizations,
- zero-install LAN/browser transfer and WebRTC fast path.

Older development checkpoint notes were consolidated here to keep the public repository focused and avoid duplicate/stale documentation.
