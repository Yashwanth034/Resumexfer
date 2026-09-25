# Security

Resumexfer is designed for transfers between devices you control on a trusted local network.

## Security model

- No Resumexfer cloud account is required.
- File payloads are not uploaded to a Resumexfer-hosted cloud service.
- Portal sessions use random temporary capability URLs.
- Portal access is restricted to loopback, private, and link-local source addresses.
- Host and Origin validation reduce DNS-rebinding and cross-origin abuse.
- Portal responses use no-store/cache and defensive browser headers.
- WebRTC DataChannels use DTLS transport encryption.
- Linux daemon state/control paths are scoped to the current user.
- Managed file recovery validates source identity and preserved partial data before appending.
- Whole-job cleanup deletes only files proven to be owned by that Resumexfer job.

## Important LAN HTTP limitation

The zero-install browser portal currently bootstraps over plain local HTTP.

That keeps the workflow compatible with ordinary phone and laptop browsers without certificate installation, but it means an attacker who can actively intercept traffic on the same hostile LAN may be able to observe or modify the HTTP bootstrap/signaling path. WebRTC payload transport is encrypted, but HTTP-delivered browser code/signaling is not equivalent to authenticated HTTPS.

**Do not treat the current portal as appropriate for an untrusted/public hostile network.** Prefer a network you control.

## Capability links

A capability URL grants access to one temporary local transfer session. Treat it like a short-lived secret:

- do not post it publicly,
- do not forward it outside the intended devices,
- cancel/close the session when finished.

## Local files and secrets

Resumexfer does not require API keys, cloud credentials, or GitHub credentials to transfer files.

The repository's public-safety CI blocks common credential patterns, personal home-directory paths, internal RepoTunnel evidence, checkpoint files, and generated release packages from being committed.

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting / Security Advisory flow for the repository. Do not publish exploit details, private capability links, or sensitive logs in a public issue.

Include:

- affected Resumexfer version,
- operating system,
- transfer mode (USB/MTP or local-network portal),
- minimal reproduction steps,
- expected vs. observed behavior.

## Supported security updates

Security fixes are made on the current release line. Older development checkpoints are not supported releases.
