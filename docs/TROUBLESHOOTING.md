# Troubleshooting

## The other device cannot open the local link

1. Put both devices on the same Wi‑Fi/Ethernet network.
2. Confirm the printed URL uses the expected LAN address, not only `127.0.0.1`.
3. Allow Resumexfer through the local/private-network firewall.
4. Disable client isolation / guest-network isolation if your router prevents devices from talking to each other.
5. VPNs can change route selection; temporarily disconnect the VPN if it prevents LAN access.

## Android is not visible over USB on Linux

- Unlock the phone.
- Select **File Transfer / MTP** for the USB connection.
- Confirm Nemo can browse the phone before starting a managed transfer.
- Some devices expose different MTP behavior after cable reconnect; wait for GVfs to remount before retrying.

## A laptop→phone USB transfer switches to Wi‑Fi

A browser cannot append into the partial file previously created through MTP. For one remaining file, the browser transfer restarts that file. For multiple remaining files/folders, Resumexfer offers the remaining job as a ZIP with relative paths preserved and already-completed USB files excluded.

Reconnect USB instead if you want to continue the verified MTP partial itself.

## Transfer reaches 100%

Resumexfer keeps completion separate from byte progress. A transfer may briefly show verification at 100%; the UI closes only after the session is terminal/verified.

## Pause or reconnect

Do not start a second copy of the same managed job. Resume/reconnect the existing Resumexfer job so it can use the preserved manifest and partial ownership.

## Browser download resume

The WebRTC/browser fast path can keep a durable browser-side offset using IndexedDB. Clearing browser site storage removes that browser-held resume state.

## Reporting a bug

Include the Resumexfer version, OS, direction, transport, file/folder count, and the exact step where behavior diverged. Do not attach private files, capability links, credentials, or personal logs to a public issue.
