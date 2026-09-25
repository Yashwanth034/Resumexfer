# Platform support

Resumexfer 0.2.0 separates the portable local-network transfer engine from the Linux-specific USB/MTP desktop integration.

| Platform | Local Wi‑Fi/LAN Share/Receive | Browser/WebRTC transfer | Managed Android USB/MTP resume | Desktop integration |
| --- | --- | --- | --- | --- |
| Linux Mint / Ubuntu + Nemo | ✅ Supported | ✅ Supported | ✅ Supported | ✅ Nemo + GVfs |
| Other Linux desktops | ✅ Standalone CLI | ✅ Supported | ⚠️ No managed file-manager integration | CLI |
| Windows 10/11 | ✅ Standalone CLI | ✅ Build-validated | ❌ Not yet | CLI |
| macOS Intel | ✅ Standalone CLI | ✅ Build-validated | ❌ Not yet | CLI |
| macOS Apple Silicon | ✅ Standalone CLI | ✅ Build-validated | ❌ Not yet | CLI |

## What "build-validated" means

The same `internal/portal` engine and standalone `resumexfer` command are cross-compiled in CI for Windows and macOS. The project does **not** claim native Explorer/Finder MTP interception or Android USB resume on those systems yet.

Linux/Nemo/GVfs is the platform where the full managed USB/MTP path has been physically exercised.

## Standalone CLI

The other device does not need Resumexfer installed; it only needs a browser on the same local network.

### Share

```text
resumexfer share <file-or-folder> [more paths...]
```

Open the printed local URL on the receiving device.

### Receive

```text
resumexfer receive <destination-folder>
```

Open the printed URL on the sending device, then select files or a folder in the browser.

### Windows

Download the matching `.exe` release artifact and run it from PowerShell:

```powershell
.\resumexfer_0.2.0_windows_amd64.exe share "C:\Users\user\Downloads\Example"
```

Windows Firewall may ask whether the program can accept connections on private networks. Allow private/local networks if you want another device on the LAN to connect.

### macOS

Download the matching `darwin_amd64` or `darwin_arm64` artifact:

```bash
chmod +x resumexfer_0.2.0_darwin_arm64
./resumexfer_0.2.0_darwin_arm64 share ~/Downloads/Example
```

macOS may ask for local-network permission/firewall access.

## Future native integration

Native Windows Explorer and macOS Finder integrations are intentionally separate roadmap items. They require OS-specific file-manager/MTP semantics and will not be advertised as supported until they have real platform validation.
