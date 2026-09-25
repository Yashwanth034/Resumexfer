package main

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"resumexfer/internal/portal"
)

var version = "dev"

const usage = `Resumexfer - resumable local file transfer

Usage:
  resumexfer share <file-or-folder> [more paths...]
  resumexfer receive <destination-folder>
  resumexfer version

The other device only needs a modern web browser on the same local network.
Press Ctrl+C to stop an active share or receive session.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "resumexfer:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(out, usage)
		return nil
	}

	switch args[0] {
	case "help", "-h", "--help":
		fmt.Fprint(out, usage)
		return nil
	case "version", "--version", "-version":
		fmt.Fprintf(out, "Resumexfer %s\n", version)
		return nil
	case "share":
		if len(args) < 2 {
			return fmt.Errorf("share requires at least one file or folder")
		}
		return runPortalSession(ctx, out, func(manager *portal.Manager) (portal.SessionInfo, error) {
			return manager.StartShare(args[1:])
		})
	case "receive":
		if len(args) != 2 {
			return fmt.Errorf("receive requires exactly one destination folder")
		}
		return runPortalSession(ctx, out, func(manager *portal.Manager) (portal.SessionInfo, error) {
			return manager.StartReceive(args[1])
		})
	default:
		return fmt.Errorf("unknown command %q; use 'resumexfer help'", args[0])
	}
}

func runPortalSession(
	ctx context.Context,
	out io.Writer,
	start func(*portal.Manager) (portal.SessionInfo, error),
) error {
	// An ephemeral port avoids colliding with the Linux background daemon and
	// makes the standalone CLI safe to run on Linux, Windows, and macOS.
	manager := portal.New(portal.Config{ListenAddress: "0.0.0.0:0"})
	defer manager.Close()

	info, err := start(manager)
	if err != nil {
		return err
	}
	printSession(out, info)

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(out, "Stopped.")
			return nil
		case <-ticker.C:
			current, ok := manager.Snapshot(info.ID)
			if !ok {
				return fmt.Errorf("transfer session expired or was closed")
			}
			if current.Complete {
				fmt.Fprintf(
					out,
					"Completed: %d/%d files, %s transferred.\n",
					current.DoneFiles,
					current.TotalFiles,
					formatBytes(current.BytesDone),
				)
				// Let the browser receive its final response before shutting down
				// the ephemeral local server.
				select {
				case <-ctx.Done():
				case <-time.After(500 * time.Millisecond):
				}
				return nil
			}
		}
	}
}

func printSession(out io.Writer, info portal.SessionInfo) {
	mode := "Share"
	if info.Mode == portal.ModeReceive {
		mode = "Receive"
	}
	fmt.Fprintf(out, "Resumexfer %s - %s\n", version, mode)
	fmt.Fprintln(out, "Open this link on the other device (same local network):")

	remoteFound := false
	for _, route := range info.Routes {
		fmt.Fprintf(out, "  %s\n", route.URL)
		if !isLoopbackURL(route.URL) {
			remoteFound = true
		}
	}
	if len(info.Routes) == 0 {
		for _, rawURL := range info.URLs {
			fmt.Fprintf(out, "  %s\n", rawURL)
			if !isLoopbackURL(rawURL) {
				remoteFound = true
			}
		}
	}
	if !remoteFound {
		fmt.Fprintln(out, "  No non-loopback LAN address was detected. Connect both devices to the same Wi-Fi/Ethernet network and try again.")
	}
	fmt.Fprintln(out, "The capability link is temporary. Do not share it outside your local network.")
	fmt.Fprintln(out, "Press Ctrl+C to stop.")
}

func isLoopbackURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := strings.Trim(parsed.Hostname(), "[]")
	return host == "127.0.0.1" || host == "::1" || strings.EqualFold(host, "localhost")
}

func formatBytes(value int64) string {
	const unit = int64(1024)
	if value < unit {
		return fmt.Sprintf("%d B", value)
	}
	div, exp := unit, 0
	for n := value / unit; n >= unit && exp < 5; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(value)/float64(div), "KMGTPE"[exp])
}
